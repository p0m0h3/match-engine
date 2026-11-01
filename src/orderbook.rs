//! Order Book implementation supporting Price-Time Priority (FIFO).

use std::collections::{BTreeMap, HashMap, VecDeque};
use crate::types::{Order, OrderId, Price, Quantity, Side, Trade};

/// Orders aggregated at the exact same price level.
/// In FIFO price-time priority, older orders are at the front of the queue.
#[derive(Debug, Default)]
pub struct PriceLevel {
    pub total_quantity: u64,
    pub orders: VecDeque<Order>,
}

impl PriceLevel {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn push_back(&mut self, order: Order) {
        self.total_quantity += order.quantity.0;
        self.orders.push_back(order);
    }
}

/// In-memory Limit Order Book for a single trading pair.
#[derive(Debug, Default)]
pub struct OrderBook {
    /// Bids sorted in descending order (highest price first).
    /// Using std::cmp::Reverse so BTreeMap's natural order places highest price at the front.
    pub bids: BTreeMap<std::cmp::Reverse<Price>, PriceLevel>,
    /// Asks sorted in ascending order (lowest price first).
    pub asks: BTreeMap<Price, PriceLevel>,
    /// O(1) lookups for active orders by OrderId -> (Price, Side).
    pub order_locations: HashMap<OrderId, (Price, Side)>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// Best Bid price, if any exists.
    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first_key_value().map(|(rev_p, _)| rev_p.0)
    }

    /// Best Ask / Offer price, if any exists.
    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    /// Submit a limit order. Any executable portion is matched immediately;
    /// remaining unfilled quantity rests on the book.
    pub fn place_limit_order(&mut self, mut order: Order) -> Vec<Trade> {
        let mut trades = Vec::new();

        match order.side {
            Side::Buy => {
                // Match against resting asks while incoming buy price >= best ask
                while order.quantity.0 > 0 {
                    let mut entry = match self.asks.first_entry() {
                        Some(e) => e,
                        None => break,
                    };

                    let ask_price = *entry.key();
                    if order.price < ask_price {
                        break; // Spread not crossed
                    }

                    let price_level = entry.get_mut();
                    while order.quantity.0 > 0 && !price_level.orders.is_empty() {
                        let resting = price_level.orders.front_mut().unwrap();
                        let match_qty = order.quantity.0.min(resting.quantity.0);

                        trades.push(Trade {
                            maker_order_id: resting.id,
                            taker_order_id: order.id,
                            price: resting.price,
                            quantity: Quantity(match_qty),
                        });

                        order.quantity.0 -= match_qty;
                        resting.quantity.0 -= match_qty;
                        price_level.total_quantity -= match_qty;

                        if resting.quantity.0 == 0 {
                            let filled = price_level.orders.pop_front().unwrap();
                            self.order_locations.remove(&filled.id);
                        }
                    }

                    if price_level.orders.is_empty() {
                        entry.remove();
                    }
                }

                // If remaining quantity, rest order on bids
                if order.quantity.0 > 0 {
                    self.order_locations.insert(order.id, (order.price, order.side));
                    self.bids
                        .entry(std::cmp::Reverse(order.price))
                        .or_default()
                        .push_back(order);
                }
            }
            Side::Sell => {
                // Match against resting bids while incoming sell price <= best bid
                while order.quantity.0 > 0 {
                    let mut entry = match self.bids.first_entry() {
                        Some(e) => e,
                        None => break,
                    };

                    let bid_price = entry.key().0;
                    if order.price > bid_price {
                        break; // Spread not crossed
                    }

                    let price_level = entry.get_mut();
                    while order.quantity.0 > 0 && !price_level.orders.is_empty() {
                        let resting = price_level.orders.front_mut().unwrap();
                        let match_qty = order.quantity.0.min(resting.quantity.0);

                        trades.push(Trade {
                            maker_order_id: resting.id,
                            taker_order_id: order.id,
                            price: resting.price,
                            quantity: Quantity(match_qty),
                        });

                        order.quantity.0 -= match_qty;
                        resting.quantity.0 -= match_qty;
                        price_level.total_quantity -= match_qty;

                        if resting.quantity.0 == 0 {
                            let filled = price_level.orders.pop_front().unwrap();
                            self.order_locations.remove(&filled.id);
                        }
                    }

                    if price_level.orders.is_empty() {
                        entry.remove();
                    }
                }

                // If remaining quantity, rest order on asks
                if order.quantity.0 > 0 {
                    self.order_locations.insert(order.id, (order.price, order.side));
                    self.asks
                        .entry(order.price)
                        .or_default()
                        .push_back(order);
                }
            }
        }

        trades
    }

    /// Cancel a resting order by OrderId. Returns the cancelled order if found.
    pub fn cancel_order(&mut self, order_id: OrderId) -> Option<Order> {
        let (price, side) = self.order_locations.remove(&order_id)?;

        let price_level = match side {
            Side::Buy => self.bids.get_mut(&std::cmp::Reverse(price))?,
            Side::Sell => self.asks.get_mut(&price)?,
        };

        let pos = price_level.orders.iter().position(|o| o.id == order_id)?;
        let cancelled = price_level.orders.remove(pos)?;
        price_level.total_quantity -= cancelled.quantity.0;

        if price_level.orders.is_empty() {
            match side {
                Side::Buy => { self.bids.remove(&std::cmp::Reverse(price)); }
                Side::Sell => { self.asks.remove(&price); }
            }
        }

        Some(cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passive_limit_orders() {
        let mut book = OrderBook::new();
        let buy = Order::new(OrderId(1), Side::Buy, Price(100), Quantity(10));
        let sell = Order::new(OrderId(2), Side::Sell, Price(105), Quantity(10));

        let trades1 = book.place_limit_order(buy);
        let trades2 = book.place_limit_order(sell);

        assert!(trades1.is_empty());
        assert!(trades2.is_empty());
        assert_eq!(book.best_bid(), Some(Price(100)));
        assert_eq!(book.best_ask(), Some(Price(105)));
    }

    #[test]
    fn test_crossing_limit_order() {
        let mut book = OrderBook::new();
        // Maker resting order: Sell 10 @ 100
        book.place_limit_order(Order::new(OrderId(1), Side::Sell, Price(100), Quantity(10)));

        // Taker crossing order: Buy 4 @ 101
        let trades = book.place_limit_order(Order::new(OrderId(2), Side::Buy, Price(101), Quantity(4)));

        assert_eq!(trades.len(), 1);
        assert_eq!(
            trades[0],
            Trade {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                price: Price(100),
                quantity: Quantity(4),
            }
        );

        // Remaining ask on book should be 6 @ 100
        assert_eq!(book.best_ask(), Some(Price(100)));
    }

    #[test]
    fn test_cancel_order() {
        let mut book = OrderBook::new();
        book.place_limit_order(Order::new(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        assert_eq!(book.best_bid(), Some(Price(100)));

        let cancelled = book.cancel_order(OrderId(1));
        assert!(cancelled.is_some());
        assert_eq!(book.best_bid(), None);
    }
}
