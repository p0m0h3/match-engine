//! Order Book implementation supporting Price-Time Priority (FIFO),
//! partial fills, and core order execution rules.

use std::collections::{BTreeMap, HashMap, VecDeque};
use crate::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce, Trade};

/// Aggregated orders resting at the exact same price level.
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
        self.total_quantity += order.leaves_quantity.0;
        self.orders.push_back(order);
    }
}

/// Result of placing an order into the order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderPlacementResult {
    /// Order was accepted, possibly generating trades and/or resting on the book.
    Accepted {
        trades: Vec<Trade>,
        resting_order: Option<Order>,
    },
    /// Order was rejected due to policy constraints (e.g., PostOnly would cross).
    Rejected {
        reason: &'static str,
    },
}

/// In-memory Limit Order Book for a single market instrument.
#[derive(Debug, Default)]
pub struct OrderBook {
    /// Bids sorted in descending order (highest price first).
    pub bids: BTreeMap<std::cmp::Reverse<Price>, PriceLevel>,
    /// Asks sorted in ascending order (lowest price first).
    pub asks: BTreeMap<Price, PriceLevel>,
    /// O(1) lookups for resting orders by OrderId -> (Price, Side).
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

    /// Process an incoming order following FIFO price-time priority rules.
    pub fn place_order(&mut self, mut order: Order) -> OrderPlacementResult {
        // 1. Post-Only check: Reject if it would cross the spread
        if order.time_in_force == TimeInForce::PostOnly {
            let would_cross = match order.side {
                Side::Buy => self.best_ask().map_or(false, |ask| order.price >= ask),
                Side::Sell => self.best_bid().map_or(false, |bid| order.price <= bid),
            };
            if would_cross {
                return OrderPlacementResult::Rejected {
                    reason: "Post-only order would cross the spread",
                };
            }
        }

        // 2. FOK (Fill-Or-Kill) check: Order must fully fill immediately or abort with 0 trades
        if order.time_in_force == TimeInForce::Fok {
            let available_qty = self.simulated_available_liquidity(order.side.opposite(), order.price, order.order_type);
            if available_qty < order.leaves_quantity.0 {
                return OrderPlacementResult::Rejected {
                    reason: "Fill-or-kill order cannot be fully filled immediately",
                };
            }
        }

        let mut trades = Vec::new();

        // 3. Execution / Matching Loop
        match order.side {
            Side::Buy => {
                while order.leaves_quantity.0 > 0 {
                    let mut entry = match self.asks.first_entry() {
                        Some(e) => e,
                        None => break,
                    };

                    let ask_price = *entry.key();
                    if order.order_type == OrderType::Limit && order.price < ask_price {
                        break; // Spread not crossed
                    }

                    let price_level = entry.get_mut();
                    while order.leaves_quantity.0 > 0 && !price_level.orders.is_empty() {
                        let resting = price_level.orders.front_mut().unwrap();
                        let match_qty = order.leaves_quantity.0.min(resting.leaves_quantity.0);

                        order.leaves_quantity.0 -= match_qty;
                        resting.leaves_quantity.0 -= match_qty;
                        price_level.total_quantity -= match_qty;

                        trades.push(Trade {
                            maker_order_id: resting.id,
                            taker_order_id: order.id,
                            price: resting.price,
                            quantity: Quantity(match_qty),
                            maker_leaves_quantity: resting.leaves_quantity,
                            taker_leaves_quantity: order.leaves_quantity,
                        });

                        if resting.is_filled() {
                            let filled = price_level.orders.pop_front().unwrap();
                            self.order_locations.remove(&filled.id);
                        }
                    }

                    if price_level.orders.is_empty() {
                        entry.remove();
                    }
                }

                // 4. Residual handling
                let can_rest = order.order_type == OrderType::Limit
                    && matches!(order.time_in_force, TimeInForce::Gtc | TimeInForce::PostOnly);

                if !order.is_filled() && can_rest {
                    self.order_locations.insert(order.id, (order.price, order.side));
                    self.bids
                        .entry(std::cmp::Reverse(order.price))
                        .or_default()
                        .push_back(order);

                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order: Some(order),
                    }
                } else {
                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order: None,
                    }
                }
            }
            Side::Sell => {
                while order.leaves_quantity.0 > 0 {
                    let mut entry = match self.bids.first_entry() {
                        Some(e) => e,
                        None => break,
                    };

                    let bid_price = entry.key().0;
                    if order.order_type == OrderType::Limit && order.price > bid_price {
                        break; // Spread not crossed
                    }

                    let price_level = entry.get_mut();
                    while order.leaves_quantity.0 > 0 && !price_level.orders.is_empty() {
                        let resting = price_level.orders.front_mut().unwrap();
                        let match_qty = order.leaves_quantity.0.min(resting.leaves_quantity.0);

                        order.leaves_quantity.0 -= match_qty;
                        resting.leaves_quantity.0 -= match_qty;
                        price_level.total_quantity -= match_qty;

                        trades.push(Trade {
                            maker_order_id: resting.id,
                            taker_order_id: order.id,
                            price: resting.price,
                            quantity: Quantity(match_qty),
                            maker_leaves_quantity: resting.leaves_quantity,
                            taker_leaves_quantity: order.leaves_quantity,
                        });

                        if resting.is_filled() {
                            let filled = price_level.orders.pop_front().unwrap();
                            self.order_locations.remove(&filled.id);
                        }
                    }

                    if price_level.orders.is_empty() {
                        entry.remove();
                    }
                }

                // 4. Residual handling
                let can_rest = order.order_type == OrderType::Limit
                    && matches!(order.time_in_force, TimeInForce::Gtc | TimeInForce::PostOnly);

                if !order.is_filled() && can_rest {
                    self.order_locations.insert(order.id, (order.price, order.side));
                    self.asks
                        .entry(order.price)
                        .or_default()
                        .push_back(order);

                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order: Some(order),
                    }
                } else {
                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order: None,
                    }
                }
            }
        }
    }

    /// Cancel a resting order by OrderId.
    pub fn cancel_order(&mut self, order_id: OrderId) -> Option<Order> {
        let (price, side) = self.order_locations.remove(&order_id)?;

        let price_level = match side {
            Side::Buy => self.bids.get_mut(&std::cmp::Reverse(price))?,
            Side::Sell => self.asks.get_mut(&price)?,
        };

        let pos = price_level.orders.iter().position(|o| o.id == order_id)?;
        let cancelled = price_level.orders.remove(pos)?;
        price_level.total_quantity -= cancelled.leaves_quantity.0;

        if price_level.orders.is_empty() {
            match side {
                Side::Buy => { self.bids.remove(&std::cmp::Reverse(price)); }
                Side::Sell => { self.asks.remove(&price); }
            }
        }

        Some(cancelled)
    }

    /// Check total available volume up to the specified limit price.
    fn simulated_available_liquidity(&self, opposing_side: Side, limit_price: Price, order_type: OrderType) -> u64 {
        let mut total = 0;
        match opposing_side {
            Side::Sell => {
                for (ask_price, level) in &self.asks {
                    if order_type == OrderType::Limit && *ask_price > limit_price {
                        break;
                    }
                    total += level.total_quantity;
                }
            }
            Side::Buy => {
                for (rev_bid_price, level) in &self.bids {
                    let bid_price = rev_bid_price.0;
                    if order_type == OrderType::Limit && bid_price < limit_price {
                        break;
                    }
                    total += level.total_quantity;
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_fill_fifo_queue() {
        let mut book = OrderBook::new();

        // Two maker asks at price 100:
        // Order #1: size 10
        // Order #2: size 15
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Sell, Price(100), Quantity(15)));

        // Taker buys 14 @ 100 -> matches all of #1 (10) and partially fills #2 (4 filled, 11 leaves)
        let res = book.place_order(Order::new_limit(OrderId(3), Side::Buy, Price(100), Quantity(14)));

        if let OrderPlacementResult::Accepted { trades, resting_order } = res {
            assert_eq!(trades.len(), 2);
            assert_eq!(resting_order, None);

            // First match against Order 1 (complete fill)
            assert_eq!(trades[0].maker_order_id, OrderId(1));
            assert_eq!(trades[0].quantity, Quantity(10));
            assert_eq!(trades[0].maker_leaves_quantity, Quantity(0));
            assert_eq!(trades[0].taker_leaves_quantity, Quantity(4));

            // Second match against Order 2 (partial fill)
            assert_eq!(trades[1].maker_order_id, OrderId(2));
            assert_eq!(trades[1].quantity, Quantity(4));
            assert_eq!(trades[1].maker_leaves_quantity, Quantity(11));
            assert_eq!(trades[1].taker_leaves_quantity, Quantity(0));
        } else {
            panic!("Expected order to be accepted");
        }

        // Remaining ask on the book should still be at 100 with 11 quantity
        assert_eq!(book.best_ask(), Some(Price(100)));
    }

    #[test]
    fn test_post_only_rejection() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));

        // Post-only buy at 101 crosses the ask at 100 -> must be rejected
        let post_only_order = Order::new(
            OrderId(2),
            Side::Buy,
            OrderType::Limit,
            TimeInForce::PostOnly,
            Price(101),
            Quantity(5),
        );

        let res = book.place_order(post_only_order);
        assert!(matches!(res, OrderPlacementResult::Rejected { .. }));
    }

    #[test]
    fn test_ioc_cancellation() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));

        // IOC buy for 15 @ 100 -> fills 10, cancels remaining 5 without resting
        let ioc_order = Order::new(
            OrderId(2),
            Side::Buy,
            OrderType::Limit,
            TimeInForce::Ioc,
            Price(100),
            Quantity(15),
        );

        let res = book.place_order(ioc_order);
        if let OrderPlacementResult::Accepted { trades, resting_order } = res {
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].quantity, Quantity(10));
            assert_eq!(resting_order, None); // IOC does not rest
        } else {
            panic!("Expected order to be accepted");
        }

        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn test_fok_rejection_vs_execution() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));

        // FOK buy for 15 @ 100 -> only 10 available, must be rejected with 0 trades
        let fok_order = Order::new(
            OrderId(2),
            Side::Buy,
            OrderType::Limit,
            TimeInForce::Fok,
            Price(100),
            Quantity(15),
        );

        let res = book.place_order(fok_order);
        assert!(matches!(res, OrderPlacementResult::Rejected { .. }));
        assert_eq!(book.best_ask(), Some(Price(100))); // Resting order remains untouched
    }
}
