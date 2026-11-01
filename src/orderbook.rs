//! High-Performance Order Book with contiguous arena memory & intrusive doubly-linked list.

use std::collections::BTreeMap;
use rustc_hash::FxHashMap;
use crate::arena::{OrderArena, PriceLevelQueue, SlotIdx};
use crate::types::{Order, OrderId, OrderType, Price, Quantity, Side, TimeInForce, Trade};

/// Result of placing an order into the order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderPlacementResult {
    Accepted {
        trades: Vec<Trade>,
        resting_order_id: Option<OrderId>,
    },
    Rejected {
        reason: &'static str,
    },
}

/// In-memory Limit Order Book using arena-backed price queues.
#[derive(Debug)]
pub struct OrderBook {
    /// Bids sorted descending.
    pub bids: BTreeMap<std::cmp::Reverse<Price>, PriceLevelQueue>,
    /// Asks sorted ascending.
    pub asks: BTreeMap<Price, PriceLevelQueue>,
    /// Maps active OrderId -> (Price, Side, SlotIdx).
    pub order_locations: FxHashMap<OrderId, (Price, Side, SlotIdx)>,
    /// Pre-allocated arena for zero-heap order operations.
    pub arena: OrderArena,
}

impl OrderBook {
    pub const DEFAULT_CAPACITY: usize = 100_000;

    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_locations: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            arena: OrderArena::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first_key_value().map(|(rev_p, _)| rev_p.0)
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    /// Generate an L2 Market Depth snapshot of the top N price levels with zero heap allocation.
    pub fn get_l2_snapshot<const DEPTH: usize>(&self) -> crate::market_data::L2Snapshot<DEPTH> {
        let mut snapshot = crate::market_data::L2Snapshot::<DEPTH>::default();

        for (rev_price, queue) in self.bids.iter().take(DEPTH) {
            snapshot.bids[snapshot.bid_count] = crate::market_data::DepthLevel::new(
                rev_price.0,
                Quantity(queue.total_quantity),
            );
            snapshot.bid_count += 1;
        }

        for (price, queue) in self.asks.iter().take(DEPTH) {
            snapshot.asks[snapshot.ask_count] = crate::market_data::DepthLevel::new(
                *price,
                Quantity(queue.total_quantity),
            );
            snapshot.ask_count += 1;
        }

        snapshot
    }

    /// Process an order in FIFO price-time priority with duplicate protection.
    pub fn place_order(&mut self, mut order: Order) -> OrderPlacementResult {
        // 0. Idempotency Check: Reject if OrderId is already active
        if self.order_locations.contains_key(&order.id) {
            return OrderPlacementResult::Rejected {
                reason: "Duplicate OrderId: order is already active on the book",
            };
        }

        // 1. Post-Only check
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

        // 2. FOK (Fill-Or-Kill) check
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
                        break;
                    }

                    let queue = entry.get_mut();
                    while order.leaves_quantity.0 > 0 && !queue.is_empty() {
                        let resting_slot = SlotIdx(queue.head);
                        let resting_node = self.arena.get_mut(resting_slot);
                        let match_qty = order.leaves_quantity.0.min(resting_node.order.leaves_quantity.0);

                        order.leaves_quantity.0 -= match_qty;
                        resting_node.order.leaves_quantity.0 -= match_qty;
                        queue.total_quantity -= match_qty;

                        trades.push(Trade {
                            maker_order_id: resting_node.order.id,
                            taker_order_id: order.id,
                            price: resting_node.order.price,
                            quantity: Quantity(match_qty),
                            maker_leaves_quantity: resting_node.order.leaves_quantity,
                            taker_leaves_quantity: order.leaves_quantity,
                        });

                        if resting_node.order.is_filled() {
                            let filled_slot = queue.pop_front(&mut self.arena).unwrap();
                            let filled_order_id = self.arena.get(filled_slot).order.id;
                            self.order_locations.remove(&filled_order_id);
                            self.arena.dealloc(filled_slot);
                        }
                    }

                    if queue.is_empty() {
                        entry.remove();
                    }
                }

                // 4. Residual handling
                let can_rest = order.order_type == OrderType::Limit
                    && matches!(order.time_in_force, TimeInForce::Gtc | TimeInForce::PostOnly);

                if !order.is_filled() && can_rest {
                    let order_id = order.id;
                    let price = order.price;
                    let side = order.side;

                    match self.arena.alloc(order) {
                        Ok(slot) => {
                            self.order_locations.insert(order_id, (price, side, slot));
                            self.bids
                                .entry(std::cmp::Reverse(price))
                                .or_default()
                                .push_back(&mut self.arena, slot);

                            OrderPlacementResult::Accepted {
                                trades,
                                resting_order_id: Some(order_id),
                            }
                        }
                        Err(err) => OrderPlacementResult::Rejected { reason: err },
                    }
                } else {
                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order_id: None,
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
                        break;
                    }

                    let queue = entry.get_mut();
                    while order.leaves_quantity.0 > 0 && !queue.is_empty() {
                        let resting_slot = SlotIdx(queue.head);
                        let resting_node = self.arena.get_mut(resting_slot);
                        let match_qty = order.leaves_quantity.0.min(resting_node.order.leaves_quantity.0);

                        order.leaves_quantity.0 -= match_qty;
                        resting_node.order.leaves_quantity.0 -= match_qty;
                        queue.total_quantity -= match_qty;

                        trades.push(Trade {
                            maker_order_id: resting_node.order.id,
                            taker_order_id: order.id,
                            price: resting_node.order.price,
                            quantity: Quantity(match_qty),
                            maker_leaves_quantity: resting_node.order.leaves_quantity,
                            taker_leaves_quantity: order.leaves_quantity,
                        });

                        if resting_node.order.is_filled() {
                            let filled_slot = queue.pop_front(&mut self.arena).unwrap();
                            let filled_order_id = self.arena.get(filled_slot).order.id;
                            self.order_locations.remove(&filled_order_id);
                            self.arena.dealloc(filled_slot);
                        }
                    }

                    if queue.is_empty() {
                        entry.remove();
                    }
                }

                // 4. Residual handling
                let can_rest = order.order_type == OrderType::Limit
                    && matches!(order.time_in_force, TimeInForce::Gtc | TimeInForce::PostOnly);

                if !order.is_filled() && can_rest {
                    let order_id = order.id;
                    let price = order.price;
                    let side = order.side;

                    match self.arena.alloc(order) {
                        Ok(slot) => {
                            self.order_locations.insert(order_id, (price, side, slot));
                            self.asks
                                .entry(price)
                                .or_default()
                                .push_back(&mut self.arena, slot);

                            OrderPlacementResult::Accepted {
                                trades,
                                resting_order_id: Some(order_id),
                            }
                        }
                        Err(err) => OrderPlacementResult::Rejected { reason: err },
                    }
                } else {
                    OrderPlacementResult::Accepted {
                        trades,
                        resting_order_id: None,
                    }
                }
            }
        }
    }

    /// Guaranteed O(1) order cancellation using intrusive unlink without queue scan.
    pub fn cancel_order(&mut self, order_id: OrderId) -> Option<Order> {
        let (price, side, slot) = self.order_locations.remove(&order_id)?;

        let queue = match side {
            Side::Buy => self.bids.get_mut(&std::cmp::Reverse(price))?,
            Side::Sell => self.asks.get_mut(&price)?,
        };

        // True O(1) unlinking: No linear scanning through orders!
        queue.unlink(&mut self.arena, slot);

        if queue.is_empty() {
            match side {
                Side::Buy => { self.bids.remove(&std::cmp::Reverse(price)); }
                Side::Sell => { self.asks.remove(&price); }
            }
        }

        let cancelled_order = self.arena.get(slot).order;
        self.arena.dealloc(slot);

        Some(cancelled_order)
    }

    fn simulated_available_liquidity(&self, opposing_side: Side, limit_price: Price, order_type: OrderType) -> u64 {
        let mut total = 0;
        match opposing_side {
            Side::Sell => {
                for (ask_price, queue) in &self.asks {
                    if order_type == OrderType::Limit && *ask_price > limit_price {
                        break;
                    }
                    total += queue.total_quantity;
                }
            }
            Side::Buy => {
                for (rev_bid_price, queue) in &self.bids {
                    let bid_price = rev_bid_price.0;
                    if order_type == OrderType::Limit && bid_price < limit_price {
                        break;
                    }
                    total += queue.total_quantity;
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
    fn test_duplicate_active_order_rejected() {
        let mut book = OrderBook::new();
        let res1 = book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        assert!(matches!(res1, OrderPlacementResult::Accepted { .. }));

        // Attempt to submit identical OrderId while active
        let res2 = book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        assert!(matches!(res2, OrderPlacementResult::Rejected { .. }));
    }

    #[test]
    fn test_l2_snapshot_aggregation() {
        let mut book = OrderBook::new();

        // Add 3 resting bids across 2 price levels
        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(100), Quantity(25)));
        book.place_order(Order::new_limit(OrderId(3), Side::Buy, Price(99), Quantity(15)));

        // Add 2 resting asks across 2 price levels
        book.place_order(Order::new_limit(OrderId(4), Side::Sell, Price(101), Quantity(5)));
        book.place_order(Order::new_limit(OrderId(5), Side::Sell, Price(102), Quantity(40)));

        // Query top-5 L2 snapshot
        let snapshot = book.get_l2_snapshot::<5>();

        assert_eq!(snapshot.bid_count, 2);
        assert_eq!(snapshot.bids[0].price, Price(100));
        assert_eq!(snapshot.bids[0].total_quantity, Quantity(35)); // 10 + 25
        assert_eq!(snapshot.bids[1].price, Price(99));
        assert_eq!(snapshot.bids[1].total_quantity, Quantity(15));

        assert_eq!(snapshot.ask_count, 2);
        assert_eq!(snapshot.asks[0].price, Price(101));
        assert_eq!(snapshot.asks[0].total_quantity, Quantity(5));
        assert_eq!(snapshot.asks[1].price, Price(102));
        assert_eq!(snapshot.asks[1].total_quantity, Quantity(40));
    }

    #[test]
    fn test_partial_fill_fifo_queue() {
        let mut book = OrderBook::new();

        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Sell, Price(100), Quantity(15)));

        let res = book.place_order(Order::new_limit(OrderId(3), Side::Buy, Price(100), Quantity(14)));

        if let OrderPlacementResult::Accepted { trades, resting_order_id } = res {
            assert_eq!(trades.len(), 2);
            assert_eq!(resting_order_id, None);

            assert_eq!(trades[0].maker_order_id, OrderId(1));
            assert_eq!(trades[0].quantity, Quantity(10));
            assert_eq!(trades[0].maker_leaves_quantity, Quantity(0));

            assert_eq!(trades[1].maker_order_id, OrderId(2));
            assert_eq!(trades[1].quantity, Quantity(4));
            assert_eq!(trades[1].maker_leaves_quantity, Quantity(11));
        } else {
            panic!("Expected order to be accepted");
        }

        assert_eq!(book.best_ask(), Some(Price(100)));
    }

    #[test]
    fn test_cancel_order_o1() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(100), Quantity(20)));
        book.place_order(Order::new_limit(OrderId(3), Side::Buy, Price(100), Quantity(30)));

        // Cancel the middle order (OrderId 2)
        let cancelled = book.cancel_order(OrderId(2));
        assert!(cancelled.is_some());
        assert_eq!(cancelled.unwrap().id, OrderId(2));

        // Queue at 100 should now have 10 + 30 = 40 total quantity
        let rev_p = std::cmp::Reverse(Price(100));
        let queue = book.bids.get(&rev_p).unwrap();
        assert_eq!(queue.total_quantity, 40);
        assert_eq!(queue.order_count, 2);

        // Ensure Order 1 is still head, and Order 3 is still tail
        let head_slot = SlotIdx(queue.head);
        assert_eq!(book.arena.get(head_slot).order.id, OrderId(1));

        let tail_slot = SlotIdx(queue.tail);
        assert_eq!(book.arena.get(tail_slot).order.id, OrderId(3));
    }

    #[test]
    fn test_post_only_rejection() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));

        let post_only = Order::new(
            OrderId(2),
            Side::Buy,
            OrderType::Limit,
            TimeInForce::PostOnly,
            Price(101),
            Quantity(5),
        );

        let res = book.place_order(post_only);
        assert!(matches!(res, OrderPlacementResult::Rejected { .. }));
    }

    #[test]
    fn test_fok_policy() {
        let mut book = OrderBook::new();
        book.place_order(Order::new_limit(OrderId(1), Side::Sell, Price(100), Quantity(10)));

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
        assert_eq!(book.best_ask(), Some(Price(100)));
    }
}
