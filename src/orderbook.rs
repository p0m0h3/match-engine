//! High-Performance Order Book with contiguous arena memory, intrusive doubly-linked list,
//! and Direct-Indexed FlatTickBook price indexing.

use std::collections::BTreeMap;
use rustc_hash::FxHashMap;
use crate::arena::{OrderArena, PriceLevelQueue, SlotIdx};
use crate::flat_book::FlatTickBook;
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

/// In-memory Limit Order Book using Direct-Indexed FlatTickBook with a sparse BTreeMap fallback.
pub struct OrderBook {
    /// Direct-indexed array for primary trading band (e.g. 10,000 tick levels).
    pub flat_bids: FlatTickBook<10_000>,
    pub flat_asks: FlatTickBook<10_000>,
    /// Sparse fallback for extreme outlier prices.
    pub sparse_bids: BTreeMap<std::cmp::Reverse<Price>, PriceLevelQueue>,
    pub sparse_asks: BTreeMap<Price, PriceLevelQueue>,
    /// Maps active OrderId -> (Price, Side, SlotIdx).
    pub order_locations: FxHashMap<OrderId, (Price, Side, SlotIdx)>,
    /// Pre-allocated arena for zero-heap order operations.
    pub arena: OrderArena,
}

impl OrderBook {
    pub const DEFAULT_CAPACITY: usize = 100_000;
    pub const DEFAULT_BASE_PRICE: u64 = 0;

    pub fn new() -> Self {
        Self::with_capacity_and_base(Self::DEFAULT_CAPACITY, Self::DEFAULT_BASE_PRICE)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_base(capacity, Self::DEFAULT_BASE_PRICE)
    }

    pub fn with_capacity_and_base(capacity: usize, base_price: u64) -> Self {
        Self {
            flat_bids: FlatTickBook::new(base_price),
            flat_asks: FlatTickBook::new(base_price),
            sparse_bids: BTreeMap::new(),
            sparse_asks: BTreeMap::new(),
            order_locations: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            arena: OrderArena::with_capacity(capacity),
        }
    }

    /// Best Bid price, if any exists.
    #[inline(always)]
    pub fn best_bid(&self) -> Option<Price> {
        let flat_best = self.flat_bids.highest_occupied_price();
        let sparse_best = self.sparse_bids.first_key_value().map(|(rev_p, _)| rev_p.0);

        match (flat_best, sparse_best) {
            (Some(fb), Some(sb)) => Some(fb.max(sb)),
            (Some(fb), None) => Some(fb),
            (None, Some(sb)) => Some(sb),
            (None, None) => None,
        }
    }

    /// Best Ask / Offer price, if any exists.
    #[inline(always)]
    pub fn best_ask(&self) -> Option<Price> {
        let flat_best = self.flat_asks.lowest_occupied_price();
        let sparse_best = self.sparse_asks.first_key_value().map(|(p, _)| *p);

        match (flat_best, sparse_best) {
            (Some(fa), Some(sa)) => Some(fa.min(sa)),
            (Some(fa), None) => Some(fa),
            (None, Some(sa)) => Some(sa),
            (None, None) => None,
        }
    }

    /// Generate an L2 Market Depth snapshot of the top N price levels with zero heap allocation.
    pub fn get_l2_snapshot<const DEPTH: usize>(&self) -> crate::market_data::L2Snapshot<DEPTH> {
        let mut snapshot = crate::market_data::L2Snapshot::<DEPTH>::default();

        // Direct-indexed levels are fast and cache-local
        for (price, queue) in self.flat_bids.iter_descending() {
            if snapshot.bid_count >= DEPTH {
                break;
            }
            snapshot.bids[snapshot.bid_count] = crate::market_data::DepthLevel::new(
                price,
                Quantity(queue.total_quantity),
            );
            snapshot.bid_count += 1;
        }

        for (price, queue) in self.flat_asks.iter_ascending() {
            if snapshot.ask_count >= DEPTH {
                break;
            }
            snapshot.asks[snapshot.ask_count] = crate::market_data::DepthLevel::new(
                price,
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
                    let best_ask = match self.best_ask() {
                        Some(p) => p,
                        None => break,
                    };

                    if order.order_type == OrderType::Limit && order.price < best_ask {
                        break;
                    }

                    // Direct array matching path
                    if self.flat_asks.in_bounds(best_ask) {
                        while order.leaves_quantity.0 > 0 {
                            let (resting_slot, resting_leaves, resting_id, resting_price) = {
                                let queue = self.flat_asks.get_level_mut(best_ask).unwrap();
                                if queue.is_empty() {
                                    break;
                                }
                                let slot = SlotIdx(queue.head);
                                let node = self.arena.get(slot);
                                (slot, node.order.leaves_quantity.0, node.order.id, node.order.price)
                            };

                            let match_qty = order.leaves_quantity.0.min(resting_leaves);
                            order.leaves_quantity.0 -= match_qty;

                            // Adjust resting node
                            let resting_node = self.arena.get_mut(resting_slot);
                            resting_node.order.leaves_quantity.0 -= match_qty;
                            let leaves_after = resting_node.order.leaves_quantity.0;

                            // Update price level total quantity
                            let queue = self.flat_asks.get_level_mut(best_ask).unwrap();
                            queue.total_quantity -= match_qty;

                            trades.push(Trade {
                                maker_order_id: resting_id,
                                taker_order_id: order.id,
                                price: resting_price,
                                quantity: Quantity(match_qty),
                                maker_leaves_quantity: Quantity(leaves_after),
                                taker_leaves_quantity: order.leaves_quantity,
                            });

                            if leaves_after == 0 {
                                let filled_slot = self.flat_asks.pop_front(&mut self.arena, best_ask).unwrap();
                                let filled_order_id = self.arena.get(filled_slot).order.id;
                                self.order_locations.remove(&filled_order_id);
                                self.arena.dealloc(filled_slot);
                            }
                        }
                    } else {
                        // Sparse fallback path
                        let mut entry = self.sparse_asks.first_entry().unwrap();
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
                            if self.flat_bids.in_bounds(price) {
                                self.flat_bids.push_back(&mut self.arena, price, slot).unwrap();
                            } else {
                                self.sparse_bids
                                    .entry(std::cmp::Reverse(price))
                                    .or_default()
                                    .push_back(&mut self.arena, slot);
                            }

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
                    let best_bid = match self.best_bid() {
                        Some(p) => p,
                        None => break,
                    };

                    if order.order_type == OrderType::Limit && order.price > best_bid {
                        break;
                    }

                    // Direct array matching path
                    if self.flat_bids.in_bounds(best_bid) {
                        while order.leaves_quantity.0 > 0 {
                            let (resting_slot, resting_leaves, resting_id, resting_price) = {
                                let queue = self.flat_bids.get_level_mut(best_bid).unwrap();
                                if queue.is_empty() {
                                    break;
                                }
                                let slot = SlotIdx(queue.head);
                                let node = self.arena.get(slot);
                                (slot, node.order.leaves_quantity.0, node.order.id, node.order.price)
                            };

                            let match_qty = order.leaves_quantity.0.min(resting_leaves);
                            order.leaves_quantity.0 -= match_qty;

                            // Adjust resting node
                            let resting_node = self.arena.get_mut(resting_slot);
                            resting_node.order.leaves_quantity.0 -= match_qty;
                            let leaves_after = resting_node.order.leaves_quantity.0;

                            // Update price level total quantity
                            let queue = self.flat_bids.get_level_mut(best_bid).unwrap();
                            queue.total_quantity -= match_qty;

                            trades.push(Trade {
                                maker_order_id: resting_id,
                                taker_order_id: order.id,
                                price: resting_price,
                                quantity: Quantity(match_qty),
                                maker_leaves_quantity: Quantity(leaves_after),
                                taker_leaves_quantity: order.leaves_quantity,
                            });

                            if leaves_after == 0 {
                                let filled_slot = self.flat_bids.pop_front(&mut self.arena, best_bid).unwrap();
                                let filled_order_id = self.arena.get(filled_slot).order.id;
                                self.order_locations.remove(&filled_order_id);
                                self.arena.dealloc(filled_slot);
                            }
                        }
                    } else {
                        // Sparse fallback path
                        let mut entry = self.sparse_bids.first_entry().unwrap();
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
                            if self.flat_asks.in_bounds(price) {
                                self.flat_asks.push_back(&mut self.arena, price, slot).unwrap();
                            } else {
                                self.sparse_asks
                                    .entry(price)
                                    .or_default()
                                    .push_back(&mut self.arena, slot);
                            }

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

    /// Modify/amend an existing order's quantity or price atomically.
    /// - Decreasing quantity: Retains exact FIFO queue priority in O(1).
    /// - Increasing quantity / Changing price: Loses priority (unlinks and re-inserts).
    pub fn modify_order(
        &mut self,
        order_id: OrderId,
        new_price: Option<Price>,
        new_quantity: Quantity,
    ) -> Result<(Order, Vec<Trade>), &'static str> {
        let (price, side, slot) = match self.order_locations.get(&order_id) {
            Some(&loc) => loc,
            None => return Err("Order not found for modification"),
        };

        let current_order = self.arena.get(slot).order;
        let target_price = new_price.unwrap_or(price);

        // Case 1: Same price and decreasing quantity -> In-place O(1) update with priority retained
        if target_price == price && new_quantity.0 <= current_order.leaves_quantity.0 {
            if new_quantity.0 == 0 {
                // Modified down to 0 is an effective cancel
                let cancelled = self.cancel_order(order_id).unwrap();
                return Ok((cancelled, Vec::new()));
            }

            let delta = current_order.leaves_quantity.0 - new_quantity.0;

            // Adjust price level total quantity
            match side {
                Side::Buy => {
                    if self.flat_bids.in_bounds(price) {
                        let queue = self.flat_bids.get_level_mut(price).unwrap();
                        queue.total_quantity -= delta;
                    } else if let Some(queue) = self.sparse_bids.get_mut(&std::cmp::Reverse(price)) {
                        queue.total_quantity -= delta;
                    }
                }
                Side::Sell => {
                    if self.flat_asks.in_bounds(price) {
                        let queue = self.flat_asks.get_level_mut(price).unwrap();
                        queue.total_quantity -= delta;
                    } else if let Some(queue) = self.sparse_asks.get_mut(&price) {
                        queue.total_quantity -= delta;
                    }
                }
            }

            // Adjust resting node leaves_quantity
            let node = self.arena.get_mut(slot);
            node.order.leaves_quantity = new_quantity;

            return Ok((node.order, Vec::new()));
        }

        // Case 2: Changing price or increasing quantity -> Loses priority (Cancel-Replace)
        let mut order_to_reinsert = self.cancel_order(order_id).unwrap();
        order_to_reinsert.price = target_price;
        order_to_reinsert.initial_quantity = new_quantity;
        order_to_reinsert.leaves_quantity = new_quantity;

        match self.place_order(order_to_reinsert) {
            OrderPlacementResult::Accepted { trades, .. } => Ok((order_to_reinsert, trades)),
            OrderPlacementResult::Rejected { reason } => Err(reason),
        }
    }

    /// Guaranteed O(1) order cancellation using intrusive unlink without queue scan.
    pub fn cancel_order(&mut self, order_id: OrderId) -> Option<Order> {
        let (price, side, slot) = self.order_locations.remove(&order_id)?;

        match side {
            Side::Buy => {
                if self.flat_bids.in_bounds(price) {
                    self.flat_bids.unlink(&mut self.arena, price, slot);
                } else if let Some(queue) = self.sparse_bids.get_mut(&std::cmp::Reverse(price)) {
                    queue.unlink(&mut self.arena, slot);
                    if queue.is_empty() {
                        self.sparse_bids.remove(&std::cmp::Reverse(price));
                    }
                }
            }
            Side::Sell => {
                if self.flat_asks.in_bounds(price) {
                    self.flat_asks.unlink(&mut self.arena, price, slot);
                } else if let Some(queue) = self.sparse_asks.get_mut(&price) {
                    queue.unlink(&mut self.arena, slot);
                    if queue.is_empty() {
                        self.sparse_asks.remove(&price);
                    }
                }
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
                for (price, queue) in self.flat_asks.iter_ascending() {
                    if order_type == OrderType::Limit && price > limit_price {
                        break;
                    }
                    total += queue.total_quantity;
                }
            }
            Side::Buy => {
                for (price, queue) in self.flat_bids.iter_descending() {
                    if order_type == OrderType::Limit && price < limit_price {
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
    fn test_modify_order_decrease_retains_priority() {
        let mut book = OrderBook::new();

        // Seed two orders at price 100: Order 1 (10 units), Order 2 (20 units)
        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(100), Quantity(20)));

        // Modify Order 1 down from 10 to 4 units
        let (modified, trades) = book.modify_order(OrderId(1), None, Quantity(4)).unwrap();
        assert_eq!(modified.leaves_quantity, Quantity(4));
        assert!(trades.is_empty());

        // L2 depth should now reflect 4 + 20 = 24 total volume
        let l2 = book.get_l2_snapshot::<1>();
        assert_eq!(l2.bids[0].total_quantity, Quantity(24));

        // Submit crossing sell for 5 units:
        // Order 1 should fill completely (4 units) FIRST, proving priority retention!
        let res = book.place_order(Order::new_limit(OrderId(3), Side::Sell, Price(100), Quantity(5)));
        if let OrderPlacementResult::Accepted { trades, .. } = res {
            assert_eq!(trades.len(), 2);
            assert_eq!(trades[0].maker_order_id, OrderId(1));
            assert_eq!(trades[0].quantity, Quantity(4));
            assert_eq!(trades[1].maker_order_id, OrderId(2));
            assert_eq!(trades[1].quantity, Quantity(1));
        } else {
            panic!("Expected order to match");
        }
    }

    #[test]
    fn test_modify_order_increase_loses_priority() {
        let mut book = OrderBook::new();

        // Seed two orders at price 100: Order 1 (10 units), Order 2 (20 units)
        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(100), Quantity(20)));

        // Modify Order 1 UP from 10 to 15 units (loses priority, moves to back behind Order 2)
        let (modified, _) = book.modify_order(OrderId(1), None, Quantity(15)).unwrap();
        assert_eq!(modified.leaves_quantity, Quantity(15));

        // Submit crossing sell for 10 units:
        // Order 2 must match FIRST because Order 1 lost priority!
        let res = book.place_order(Order::new_limit(OrderId(3), Side::Sell, Price(100), Quantity(10)));
        if let OrderPlacementResult::Accepted { trades, .. } = res {
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].maker_order_id, OrderId(2)); // Order 2 was head of queue!
            assert_eq!(trades[0].quantity, Quantity(10));
        } else {
            panic!("Expected order to match");
        }
    }

    #[test]
    fn test_l2_snapshot_aggregation() {
        let mut book = OrderBook::new();

        book.place_order(Order::new_limit(OrderId(1), Side::Buy, Price(100), Quantity(10)));
        book.place_order(Order::new_limit(OrderId(2), Side::Buy, Price(100), Quantity(25)));
        book.place_order(Order::new_limit(OrderId(3), Side::Buy, Price(99), Quantity(15)));

        book.place_order(Order::new_limit(OrderId(4), Side::Sell, Price(101), Quantity(5)));
        book.place_order(Order::new_limit(OrderId(5), Side::Sell, Price(102), Quantity(40)));

        let snapshot = book.get_l2_snapshot::<5>();

        assert_eq!(snapshot.bid_count, 2);
        assert_eq!(snapshot.bids[0].price, Price(100));
        assert_eq!(snapshot.bids[0].total_quantity, Quantity(35));
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

        let cancelled = book.cancel_order(OrderId(2));
        assert!(cancelled.is_some());
        assert_eq!(cancelled.unwrap().id, OrderId(2));

        let l2 = book.get_l2_snapshot::<1>();
        assert_eq!(l2.bids[0].price, Price(100));
        assert_eq!(l2.bids[0].total_quantity, Quantity(40));
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
