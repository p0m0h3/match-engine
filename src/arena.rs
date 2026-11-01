//! Pre-allocated memory arena and intrusive doubly-linked list for ultra-low latency order queues.

use crate::types::Order;

/// 32-bit slot handle indexing into the pre-allocated arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotIdx(pub u32);

impl SlotIdx {
    pub const NONE: u32 = u32::MAX;

    #[inline(always)]
    pub fn is_none(self) -> bool {
        self.0 == Self::NONE
    }

    #[inline(always)]
    pub fn is_some(self) -> bool {
        self.0 != Self::NONE
    }
}

/// An order node inside the contiguous arena storage.
#[derive(Debug, Clone)]
pub struct OrderNode {
    pub order: Order,
    pub prev: u32,
    pub next: u32,
}

/// Pre-allocated contiguous slab pool with an embedded free list.
#[derive(Debug)]
pub struct OrderArena {
    nodes: Vec<OrderNode>,
    free_head: u32,
    capacity: usize,
}

impl OrderArena {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut nodes = Vec::with_capacity(capacity);
        // Pre-populate the arena nodes and link the free list
        for i in 0..capacity {
            let next_free = if i + 1 < capacity {
                (i + 1) as u32
            } else {
                SlotIdx::NONE
            };
            nodes.push(OrderNode {
                order: Order::new_limit(
                    crate::types::OrderId(0),
                    crate::types::Side::Buy,
                    crate::types::Price(0),
                    crate::types::Quantity(0),
                ),
                prev: SlotIdx::NONE,
                next: next_free,
            });
        }

        Self {
            nodes,
            free_head: 0,
            capacity,
        }
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    pub fn alloc(&mut self, order: Order) -> Result<SlotIdx, &'static str> {
        if self.free_head == SlotIdx::NONE {
            return Err("OrderArena capacity exhausted");
        }

        let idx = self.free_head;
        let node = unsafe { self.nodes.get_unchecked_mut(idx as usize) };
        self.free_head = node.next;

        node.order = order;
        node.prev = SlotIdx::NONE;
        node.next = SlotIdx::NONE;

        Ok(SlotIdx(idx))
    }

    #[inline(always)]
    pub fn dealloc(&mut self, idx: SlotIdx) {
        let node = unsafe { self.nodes.get_unchecked_mut(idx.0 as usize) };
        node.next = self.free_head;
        node.prev = SlotIdx::NONE;
        self.free_head = idx.0;
    }

    #[inline(always)]
    pub fn get(&self, idx: SlotIdx) -> &OrderNode {
        unsafe { self.nodes.get_unchecked(idx.0 as usize) }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: SlotIdx) -> &mut OrderNode {
        unsafe { self.nodes.get_unchecked_mut(idx.0 as usize) }
    }
}

/// Intrusive Doubly-Linked List representing the FIFO queue at a single price level.
#[derive(Debug, Clone, Copy)]
pub struct PriceLevelQueue {
    pub head: u32,
    pub tail: u32,
    pub total_quantity: u64,
    pub order_count: u32,
}

impl Default for PriceLevelQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl PriceLevelQueue {
    pub const fn new() -> Self {
        Self {
            head: SlotIdx::NONE,
            tail: SlotIdx::NONE,
            total_quantity: 0,
            order_count: 0,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.head == SlotIdx::NONE
    }

    /// Append an order to the tail of the price queue (FIFO priority).
    #[inline]
    pub fn push_back(&mut self, arena: &mut OrderArena, slot: SlotIdx) {
        let qty = arena.get(slot).order.leaves_quantity.0;
        self.total_quantity += qty;
        self.order_count += 1;

        let node = arena.get_mut(slot);
        node.prev = self.tail;
        node.next = SlotIdx::NONE;

        if self.tail != SlotIdx::NONE {
            let tail_node = arena.get_mut(SlotIdx(self.tail));
            tail_node.next = slot.0;
        } else {
            self.head = slot.0;
        }

        self.tail = slot.0;
    }

    /// Pop an order from the head of the price queue (oldest order matches first).
    #[inline]
    pub fn pop_front(&mut self, arena: &mut OrderArena) -> Option<SlotIdx> {
        if self.is_empty() {
            return None;
        }

        let head_idx = SlotIdx(self.head);
        let head_node = arena.get(head_idx);
        let next_idx = head_node.next;
        let qty = head_node.order.leaves_quantity.0;

        self.total_quantity -= qty;
        self.order_count -= 1;

        if next_idx != SlotIdx::NONE {
            arena.get_mut(SlotIdx(next_idx)).prev = SlotIdx::NONE;
            self.head = next_idx;
        } else {
            self.head = SlotIdx::NONE;
            self.tail = SlotIdx::NONE;
        }

        Some(head_idx)
    }

    /// Unlink an arbitrary order from anywhere in the queue in guaranteed O(1) time.
    #[inline]
    pub fn unlink(&mut self, arena: &mut OrderArena, slot: SlotIdx) {
        let (prev_idx, next_idx, qty) = {
            let node = arena.get(slot);
            (node.prev, node.next, node.order.leaves_quantity.0)
        };

        self.total_quantity -= qty;
        self.order_count -= 1;

        if prev_idx != SlotIdx::NONE {
            arena.get_mut(SlotIdx(prev_idx)).next = next_idx;
        } else {
            self.head = next_idx;
        }

        if next_idx != SlotIdx::NONE {
            arena.get_mut(SlotIdx(next_idx)).prev = prev_idx;
        } else {
            self.tail = prev_idx;
        }
    }
}
