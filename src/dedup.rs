//! Zero-allocation sliding deduplication filter for inbound command idempotency.

use crate::types::OrderId;
use rustc_hash::FxHashSet;

/// Sliding circular deduplication window tracking recently seen order IDs.
/// Uses a pre-allocated fixed-size ring array and FxHashSet for O(1) membership checks.
#[derive(Debug)]
pub struct SlidingDedupFilter {
    ring: Vec<u64>,
    set: FxHashSet<u64>,
    head: usize,
    capacity: usize,
}

impl SlidingDedupFilter {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            ring: vec![0; capacity],
            set: FxHashSet::with_capacity_and_hasher(capacity, Default::default()),
            head: 0,
            capacity,
        }
    }

    /// Check if an OrderId was already processed recently.
    #[inline(always)]
    pub fn contains(&self, id: OrderId) -> bool {
        self.set.contains(&id.0)
    }

    /// Record a processed OrderId into the sliding deduplication window.
    /// Evicts the oldest item when the capacity is exceeded in O(1).
    #[inline]
    pub fn record(&mut self, id: OrderId) {
        let old_id = self.ring[self.head];
        if old_id != 0 {
            self.set.remove(&old_id);
        }

        self.ring[self.head] = id.0;
        self.set.insert(id.0);

        self.head = (self.head + 1) % self.capacity;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_window_eviction() {
        let mut filter = SlidingDedupFilter::with_capacity(3);

        assert!(!filter.contains(OrderId(10)));
        filter.record(OrderId(10));
        assert!(filter.contains(OrderId(10)));

        filter.record(OrderId(20));
        filter.record(OrderId(30));
        assert!(filter.contains(OrderId(10)));
        assert!(filter.contains(OrderId(20)));
        assert!(filter.contains(OrderId(30)));

        // 4th insert evicts OrderId(10)
        filter.record(OrderId(40));
        assert!(!filter.contains(OrderId(10)));
        assert!(filter.contains(OrderId(40)));
    }
}
