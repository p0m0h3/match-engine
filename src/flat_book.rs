//! Direct-indexed flat tick array and bitset for true O(1) price-level operations.

use crate::arena::{OrderArena, PriceLevelQueue, SlotIdx};
use crate::types::Price;

/// Flat direct-indexed array of price level queues with a bitset for instant BBO scanning.
pub struct FlatTickBook<const CAPACITY: usize> {
    min_price: u64,
    max_price: u64,
    levels: Vec<PriceLevelQueue>,
    /// Bitmask tracking which levels currently have resting volume.
    /// Bit = 1 means level has volume; Bit = 0 means level is empty.
    occupied_bitmap: Vec<u64>,
}

impl<const CAPACITY: usize> FlatTickBook<CAPACITY> {
    pub fn new(min_price: u64) -> Self {
        let max_price = min_price + CAPACITY as u64;
        let bitmap_words = CAPACITY.div_ceil(64);

        Self {
            min_price,
            max_price,
            levels: vec![PriceLevelQueue::new(); CAPACITY],
            occupied_bitmap: vec![0; bitmap_words],
        }
    }

    #[inline(always)]
    pub fn in_bounds(&self, price: Price) -> bool {
        price.0 >= self.min_price && price.0 < self.max_price
    }

    #[inline(always)]
    fn price_to_index(&self, price: Price) -> usize {
        (price.0 - self.min_price) as usize
    }

    #[inline(always)]
    fn index_to_price(&self, index: usize) -> Price {
        Price(self.min_price + index as u64)
    }

    #[inline(always)]
    pub fn get_level(&self, price: Price) -> Option<&PriceLevelQueue> {
        if self.in_bounds(price) {
            Some(&self.levels[self.price_to_index(price)])
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn get_level_mut(&mut self, price: Price) -> Option<&mut PriceLevelQueue> {
        if self.in_bounds(price) {
            let idx = self.price_to_index(price);
            Some(&mut self.levels[idx])
        } else {
            None
        }
    }

    /// Append order to tail of price level in O(1).
    #[inline(always)]
    pub fn push_back(
        &mut self,
        arena: &mut OrderArena,
        price: Price,
        slot: SlotIdx,
    ) -> Result<(), &'static str> {
        if !self.in_bounds(price) {
            return Err("Price out of bounds for direct-indexed array");
        }

        let idx = self.price_to_index(price);
        self.levels[idx].push_back(arena, slot);

        // Mark bitmap as occupied
        let word_idx = idx / 64;
        let bit_offset = idx % 64;
        self.occupied_bitmap[word_idx] |= 1u64 << bit_offset;

        Ok(())
    }

    /// Pop head order from price level in O(1).
    #[inline(always)]
    pub fn pop_front(&mut self, arena: &mut OrderArena, price: Price) -> Option<SlotIdx> {
        if !self.in_bounds(price) {
            return None;
        }

        let idx = self.price_to_index(price);
        let slot = self.levels[idx].pop_front(arena)?;

        if self.levels[idx].is_empty() {
            let word_idx = idx / 64;
            let bit_offset = idx % 64;
            self.occupied_bitmap[word_idx] &= !(1u64 << bit_offset);
        }

        Some(slot)
    }

    /// Unlink slot from price level in O(1).
    #[inline(always)]
    pub fn unlink(&mut self, arena: &mut OrderArena, price: Price, slot: SlotIdx) {
        if !self.in_bounds(price) {
            return;
        }

        let idx = self.price_to_index(price);
        self.levels[idx].unlink(arena, slot);

        if self.levels[idx].is_empty() {
            let word_idx = idx / 64;
            let bit_offset = idx % 64;
            self.occupied_bitmap[word_idx] &= !(1u64 << bit_offset);
        }
    }

    /// Find the lowest occupied price (Best Ask) in fast bit-scanning time.
    #[inline(always)]
    pub fn lowest_occupied_price(&self) -> Option<Price> {
        for (word_idx, &word) in self.occupied_bitmap.iter().enumerate() {
            if word != 0 {
                let tz = word.trailing_zeros() as usize;
                let level_idx = word_idx * 64 + tz;
                return Some(self.index_to_price(level_idx));
            }
        }
        None
    }

    /// Find the highest occupied price (Best Bid) in fast bit-scanning time.
    #[inline(always)]
    pub fn highest_occupied_price(&self) -> Option<Price> {
        for (word_idx, &word) in self.occupied_bitmap.iter().enumerate().rev() {
            if word != 0 {
                let lz = word.leading_zeros() as usize;
                let bit_offset = 63 - lz;
                let level_idx = word_idx * 64 + bit_offset;
                return Some(self.index_to_price(level_idx));
            }
        }
        None
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.occupied_bitmap.iter().all(|&w| w == 0)
    }

    /// Iterator yielding occupied (Price, &PriceLevelQueue) in ascending order.
    pub fn iter_ascending(&self) -> impl Iterator<Item = (Price, &PriceLevelQueue)> {
        self.occupied_bitmap
            .iter()
            .enumerate()
            .flat_map(move |(word_idx, &word)| {
                let mut w = word;
                std::iter::from_fn(move || {
                    if w == 0 {
                        None
                    } else {
                        let tz = w.trailing_zeros();
                        w &= w - 1; // Clear lowest bit
                        let level_idx = word_idx * 64 + tz as usize;
                        let price = self.index_to_price(level_idx);
                        Some((price, &self.levels[level_idx]))
                    }
                })
            })
    }

    /// Iterator yielding occupied (Price, &PriceLevelQueue) in descending order.
    pub fn iter_descending(&self) -> impl Iterator<Item = (Price, &PriceLevelQueue)> {
        let bitmap_len = self.occupied_bitmap.len();
        (0..bitmap_len).rev().flat_map(move |word_idx| {
            let mut w = self.occupied_bitmap[word_idx];
            std::iter::from_fn(move || {
                if w == 0 {
                    None
                } else {
                    let lz = w.leading_zeros();
                    let bit_offset = 63 - lz;
                    w &= !(1u64 << bit_offset); // Clear highest bit
                    let level_idx = word_idx * 64 + bit_offset as usize;
                    let price = self.index_to_price(level_idx);
                    Some((price, &self.levels[level_idx]))
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Order, OrderId, Quantity, Side};

    #[test]
    fn test_flat_tick_book_operations() {
        let mut arena = OrderArena::with_capacity(100);
        let mut flat_book = FlatTickBook::<10_000>::new(1000); // 1000 to 11000

        assert_eq!(flat_book.lowest_occupied_price(), None);
        assert_eq!(flat_book.highest_occupied_price(), None);

        // Add order at price 1050
        let o1 = Order::new_limit(OrderId(1), Side::Buy, Price(1050), Quantity(10));
        let slot1 = arena.alloc(o1).unwrap();
        flat_book.push_back(&mut arena, Price(1050), slot1).unwrap();

        // Add order at price 1080
        let o2 = Order::new_limit(OrderId(2), Side::Buy, Price(1080), Quantity(20));
        let slot2 = arena.alloc(o2).unwrap();
        flat_book.push_back(&mut arena, Price(1080), slot2).unwrap();

        // Best Bid (highest) should be 1080
        assert_eq!(flat_book.highest_occupied_price(), Some(Price(1080)));
        // Lowest occupied should be 1050
        assert_eq!(flat_book.lowest_occupied_price(), Some(Price(1050)));

        // Unlink order 2
        flat_book.unlink(&mut arena, Price(1080), slot2);
        assert_eq!(flat_book.highest_occupied_price(), Some(Price(1050)));

        // Pop order 1
        let popped = flat_book.pop_front(&mut arena, Price(1050)).unwrap();
        assert_eq!(popped, slot1);
        assert_eq!(flat_book.highest_occupied_price(), None);
        assert_eq!(flat_book.lowest_occupied_price(), None);
    }
}
