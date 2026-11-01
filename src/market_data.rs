//! Market Data structures and zero-allocation L2 depth aggregation.

use crate::types::{Price, Quantity, Side};

/// An aggregated price level in an L2 depth feed.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DepthLevel {
    pub price: Price,
    pub total_quantity: Quantity,
}

impl DepthLevel {
    pub const fn new(price: Price, total_quantity: Quantity) -> Self {
        Self {
            price,
            total_quantity,
        }
    }
}

/// An incremental L2 delta update emitted when a price level's aggregated volume changes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2DeltaUpdate {
    pub side: Side,
    pub price: Price,
    /// If total_quantity == 0, this price level is exhausted/deleted.
    pub new_total_quantity: Quantity,
}

/// A zero-allocation fixed-size L2 snapshot of the top N price levels.
#[derive(Debug, Clone, Copy)]
pub struct L2Snapshot<const DEPTH: usize> {
    pub bids: [DepthLevel; DEPTH],
    pub asks: [DepthLevel; DEPTH],
    pub bid_count: usize,
    pub ask_count: usize,
}

impl<const DEPTH: usize> Default for L2Snapshot<DEPTH> {
    fn default() -> Self {
        Self {
            bids: [DepthLevel::default(); DEPTH],
            asks: [DepthLevel::default(); DEPTH],
            bid_count: 0,
            ask_count: 0,
        }
    }
}
