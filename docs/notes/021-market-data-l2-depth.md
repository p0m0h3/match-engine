# 021. L2 Market Data Aggregation & Incremental Feeds

## Decisions Made
1. **Separation of L2 Snapshots vs. Incremental Deltas**:
   - **L2 Snapshot**: Complete picture of the top $N$ price levels (e.g. top 20 bids and asks) with total aggregated volume at each level.
   - **L2 Incremental Delta**: Lightest possible wire update (`price`, `side`, `new_quantity`). Emitted only when the aggregate volume at a price level changes, an order rests, or a level is exhausted (`new_quantity == 0`).
2. **Deterministic Sequence Synchronization**:
   - Downstream consumers (e.g. WebSocket ticker or market-making bot) take an initial snapshot with sequence $S_0$, and apply all incoming deltas where $S > S_0$.
3. **Zero Dynamic Allocation on Depth Generation**:
   - Top-$N$ depth snapshots use fixed-size stack arrays:
     ```rust
     #[repr(C)]
     pub struct DepthSnapshot<const DEPTH: usize> {
         pub sequence_id: u64,
         pub bids: [(u64, u64); DEPTH], // (price, total_quantity)
         pub asks: [(u64, u64); DEPTH],
         pub bid_count: usize,
         pub ask_count: usize,
     }
     ```
   - No vector reallocations on the event stream.
4. **Outbound Stream Integration**:
   - Deltas and snapshots are assigned standard sequence IDs and emitted across the Outbound Broadcaster (port 9002).
