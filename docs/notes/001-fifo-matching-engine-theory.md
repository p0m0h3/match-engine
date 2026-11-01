# 001. FIFO Matching Strategy

## Decisions Made
1. **Price-Time Priority (FIFO) Execution Model**:
   - Primary: Best price (highest bid, lowest ask).
   - Secondary: Arrival time at that price.
2. **O(1) Hot Path Operations**:
   - Order placement, matching, and cancellation must target $O(1)$ algorithmic complexity.
3. **No Floats (Integer Arithmetic)**:
   - Price and Quantity represented as integers (`u64`) to eliminate IEEE-754 precision and rounding bugs.
4. **Zero-Allocation Target**:
   - Hot path must avoid heap allocations (`malloc`/`free`) to eliminate latency jitter. Pre-allocated arenas/slabs will be used.
