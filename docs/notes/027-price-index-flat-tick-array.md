# 027. Price Index Optimization: Direct-Indexed Flat Tick Array vs Radix

## Decisions Made
1. **The Cost of `BTreeMap` on Price Lookups**:
   - `BTreeMap<Price, PriceLevelQueue>` incurs pointer chasing across tree nodes and $O(\log P)$ search overhead.
   - For real-world markets, prices move within a bounded active band around the fair price (the "trading collar" or circuit breaker band).
2. **Direct-Indexed Flat Tick Array (LMAX-style)**:
   - For bounded tick spaces or clustered price bands, price indexing can be a flat contiguous array:
     $$\text{Index} = \text{price} - \text{min\_price}$$
   - **True $O(1)$ Direct Addressing**: Reading or mutating a price level is a single direct array offset lookup in CPU cache with zero pointer chasing.
   - **Fast Best-Bid/Ask Tracking via Bitmaps / Sparse Head Pointer**:
     - Maintain a bitset or cursor tracking the lowest active ask and highest active bid.
     - Finding the best price is a bitwise scan or direct pointer read ($O(1)$).
3. **Hybrid Fallback**:
   - For orders outside the direct array window (rare extreme tail prices), the engine can fall back to the sparse tree or reject off-market limit orders.
4. **Target Improvement**:
   - Drop the remaining 50–60 ns insertion down to < 20 ns, matching the flat array direct memory access speed.
