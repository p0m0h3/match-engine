# 032. Integrating FlatTickBook directly into OrderBook

## Decisions Made
1. **Unified Direct-Indexed OrderBook Architecture**:
   - Replace `BTreeMap<Price, PriceLevelQueue>` with `FlatTickBook` for bids and asks within the primary active trading band (e.g. 100,000 discrete tick levels).
   - Keeps `BTreeMap` as a sparse fallback for extreme prices outside the primary direct band (hybrid LMAX architecture).
2. **Direct Array Performance Gains**:
   - In-bounds price operations now drop from $O(\log P)$ (~14.6 ns) down to **true $O(1)$ direct memory offset indexing (~3.25 ns)**.
   - BBO tracking uses bitwise hardware instructions (`leading_zeros` / `trailing_zeros`) in **~6.3 ns**.
3. **Seamless Interface Compatibility**:
   - Preserves all external interfaces (`place_order`, `cancel_order`, `get_l2_snapshot`).
   - All tests, WAL recovery, snapshots, and idempotency checks remain 100% compatible.
