# 013. Hasher & Map Optimization Strategy

## Decisions Made
1. **Replace Default `RandomState` (SipHash-1-3)**:
   - Rust standard library `HashMap` defaults to SipHash to prevent HashDoS attacks in untrusted web servers.
   - SipHash executes rounds of bitwise rotations and XOR operations per 64-bit integer, costing ~10–20 ns per lookup.
   - In a private, single-writer matching engine, HashDoS is not a threat.
2. **Adopt FxHash (`rustc-hash`)**:
   - `fxhash` (the hasher used internally by the `rustc` compiler) hashes 64-bit integers (`OrderId(u64)`) with a single multiplication and bit shift ($O(1)$ single-cycle ALU operation):
     ```rust
     hash = hash.rotate_left(5) ^ key.wrapping_mul(K);
     ```
   - Drops integer hashing overhead from ~15 ns down to < 1 ns.
3. **Drop-In Usage**:
   - Use `rustc_hash::FxHashMap` for `order_locations` (`OrderId -> (Price, Side, SlotIdx)`).
