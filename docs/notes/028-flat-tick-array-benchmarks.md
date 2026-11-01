# 028. Direct-Indexed Tick Array Benchmark Comparison

## Micro-Benchmark Results: `BTreeMap` vs. `FlatTickBook`

Tested across identical price space (1,000 discrete price levels):

| Benchmark Scenario | BTreeMap | FlatTickBook (Direct Index) | Speedup Factor |
|---|---|---|---|
| **Price Level Lookup** | 14.65 ns | **3.25 ns** | **~4.5x Faster** |
| **Best Bid/Ask Search (BBO Scan)** | ~15–25 ns (first_entry) | **6.30 ns (Bitwise CLZ/CTZ)** | **~3x Faster** |

## Architectural Analysis
1. **Zero Pointer Chasing**:
   - `BTreeMap` navigates child pointers and performs binary searches inside nodes, costing ~14.6 ns.
   - `FlatTickBook` performs an offset subtraction (`price - min_price`) and direct memory read: **3.25 ns** (single CPU L1 cache line hit).
2. **Instant BBO via CPU Instructions**:
   - Scanning for the best bid/ask uses `trailing_zeros()` / `leading_zeros()`, mapping directly to single hardware instructions (`rbit` / `clz` on ARM64, `bsf` / `tzcnt` / `lzcnt` on x86_64).
   - Scanning 2,000 price levels takes **6.30 nanoseconds**.
