# 010. Baseline Benchmark Results

## Benchmark Setup
- **Tool**: Criterion.rs (`0.5`)
- **Hardware**: Apple Silicon (aarch64-apple-darwin)
- **Baseline Implementation**: Standard library collections (`BTreeMap` + `VecDeque` + `HashMap`).

## Baseline Results

| Scenario | Batch Size | Total Batch Time | Per-Operation Latency | Throughput |
|---|---|---|---|---|
| **Passive Limit Order Placement** | 1,000 orders across 50 price levels | **~60.25 µs** | **~60.2 ns / order** | ~16.6 Million orders/sec |
| **Aggressive Crossing Matches** | 200 crossing orders hitting resting asks | **~13.05 µs** | **~65.2 ns / match** | ~15.3 Million matches/sec |
| **Order Cancellation** | 500 orders cancelled from queues | **~14.88 µs** | **~29.7 ns / cancel** | ~33.6 Million cancels/sec |

## Observations & Next Optimization Targets
1. **Outliers & Jitter**: Noticeable high-severe outliers (12% in insertion, 6% in matching/cancel) caused by memory reallocations inside `HashMap` and `BTreeMap`.
2. **Deep Queue Degradation**: In the cancellation test with shallow price queues (2,000 orders across 10 levels = ~200 orders/level), `VecDeque::remove(pos)` is acceptable, but degrades as $O(N)$ with deeper books.
3. **Goal for Arena + Intrusive List**:
   - Eliminate heap allocations completely on the hot path (0 outliers / 0 jitter).
   - Guarantee deterministic $O(1)$ unlinking for cancellations regardless of book depth.
