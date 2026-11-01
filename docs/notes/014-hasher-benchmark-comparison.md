# 014. Hasher Benchmark Comparison

## Impact of Switching from `std::collections::HashMap` (SipHash) to `rustc_hash::FxHashMap`

### Criterion Benchmark Deltas:

| Benchmark Scenario | Standard HashMap (SipHash) | FxHashMap | Improvement |
|---|---|---|---|
| **Passive Limit Order Placement** | 62.01 µs | **51.41 µs** | **~22% Faster** |
| **Order Cancellation (500 cancels)** | 46.30 µs | **32.66 µs** | **~29% Faster** |
| **Aggressive Crossing Matches** | 22.85 µs | **21.17 µs** | **~9.5% Faster** |

### Queue Depth Cancellation Scaling:
- Depth 100: **757 ns**
- Depth 500: **710 ns**
- Depth 2,000: **699 ns**
- Depth 5,000: **661 ns**

### Conclusion:
- Dropping cryptographic hashing overhead directly yielded an immediate **22% to 29% performance boost** across order insertions and cancellations without altering any business logic.
