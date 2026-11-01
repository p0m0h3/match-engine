# 009. Benchmarking Strategy

## Decisions Made
1. **Criterion.rs for Micro-Benchmarking**:
   - Establish baseline latency and throughput measurements before optimizing data structures.
2. **Three Core Micro-Benchmark Scenarios**:
   - **Passive Order Insertion**: Measures overhead of price-level lookup + queue insertion without matches.
   - **Crossing Matches (Aggressive Limit Orders)**: Measures the execution loop, trade generation, and queue pop latency.
   - **Order Cancellation**: Measures ID lookup and removal from price levels (will expose the $O(N)$ linear scan of `VecDeque`).
3. **Black Box Optimization Prevention**:
   - Use `std::hint::black_box` to prevent LLVM compiler from optimizing away order placement and state transitions during benchmark loops.
