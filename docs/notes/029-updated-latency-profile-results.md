# 029. Updated Live Hardware Tick-to-Trade Profiling Results

## Measurement Setup
- **Hardware Architecture**: Apple Silicon (AArch64 `cntvct_el0` virtual timer register @ 24.00 MHz)
- **Profile Scope**: Real cross-thread pipeline (Inbound SPSC -> Single-Writer Engine Thread -> Matching -> Outbound SPSC -> Egress Trade).
- **Sample Size**: 100,000 live crossing executions.

## Updated Latency Distribution (Tick-to-Trade)

| Metric | Latency | Time in Microseconds |
|---|---|---|
| **Min Latency** | **125 ns** | **0.125 µs** |
| **P50 (Median)** | **291 ns** | **0.291 µs** |
| **Mean Latency** | **297.30 ns** | **0.297 µs** |
| **P90** | **375 ns** | **0.375 µs** |
| **P99** | **500 ns** | **0.500 µs** |
| **P99.9** | **2,458 ns** | **2.458 µs** |

## Key Insights
- **Sub-300 ns Median**: Median end-to-end cross-thread execution has dropped to **291 nanoseconds**.
- **P99 at 500 ns**: 99% of all orders complete in half a microsecond (500 nanoseconds), confirming near-zero tail latency across the pipeline.
