# 003. Project Structure & Packaging

## Decisions Made
1. **Dual Library + Binary Structure**:
   - `src/lib.rs`: Exposes the pure, deterministic matching engine logic for zero-dependency embedding, unit testing, and benchmarks.
   - `src/main.rs`: The executable server daemon responsible for core pinning, I/O channels, and driving the event loop.
2. **Why**:
   - Allows fuzzing, unit-testing, and micro-benchmarking without spawning network stacks or runtime daemons.
