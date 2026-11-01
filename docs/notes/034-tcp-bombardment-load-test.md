# 034. TCP Bombardment Load Test & Gateway Streaming Optimizations

## Context
A market matching engine must withstand sustained, multi-client high-volume bombardment over network sockets without dropping packets, stalling the single-writer matching thread, or experiencing queue starvation.

Under preliminary tests, running massive TCP traffic revealed two network-layer bottlenecks in the gateway:
1. **Unbuffered Socket Syscalls**: Ingesting raw 40-byte `WireCommandFrame` and emitting 56-byte `WireEventFrame` one-by-one results in one OS syscall per frame. At 500,000 orders, this generates 1,000,000 context switches between user and kernel space.
2. **Coarse-Grained Broadcaster Sleep**: The outbound broadcaster thread called `thread::sleep(1ms)` on queue empty (`try_pop() == None`). Whenever the matching engine momentarily caught up, the broadcaster slept for 1,000,000 ns, stalling outbound event drainage and backpressuring the matching engine ring buffer.

## Key Decisions

1. **Buffered Socket I/O (`BufReader` / `BufWriter`)**:
   - Wrap TCP streams in `BufReader` with 64 KB user-space buffers.
   - A single kernel `read` fetches up to 1,638 command frames directly into user memory, reducing syscall overhead by up to 99%.

2. **Adaptive Spin-Yield Drainage in Broadcaster**:
   - Replace unconditional 1 ms sleeps in the broadcaster event consumer with an adaptive spin-yield strategy:
     - Spin with `core::hint::spin_loop()` for immediate bursts.
     - Yield thread execution (`thread::yield_now()`) under moderate idle.
     - Minimal sleep (1 ms) only under prolonged inactivity (> 1,000 consecutive idle loops).

3. **Explicit Blocking Mode on Accepted Streams (`stream.set_nonblocking(false)`)**:
   - On Unix/macOS, sockets accepted from a non-blocking `TcpListener` inherit non-blocking status.
   - Calling `read_exact` on a non-blocking stream causes partial-read drops whenever `WouldBlock` is encountered mid-frame, permanently desynchronizing the 40-byte wire frame boundary.
   - Calling `stream.set_nonblocking(false)` on accepted streams guarantees atomic, framed blocking reads without frame corruption.

4. **Dedicated Bombardment Client Binary (`src/bin/bombardment_client.rs`)**:
   - **Workload Mix**:
     - 40% Resting Limit Orders (building bid/ask depth across price tiers).
     - 35% Aggressive Crossing Orders (triggering immediate trades and partial fills).
     - 15% Cancellations (testing $O(1)$ intrusive queue unlinking under load).
     - 10% Modifications (testing atomic in-place quantity reductions and price changes).
   - **Full Outbound Sequence Verification**: Dedicated background receiver thread reading Port 9002, verifying strictly monotonic sequence IDs ($1, 2, 3, \dots, N$) and zero dropped events.
   - **Metrics Tracked**: Ingestion throughput (orders/sec), egress throughput (events/sec), total execution time, and event breakdown.

## Verification & 1,000,000 Order Benchmark Results

Measured over real TCP loopback (`127.0.0.1:19001` Ingest $\rightarrow$ Single-Writer Match Core $\rightarrow$ `127.0.0.1:19002` Egress):

```text
=== Bombardment Results (1000000 Orders) ===
 Total Duration:          4.435 s (4435.22 ms)
 Ingestion Rate (Orders):     225,468 orders/sec
 Egress Rate (Events):        306,019 events/sec
 Total Events Emitted:       1,357,261
 Last Sequence ID:           1,357,261
 Sequence Errors:                  0 ✓ (Strictly Monotonic)
-------------------------------------------------------
 Event Breakdown:
   - OrderAccepted:           634,584
   - TradeExecuted:           357,260
   - OrderCancelled:          119,103
   - OrderModified:            96,010
   - OrderRejected:           150,303
=======================================================
 ✓ Bombardment Load Test Passed Successfully.
```

