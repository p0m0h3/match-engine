# High-Performance Match Engine

An ultra-low latency, deterministic Price-Time Priority (FIFO) market matching engine written in Rust.

Designed following the single-writer architecture (LMAX Disruptor pattern) with zero heap allocations on the hot path, cache-aligned lock-free ring buffers, fixed-size binary wire protocols, Write-Ahead Logging (WAL) with atomic state snapshots, and full end-to-end idempotency.

---

## Key Features

- **Price-Time Priority (FIFO)**: Guaranteed primary price priority and secondary arrival-time ordering.
- **Sub-Microsecond Latency**:
  - Pre-allocated **contiguous slot arena** with embedded free lists for zero dynamic allocation on the hot path.
  - **Intrusive Doubly-Linked Lists** for price levels, providing true $O(1)$ unlinking for order cancellations (~700 ns) regardless of queue depth.
  - Integer arithmetic for prices and quantities (`u64` tick units, no floating-point rounding errors).
  - Fast non-cryptographic integer hashing (`rustc-hash` / `FxHashMap`).
- **Single-Writer Event Loop**:
  - Dedicated matching thread runs lock-free and non-blocking, eliminating mutex contention and cache coherency storms.
  - Thread-to-thread communication via cache-line padded (`#[repr(align(64))]`) lock-free Single-Producer Single-Consumer (SPSC) ring buffers.
- **Dual-Port Network Gateway**:
  - **Inbound Gateway (Port 9001)**: Dedicated TCP ingestion socket for Order Management Systems (OMS).
  - **Outbound Broadcaster (Port 9002)**: Dedicated TCP event stream for external sidecars/translators (e.g. Kafka bridges, market data tickers, clearing systems).
  - Zero-copy binary wire protocol (`#[repr(C)]` fixed-size frames).
- **Market Data Aggregation (L2 / Depth Feeds)**:
  - Zero-heap fixed-stack array snapshots (`L2Snapshot<const DEPTH: usize>`) generating top-20 depth in **~62 nanoseconds** (>16M snapshots/sec).
  - Compact incremental book deltas (`L2DeltaUpdate`) emitted on the outbound stream.
- **Persistence, Crash Recovery & Snapshots**:
  - Binary Write-Ahead Log (WAL) with record-level XOR checksums.
  - Atomic state checkpointing (`SnapshotManager`) dumping resting orders to disk, enabling fast restarts and safe WAL compaction.
- **End-to-End Idempotency**:
  - **Inbound**: Sliding circular deduplication window (`SlidingDedupFilter`) and active ID protection to discard retransmitted commands.
  - **Outbound**: Strict monotonic sequence IDs (`sequence_id`) enabling downstream Kafka producers and OMS replicas to achieve Exactly-Once processing.
- **Order Types & Execution Policies**:
  - `Limit` and `Market` orders.
  - Time-In-Force: `GTC` (Good 'Til Cancel), `IOC` (Immediate-Or-Cancel), `FOK` (Fill-Or-Kill), and `PostOnly` (Maker-Only).
  - First-class support for partial fills.

---

## Project Structure

```
.
├── benches/
│   ├── orderbook_bench.rs         # Criterion benchmarks for placement, matching, and cancel
│   ├── cancellation_scaling.rs    # Depth scaling benchmark (100 to 5,000 orders)
│   └── l2_depth_bench.rs          # L2 market depth snapshot benchmark (top 20 levels)
├── docs/
│   └── notes/                     # Architecture Decision Records (001 to 024)
├── src/
│   ├── arena.rs                   # Pre-allocated slot pool & intrusive doubly-linked list
│   ├── dedup.rs                   # Zero-allocation sliding deduplication filter
│   ├── engine.rs                  # Single-writer matching engine event loop
│   ├── gateway.rs                 # Dual-port TCP gateway (Inbound 9001, Outbound 9002)
│   ├── lib.rs                     # Core library exports
│   ├── main.rs                    # Executable demo server
│   ├── market_data.rs             # L2 depth aggregation structures & deltas
│   ├── orderbook.rs               # L3 order book matching core
│   ├── ring_buffer.rs             # Cache-aligned lock-free SPSC ring buffer
│   ├── snapshot.rs                # Atomic binary state snapshots & WAL compaction
│   ├── types.rs                   # Strong newtypes (OrderId, Price, Quantity, Side, Trade)
│   ├── wal.rs                     # Binary Write-Ahead Log & replay recovery
│   └── wire.rs                    # Fixed-size binary wire frames (WireCommandFrame, WireEventFrame)
└── Cargo.toml
```

---

## Getting Started

### Prerequisites

- Rust 1.80+ (tested on `1.99.1` stable).

### Build

```bash
cargo build --release
```

### Run the Demo Binary

Runs an end-to-end simulation: boots the dual-port gateway, connects a simulated Kafka translator, connects an external OMS client over TCP, executes crossing trades, and shuts down cleanly:

```bash
cargo run
```

### Run Unit & Integration Tests

```bash
cargo test
```

### Run Micro-Benchmarks

Run the Criterion benchmark suite to measure latency and throughput:

```bash
# General order book operations
cargo bench --bench orderbook_bench

# Order cancellation latency across price queue depths (100 -> 5,000 orders)
cargo bench --bench cancellation_scaling

# Top-20 L2 market depth snapshot latency
cargo bench --bench l2_depth_bench
```

---

## Wire Protocol Reference

### Inbound Command Frame (`WireCommandFrame`, 40 bytes)
Sent by the OMS to port `9001`:

| Offset | Field | Type | Description |
|---|---|---|---|
| 0 | `order_id` | `u64` | Unique order identifier |
| 8 | `price` | `u64` | Limit price in tick units |
| 16 | `quantity` | `u64` | Order size in lot units |
| 24 | `correlation_id` | `u64` | Client tracking ID echoed in responses |
| 32 | `msg_type` | `u8` | `1` = PlaceOrder, `2` = CancelOrder, `255` = Shutdown |
| 33 | `side` | `u8` | `0` = Buy, `1` = Sell |
| 34 | `order_type` | `u8` | `0` = Limit, `1` = Market |
| 35 | `time_in_force` | `u8` | `0` = GTC, `1` = IOC, `2` = FOK, `3` = PostOnly |
| 36 | `_padding` | `[u8; 4]` | 8-byte natural alignment |

### Outbound Event Frame (`WireEventFrame`, 56 bytes)
Streamed to connected translators/subscribers on port `9002`:

| Offset | Field | Type | Description |
|---|---|---|---|
| 0 | `sequence_id` | `u64` | Monotonically increasing sequence number |
| 8 | `timestamp_ns` | `u64` | Hardware nanosecond timestamp |
| 16 | `order_id` | `u64` | Primary order ID (or taker ID for trades) |
| 24 | `maker_order_id` | `u64` | Resting maker order ID (or side for L2 updates) |
| 32 | `price` | `u64` | Execution / order price in ticks |
| 40 | `quantity` | `u64` | Executed quantity / new total quantity |
| 48 | `leaves_quantity` | `u64` | Remaining unfilled quantity |
| 56 | `event_type` | `u8` | `1` = Accepted, `2` = Trade, `3` = Cancelled, `4` = Rejected, `5` = L2Update, `255` = Shutdown |
| 57 | `_padding` | `[u8; 7]` | 8-byte natural alignment |
