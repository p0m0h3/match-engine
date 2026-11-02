# Future Work & Recommended Features

This document outlines recommended improvements and feature additions for `match-engine`,
organized by priority. Items at the top of each section have the highest return on
investment relative to effort.

---

## 1. Correctness & Safety (Do First)

### 1.1 Fix Known Review Issues
Resolve all findings from `code-review.md` before any feature work. The three critical
items (endian safety, `OrderId(0)` dedup sentinel, WAL sequence reset) are prerequisites
for a production deployment.

### 1.2 Fuzz Testing with `cargo-fuzz`
The wire deserialization path (`WireCommandFrame::to_engine_command`,
`WalRecord::to_command`) accepts arbitrary bytes from the network and disk. A fuzzer will
find panics, unexpected rejects, and edge cases that unit tests miss.

```
fuzz_targets/
  fuzz_wire_command.rs    -- feed random 40-byte frames
  fuzz_wal_record.rs      -- feed random WAL record bytes
  fuzz_orderbook.rs       -- feed random valid Order sequences
```

### 1.3 Miri / Sanitizer CI Pass
Run `cargo miri test` and `cargo test` under AddressSanitizer in CI to catch undefined
behavior in the `unsafe` blocks in `arena.rs`, `wire.rs`, and `snapshot.rs`.

---

## 2. Multi-Instrument Support

### 2.1 Instrument Registry
The engine currently owns a single `OrderBook`. Wrap it in an `InstrumentRegistry`:

```rust
pub struct InstrumentRegistry {
    books: FxHashMap<InstrumentId, OrderBook>,
}
```

- Add an `instrument_id: u32` field to `WireCommandFrame` (fits in existing `_padding`).
- Route commands to the correct book in the engine loop.
- Emit `InstrumentId` on all outbound events.

### 2.2 Per-Instrument Configuration
Allow each instrument to carry its own:
- Tick size (price increment granularity)
- Lot size (minimum order quantity)
- Price band (flat book range, e.g. `FlatTickBook<50_000>`)
- Circuit-breaker thresholds

---

## 3. Order Book Enhancements

### 3.1 Iceberg / Reserve Orders
Support disclosed + hidden quantity on a single order. The resting quantity shown in the
L2 feed is the `disclosed_qty`; on fill the order is automatically replenished from
`reserve_qty` and re-inserted at the back of the queue.

```rust
pub struct Order {
    // existing fields ...
    pub disclosed_quantity: Quantity,   // visible portion
    pub reserve_quantity: Quantity,     // hidden portion
}
```

### 3.2 Stop Orders
Triggered when the last trade price crosses the stop price. Requires a secondary sorted
data structure (a `BTreeMap<Price, Vec<OrderId>>`) monitored after every trade event.
Stop-limit and stop-market variants.

### 3.3 Pegged / Mid-Price Orders
An order whose price tracks the BBO mid-point. Requires a re-pricing pass whenever the
BBO changes. Useful for market-maker strategies.

### 3.4 Minimum Quantity (MinQty) Constraint
Reject or IOC-cancel any order that cannot immediately fill at least `min_qty` lots.
Common in institutional venues.

### 3.5 Self-Trade Prevention (STP)
Prevent a participant's own resting order from matching against their own incoming order.
Add a `firm_id: u32` field to `Order` and check it before executing a trade.

```rust
if resting_order.firm_id == incoming_order.firm_id {
    // STP: cancel resting, reject taker, or cancel both — configurable per mode
}
```

---

## 4. Market Data

### 4.1 Full Depth (Level 3) Feed
In addition to the existing L2 aggregated feed, publish individual order events
(New / Modify / Cancel / Fill) so subscribers can reconstruct a full order-by-order book.
This is the `EngineEvent` stream already — it just needs to be formally specified and
documented as an L3 feed.

### 4.2 VWAP & Trade Statistics
Maintain rolling statistics in the engine loop with zero extra allocation:
- Last trade price, high/low of session
- VWAP (volume-weighted average price) updated on every trade
- Total volume traded

Emit these as a periodic `StatisticsSnapshot` event on the outbound stream.

### 4.3 Real-Time L2 Delta Updates
`L2DeltaUpdate` exists in `market_data.rs` and `EngineEvent::L2Update` is defined, but
the engine never emits it. Wire it up so every price-level change fires a delta event,
enabling subscribers to maintain a synchronized L2 book without polling snapshots.

### 4.4 Configurable Snapshot Interval
Add a `--snapshot-interval-secs` flag to the daemon. Periodically save a snapshot and
truncate the WAL to keep disk usage bounded.

---

## 5. Persistence & Recovery

### 5.1 WAL Compaction & Truncation
After a snapshot is saved, delete WAL records older than `last_snapshot_seq`. This keeps
the WAL from growing unboundedly during long sessions.

### 5.2 Ordered WAL Replay with Dedup
During WAL replay, pipe commands through the `SlidingDedupFilter` so a crash during a
cancel-replace does not double-cancel on recovery.

### 5.3 Atomic Snapshot + WAL Rotation
Ensure the sequence: `flush WAL → save snapshot → rotate WAL` is atomic from a crash
perspective (e.g., write a `CHECKPOINT` record to the WAL after the snapshot succeeds).

### 5.4 Distributed Replication (Raft / Standalone Follower)
For high availability, stream WAL records to a standby instance over a dedicated
replication channel. The standby replays commands in order and can take over with minimal
gap on primary failure.

---

## 6. Networking & Protocol

### 6.1 Multi-Client Inbound Gateway
Replace the single-connection inbound loop with a thread-per-client or `io_uring`-based
approach so multiple OMS connections can submit orders concurrently. The engine loop
remains single-writer — the gateway threads are the serialization point.

### 6.2 UDP Multicast Market Data
For low-latency L2/L3 broadcast to many subscribers, emit `WireEventFrame` packets over
UDP multicast in addition to (or instead of) the TCP fan-out. Include sequence numbers
and a gap-fill mechanism for recovery.

### 6.3 QUIC Transport Option
Replace TCP with QUIC (via `quinn`) for the inbound channel to get built-in TLS,
multiplexing, and connection migration without head-of-line blocking.

### 6.4 TLS / mTLS on Inbound Port
Add mutual TLS authentication so only authorized OMS clients can submit orders. Use
`rustls` to avoid a C dependency.

### 6.5 FIX Protocol Gateway
Expose an optional FIX 4.2/4.4 adapter as a separate binary that translates FIX
`NewOrderSingle` (D), `OrderCancelRequest` (F), and `OrderCancelReplaceRequest` (G)
messages into `WireCommandFrame` packets. This enables connectivity with standard
institutional OMS systems.

---

## 7. Observability

### 7.1 Prometheus Metrics Endpoint
Expose key metrics over HTTP for scraping by Prometheus / Grafana:

| Metric | Type |
|---|---|
| `engine_orders_placed_total` | Counter |
| `engine_trades_executed_total` | Counter |
| `engine_orders_resting` | Gauge |
| `engine_tick_to_trade_ns_p99` | Histogram |
| `engine_ring_buffer_utilization` | Gauge |
| `engine_arena_utilization` | Gauge |

Use a lock-free `AtomicU64` counter approach to avoid any contention on the hot path.

### 7.2 Structured Logging
Replace `println!` calls with `tracing` spans and events. Use `tracing-subscriber` with
JSON formatting for log aggregation pipelines (Loki, Elasticsearch).

### 7.3 OpenTelemetry Distributed Tracing
Instrument the order lifecycle (receive → match → emit) with OTLP spans so latency
outliers can be traced end-to-end across the OMS → engine → Kafka path.

---

## 8. Performance

### 8.1 CPU Core Pinning
Pin the engine thread and each gateway thread to dedicated CPU cores using `libc::sched_setaffinity`
(Linux) to eliminate scheduler jitter from the hot path.

```rust
fn pin_to_core(core_id: usize) {
    // libc::sched_setaffinity(0, &cpu_set)
}
```

### 8.2 NUMA-Aware Memory Allocation
Pre-allocate the `OrderArena` on the same NUMA node as the pinned engine core using
`libnuma` to avoid cross-node memory access latency.

### 8.3 Huge Pages for Arena
Use `MAP_HUGETLB` (Linux) or `mmap` with huge page alignment for the arena `Vec` backing
store to reduce TLB pressure when the arena is large (>2 MB).

### 8.4 Kernel Bypass Networking (DPDK / io_uring)
For sub-microsecond tick-to-trade targets, replace the kernel TCP stack with:
- **`io_uring`** (Linux 5.6+) for batched, kernel-bypassed I/O with the existing TCP socket
- **DPDK / RDMA** for full kernel bypass on supported NICs

### 8.5 Batch Command Processing
Process up to N commands per engine loop iteration before emitting events, reducing the
per-command overhead of the event-producer spin:

```rust
let mut batch_count = 0;
while batch_count < MAX_BATCH && running.load(Relaxed) {
    if let Some(cmd) = cmd_consumer.try_pop() {
        process(cmd);
        batch_count += 1;
    } else { break; }
}
emit_pending_events();
```

---

## 9. Developer Experience

### 9.1 CLI Configuration File
Replace hard-coded ports and capacities in `main.rs` with a TOML configuration file:

```toml
[network]
inbound_port  = 9001
outbound_port = 9002

[engine]
arena_capacity   = 100_000
ring_buffer_size = 65_536

[persistence]
wal_dir              = "./data/wal"
snapshot_dir         = "./data/snapshots"
snapshot_interval_s  = 300
```

Parse with `serde` + `toml`.

### 9.2 `cargo xtask` Build Automation
Add an `xtask` crate for common development workflows:

```
cargo xtask bench       -- run all benchmarks and diff against baseline
cargo xtask flamegraph  -- profile the demo simulation with cargo-flamegraph
cargo xtask fuzz        -- run all fuzz targets for 60 seconds
cargo xtask test-all    -- run tests + clippy + miri
```

### 9.3 Replay Tool (`cargo run --bin replay`)
A standalone binary that reads a WAL file and prints a human-readable log of every
command, useful for debugging and auditing:

```
$ cargo run --bin replay -- --wal ./data/wal/engine.wal
[seq=1  ts=1000ns] PlaceOrder  id=1  side=Sell  price=100  qty=50  GTC
[seq=2  ts=2000ns] PlaceOrder  id=2  side=Buy   price=90   qty=30  GTC
[seq=3  ts=3000ns] CancelOrder id=1
```

### 9.4 Docker / Compose Setup
Add a `Dockerfile` (multi-stage build: `rust:slim` builder → `debian:slim` runtime) and
a `docker-compose.yml` that spins up the engine + a Kafka broker + the `kafka_adapter`
with a single `docker compose up`.

---

## Priority Roadmap

```
Phase 1 — Hardening (now)
  ├── Fix all code-review critical items
  ├── Add fuzz targets
  └── Wire up L2 delta events

Phase 2 — Observability & Ops (1–2 months)
  ├── Prometheus metrics
  ├── Structured logging (tracing)
  ├── CLI config file
  └── WAL compaction + snapshot rotation

Phase 3 — Multi-Instrument (2–4 months)
  ├── InstrumentRegistry
  ├── Per-instrument config
  └── Iceberg & Stop orders

Phase 4 — Connectivity (4–6 months)
  ├── FIX protocol gateway
  ├── Multi-client inbound
  ├── UDP multicast market data
  └── mTLS on inbound port

Phase 5 — Ultra-Low Latency (ongoing)
  ├── CPU core pinning
  ├── Huge-page arena
  └── io_uring / DPDK networking
```
