# Code Review: `match-engine`

> All 19 tests pass. `cargo clippy` is clean. The project is in excellent shape overall.

---

## Architecture Overview

```
OMS / Client (TCP :9001)
        │ WireCommandFrame (40 B)
        ▼
Inbound Gateway  ──SPSC Ring Buffer──▶  Engine Loop  ──SPSC Ring Buffer──▶  Outbound Broadcaster
                                              │                                       │ WireEventFrame (56 B)
                                              ├──▶ OrderBook                          ▼
                                              │    (FlatTickBook + Arena + BTreeMap)  Kafka Adapter / Subscribers (TCP :9002)
                                              ├──▶ WAL Writer
                                              └──▶ Dedup Filter
```

The engine follows a **single-writer / LMAX-Disruptor-style** pattern: one thread owns
the order book and communicates via zero-lock SPSC channels. The layering is clean:

```
types → arena → flat_book → orderbook → engine → gateway
```

---

## What's Done Well

### Data Structures & Matching Core

- **FlatTickBook** (`flat_book.rs`) — direct-indexed `Vec<PriceLevelQueue>` with a
  **64-bit occupied bitmap** for O(1) best-bid/best-ask scanning via `trailing_zeros` /
  `leading_zeros`. Elegant and cache-friendly.
- **OrderArena** (`arena.rs`) — pre-allocated slab with an **embedded free-list**. Zero
  heap allocation during the hot path. `get` / `get_mut` use `get_unchecked` guarded by
  the free-list invariant.
- **Intrusive doubly-linked list** (`PriceLevelQueue`) — O(1) cancel anywhere in the
  queue using `unlink`. All three pointers (`head`, `tail`, `prev/next`) stored inline in
  the arena node — no pointer chasing through the allocator.
- **Hybrid price-level storage** — `FlatTickBook<10_000>` for the primary band,
  `BTreeMap` sparse fallback for extreme prices. A sensible engineering tradeoff.
- **FIFO priority semantics** are correct: decrease-in-quantity stays in place;
  increase-in-quantity or price-change does a cancel-replace.

### Order Types & TIF

- GTC, IOC, FOK, Post-Only all correctly implemented.
- FOK uses `simulated_available_liquidity` before touching any state — atomic reject with
  no partial side effects.
- Post-Only cross-check is a simple pre-flight against BBO.

### SPSC Ring Buffer (`ring_buffer.rs`)

- Cache-line-padded atomic indices prevent **false sharing** between producer and consumer
  cores.
- Cached-head / cached-tail optimization reduces atomic loads on the hot path.
- Power-of-two check + bitmask wrapping is correct.
- `UnsafeCell<Option<T>>` is the right primitive for this; `Send` impls are justified.

### Persistence Layer

- **WAL** (`wal.rs`) — magic header, per-record XOR checksum, buffered writes.
  Append-only, zero-copy (raw struct bytes via `slice::from_raw_parts`).
- **Snapshot** (`snapshot.rs`) — write-to-tmp then `fs::rename` for **atomic
  crash-safe saves**. Checksum validated on load. Snapshot + WAL replay gives correct
  recovery.

### Gateway & Protocol

- Fixed-size binary frames (40 B in, 56 B out) — no framing overhead, trivially
  `memcpy`-able.
- Adaptive idle strategy in the broadcaster: spin → yield → sleep, a good tradeoff
  between latency and CPU burn when idle.
- `BufReader`/`BufWriter` with 64 KB buffers avoids per-frame syscalls.

### Tests & Coverage

- 19 tests covering: FIFO priority, cancel O(1), L2 aggregation, FOK/PostOnly rejection,
  WAL round-trip, snapshot round-trip, SPSC correctness, full TCP integration, client SDK,
  and dedup filter eviction.

---

## Issues & Recommendations

### 🔴 Critical / Correctness

#### 1. Unsound `unsafe` in wire serialization (endian + alignment UB)

**Files:** `src/wire.rs:136–142`, `src/snapshot.rs:67–80`, `src/wal.rs:180–188`

```rust
// Current — UB on big-endian hosts; layout depends on compiler padding
pub fn as_bytes(&self) -> &[u8] {
    unsafe { std::slice::from_raw_parts((self as *const Self) as *const u8, Self::SIZE) }
}
```

`#[repr(C)]` fixes field order but **not** byte order. On a big-endian host the wire
format changes silently. More practically, this makes WAL and snapshot files
**non-portable** across architectures and Rust compiler versions.

**Recommendation:** Either:

- Document this as "little-endian only" with a compile-time `cfg` assert:
  ```rust
  const _: () = assert!(cfg!(target_endian = "little"), "Wire protocol assumes little-endian");
  ```
- Or replace with explicit `u64::to_le_bytes()` field serialization for true portability.

---

#### 2. Dedup filter silently drops `OrderId(0)` records

**File:** `src/dedup.rs:37–39`

```rust
let old_id = self.ring[self.head];
if old_id != 0 {          // ← OrderId(0) can never be evicted from the set
    self.set.remove(&old_id);
}
```

If `OrderId(0)` is ever used (it is the default in arena pre-population), it is recorded
but never evicted. The `HashSet` grows without bound for that slot. Use `Option<u64>` or
`u64::MAX` as the sentinel, or reject `OrderId(0)` at the gateway.

```rust
// Fix: use Option sentinel
ring: vec![None::<u64>; capacity],
// evict:
if let Some(old) = self.ring[self.head] {
    self.set.remove(&old);
}
self.ring[self.head] = Some(id.0);
```

---

#### 3. `WalWriter::open` resets sequence counter on re-open

**File:** `src/wal.rs:155–169`

```rust
Ok(Self { writer, current_seq: 0 })
```

When the engine restarts and re-opens an existing WAL file for append, `current_seq`
resets to 0. New records will be numbered starting at 1 again, colliding with existing
sequence IDs. The WAL reader will not detect this because the checksum includes the now-
duplicated sequence number.

**Recommendation:** After opening an existing file, scan to find `max(sequence_id)` and
initialize `current_seq` from it:

```rust
// After open, if file_exists: read last WalRecord and set current_seq = last.sequence_id
```

---

### 🟡 Design / Robustness

#### 4. Gateway handles only one inbound client at a time

**File:** `src/gateway.rs:28–56`

The inbound gateway uses a sequential accept loop — when one OMS client disconnects it
blocks on `listener.accept()` again but cannot handle a second OMS while the first is
connected. This is a valid design for a single-venue engine, but it is not documented.
A reconnecting OMS will silently stall while the previous connection is in the blocked
inner loop.

**Recommendation:** Add a comment documenting the single-client constraint, and consider
breaking out of the inner loop on any I/O error so the listener can re-accept promptly.

---

#### 5. FOK liquidity scan ignores the sparse `BTreeMap` book

**File:** `src/orderbook.rs:534–559`

`simulated_available_liquidity` only iterates `flat_asks` / `flat_bids` — the sparse
`BTreeMap` fallback is never scanned:

```rust
fn simulated_available_liquidity(...) -> u64 {
    for (price, queue) in self.flat_asks.iter_ascending() { ... }
    // ← sparse_asks not scanned!
}
```

A FOK buy order with a limit price above `flat_asks.max_price` will see `available = 0`
even if matching liquidity exists in the sparse book, causing an incorrect rejection.

**Recommendation:** After the flat loop, also accumulate from the corresponding
`BTreeMap`:

```rust
// After flat loop for Sell side:
for (price, queue) in &self.sparse_asks {
    if order_type == OrderType::Limit && *price > limit_price { break; }
    total += queue.total_quantity;
}
```

---

#### 6. Flat/sparse boundary crossing needs a targeted integration test

**File:** `src/orderbook.rs:160–252`

The outer matching loop calls `best_ask()` (which correctly combines flat + sparse), then
the inner check `flat_asks.in_bounds(best_ask)` routes to the flat or sparse path. A
test that places resting orders spanning the flat/sparse boundary and then matches them
would give confidence that the routing is correct under all price combinations.

---

#### 7. Missing compile-time size assert on serialized `Order`

**File:** `src/snapshot.rs:139–141`

`Order` is serialized as raw bytes via `std::ptr::read`. This is correct as-is but
relies on `Order` having no unexpected padding and no pointer members. A compile-time
assert guards against future field additions silently breaking the binary format:

```rust
const _: () = assert!(
    std::mem::size_of::<Order>() == 40,
    "Order struct size changed — snapshot binary format is broken"
);
```

---

#### 8. Rejection reason lost in `WireEventFrame`

**File:** `src/wire.rs:220–229`

The `WireEventFrame` for `OrderRejected` discards `reason: &'static str`. Downstream
systems (OMS, Kafka adapter) cannot distinguish between "duplicate ID", "post-only cross",
"FOK insufficient liquidity", etc.

**Recommendation:** Add a 1-byte `rejection_code` field to `WireEventFrame` (repurposing
one of the `_padding` bytes) and define an enum:

```
0 = Unknown
1 = DuplicateOrderId
2 = PostOnlyCross
3 = FokInsufficientLiquidity
4 = OrderNotFound
```

---

### 🟢 Minor / Polish

#### 9. Hard-coded 3 GHz assumption for x86 latency conversion

**File:** `src/latency.rs:39–41`

```rust
#[cfg(not(target_arch = "aarch64"))]
{ 3_000_000_000 } // Default estimate: 3.0 GHz
```

On modern Alder Lake, Zen 4, or throttled laptop CPUs this is inaccurate by 10–40%.
Consider reading the TSC frequency from `/proc/cpuinfo` or via CPUID at startup and
caching it in a `OnceLock<u64>`.

---

#### 10. Missing `#[must_use]` on `WalWriter::append`

**File:** `src/wal.rs:173`

`append` returns `io::Result<Option<u64>>`. Silently ignoring the return in engine code
means WAL write failures go undetected.

```rust
#[must_use]
pub fn append(&mut self, timestamp_ns: u64, cmd: &EngineCommand) -> io::Result<Option<u64>> {
```

---

#### 11. Buy and Sell matching arms are near-identical

**File:** `src/orderbook.rs:158–431`

~120 lines of near-identical matching logic is duplicated for Buy vs. Sell. A generic
helper parameterized by the opposing side's accessor would halve the code and eliminate
the risk of the two arms diverging in future changes:

```rust
fn match_against_opposing_side(
    &mut self,
    order: &mut Order,
    trades: &mut Vec<Trade>,
) { ... }
```

---

#### 12. `get_l2_snapshot` silently omits sparse price levels

**File:** `src/orderbook.rs:94–117`

`get_l2_snapshot` only iterates `flat_bids` and `flat_asks`. Orders at extreme prices in
the sparse `BTreeMap` are invisible in the market-data snapshot. This is an acceptable
performance tradeoff for the hot-path L2 feed, but it should be documented in the
function's doc comment so callers are not surprised.

---

## Summary

| Severity | # | Area | Finding |
|---|---|---|---|
| 🔴 Critical | 1 | `wire`, `snapshot`, `wal` | Endian-unsafe raw struct serialization |
| 🔴 Critical | 2 | `dedup` | `OrderId(0)` sentinel collision — `HashSet` leak |
| 🔴 Critical | 3 | `wal` | Sequence counter reset to 0 on engine restart |
| 🟡 Design | 4 | `gateway` | Single-client-at-a-time inbound, undocumented |
| 🟡 Design | 5 | `orderbook` | FOK liquidity scan ignores sparse book |
| 🟡 Design | 6 | `orderbook` | Flat/sparse boundary crossing lacks a targeted test |
| 🟡 Design | 7 | `snapshot` | Missing `size_of::<Order>()` compile-time assert |
| 🟡 Design | 8 | `wire` | Rejection reason lost in `WireEventFrame` |
| 🟢 Minor | 9 | `latency` | Hard-coded 3 GHz x86 frequency estimate |
| 🟢 Minor | 10 | `wal` | Missing `#[must_use]` on `append` |
| 🟢 Minor | 11 | `orderbook` | Buy/Sell matching arms duplicated (~120 lines) |
| 🟢 Minor | 12 | `orderbook` | `get_l2_snapshot` silently drops sparse levels |
