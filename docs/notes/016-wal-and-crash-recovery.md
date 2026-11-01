# 016. Write-Ahead Log (WAL) & Crash Recovery Strategy

## Decisions Made
1. **Never Block the Matching Engine on Disk I/O (`fsync`)**:
   - Calling `fsync` on every order takes 100 µs to 10 ms (magnetic or SSD controller latency), killing sub-microsecond throughput.
   - The matching engine writes commands to an **asynchronous journaling ring buffer** or memory-mapped log (`mmap`) handled by a dedicated logger thread.
2. **Fixed-Size Binary Log Frames (Zero-Copy)**:
   - Variable-length logs require parsing and dynamic allocations during recovery.
   - Every log record has a fixed byte length with a header:
     - `sequence_id: u64`: Monotonically increasing sequence number.
     - `timestamp_ns: u64`: Nanosecond hardware timestamp.
     - `payload: LogEntry`: Binary `#[repr(C)]` struct.
     - `crc32: u32`: Checksum for corruption detection on sudden power loss.
3. **Deterministic Replay (State Recovery)**:
   - On startup, the engine checks for the latest snapshot (if any) and replays the binary log forward starting from `sequence_id = snapshot.seq + 1`.
   - Because the matching engine is a strictly deterministic state machine, replaying the raw inbound commands reconstructs the exact in-memory order book state.
