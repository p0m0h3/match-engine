# 022. State Snapshot Checkpointing & WAL Compaction

## Decisions Made
1. **The Problem with Unbounded WAL**:
   - Over days of continuous trading, a Write-Ahead Log accumulates millions of orders and trades.
   - Replaying a 10 GB WAL file on startup takes seconds to minutes, violating fast restart and recovery SLOs.
2. **Snapshot Checkpointing Strategy**:
   - Periodically (e.g. every $N$ events, or off-peak), the system dumps the complete resting order book state to a binary snapshot file (`snapshot_<seq>.bin`).
   - Format includes magic header (`MESNAP01`), `last_sequence_id`, total order count, and the exact resting orders.
3. **Atomic File Replacement**:
   - Snapshots are written to a temporary file (`snapshot.tmp`) and atomically renamed via `std::fs::rename` so a crash mid-write never leaves a corrupted snapshot.
4. **WAL Compaction & Recovery Sequence**:
   - On recovery:
     1. Find and load the latest valid `snapshot_<seq>.bin`.
     2. Restore the `OrderBook` instantly.
     3. Replay only WAL records with `sequence_id > snapshot.last_sequence_id`.
   - The old WAL segments prior to the snapshot sequence can be safely archived or deleted.
