# 005. Concurrency and Replication Strategy

## Decisions Made
1. **Single-Threaded Execution Per Symbol / Partition**:
   - Within a single trading pair (e.g. `BTC-USDT` or `AAPL`), matching **must** be strictly single-threaded.
   - Cross-thread synchronization (locks/atomics) on an order book causes cache line bouncing and ruins latency.
   - **Horizontal Scaling**: Scales by partitioning independent symbols across separate worker threads or processes (Actor/Shared-Nothing model). Symbol A never contends with Symbol B.

2. **Active-Passive Replicas with Deterministic Replication**:
   - **Never Active-Active for the same book**: Distributing an order book across multiple active nodes requires distributed consensus (Raft/Paxos) on every single order, driving latency from nanoseconds to tens of milliseconds.
   - **Primary (Active)**: Sequenced, single-threaded matching core; writes inbound event stream to an append-only journal/multicast.
   - **Secondary (Hot Standby / Passive)**: Subscribes to the same sequenced event stream, replaying state in lockstep.
   - **Failover**: If Primary heartbeats fail, Secondary promotes itself to Primary with zero state loss and zero split-brain risk.
