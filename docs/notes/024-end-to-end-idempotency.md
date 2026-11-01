# 024. End-to-End Idempotency (Inputs vs. Outputs)

## Decisions Made
1. **Inbound Idempotency (Command Deduping)**:
   - **Goal**: Prevent duplicate execution (e.g. double-filling an order or duplicate cancellation).
   - **Mechanism**:
     - Monotonic sequence verification (`inbound_seq > last_inbound_seq`).
     - Duplicate active `OrderId` rejection.
     - Sliding circular ring filter of recent completed order IDs.
2. **Outbound Idempotency (Event Stream Deduplication & Replay)**:
   - **Goal**: Enable downstream subscribers (Kafka bridge, OMS replicas, Clearing) to achieve **Exactly-Once Processing**.
   - **Mechanism**:
     - Every emitted `EngineEvent` and `WireEventFrame` carries a deterministic, monotonic `sequence_id` (`1, 2, 3...`).
     - **Kafka Mapping**: Downstream Kafka translator maps `sequence_id` directly to:
       - Kafka Message Key / Offset.
       - Kafka Idempotent Producer / Transaction ID.
     - **Subscriber Invariant**: If a consumer disconnects and reconnects, it discards any events where `sequence_id <= subscriber_last_seen_seq`.
3. **End-to-End Idempotency Guarantee**:
   - Given identical inbound input streams $\rightarrow$ the engine produces the identical outbound event sequence with identical IDs.
   - Prevents duplicate accounting across distributed ledgers.
