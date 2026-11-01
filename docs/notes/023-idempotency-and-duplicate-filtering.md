# 023. Idempotency & Duplicate Command Filtering

## Decisions Made
1. **The Distributed Idempotency Problem**:
   - In financial exchanges and distributed OMS architectures, network reconnects, gateway failovers, or Kafka retries can re-transmit the same command twice.
   - If a duplicate `PlaceOrder` is processed, the trader gets double-filled; if a duplicate `CancelOrder` is processed, it generates spurious errors.
2. **Deterministic Sequence Filtering (Primary Invariant)**:
   - Every command entering the engine from an upstream sequencer carries a monotonically increasing `sequence_id: u64`.
   - **Rule**: The engine asserts `sequence_id == expected_seq`. Any command where `sequence_id <= last_processed_seq` is dropped as a duplicate replay with zero state changes.
3. **Client Order ID / Deduplication Window**:
   - To prevent an OMS from accidentally sending two orders with the same `OrderId` across different sessions, the engine maintains an active `order_locations` check:
     - If `order_locations.contains_key(&order.id)`: Reject immediately with `DuplicateOrderId`.
   - For completed/historical orders, the gateway/sequencer layer maintains a sliding bitset or ring-buffer Bloom filter of recently seen `correlation_id`s.
4. **Idempotent Cancellations**:
   - Cancelling an already-cancelled or already-filled order returns an explicit, deterministic rejection (`OrderNotFound` / `AlreadyFilled`) without corrupting queue state.
