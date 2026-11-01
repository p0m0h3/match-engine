# 030. Downstream Kafka Adapter & Idempotent Publishing

## Decisions Made
1. **Decoupled Outbound Consumer**:
   - The Kafka adapter is built as an independent downstream worker (`src/bin/kafka_adapter.rs`).
   - Connects to the engine's Outbound Broadcaster TCP port (`127.0.0.1:9002`).
   - Receives fixed-size binary `WireEventFrame`s (56 bytes) at wire speed.
2. **Translation to Kafka Message Model**:
   - Translates raw binary events into structured JSON / binary Kafka payloads:
     - **Kafka Topic**: `exchange.market.events`
     - **Kafka Key**: `order_id` (guarantees per-order partition ordering).
     - **Kafka Header / Idempotency Token**: `sequence_id` (anchors exactly-once deduplication).
3. **Idempotency Tracking on the Adapter**:
   - The adapter maintains `last_published_sequence_id: u64` (persisted or tracked against broker offsets).
   - If the network reconnects and re-receives events where `sequence_id <= last_published_seq`, the adapter silently discards them, preventing duplicate Kafka message generation.
