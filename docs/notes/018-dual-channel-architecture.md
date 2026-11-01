# 018. Dual-Channel Architecture: Inbound Ingestion vs. Outbound Event Stream

## Decisions Made
1. **Asymmetric Network Ports (Dual-Channel Architecture)**:
   - **Inbound Command Port (e.g. TCP 9001)**:
     - Dedicated point-to-point ingestion socket from the active OMS / Gateway.
     - Low-overhead binary command frames pushed into the engine's inbound SPSC ring buffer.
   - **Outbound Event Stream Port (e.g. TCP 9002)**:
     - Dedicated broadcast/fan-out socket.
     - Outbound stream workers (e.g. a Kafka Bridge, Market Data feed, or Clearing service) connect here to consume a continuous, sequenced stream of `EngineEvent`s.
2. **Why Decouple Inbound from Outbound?**:
   - **No Backpressure on Matching Core**: If Kafka or downstream distributed brokers experience a lag spike or broker rebalance, it must never block the OMS from sending cancellations or orders.
   - **Multi-Consumer Fan-Out**: Multiple downstream services (e.g. Kafka Connector, WebSocket Ticker, Drop-Copy Auditor) can subscribe to the outbound event stream simultaneously.
   - **Distributed OMS Replication**: Downstream OMS replicas consume the raw event stream from Kafka to update distributed account balances and state without touching the matching engine.
3. **Framing Format**:
   - Fixed-length binary event frames prefixed with a sequence number, enabling downstream Kafka producers to translate them directly into Kafka messages with exactly-once / partitioned ordering.
