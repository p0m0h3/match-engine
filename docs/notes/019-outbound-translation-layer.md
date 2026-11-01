# 019. Outbound Event Channel & Translation Decoupling

## Decisions Made
1. **Zero Kafka Dependencies in the Match Engine**:
   - The matching engine codebase will contain **no Kafka drivers, dependencies, or broker configurations**.
   - The engine remains pure, ultra-low latency, and technology-agnostic.
2. **Dedicated Outbound Event Stream**:
   - The engine simply emits a clean, high-performance, strictly sequenced binary event stream on a dedicated outbound interface (e.g. dedicated TCP socket or Unix socket).
3. **Plug-and-Play Translator Layer (Sidecar / Adapter Pattern)**:
   - Any external translator/bridge process can connect to this outbound stream.
   - The translator reads the raw engine events and translates them into whatever format or messaging bus is needed:
     - Kafka producer (for distributed OMS state replication and event sourcing).
     - RabbitMQ / NATS / Redis Streams.
     - WebSocket server (for UI ticker updates).
   - If the downstream bus goes down or lags, it only affects the translator; the matching engine continues operating with zero jitter.
