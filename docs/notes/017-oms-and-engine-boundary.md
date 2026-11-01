# 017. OMS-to-Engine Boundary & Network Gateways

## Decisions Made
1. **Strict Separation of Concerns (OMS vs. Engine)**:
   - **Order Management System (OMS)**:
     - Handles external clients, authentication, margin/risk checks, portfolio limits, currency balances, and multi-symbol routing.
     - Runs on the outer network layer (REST, WebSocket, FIX protocol, gRPC).
   - **Matching Engine**:
     - Stripped of all business/account logic.
     - Operates strictly on integer `OrderId`, `Price`, and `Quantity`.
2. **Boundary Protocols (How OMS talks to Engine)**:
   - **Co-located / Same Server (Sub-microsecond)**:
     - Shared Memory (`shm` / memory-mapped ring buffers) using `#[repr(C)]` binary structs.
   - **Distributed / Networked (Containers / Separate Nodes)**:
     - Low-latency binary TCP stream with `TCP_NODELAY` using fixed-size binary framing (or SBE / FlatBuffers).
3. **Correlation ID & Sequence Tracking**:
   - The engine accepts a `cl_ord_id` (Client Order ID / Account ID) or passes through a `correlation_id` (`u64`) so the OMS can immediately map execution reports back to the originating user session without the engine needing account tables.
4. **Outbound Execution Fan-Out**:
   - The engine emits raw `EngineEvent`s to an outbound ring buffer.
   - An independent **Gateway/Disseminator thread** consumes this queue and forwards:
     - Private execution reports to the OMS (via TCP / Shared Memory / Kafka).
     - Public trade ticks and book updates to the Market Data ticker (via UDP Multicast or WebSocket).
