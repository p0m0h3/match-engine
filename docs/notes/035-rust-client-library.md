# 035. Strongly-Typed Rust Client SDK

## Context
Downstream systems (Order Management Systems, market makers, trade feeds, Kafka bridges) currently interact with the matching engine by manually packing 40-byte `WireCommandFrame` structs and decoding 56-byte `WireEventFrame` buffers over raw TCP sockets.

To eliminate boilerplate, prevent protocol misalignments, and guarantee optimal TCP socket configuration (e.g., `TCP_NODELAY`, 64 KB user-space buffering, pipelining), we need a first-class, idiomatic Rust client SDK.

## Key Decisions

1. **Decoupled Client Roles (Dual-Channel Alignment)**:
   - **`InboundClient`**: Manages the order submission pipeline to port 9001 (OMS role).
     - Provides high-level helper methods: `place_limit`, `place_market`, `cancel_order`, `modify_order`, and `shutdown`.
     - Employs `BufWriter<TcpStream>` for transparent batching and pipelining.
   - **`EventSubscriber`**: Connects to the event stream on port 9002 (market data / execution report role).
     - Implements `Iterator` yielding decoded, strongly-typed domain events (`ClientEvent`).
     - Automatically verifies strictly monotonic sequence numbers (`sequence_id`) and detects gaps.
   - **`MatchEngineClient`**: Convenient composite bundle providing both channels in a single handle for full-duplex agents.

2. **Zero Heap Allocations on Wire Conversion**:
   - SDK serializes method arguments directly into stack-allocated fixed-size `WireCommandFrame` frames.
   - Deserialization in `EventSubscriber` reads directly into stack-allocated 56-byte `WireEventFrame` buffers.

3. **Strongly-Typed Domain Abstraction (`ClientEvent`)**:
   - Converts raw integer wire tags into native domain types (`OrderId`, `Price`, `Quantity`, `TradeExecution`).
