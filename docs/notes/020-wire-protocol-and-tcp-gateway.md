# 020. Wire Protocol & Dual-Port TCP Gateway

## Decisions Made
1. **Fixed-Size Binary Framing (`#[repr(C)]`)**:
   - Zero dynamic allocations and zero parsing overhead.
   - Messages are cast directly from byte buffers.
2. **Inbound Wire Message Format (`WireCommandFrame`, 40 bytes)**:
   - `msg_type: u8` (`1 = PlaceOrder`, `2 = CancelOrder`, `255 = Shutdown`)
   - `side: u8` (`0 = Buy`, `1 = Sell`)
   - `order_type: u8` (`0 = Limit`, `1 = Market`)
   - `time_in_force: u8` (`0 = Gtc`, `1 = Ioc`, `2 = Fok`, `3 = PostOnly`)
   - `order_id: u64`
   - `price: u64`
   - `quantity: u64`
   - `correlation_id: u64` (Echoed back to OMS for client reconciliation)
3. **Outbound Wire Event Format (`WireEventFrame`, 48 bytes)**:
   - `sequence_id: u64` (Monotonic counter for deterministic stream ordering)
   - `timestamp_ns: u64` (Engine dispatch timestamp)
   - `event_type: u8` (`1 = Accepted`, `2 = Trade`, `3 = Cancelled`, `4 = Rejected`, `255 = Shutdown`)
   - `order_id: u64`
   - `maker_order_id: u64` (For trades)
   - `price: u64`
   - `quantity: u64`
   - `leaves_quantity: u64`
4. **Dual TCP Servers**:
   - `InboundGateway` (Port 9001): Ingests `WireCommandFrame`s $\rightarrow$ pushes to Inbound SPSC queue.
   - `OutboundBroadcaster` (Port 9002): Pulls `EngineEvent` from Outbound SPSC queue $\rightarrow$ writes `WireEventFrame`s to all connected translators (e.g. Kafka bridge).
