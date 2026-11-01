# 006. Engine Scope & Symbol Isolation

## Decisions Made
1. **The Core Engine is Single-Symbol / Single-Book**:
   - The matching engine core should only manage a **single order book** (one instrument/market).
   - It does not know or care about ticker symbols, currency conversions, cross-market routing, or external business logic.
2. **External Router / Partition Manager**:
   - Symbols and routing belong in the outer orchestration/gateway layer.
   - If the system hosts 100 markets, the host runs 100 isolated engine instances (or worker threads), routing messages by `symbol_id`.
3. **Benefits**:
   - **Simplicity**: No symbol lookup map on the critical matching path.
   - **Isolation**: High trading volume or a crash in Symbol A can never affect or stall Symbol B.
   - **Cache Efficiency**: Maximizes L1/L2 cache utilization for that single book's state.
