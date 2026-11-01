# 008. Order Types & Time-in-Force (TIF)

## Decisions Made
1. **Separation of Order Type from Time-In-Force (TIF)**:
   - Exchanges decompose execution instructions into two orthogonal dimensions:
     - **Order Type**: Determines price constraints (`Limit` vs `Market`).
     - **Time-In-Force**: Determines lifetime and cancellation policy (`GTC`, `IOC`, `FOK`, `PostOnly`).

2. **Core Order Types Supported**:
   - **Limit**: Matches at the specified price or better. Unfilled balance rests on the book (unless restricted by TIF).
   - **Market**: Matches immediately against whatever liquidity is available regardless of price. Must never rest on the book; any unfilled balance is cancelled immediately.

3. **Time-In-Force (TIF) Policies Supported**:
   - **GTC (Good 'Til Cancel)**: Default behavior. Aggressive portion matches immediately; remainder rests on the book until cancelled or filled.
   - **IOC (Immediate-Or-Cancel)**: Matches immediately against resting orders. Any remaining unfilled quantity is cancelled, never resting.
   - **FOK (Fill-Or-Kill)**: All-or-nothing. The order must be filled completely and immediately across available resting liquidity, or rejected/cancelled entirely with zero matches.
   - **Post-Only (Maker-Only)**: Guarantees the order adds liquidity. If the price crosses resting orders (would take liquidity), it is rejected immediately without executing.

4. **Zero-Cost Representation**:
   - Both `OrderType` and `TimeInForce` are represented as compact `u8` enums (`#[repr(u8)]`) to keep orders cache-aligned.
