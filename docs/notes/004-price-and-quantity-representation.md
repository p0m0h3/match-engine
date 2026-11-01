# 004. Price and Quantity Representation

## Decisions Made
1. **No General-Purpose `Decimal` Crates on the Hot Path**:
   - General decimal types (e.g. `rust_decimal`) store variable scales or 128-bit integers with dynamic normalization logic and division in their arithmetic, introducing branch mispredictions and CPU cycle overhead.
2. **Fixed-Point / Raw Ticks as `u64`**:
   - The matching engine only needs relative comparison (`>`, `<`, `==`) and integer addition/subtraction.
   - Prices are represented internally as integer **Tick Units** (or atomic units):
     - Example: If tick size = $0.01, $100.50 is stored as integer `10050`.
     - Example: If tick size = $0.00001, $1.23456 is stored as integer `123456`.
3. **Decoupled Formatting at the Boundary**:
   - Human-readable conversions (e.g., parsing `"100.50"` or printing JSON) occur at the Gateway / API layer, not inside the matching core.
   - The engine operates purely on raw integer ticks for maximum CPU register efficiency.
