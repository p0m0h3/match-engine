# 007. Order Lifecycle & Partial Fills

## Decisions Made
1. **Partial Fills are First-Class Execution Events**:
   - In any match where `match_qty < order.leaves_qty`, the order experiences a partial fill.
   - A single incoming order can generate multiple partial fills across multiple price levels and resting orders.
2. **Explicit Quantity Tracking**:
   - An order tracks:
     - `initial_quantity`: Original requested size.
     - `leaves_quantity`: Remaining unfilled size currently resting or matching.
     - `cum_quantity`: Cumulative filled size (`initial - leaves`).
3. **FIFO Priority Retention**:
   - A resting maker order that is partially filled **retains its exact FIFO queue position** for the remaining `leaves_quantity`. It is never re-queued or deprioritized.
4. **Execution Event Semantics**:
   - Each trade event reports:
     - `match_quantity`: Quantity filled in this specific trade step.
     - `maker_order_id`, `taker_order_id`, `price`.
     - `maker_leaves_quantity` & `taker_leaves_quantity` (or explicit `Filled` / `PartiallyFilled` status tags).
   - Once `leaves_quantity == 0`, the order is marked `Filled` and removed from memory.
