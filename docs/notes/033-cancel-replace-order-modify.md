# 033. Cancel-Replace & Order Amendment (OrderModify)

## Decisions Made
1. **The Role of Order Amendment**:
   - High-frequency market-making algorithms frequently adjust quotes as the market spread shifts.
   - Separate cancel + new-order calls consume two network round-trips and risk losing queue priority unnecessarily.
   - An atomic `ModifyOrder` command resolves both actions in a single atomic transaction.
2. **FIFO Priority Retention Rules**:
   - **Decreasing Quantity (`new_qty < current_qty`)**:
     - **Retains its exact queue position**.
     - Updates `leaves_quantity` and adjusts the price level's `total_quantity` in $O(1)$.
     - Generates zero re-queuing overhead.
   - **Increasing Quantity (`new_qty > current_qty`)**:
     - **Loses time priority**.
     - Must be unlinked and re-appended to the tail of the price queue.
   - **Price Change**:
     - Must be unlinked from the old price level and re-evaluated against the opposing book (may cross and match immediately, or rest at the new level).
3. **Wire Protocol Integration**:
   - `WireCommandFrame`: Added `msg_type = 3` (`ModifyOrder`), where `quantity` carries the requested new size.
   - `WireEventFrame`: Added `event_type = 6` (`OrderModified`).
