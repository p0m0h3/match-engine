# 011. Arena & Intrusive Doubly-Linked List Design

## Decisions Made
1. **Pre-allocated Arena (Slot Pool)**:
   - Contiguous memory array allocated once during startup.
   - Index-based pointers (`u32` slot handles) instead of raw 64-bit heap pointers (`NonNull<T>` or `Box<T>`).
   - Fits nicely in CPU L1/L2 caches with no pointer chasing across arbitrary memory addresses.
2. **Intrusive Doubly-Linked List for Price Levels**:
   - Every slot in the arena stores:
     - `order`: The [`Order`] data.
     - `prev`: `Option<SlotIdx>` (previous order in the price queue).
     - `next`: `Option<SlotIdx>` (next order in the price queue).
3. **True O(1) Operations**:
   - **Append to Tail**: Update `tail.next = new_idx; new.prev = tail;` ($O(1)$).
   - **Pop from Head**: Update `head = head.next; head.prev = None;` ($O(1)$).
   - **Unlink from Middle (Cancel)**: Jump directly to slot index from Order ID lookup map, unlink `prev` and `next` pointers ($O(1)$ without queue scanning or shifting).
4. **Free List (Recycling)**:
   - When an order fills or cancels, its slot is pushed to a recycled free list in $O(1)$.
   - Zero kernel calls (`malloc` / `free`) on the hot path.
