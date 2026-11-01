# 015. Lock-Free SPSC Ring Buffer & Event Loop

## Decisions Made
1. **Custom Zero-Dependency Lock-Free SPSC Ring Buffer**:
   - Single-Producer Single-Consumer queue based on circular ring buffer.
   - Bounded, fixed-size array pre-allocated up front.
2. **Cache-Line Padding (False Sharing Immunity)**:
   - Producer tail and Consumer head atomic indices are padded with `#[repr(align(64))]` to separate CPU cache lines.
   - Prevents L1/L2 cache invalidations across producer and consumer CPU cores.
3. **Bitwise Masking**:
   - Buffer size restricted to powers of 2.
   - Array index calculation uses bitwise AND (`index & (CAPACITY - 1)`), eliminating hardware division instructions.
4. **Memory Ordering**:
   - Push uses `Release` store on tail; Pop uses `Acquire` load on tail.
   - Pop uses `Release` store on head; Push uses `Acquire` load on head.
   - Guarantees happens-before visibility of order data without locking mutexes.
