# 002. Communications Architecture

## Decisions Made
1. **Single-Writer Matching Core**:
   - The matching engine runs single-threaded, pinned to a dedicated CPU core.
   - Eliminates mutex/lock contention, OS context switching, and cache-line invalidation storms.
2. **Lock-Free SPSC Ring Buffers for Inter-Thread I/O**:
   - Inbound orders and outbound trades flow through Single-Producer Single-Consumer queues with 64-byte cache-line padding to prevent false sharing.
3. **Shared Memory (`mmap`/`shm`) for IPC**:
   - Gateways/risk checks on the same host communicate with the engine via shared memory, bypassing the network stack and kernel overhead.
4. **Zero-Copy Binary Structs**:
   - No JSON or Protobuf on the hot path. Messages will be fixed-width, byte-aligned structs (`#[repr(C)]`) cast directly from memory buffers.
