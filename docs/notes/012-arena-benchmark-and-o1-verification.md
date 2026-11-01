# 012. Arena Benchmark & O(1) Cancellation Verification

## Key Findings

### 1. Proof of True $O(1)$ Cancellation Scalability
We ran cancellation benchmarks at increasing queue depths (100, 500, 2,000, and 5,000 orders resting at the exact same price level):

| Price Queue Depth | Time to Cancel Order in Middle |
|---|---|
| **100 orders** | **761 ns** |
| **500 orders** | **751 ns** |
| **2,000 orders** | **740 ns** |
| **5,000 orders** | **736 ns** |

- **Conclusion**: Cancellation latency is **completely flat** regardless of queue depth. With `VecDeque`, cancelling an order at depth 5,000 would require scanning and shifting 2,500 elements. With our intrusive list, it unlinks directly in $O(1)$ time.

### 2. Next Latency Bottleneck: `BTreeMap` and `HashMap`
- While individual order queue operations are now zero-allocation and $O(1)$, the remaining latency is dominated by:
  1. `BTreeMap` price lookup tree traversal ($O(\log P)$ with node pointer chasing).
  2. `HashMap` order lookup with default SipHash hasher (cryptographic hashing overhead).
- **Next optimization**: Replacing the default hasher with a fast non-cryptographic hasher (like `FxHash` or `AHash`), and migrating sparse price trees to flat arrays or radix structures.
