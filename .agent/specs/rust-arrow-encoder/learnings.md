# Learnings: Rust Arrow Encoder Optimization

## Performance Findings (2026-01-25)

We attempted to replace the Python/`nanoarrow` implementation of `encode_arrow` with a native Rust version using PyO3. The goal was a 5x speedup.

### Benchmarks (100,000 rows, 4 cols)
- **Baseline (Python + nanoarrow C ext):** ~62ms
- **Rust (Naive PyO3 iteration):** ~174ms (~3x slower)
- **Rust (Hybrid: msgspec -> msgpack bytes -> Rust):** ~302ms (~5x slower)

### Root Cause Analysis
1.  **PyO3 Overhead:** The "safe" PyO3 wrappers (`PyList`, `PyTuple`, `PyAny::extract`) incur significant C-API overhead (refcounting, bounds checking, error handling) for *every single cell*. This is O(N) where N is total cells.
2.  **Comparison with `nanoarrow`:** `nanoarrow` is a C extension that likely uses raw C pointers and fast-path macros (`PyList_GET_ITEM`), bypassing the overhead we hit in safe Rust.
3.  **Serialization Cost:** The hybrid approach (serializing to msgpack first) adds a full serialization pass in Python, which dominates the runtime. Rust decoding the bytes is fast, but the prep work is too expensive.

### Failed Strategies
- **Optimization 1: Type Sampling:** Creating schema from first 100 rows didn't save enough time; the cost is in *data* extraction, not schema inference.
- **Optimization 2: `PyList`/`PyTuple` Fast Paths:** Explicitly downcasting to `PyList` saved some generic sequence overhead but was still dominated by `get_item`.
- **Optimization 3: `rmp` (Rust MessagePack):** Decoding msgpack byte-by-byte (`Read::read_exact`) in Rust was surprisingly slow compared to bulk memory operations, likely due to lack of buffering or method call overhead in the inner loop.

### Conclusion
The "Rust is always faster" assumption does not hold for iterating Python object collections (lists of tuples) when compared to optimized C extensions like `nanoarrow`. To beat `nanoarrow`, we would need to write "Unsafe Rust" that essentially replicates C pointer arithmetic, resulting in high complexity and low safety.

**Decision:** The task "Port Arrow Encoder to Rust" is blocked/failed for the current performance targets. We should stick with `nanoarrow` for the time being.
