# Performance Tuning Guide

## Slot Sizing

- Keep slot sizes power-of-two and cacheline-aligned.
- For small control messages, 256B slots are usually enough.
- For larger payloads, use a buffer pool and send metadata.

## Ring Capacity

- Start with 64–1024 slots depending on burstiness.
- Larger rings increase memory footprint; smaller rings increase backpressure.

## Shared Memory Backing

- Linux: `memfd_create` fast-path (seals enabled) with `shm_open` fallback.
- macOS: `shm_open` + `mmap`.
- Windows: `CreateFileMapping` + `MapViewOfFile`.

## Synchronization

- The fast path is lock-free; wait/wake is only on backpressure.
- Timeout paths use OS waits (`wait_on_address_timeout`) instead of busy-yield loops.
- macOS defaults to a bounded spin + nanosleep fallback; enable `macos-ulock`
  for lower-latency waits.
- File-lock fallback is available if wait primitives are unavailable.

## Benchmarking

Use `cargo bench -p vertebra-core` and capture latency distributions with
Criterion. For deeper profiling, use `perf` + `flamegraph` on Linux.

## Python Zero-Copy Notes

- `BufferView.as_memoryview()` exposes a writable view when the underlying
  buffer supports the Python buffer protocol; this enables msgspec to decode
  without copying.
- When a writable memoryview is unavailable, `BufferView` falls back to a
  mutable proxy that writes through to the buffer on mutation (copy-based).
- Zero-copy is active when `BufferView.as_memoryview()` returns a writable
  `memoryview` (`mv.readonly` is false).

### Serialization Benchmarks

Run the Python serialization benchmarks with:

```bash
uv run python benchmarks/py_serialization.py
```

This reports msgspec decode from bytes vs memoryview/BufferView and pydantic
encode/decode across 1KB/10KB/100KB payload sizes.

For IPC round-trip latency (Python wrappers + Rust ring), run:

```bash
uv run python benchmarks/py_ipc_latency.py
```
