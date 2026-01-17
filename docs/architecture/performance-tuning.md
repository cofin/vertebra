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
- File-lock fallback is available if wait primitives are unavailable.

## Benchmarking

Use `cargo bench -p vertebra-core` and capture latency distributions with
Criterion. For deeper profiling, use `perf` + `flamegraph` on Linux.
