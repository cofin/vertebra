---
name: review-performance
description: Use when reviewing hot-path allocation discipline, zero-copy guarantees, Criterion micro-benchmarks, or free-threaded Python 3.14t/3.15t contention.
---

<!-- truth: start -->
- **Baselines**: Hot-path changes require Criterion or micro-benchmark numbers on the same machine (`PERF-01`).
- **Allocations**: No new per-request heap allocations (`Vec`, `String`, `Box`, Python list/dict churn) (`PERF-02`).
- **Wakeups**: Batch granularity preserved without per-request cross-thread wakeups (`PERF-03`).
- **Zero-copy**: SHM slices and FFI views remain zero-copy without added memory copies (`PERF-04`).
- **Imports**: Package import times stay within documented memory and latency budgets (`PERF-05`).
- **Suppressions**: Respect `FP-PERF-01` in the shared catalog.
<!-- truth: end -->

# Performance Review (`review-performance`)

## Scope

- Paths: `[areas.performance]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `PERF-01`..`PERF-09` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Benchmarks and free threaded](../../bundles/knowledge/testing/benchmarks-and-free-threaded.md)
- [Packaging and PGO](../../bundles/knowledge/domains/frontend/packaging-and-pgo.md)
- [Hot path and async dispatch](../../bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md)
- Benchmark protocol reference: [`references/benchmark-protocol.md`](references/benchmark-protocol.md)

## Review Process

1. **Hot path allocation audit**: Inspect changes in ring dispatch and codec loops for heap allocations (`PERF-02`).
2. **Lock and wakeup frequency**: Confirm locks and futex wakes scale with batch count, not request count (`PERF-03`).
3. **Zero-copy validation**: Ensure `try_send_with` and `body_view` preserve direct pointer views (`PERF-04`).
4. **Import time verification**: Measure `import vertebra` latency using Python `-X importtime` (`PERF-05`).
5. **Dependency isolation**: Ensure benchmark-only libraries remain in `[dependency-groups].bench` (`PERF-06`).
6. **3.14t contention checks**: Verify shared counters and dispatcher state scale on free-threaded runs (`PERF-07`).

## Suppressions Index

Shared catalog: [`false-positive-suppressions.md`](../review-core/references/false-positive-suppressions.md).

- `FP-PERF-01`: Dispatch spec caching

## Focused Commands

```bash
make rs-bench
uv run --group bench python -m tools.benchmarks.micro
```
