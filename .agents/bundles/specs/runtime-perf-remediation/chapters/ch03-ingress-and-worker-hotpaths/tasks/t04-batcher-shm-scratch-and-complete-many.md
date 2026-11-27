---
type: Task
id: "runtime-perf-remediation:ch03:t04"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Reusable ShmDispatchScratch and single-shard complete_many fast path
description: Own reusable scratch buffers inside RequestBatcher, use batch-level insert_with_now in ShmDispatcher, and add a single-shard fast path in complete_many.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch03:t03"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-http/src/batcher.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
files:
  - src/rs/vertebra-http/src/batcher.rs
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
tests:
  - src/rs/vertebra-http/src/dispatcher/shm.rs
  - src/rs/vertebra-http/src/batcher.rs
estimated_loc: 180
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-http-batcher
    resource: src/rs/vertebra-http/src/batcher.rs::RequestBatcher
    title: Per-shard RequestBatcher loop
  - id: src-http-shm
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher
    title: ShmDispatcher batch serialization and response completion
tags:
  - shm-dispatch
  - batching
  - performance
---

# Reusable ShmDispatchScratch and Single-Shard complete_many Fast Path

## Objective & Scope Boundary

Eliminate three per-batch heap `Vec` allocations in `ShmDispatcher`, amortize `Instant::now()` once per SHM batch via `insert_with_now`, and add a zero-bucket single-shard fast path to `ShmDispatcher::complete_many` [^src-http-batcher] [^src-http-shm].

Out of scope: `ShmBatchWriter` wire format in `vertebra-core` and `BridgeCorrelation` (handled in `t03`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/rs/vertebra-http/src/batcher.rs`, `src/rs/vertebra-http/src/dispatcher/shm.rs`, `src/rs/vertebra-http/src/dispatcher/mod.rs`

## Contracts & Edge Cases

Follow `ShmDispatchScratch`, `ShmDispatcher::dispatch_shm_vec_from_shard_with_scratch`, and `Dispatcher::dispatch_requests_from_shard_with_scratch` in `spec.md` Section 2.2:

- **`ShmDispatchScratch`**: Owned by `RequestBatcher::run` (allocated once with `with_capacity(max_size)` for `staged`, `nodes`, and `components` at `max_size * 4`, plus `rejected_replies`). Always cleared at the start and end of every dispatch call so dropped batches never retain `Bytes` or `oneshot::Sender` handles.
- **Safe zero-unsafe `components` reuse**: Convert empty `Vec<&'static [u8]>` <-> `Vec<&'a [u8]>` via a small helper `recycle_slice_vec` (`v.clear(); v.into_iter().map(|_| &[][..]).collect()`), destructuring `let ShmBatchWriter { nodes, components, .. } = writer;` after each sub-batch send so both `nodes` and `components` return to `scratch` with capacity preserved.
- **Batch-level `Instant::now()`**: Capture `let batch_now = std::time::Instant::now();` once before the correlation-allocation loop in `dispatch_shm_vec_from_shard_with_scratch` and call `shard_guard.insert_with_now(..., batch_now)`. Keep `dispatch_shm_vec_from_shard` as a thin wrapper creating a local `ShmDispatchScratch::default()` for direct callers.
- **Dispatcher forwarding**: Add `Dispatcher::dispatch_requests_from_shard_with_scratch` in `dispatcher/mod.rs` forwarding SHM backends to `dispatch_shm_vec_from_shard_with_scratch` and other backends to existing dispatch methods. Update `RequestBatcher::drain` to pass `&mut self.shm_scratch`.
- **`ShmDispatcher::complete_many` single-shard fast path**: For `responses.len() > 1`, compute `first_shard = CorrelationId::from_raw(responses[0].0).shard() & SHM_CORRELATION_MASK`. If every remaining entry matches `first_shard`, lock `self.correlation_shards[first_shard]` once, collect `(tx, response, worker_id)` into a pre-sized `ready` `Vec`, drop the lock, decrement `active_requests` and per-worker `in_flight`, and send all replies without allocating the 8-bucket array. If any ID belongs to a different shard, fall back to the existing 8-bucket grouped path.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| `ShmDispatchScratch` capacity reuse | Dispatch two multi-request batches using one `ShmDispatchScratch` and assert `staged.capacity()`, `nodes.capacity()`, and `components.capacity()` remain non-zero and `staged.is_empty()` after each call. |
| Multi-sub-batch / overflow preservation | Dispatch a batch requiring multiple sub-batches (or mixed flags) through `ShmDispatchScratch` and verify all requests deliver in order with zero leaked scratch items. |
| `complete_many` single-shard fast path | Complete a batch of 8 requests from one correlation shard and verify all 8 receive responses and `in_flight()` + `worker_in_flight()` return to `0`. |
| `complete_many` mixed-shard fallback | Allocate correlation IDs across two distinct shards, pass them together to `complete_many`, and verify all replies complete and counters return to `0`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http -- dispatcher::shm batcher
```

## Acceptance Criteria

- `RequestBatcher::run` allocates `ShmDispatchScratch` once per task and reuses `staged`, `nodes`, and `components` capacities across batches with zero `unsafe`.
- `ShmDispatcher::dispatch_shm_vec_from_shard_with_scratch` invokes `Instant::now()` once per batch.
- `ShmDispatcher::complete_many` avoids 8-bucket allocation on single-shard batches and handles mixed-shard batches cleanly.
- All SHM dispatcher and batcher unit tests pass.

[^src-http-batcher]: `src/rs/vertebra-http/src/batcher.rs::RequestBatcher` — Per-shard RequestBatcher loop.
[^src-http-shm]: `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher` — ShmDispatcher batch serialization and response completion.
