---
type: Task
id: "runtime-perf-remediation:ch02:t03"
chapter_id: ch02-embedded-bridge-and-batching
title: Batch-lock BridgeCorrelation allocation and add single-shard complete_many fast path
description: Acquire one shard Mutex and one active_requests fetch_add per batch in dispatch_bridge_vec, move PathAndQuery zero-copy, register BatchResponseSink in ensure_sink_registered, and add a zero-bucket single-shard fast path in complete_many.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch02:t02"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
files:
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
  - src/rs/vertebra-http/src/dispatcher/mod.rs
tests:
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
estimated_loc: 170
plan_revision: 2
plan_commit: "86527cf1"
commit: "3024aee2"
sources:
  - id: src-http-bridge
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Sharded embedded correlation table, dispatch_bridge_vec, and complete_many
  - id: src-http-dispatcher
    resource: src/rs/vertebra-http/src/dispatcher/mod.rs::Dispatcher::dispatch_requests_from_shard
    title: Dispatcher backend routing and shard affinity forwarding
tags:
  - vertebra-http
  - correlation
  - batch-bridge
  - locking
---

# Batch-lock BridgeCorrelation allocation and add single-shard complete_many fast path

## Objective & Scope Boundary

Cut per-batch correlation overhead in `BridgeCorrelation` from `2N` shard locks and `2N` atomic counter updates to `2` shard locks and `2` atomic updates on the common single-shard path, while eliminating path/query copies [^src-http-bridge] [^src-http-dispatcher].

Stay strictly inside `dispatcher/bridge.rs` and `dispatcher/mod.rs`. Do not touch `dispatcher/correlation.rs` or `dispatcher/shm.rs` (owned by `ch03`).

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch02-embedded-bridge-and-batching/spec.md` (Section 2.2 contracts)
- `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation`, `ensure_sink_registered`, and `dispatch_bridge_vec`
- `src/rs/vertebra-http/src/dispatcher/mod.rs::Dispatcher::dispatch_requests_from_shard`

## Contracts & Edge Cases

Implement the `BridgeCorrelation` and `dispatch_bridge_vec` contracts defined in `chapters/ch02-embedded-bridge-and-batching/spec.md`:

- Update `Dispatcher::dispatch_requests_from_shard` (`dispatcher/mod.rs`) to forward `batcher_shard` into `bridge::dispatch_bridge_vec(bridge, batcher_shard, requests)`.
- In `dispatch_bridge_vec`:
  - Select primary shard `start = batcher_shard.map_or_else(|| bridge_corr.next_batch.fetch_add(1, Ordering::Relaxed), |s| s) & BRIDGE_SHARD_MASK`.
  - Hold one shard `MutexGuard` while allocating all non-fire-and-forget correlation IDs for the batch, spilling to `(start + attempt) & BRIDGE_SHARD_MASK` only when the current shard reaches capacity.
  - Collect rejected `oneshot::Sender` replies when all 8 shards are full, drop the shard guard, perform one `active_requests.fetch_add(allocated_count, Ordering::Release)` when `allocated_count > 0`, and send `503` replies strictly outside shard locks.
  - Build `InProcessRequest::from_path_and_query(...)` directly from `req.path_and_query` (and update `dispatch_single` to use `from_path_and_query` too).
- In `ensure_sink_registered`:
  - Register `bridge.set_batch_response_sink(...)` alongside `set_response_sink` and `set_stream_sink`, upgrading `Weak<BridgeCorrelation>` once per batch and calling `corr.complete_bridge_batch(resps)`.
- In `BridgeCorrelation`:
  - Implement shared `complete_many_inner` backing both `complete_many(Vec<(u64, PendingResponse)>)` and `complete_bridge_batch(Vec<BridgeResponse>)` with zero intermediate `Vec` conversion.
  - Single-shard fast path: check whether every response maps to `first_shard`; if so, skip the 8-`Vec` bucket array, acquire `self.shards[first_shard]` once, collect matched `(tx, resp)` into one `Vec`, release the lock, execute one `active_requests.fetch_sub(ready.len(), Ordering::Release)`, and send all replies outside the lock.
  - Multi-shard fallback: preserve existing 8-bucket grouping when a batch contains IDs from multiple shards.

## Test Matrix

| Case | Scenario | Expected Result |
| :--- | :--- | :--- |
| Single-shard batch allocation | `dispatch_bridge_vec` with 16 requests, `batcher_shard = Some(0)` | All 16 correlation IDs belong to shard `0`; `in_flight() == 16` |
| Shard spill + 503 overflow | `with_capacity` small enough that shard 0 fills mid-batch, then all 8 shards fill | Requests spill into shard 1; excess requests receive `503` outside locks; `in_flight()` equals allocated count |
| Single-shard `complete_many` & `complete_bridge_batch` | Complete 16 responses from shard 0 (including 1 stale/unknown ID) | All 15 live waiters receive responses; `in_flight() == 0`; unknown ID ignored safely |
| Multi-shard fallback `complete_many` | Complete batch mixing IDs from shards 0, 1, and 3 | All waiters receive matching responses; `in_flight() == 0` |
| `ensure_sink_registered` batch delivery | Submit `Vec<BridgeResponse>` via `BatchBridge::submit_responses` | All pending `oneshot` receivers resolve through `BatchResponseSink` and `in_flight()` drops to `0` |
| Fire-and-forget mixed batch | Batch mixing `FIRE_AND_FORGET` and normal requests | FNF gets `correlation_id == 0`, does not increment `in_flight()`; normal requests tracked and completed |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http dispatcher
```

## Acceptance Criteria

- `dispatch_bridge_vec` allocates a batch under one shard lock on the unspilled path, updates `active_requests` once, and spills/rejects safely outside locks.
- `dispatch_bridge_vec` and `dispatch_single` construct `InProcessRequest` via `from_path_and_query` with zero path/query slice copies.
- `ensure_sink_registered` installs `BatchResponseSink` upgrading `Weak<BridgeCorrelation>` once per batch.
- `complete_many` and `complete_bridge_batch` execute the zero-bucket single-shard fast path and fall back cleanly for multi-shard batches.
- `cargo test -p vertebra-http dispatcher` passes.

[^src-http-bridge]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Sharded embedded correlation table, dispatch_bridge_vec, and complete_many.
[^src-http-dispatcher]: `src/rs/vertebra-http/src/dispatcher/mod.rs::Dispatcher::dispatch_requests_from_shard` — Dispatcher backend routing and shard affinity forwarding.
