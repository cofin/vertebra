---
type: Task
id: "runtime-perf-remediation:ch03:t03"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Batch-amortized Instant in InFlightRequests and BridgeCorrelation
description: Add InFlightRequests::insert_with_now accepting a caller-supplied Instant and capture Instant::now() once per batch in BridgeCorrelation.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-http/src/dispatcher/correlation.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
files:
  - src/rs/vertebra-http/src/dispatcher/correlation.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
tests:
  - src/rs/vertebra-http/src/dispatcher/correlation.rs
  - src/rs/vertebra-http/src/dispatcher/bridge.rs
estimated_loc: 45
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-http-correlation
    resource: src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests
    title: Generation-tagged O(1) correlation slab
  - id: src-http-bridge
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Embedded bridge correlation dispatcher
tags:
  - correlation
  - embedded
  - performance
---

# Batch-Amortized Instant in InFlightRequests and BridgeCorrelation

## Objective & Scope Boundary

Add `InFlightRequests::insert_with_now` so dispatchers capture `Instant::now()` once per batch instead of once per request, and adopt it in `BridgeCorrelation::dispatch_bridge_vec` (which `ch02` batch-locked) [^src-http-correlation] [^src-http-bridge].

Out of scope: `ShmDispatcher` and `RequestBatcher` updates (handled in `t04`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/rs/vertebra-http/src/dispatcher/correlation.rs`, `src/rs/vertebra-http/src/dispatcher/bridge.rs`

## Contracts & Edge Cases

Follow `InFlightRequests::insert_with_now` defined in `spec.md` Section 2.2:

- **`InFlightRequests::insert_with_now`**: Pop a slot from `free_list`, increment `generations[index]` with wrapping addition, store `CorrelationEntry { reply, created_at: now, worker_id }`, and return `Some(CorrelationId::encode(shard, index as u32, generation))` (or `None` when full).
- **`InFlightRequests::insert`**: Delegate directly to `self.insert_with_now(shard, reply, worker_id, Instant::now())` so all existing callers remain source-compatible.
- **`BridgeCorrelation::dispatch_bridge_vec`**: Capture `let batch_now = std::time::Instant::now();` once before iterating the request batch and call `insert_with_now(..., batch_now)` for every non-fire-and-forget request in the batch.
- **Expiration ordering**: A caller supplying an earlier `Instant` (e.g., `Instant::now() - timeout`) must cause `take_expired_with_worker` / `sweep_expired` to expire that entry immediately while leaving newer entries intact.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| `insert_with_now` round-trip and ABA generation | Inserting with an explicit `Instant`, completing, and re-inserting increments generation and rejects stale IDs. |
| `insert_with_now` expiry fidelity | Insert one entry with `now - 2s` and one with `now`, sweep with `1s` timeout, and assert only the older entry expires. |
| `BridgeCorrelation` batch dispatch | Existing `dispatcher::bridge` tests pass with batch-level `Instant::now()` and correlation timeout sweep unchanged. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http -- dispatcher::correlation::tests dispatcher::bridge
```

## Acceptance Criteria

- `InFlightRequests::insert_with_now` is public and `insert` delegates to it.
- `BridgeCorrelation::dispatch_bridge_vec` calls `Instant::now()` at most once per non-empty batch.
- All correlation and bridge dispatcher unit tests pass.

[^src-http-correlation]: `src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests` — Generation-tagged O(1) correlation slab.
[^src-http-bridge]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded bridge correlation dispatcher.
