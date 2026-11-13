---
type: Task
id: "broker-pool-sharing:ch01:t02"
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-lifecycle
title: "Async-Signal-Safe Owned-Segment Registry and PID Reaper"
description: "Back cleanup_all() with a fixed-slot owned-segment table, signal/panic hooks, and PID-aware startup reaping."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch01:t01"
files:
  - src/rs/vertebra-core/src/cleanup.rs
  - src/rs/vertebra-http/src/workers/factory.rs
tests:
  - src/rs/vertebra-core/tests/it/shm.rs
read_set:
  - src/rs/vertebra-core/src/cleanup.rs
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-core/tests/it/shm.rs
estimated_loc: 195
tags:
  - shm
  - cleanup
  - signals
sources:
  - id: core-cleanup
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: "Process SHM cleanup hook"
  - id: worker-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: "Supervisor worker spawning, panic hook, and stale SHM reaper"
---

# Task `t02`: Async-Signal-Safe Owned-Segment Registry and PID Reaper

## Objective & Scope Boundary

Implement `register_owned`, `unregister_owned`, `cleanup_all`, `install_signal_cleanup`, and `reap_dead_owner_segments` per [`../spec.md` §2](../spec.md) ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory]. Out of scope: Python broker errors (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/cleanup.rs` — Current empty `cleanup_all()` stub.
- `src/rs/vertebra-core/src/shm.rs` — `ShmOwnerHeader` and `owner_alive()` from `t01`.
- `src/rs/vertebra-http/src/workers/factory.rs` — `cleanup_stale_shm` and `install_panic_cleanup_hook`.
- `src/rs/vertebra-core/tests/it/shm.rs` — SHM lifecycle integration tests.

## Contracts & Edge Cases

- Store up to 256 owned POSIX SHM names in a lock-free fixed-byte slot array in `cleanup.rs` so `cleanup_all()` is async-signal-safe (`shm_unlink` only).
- Replace pattern-only `cleanup_stale_shm` in `vertebra-http/src/workers/factory.rs` with `reap_dead_owner_segments` and wire `install_signal_cleanup`.
- **Edge Case**: Segments owned by a live peer supervisor PID are preserved during startup reaping.
- **Edge Case**: Exceeding 256 owned slots logs once and leaves existing registered names intact without heap allocation in signal context.

## Test Matrix

- Subprocess creating two named segments and panicking unlinks both segments before exit.
- `reap_dead_owner_segments` removes dead-PID segments while leaving current-PID and live-child segments untouched.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http --features tls,compression,gcp
```

## Acceptance Criteria

- [ ] `cleanup_all()` unlinks all registered owned segments on panic and `SIGTERM`/`SIGINT`.
- [ ] Startup reaping checks `ShmOwnerHeader` PID liveness before unlinking.
- [ ] Zero regressions in the chapter fast verification gate.

[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Process SHM cleanup hook.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor worker spawning, panic hook, and stale SHM reaper.
