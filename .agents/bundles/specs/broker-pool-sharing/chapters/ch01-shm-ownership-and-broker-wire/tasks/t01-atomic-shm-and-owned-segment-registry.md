---
type: Task
id: broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t01
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-broker-wire
title: Enforce Atomic O_CREAT | O_EXCL SHM Creation and Supervisor OwnedShmRegistry
description: Separate create_exclusive vs attach_existing SHM constructors and track owned segments in OwnedShmRegistry for deterministic unlink.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on: []
files:
- src/rs/vertebra-core/src/shm.rs
- src/rs/vertebra-core/src/cleanup.rs
- src/rs/vertebra-http/src/workers/factory.rs
tests:
- src/rs/vertebra-core/tests/it/shm.rs
read_set:
- src/rs/vertebra-core/src/shm.rs
- src/rs/vertebra-core/tests/it/shm.rs
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-core/src/cleanup.rs
- src/rs/vertebra-http/src/workers/factory.rs
estimated_loc: 220
tags:
- shm
- ownership
- ipc
- cleanup
- signals
sources:
- id: core-shm
  resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
  title: Named POSIX shared-memory regions
- id: shm-it
  resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
  title: Existing SHM integration test suite
- id: core-cleanup
  resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
  title: Process SHM cleanup hook
- id: worker-factory
  resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
  title: Supervisor worker spawning, panic hook, and stale SHM reaper
---

# Task `t01`: Enforce Atomic O_CREAT | O_EXCL SHM Creation and Supervisor OwnedShmRegistry

## Objective & Scope Boundary

Implement atomic `ShmRegion::open_or_create_named` and `ShmOwnerHeader` liveness probing per [`../spec.md` §2](../spec.md) ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^shm-it]. Out of scope: signal handlers (`t02`) and Python bindings (`t03`).

Implement `register_owned`, `unregister_owned`, `cleanup_all`, `install_signal_cleanup`, and `reap_dead_owner_segments` per [`../spec.md` §2](../spec.md) ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory]. Out of scope: Python broker errors (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/shm.rs` — Current `create_named` pre-unlink and `open_named` implementation.
- `src/rs/vertebra-core/tests/it/shm.rs` — Existing `stale_region_replaced_on_create_named` test to invert.
- `src/rs/vertebra-core/src/broker.rs` — Downstream `ShmLeaseTable` region sizing and header layout.

- `src/rs/vertebra-core/src/cleanup.rs` — Current empty `cleanup_all()` stub.
- `src/rs/vertebra-core/src/shm.rs` — `ShmOwnerHeader` and `owner_alive()` from `t01`.
- `src/rs/vertebra-http/src/workers/factory.rs` — `cleanup_stale_shm` and `install_panic_cleanup_hook`.
- `src/rs/vertebra-core/tests/it/shm.rs` — SHM lifecycle integration tests.

## Contracts & Edge Cases

- Add `SHM_INIT_PENDING`, `SHM_INIT_READY`, `#[repr(C)] ShmOwnerHeader`, `NamedAttach`, `ShmError::{InUse, InitTimeout}`, `ShmRegion::open_or_create_named`, `owner_pid()`, and `owner_alive()`.
- Remove unconditional `shm_unlink` before `O_CREAT | O_EXCL` in `ShmRegion::create_named`; reclaim an existing name only when `owner_alive()` is `false`.
- **Edge Case**: If creator crashes while `init_state == SHM_INIT_PENDING`, attachers spin/backoff up to 2 s, re-check `owner_alive()`, and reclaim only when `ESRCH`.
- **Edge Case**: macOS XNU segment names stay `<= 31` bytes including leading `/`.

- Store up to 256 owned POSIX SHM names in a lock-free fixed-byte slot array in `cleanup.rs` so `cleanup_all()` is async-signal-safe (`shm_unlink` only).
- Replace pattern-only `cleanup_stale_shm` in `vertebra-http/src/workers/factory.rs` with `reap_dead_owner_segments` and wire `install_signal_cleanup`.
- **Edge Case**: Segments owned by a live peer supervisor PID are preserved during startup reaping.
- **Edge Case**: Exceeding 256 owned slots logs once and leaves existing registered names intact without heap allocation in signal context.

## Test Matrix

- 8-process / multi-thread contention on `open_or_create_named` yields exactly 1 `NamedAttach::Created` and 7 `NamedAttach::Attached`.
- Calling `create_named` against a segment with a live `owner_pid` returns `Err(ShmError::InUse)` without replacing the inode.
- Dead-PID segment (`ESRCH`) is unlinked and recreated with new `owner_pid` and incremented `owner_generation`.

- Subprocess creating two named segments and panicking unlinks both segments before exit.
- `reap_dead_owner_segments` removes dead-PID segments while leaving current-PID and live-child segments untouched.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http --features tls,compression,gcp
```

## Acceptance Criteria

- [ ] `ShmRegion::create_named` and `open_or_create_named` never unlink a segment owned by a live PID.
- [ ] All `shm` integration tests pass with unique segment names and clean `/dev/shm` teardown.
- [ ] `cleanup_all()` unlinks all registered owned segments on panic and `SIGTERM`/`SIGINT`.
- [ ] Startup reaping checks `ShmOwnerHeader` PID liveness before unlinking.
- [ ] Zero regressions in the fast verification gate.

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Existing SHM integration test suite.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Process SHM cleanup hook.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor worker spawning, panic hook, and stale SHM reaper.
