---
type: Task
id: "broker-pool-sharing:ch01:t01"
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-lifecycle
title: "Atomic SHM Create-or-Attach with Owner Liveness Header"
description: "Implement ShmOwnerHeader, NamedAttach, and ShmRegion::open_or_create_named without unlinking live segments."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-core/src/shm.rs
tests:
  - src/rs/vertebra-core/tests/it/shm.rs
read_set:
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/tests/it/shm.rs
  - src/rs/vertebra-core/src/broker.rs
estimated_loc: 210
tags:
  - shm
  - ownership
  - ipc
sources:
  - id: core-shm
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
    title: "Named POSIX shared-memory regions"
  - id: shm-it
    resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
    title: "Existing SHM integration test suite"
---

# Task `t01`: Atomic SHM Create-or-Attach with Owner Liveness Header

## Objective & Scope Boundary

Implement atomic `ShmRegion::open_or_create_named` and `ShmOwnerHeader` liveness probing per [`../spec.md` §2](../spec.md) ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^shm-it]. Out of scope: signal handlers (`t02`) and Python bindings (`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/shm.rs` — Current `create_named` pre-unlink and `open_named` implementation.
- `src/rs/vertebra-core/tests/it/shm.rs` — Existing `stale_region_replaced_on_create_named` test to invert.
- `src/rs/vertebra-core/src/broker.rs` — Downstream `ShmLeaseTable` region sizing and header layout.

## Contracts & Edge Cases

- Add `SHM_INIT_PENDING`, `SHM_INIT_READY`, `#[repr(C)] ShmOwnerHeader`, `NamedAttach`, `ShmError::{InUse, InitTimeout}`, `ShmRegion::open_or_create_named`, `owner_pid()`, and `owner_alive()`.
- Remove unconditional `shm_unlink` before `O_CREAT | O_EXCL` in `ShmRegion::create_named`; reclaim an existing name only when `owner_alive()` is `false`.
- **Edge Case**: If creator crashes while `init_state == SHM_INIT_PENDING`, attachers spin/backoff up to 2 s, re-check `owner_alive()`, and reclaim only when `ESRCH`.
- **Edge Case**: macOS XNU segment names stay `<= 31` bytes including leading `/`.

## Test Matrix

- 8-process / multi-thread contention on `open_or_create_named` yields exactly 1 `NamedAttach::Created` and 7 `NamedAttach::Attached`.
- Calling `create_named` against a segment with a live `owner_pid` returns `Err(ShmError::InUse)` without replacing the inode.
- Dead-PID segment (`ESRCH`) is unlinked and recreated with new `owner_pid` and incremented `owner_generation`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it shm
```

## Acceptance Criteria

- [ ] `ShmRegion::create_named` and `open_or_create_named` never unlink a segment owned by a live PID.
- [ ] All `shm` integration tests pass with unique segment names and clean `/dev/shm` teardown.
- [ ] Zero regressions in the chapter fast verification gate.

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Existing SHM integration test suite.
