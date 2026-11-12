---
type: Task
id: "broker-pool-sharing:ch01:t01"
chapter_id: ch01-shm-segment-ownership
title: Atomic create-or-attach with owner header
description: Add ShmRegion::open_or_create_named with O_EXCL-only creation, an init_state/owner_pid/owner_generation header, dead-owner replacement, and stop create_named from unlinking live segments.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/tests/it/shm.rs
  - src/rs/vertebra-core/src/broker.rs
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch01-shm-segment-ownership/spec.md
files:
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/tests/it/shm.rs
tests:
  - src/rs/vertebra-core/tests/it/shm.rs
estimated_loc: 220
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: create-named
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion::create_named
    title: Unlink-then-create region constructor
  - id: open-named
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion::open_named
    title: Named region attach
  - id: stale-test
    resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
    title: Replace-on-create test
tags:
  - shm
  - race
---

# Task t01: Atomic Create-or-Attach with Owner Header

## Objective & Scope Boundary

Implement [#95](https://github.com/cofin/vertebra/issues/95) at the region layer in `src/rs/vertebra-core/src/shm.rs` [^create-named] [^open-named]:

1. Add `ShmOwnerHeader` (first 64 bytes of every named region created through the new path) and `NamedAttach`.
2. Add `ShmRegion::open_or_create_named(name, size, init)`: `shm_open(O_RDWR)`; on `ENOENT` `shm_open(O_CREAT|O_EXCL|O_RDWR, 0o600)`, `ftruncate`, write `owner_pid = getpid()`, `owner_generation` (monotonic nanos), run `init`, then store `init_state = SHM_INIT_READY` with `Release`; on `EEXIST` loop back to open. After attach, spin-wait (`Acquire`, 1 ms sleep, 2 s cap) for `READY`; on timeout with a dead owner unlink and retry once, with a live owner return `ShmError::InitTimeout`.
3. Remove the unconditional `shm_unlink` from `create_named`; `create_named` now fails with `ShmError::InUse` on `EEXIST` when the owner is alive and unlinks only when `owner_alive()` is false.
4. Rewrite `stale_region_replaced_on_create_named` into `stale_region_replaced_only_when_owner_dead` and add race/live-owner tests.

Out of scope: caller migration (`t03`), registry (`t02`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/shm.rs` — `ShmRegion`, `create_named`, `open_named`, `ShmError`, existing unit tests.
- `src/rs/vertebra-core/tests/it/shm.rs` — `stale_region_replaced_on_create_named`.
- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseTable::create_with_statements` (header offset consumer).
- Chapter spec contracts.

## Symbol & Signature Contracts

- `pub const SHM_INIT_PENDING: u32 = 0; pub const SHM_INIT_READY: u32 = 1;`
- `#[repr(C, align(64))] pub struct ShmOwnerHeader { pub init_state: AtomicU32, pub owner_pid: AtomicU32, pub owner_generation: AtomicU64 }`
- `pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }` with `pub fn into_region(self) -> ShmRegion` and `pub fn created(&self) -> bool`.
- `pub fn open_or_create_named(name: &str, size: usize, init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>) -> Result<NamedAttach, ShmError>`.
- `pub fn owner_pid(&self) -> Option<u32>`; `pub fn owner_alive(&self) -> bool` (`libc::kill(pid, 0) == 0 || errno == EPERM`).
- `ShmError::InUse { name: String, owner_pid: u32 }`, `ShmError::InitTimeout { name: String }`.
- Payload offset: callers address payload at `ShmRegion::payload_offset() == 64`.

## Edge Cases & Error Semantics

- `EACCES` / `EINVAL` on open: returned unchanged; never falls through to create.
- `init` returns `Err`: creator unlinks its own segment and returns the error; attachers waiting see the name disappear and retry create.
- Size mismatch on attach (`fstat` smaller than `size`): `ShmError::InvalidSize`.
- macOS: `shm_open` names limited to 31 bytes; existing name validation stays.
- PID reuse: generation stored alongside PID; documented in the limitations record (`ch09 / t02`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/shm.rs`. Dependency category: local OS resource.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Race | 8 `fork()` children call `open_or_create_named("/vtb-it-race-<parent pid>", 4096, init)` behind a pipe barrier | Exactly one child reports `created() == true`; parent counts one `/dev/shm` entry (Linux) |
| Attach sees init | Child creates with `init` writing `0xAB` at payload byte 0; second opener | Attacher reads `0xAB`, `init_state == READY` |
| Live owner | Creator holds region; second `create_named` same name | `Err(ShmError::InUse { owner_pid: <creator pid> })` |
| Dead owner | Child creates then `_exit(9)` without unlink; parent `create_named` | `Ok`, new `owner_pid == getpid()` |
| Init failure | `init` returns `Err(ShmError::InvalidSize)` | Error returned; name absent afterwards |
| Non-ENOENT | Name with invalid characters `"bad/name"` | Existing name-validation error, no create attempt |

## Verification

- Red: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it shm::` fails before implementation.
- Green: same command exits `0`; `rm -f /dev/shm/vtb-it-*` before running.
- Lint: `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp -- -D warnings`.

## Acceptance Criteria

- [ ] `open_or_create_named` yields one creator under concurrent starters (R1).
- [ ] `create_named` never unlinks a segment whose owner is alive (R1).
- [ ] The rewritten stale-region test requires a dead owner (R1).

## Notes & Discoveries

- [2026-11-12 01:40] `create_named` unlinks before `O_EXCL`, so the exclusive flag is ineffective at HEAD; the race test is the regression gate.

[^create-named]: `src/rs/vertebra-core/src/shm.rs::ShmRegion::create_named` — Unlink-then-create constructor.
[^open-named]: `src/rs/vertebra-core/src/shm.rs::ShmRegion::open_named` — Named region attach.
[^stale-test]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Replace-on-create test.
