---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch01-shm-segment-ownership
title: "Chapter 1: SHM Segment Ownership"
description: Atomic create-or-attach for named SHM segments with owner PID and generation, an owned-segment registry behind cleanup_all and the panic/signal hooks, owner-aware startup reaping, and fail-loud Python broker attach.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "broker-pool-sharing:ch01:t01"
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: core-shm
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion::create_named
    title: Named region creation (unlink then O_CREAT|O_EXCL)
  - id: shm-it
    resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
    title: Test asserting replace-on-create
  - id: core-cleanup
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: Empty cleanup function
  - id: worker-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: Panic hook and pattern-based stale reaper
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: Python open-or-create fallback
tags:
  - shm
  - ownership
  - cleanup
---

# Chapter 1: SHM Segment Ownership

<!-- truth: start -->
- `ShmRegion::open_or_create_named` opens first, creates with `O_CREAT|O_EXCL` only on `ENOENT`, and on `EEXIST` re-opens and waits for the header `init_state` to reach `READY` [^core-shm].
- The region header records `owner_pid` and `owner_generation`; a segment is unlinked only when `kill(owner_pid, 0)` reports `ESRCH` [^core-shm].
- `cleanup::register_owned` / `unregister_owned` maintain a fixed-capacity, async-signal-safe owned-name table; `cleanup_all` unlinks every owned name [^core-cleanup].
- The supervisor installs the panic hook plus SIGTERM/SIGINT handlers that call `cleanup_all`; startup calls `reap_dead_owner_segments` instead of pattern-only reaping [^worker-factory].
- Python attach raises `BrokerSegmentInUseError` / `BrokerAttachError` and never calls `create_named` after a failed `open_named` [^broker-transport].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`ShmRegion::create_named` calls `shm_unlink` before `O_CREAT|O_EXCL`, so the exclusive flag never protects a live segment, and `stale_region_replaced_on_create_named` asserts that behavior [^core-shm] [^shm-it]. `_BaseSHMBrokerTransport.__init__` turns any `open_named` failure into `create_named`, so N starters or a single ABI validation error produce split pools with independent admission limits ([#95](https://github.com/cofin/vertebra/issues/95)) [^broker-transport]. `cleanup_all` is empty while the panic hook calls it, and `cleanup_stale_shm` reaps by name without checking whether the owner is alive ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].

### 1.2 Non-Goals

- Attach authentication (`ch05 / t02`); lease state changes (`ch05 / t01`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Regions | `src/rs/vertebra-core/src/shm.rs::ShmRegion::{create_named,open_named}` [^core-shm] | Local OS resource. |
| Callers | `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::create_with_statements`, `src/rs/vertebra-core/src/vector.rs`, `src/rs/vertebra-db/src/arrow_shm.rs`, `src/rs/vertebra-py/src/broker.rs::{LeaseBroker::create_named,StatementCache::create_named}` | In-process callers. |
| Cleanup | `src/rs/vertebra-core/src/cleanup.rs::cleanup_all`, `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` [^core-cleanup] [^worker-factory] | Supervisor hooks. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Ring Buffers and SHM](../../../../knowledge/architecture/ipc/ring-buffers-and-shm.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md); `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: Atomic create-or-attach with owner liveness (`t01`).
- **R2**: Owned-segment registry, signal/panic cleanup, owner-aware reaping (`t02`).
- **R3**: Fail-loud Python attach and caller audit (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
pub const SHM_INIT_PENDING: u32 = 0;
pub const SHM_INIT_READY: u32 = 1;
#[repr(C)] pub struct ShmOwnerHeader { pub init_state: AtomicU32, pub owner_pid: AtomicU32, pub owner_generation: AtomicU64 }
pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }
impl ShmRegion {
    pub fn open_or_create_named(name: &str, size: usize, init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>) -> Result<NamedAttach, ShmError>;
    pub fn owner_pid(&self) -> Option<u32>;
    pub fn owner_alive(&self) -> bool;
}
pub enum ShmError { /* existing variants */ InUse { name: String, owner_pid: u32 }, InitTimeout { name: String } }
pub fn register_owned(name: &str);
pub fn unregister_owned(name: &str);
pub fn cleanup_all();
pub fn install_signal_cleanup() -> std::io::Result<()>;
pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32;
```

```python
class BrokerAttachError(VertebraError): ...
class BrokerSegmentInUseError(BrokerAttachError): ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `ShmLeaseTable::open_or_create_with_statements` → `ShmRegion::open_or_create_named(name, size, init_header)` → `register_owned(name)` on `Created`.
- `LeaseBroker.open_or_create_named` (PyO3) → `ShmLeaseTable::open_or_create_with_statements` → maps `ShmError::InUse` to `BrokerSegmentInUseError`.
- `vertebra-http` startup → `reap_dead_owner_segments(&["/vertebra-", "/vtb-"])` → `install_panic_hook` + `install_signal_cleanup`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Race | 8 processes, one name | Exactly one `Created` | One pool |
| Initializer crash | Creator dies before `READY`, owner dead | Attachers time out after 2 s, verify owner dead, unlink and create | Progress |
| Live owner, bad ABI | ABI mismatch, owner alive | `ShmError::InUse` | No replacement |
| Non-ENOENT open | `EACCES` | Error propagates | Fail loud |
| Panic | Panic after creating two segments | Both unlinked | No leak |
| Peer segment | Owner PID alive at startup | Reaper keeps it | Live peers safe |
| SIGKILL orphan | Owner PID dead | Reaper unlinks it | Orphans reclaimed |
| Registry full | More than 256 owned names | `register_owned` logs once and skips; documented limit | Signal safety |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given 8 forked processes, when each calls `open_or_create_named("/vtb-race-<pid>")`, then one returns `Created` and `/dev/shm` holds one entry.
- Given a child that creates a segment and panics, when it exits, then the name is gone from `/dev/shm`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Owner liveness | `kill(pid, 0)` plus generation | `flock` on a sidecar file: second resource to clean up; macOS `/dev/shm` absent. |
| Registry | Fixed array of `AtomicPtr`-free byte slots | `Mutex<Vec<String>>`: not async-signal-safe. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in `ch09 / t01` and `ch09 / t02`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-atomic-create-or-attach.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs` |
| R2 | [t02](tasks/t02-owned-segment-registry.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/shm.rs` |
| R3 | [t03](tasks/t03-fail-loud-broker-attach.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch01:t01 — Atomic create-or-attach with owner header (tasks/t01-atomic-create-or-attach.md)
- [ ] broker-pool-sharing:ch01:t02 — Owned-segment registry, signal cleanup, owner-aware reaper (tasks/t02-owned-segment-registry.md)
- [ ] broker-pool-sharing:ch01:t03 — Fail-loud Python broker attach and caller audit (tasks/t03-fail-loud-broker-attach.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it shm && make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`0/3` completed).
- **Active Invariants**: No code path unlinks a segment whose owner is alive.

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion::create_named` — Named region creation.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Replace-on-create test.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Empty cleanup function.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Panic hook and reaper.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python open-or-create fallback.
