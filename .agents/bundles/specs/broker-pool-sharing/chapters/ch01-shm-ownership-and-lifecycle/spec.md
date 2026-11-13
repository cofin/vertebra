---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-lifecycle
title: "Chapter 01: SHM Segment Ownership and Lifecycle"
description: "Enforce atomic POSIX SHM create-or-attach with owner PID liveness, async-signal-safe owned-segment cleanup, and explicit Python owner/worker broker roles without destructive segment unlinking."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - shm
  - broker
  - ownership
  - cleanup
sources:
  - id: core-shm
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
    title: "Named POSIX shared-memory regions"
  - id: shm-it
    resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
    title: "Existing SHM integration test suite"
  - id: core-cleanup
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: "Process SHM cleanup hook"
  - id: worker-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: "Supervisor worker spawning, panic hook, and stale SHM reaper"
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
---

# Chapter 01: SHM Segment Ownership and Lifecycle

<!-- truth: start -->
- `ShmRegion::open_or_create_named` opens first, creates with `O_CREAT | O_EXCL` only on `ENOENT`, and never pre-unlinks a live segment ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^shm-it].
- `ShmOwnerHeader` records `init_state`, `owner_pid`, and `owner_generation`; orphan segments are reclaimed only when `kill(owner_pid, 0)` reports `ESRCH` [^core-shm].
- `cleanup::{register_owned, unregister_owned, cleanup_all, reap_dead_owner_segments}` replace the empty `cleanup_all()` and pattern-only `cleanup_stale_shm` ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].
- Python `_BaseSHMBrokerTransport` enforces explicit `role: Literal["owner", "worker"]` and raises `BrokerSegmentInUseError` / `BrokerAttachError` instead of falling back to `create_named` [^py-broker] [^broker-transport].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`ShmRegion::create_named` unconditionally calls `shm_unlink` before `shm_open(..., O_CREAT | O_EXCL)`, and `_BaseSHMBrokerTransport.__init__` catches bare `Exception` on `LeaseBroker.open_named` to invoke `create_named`, destroying or splitting live pools ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^broker-transport]. `cleanup::cleanup_all` is a no-op while `vertebra-http` `cleanup_stale_shm` sweeps `/dev/shm` by prefix without checking PID liveness ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].

**Non-Goals:** Unix socket attach authentication (`ch05`); Lease quarantine state (`ch05`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SHM regions | `src/rs/vertebra-core/src/shm.rs::ShmRegion` [^core-shm] | Atomic POSIX `/dev/shm` ownership header and liveness probes |
| Cleanup hooks | `src/rs/vertebra-core/src/cleanup.rs`, `vertebra-http/src/workers/factory.rs` [^core-cleanup] [^worker-factory] | Async-signal-safe owned-name table + PID-aware startup reaper |
| Broker attach | `src/rs/vertebra-py/src/broker.rs`, `extensions/broker/transport.py` [^py-broker] [^broker-transport] | Typed PyO3 `open_or_create_named` + Python `role` enforcement |

**Knowledge Anchors:** [Ring Buffers and SHM](../../../../knowledge/architecture/ipc/ring-buffers-and-shm.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Implement `ShmOwnerHeader`, `NamedAttach`, `ShmRegion::open_or_create_named`, `owner_pid()`, `owner_alive()`, and `ShmError::{InUse, InitTimeout}` without pre-unlinking live segments (`t01`).
- **R2**: Back `cleanup_all()` with a fixed-capacity async-signal-safe owned-segment table, wire SIGTERM/SIGINT + panic hooks, and replace prefix-only sweeping with `reap_dead_owner_segments` (`t02`).
- **R3a**: Expose `ShmLeaseTable::open_or_create_with_statements`, `LeaseBroker.open_or_create_named`, and `StatementCache.open_or_create_named` with matching `_vertebra.pyi` stubs (`t03`).
- **R3b**: Define `BrokerAttachError`, `BrokerSegmentInUseError`, `BrokerAuthenticationError`, and `LeaseQuarantinedError` in `extensions/broker/errors.py` and enforce explicit `owner`/`worker` roles in `transport.py` and `plugin.py` (`t04`).

```rust
pub const SHM_INIT_PENDING: u32 = 0;
pub const SHM_INIT_READY: u32 = 1;
#[repr(C)]
pub struct ShmOwnerHeader {
    pub init_state: AtomicU32,
    pub owner_pid: AtomicU32,
    pub owner_generation: AtomicU64,
}
pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }
impl ShmRegion {
    pub fn open_or_create_named(
        name: &str,
        size: usize,
        init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>,
    ) -> Result<NamedAttach, ShmError>;
    pub fn owner_pid(&self) -> Option<u32>;
    pub fn owner_alive(&self) -> bool;
}
pub fn register_owned(name: &str);
pub fn unregister_owned(name: &str);
pub fn cleanup_all();
pub fn install_signal_cleanup() -> std::io::Result<()>;
pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32;
```

```python
class BrokerAttachError(VertebraError): ...
class BrokerSegmentInUseError(BrokerAttachError): ...
class BrokerAuthenticationError(BrokerAttachError): ...
class LeaseQuarantinedError(VertebraError):
    slot_index: int
    generation: int
```

**Integration Flow:**
- `LeaseBroker.open_or_create_named` -> `ShmLeaseTable::open_or_create_with_statements` -> `ShmRegion::open_or_create_named` -> `cleanup::register_owned` on `Created`.
- Supervisor startup -> `reap_dead_owner_segments(&["/vertebra-", "/vtb-"])` -> `install_panic_cleanup_hook` + `install_signal_cleanup`.
- `_BaseSHMBrokerTransport(role="worker")` -> `LeaseBroker.open_named` only (raises `BrokerAttachError` on `ENOENT` or ABI mismatch without calling `create_named`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Concurrent start | 8 processes call `open_or_create_named` | 1 `Created`, 7 `Attached`; single `/dev/shm` inode | Shared admission cap |
| Live owner ABI mismatch | Header version differs while `owner_alive()` is true | `ShmError::InUse` -> `BrokerSegmentInUseError` | Live segment preserved |
| Dead owner orphan | `kill(owner_pid, 0) == ESRCH` | Unlink stale inode and recreate cleanly | SIGKILL recovery |
| Supervisor panic/signal | `panic!` or `SIGTERM` with owned segments | `cleanup_all()` unlinks owned names only | Peer segments untouched |

## 3. Documentation & OKF Knowledge Plan

User and OKF documentation updates execute in `ch08` (`t01`–`t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` | `t01` | `src/rs/vertebra-core/src/shm.rs` | `cargo test -p vertebra-core --test it shm` |
| `R2` | `t02` | `src/rs/vertebra-core/src/cleanup.rs`, `src/rs/vertebra-http/src/workers/factory.rs` | `cargo test -p vertebra-core -p vertebra-http` |
| `R3a` | `t03` | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `pytest src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| `R3b` | `t04` | `src/py/vertebra/extensions/broker/{errors,transport,plugin}.py` | `pytest src/py/tests/unit/extensions/test_shm_lease_broker.py` |

## 5. Implementation Plan

- [ ] [t01-atomic-create-or-attach.md](tasks/t01-atomic-create-or-attach.md) — Implement ShmOwnerHeader, NamedAttach, and ShmRegion::open_or_create_named without unlinking live segments.
- [ ] [t02-owned-segment-registry.md](tasks/t02-owned-segment-registry.md) — Back cleanup_all() with a fixed-slot owned-segment table, signal/panic hooks, and PID-aware startup reaping.
- [ ] [t03-pyo3-lease-broker-attach.md](tasks/t03-pyo3-lease-broker-attach.md) — Expose open_or_create_named and owner_pid on LeaseBroker and StatementCache with synchronized _vertebra.pyi stubs.
- [ ] [t04-explicit-owner-worker-roles.md](tasks/t04-explicit-owner-worker-roles.md) — Add BrokerAttachError hierarchy and enforce explicit owner/worker roles in _BaseSHMBrokerTransport and BrokerPlugin.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: No code path unlinks a POSIX SHM segment whose recorded `owner_pid` is alive.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py`

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Existing SHM integration test suite.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Process SHM cleanup hook.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor worker spawning, panic hook, and stale SHM reaper.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
