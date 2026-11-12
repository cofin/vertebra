---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch05-lease-safety-and-host
title: "Chapter 5: Lease Safety and Broker Host"
description: Quarantine state for connections with uncertain transaction outcomes, authenticated worker attach over a private Unix socket, a shared standalone/supervisor broker host lifecycle, and session affinity with VTBARROW streaming and cancel propagation.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-pool-partitions-and-dbrpc
  - ch03-engine-registry-capsule
replan_trigger: null
current_task: null
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
  - id: lease-slot
    resource: src/rs/vertebra-core/src/broker.rs::LeaseSlot
    title: Lease slot without quarantine state
  - id: reclaim
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker
    title: Reclamation by state change only
  - id: from-fd
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker::from_fd
    title: Attach from an inherited or received file descriptor
  - id: factory-env
    resource: src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle::respawn
    title: Supervisor passes VERTEBRA_BROKER_SHM_FD to workers
  - id: proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxyAsyncDriver
    title: Worker-side proxy driver
tags:
  - quarantine
  - auth
  - host
---

# Chapter 5: Lease Safety and Broker Host

<!-- truth: start -->
- `LeaseState::{Free, Leased, Quarantined}` lives in `LeaseSlot.state`; quarantined slots count in `active_leases` until `dispose_quarantined` [^lease-slot].
- `vertebra.extensions.broker.errors.LeaseQuarantinedError` reaches the worker; the broker never replays the statement.
- External workers attach through `BrokerAttachClient` over a `0600` Unix socket in a `0700` directory owned by the broker UID; the secret is compared with `hmac.compare_digest` and the SHM fd travels by `SCM_RIGHTS` to `LeaseBroker.from_fd` [^from-fd].
- Supervisor-spawned workers attach through the inherited `VERTEBRA_BROKER_SHM_FD` and need no socket [^factory-env].
- `BrokerHost` runs the same dispatch loop for `vtb broker serve` and the supervisor-hosted broker.
- Leases pin sessions across transactions; results stream as `VTBARROW` or row frames; `cancel()` propagates to the engine [^proxy].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Slots have no state for uncertain outcomes; `reclaim_worker` frees slots by state change only, so a connection whose rollback failed returns to the pool ([#98](https://github.com/cofin/vertebra/issues/98)) [^lease-slot] [^reclaim]. Attach is by name only ([#97](https://github.com/cofin/vertebra/issues/97)). There is no standalone broker host for external `uvicorn`/`granian` workers, and the proxy lacks a streaming/cancel contract on the new frames [^proxy].

### 1.2 Non-Goals

- Provider-side release logic (`ch06`); reclamation ROLLBACK (`ch08 / t02`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Lease state | `src/rs/vertebra-core/src/broker.rs::{LeaseSlot,ShmLeaseTable::release,reclaim_worker}` [^lease-slot] [^reclaim] | SHM. |
| Attach | `src/rs/vertebra-py/src/broker.rs::LeaseBroker::{from_fd,from_env}` [^from-fd] | Local Unix socket. |
| Supervisor | `src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle::respawn` [^factory-env] | Process spawning. |
| CLI | `src/py/vertebra/cli/workers.py`, `src/py/vertebra/cli/__init__.py` | `vtb` command group. |
| Proxy | `src/py/vertebra/extensions/sqlspec/proxy.py::{ProxySyncDriver,ProxyAsyncDriver}` [^proxy] | Worker process. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md); `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/python-framework/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11**: Quarantine (`t01`). **R12**: Authenticated attach (`t02`). **R13**: Broker host lifecycle (`t03`). **R14**: Session affinity and streaming (`t04`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
#[repr(u32)] pub enum LeaseState { Free = 0, Leased = 1, Quarantined = 2 }
impl ShmLeaseTable {
    pub fn quarantine(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn dispose_quarantined(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn quarantined_count(&self) -> u32;
}
```

```python
class LeaseQuarantinedError(VertebraError):
    slot_index: int
    generation: int
class BrokerAuthenticationError(BrokerAttachError): ...
class BrokerAttachServer:
    def __init__(self, socket_path: Path, secret: bytes, broker: LeaseBroker, *, timeout: float = 2.0) -> None: ...
    def serve_forever(self) -> None: ...
    def close(self) -> None: ...
class BrokerAttachClient:
    @staticmethod
    def attach(socket_path: Path, secret: bytes, *, timeout: float = 2.0) -> LeaseBroker: ...
class BrokerHost:
    def __init__(self, config: BrokerConfig, *, socket_path: Path | None = None, secret: bytes | None = None) -> None: ...
    def start(self) -> None: ...
    def stop(self, timeout: float = 10.0) -> list[str]: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `vtb broker serve --socket PATH` → `BrokerHost.start` → `LeaseBroker.open_or_create_named` + `BrokerEngineDiscovery.register_installed` + `BrokerAttachServer.serve_forever` (thread).
- External worker `BrokerConfig(attach_socket=PATH)` → `BrokerAttachClient.attach(PATH, secret from VERTEBRA_BROKER_SECRET_FILE)` → `LeaseBroker.from_fd`.
- Release with uncertain outcome → `LeaseBroker.quarantine` → host thread closes the physical connection → `dispose_quarantined`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Quarantine admission | Pool size 1, slot quarantined | Next acquire times out until disposal | Capacity accounting |
| Dispose wrong state | `dispose_quarantined` on `Leased` slot | `BrokerError::InvalidState` | State machine |
| Wrong secret | Mismatch | `BrokerAuthenticationError`; server closes; no fd sent | Authenticated pool |
| Squatted socket | Socket owner UID != `os.getuid()` or dir mode not `0700` | Client refuses before sending secret | Secret confidentiality |
| Stalled client | Client connects, sends nothing | Server times out after 2 s; second client served concurrently | No head-of-line block |
| Secret in repr | `repr(BrokerAttachServer(...))` | Contains `secret=<redacted>` | Redaction |
| Host shutdown | Undisposed quarantined connection at `stop` | Returned in report list; stop completes within timeout | Bounded shutdown |
| Cancel | Worker cancels a running `fetch_chunk` | `DbRpcStatus.Cancelled`; engine cancel invoked | Cancel propagation |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a running `vtb broker serve`, when an external process attaches with the wrong secret, then it receives `BrokerAuthenticationError` and `stats().active_leases` is unchanged.
- Given a quarantined slot, when the host disposes it, then the backend connection is closed and capacity returns.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Attach transport | Unix socket + `SCM_RIGHTS` | Shared secret in segment header: readable by anyone who can open the name. |
| Secret source | `VERTEBRA_BROKER_SECRET_FILE` (mode `0600`) | Env var secret: visible via `/proc/<pid>/environ` to same-UID processes. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; the security boundary and quarantine semantics are documented in `ch09 / t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11 | [t01](tasks/t01-lease-quarantine-state.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R12 | [t02](tasks/t02-authenticated-attach.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| R13 | [t03](tasks/t03-broker-host-lifecycle.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R14 | [t04](tasks/t04-session-affinity-and-streaming.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch05:t01 — Lease quarantine state (tasks/t01-lease-quarantine-state.md)
- [ ] broker-pool-sharing:ch05:t02 — Authenticated worker attach (tasks/t02-authenticated-attach.md)
- [ ] broker-pool-sharing:ch05:t03 — Broker host lifecycle and `vtb broker serve` (tasks/t03-broker-host-lifecycle.md)
- [ ] broker-pool-sharing:ch05:t04 — Session affinity, VTBARROW streaming, and cancel (tasks/t04-session-affinity-and-streaming.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker && uv run --no-sync pytest src/py/tests/unit/extensions -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed).
- **Active Invariants**: A quarantined connection is never re-leased; secrets never appear in repr, logs, or metrics.

[^lease-slot]: `src/rs/vertebra-core/src/broker.rs::LeaseSlot` — Lease slot.
[^reclaim]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker` — Reclamation.
[^from-fd]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker::from_fd` — fd attach.
[^factory-env]: `src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle::respawn` — Supervisor env.
[^proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxyAsyncDriver` — Proxy driver.
