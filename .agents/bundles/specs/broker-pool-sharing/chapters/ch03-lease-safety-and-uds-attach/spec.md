---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch03-lease-safety-and-uds-attach
title: 'Chapter 03: Quarantine Reclamation, Authenticated UDS Broker Attach, Session Affinity, and CLI'
description: Implement crashed-worker lease quarantine and reclamation, SCM_RIGHTS/peercred UDS broker attach, BrokerHost reclamation loop, transaction session affinity, streaming Arrow cursors, and vtb broker CLI.
status: stable
state: completed
last_reviewed_commit: ae86568f
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
- ch02-engine-capsules-and-discovery
plan_commit: 1b191388
updated_at: '2026-11-14T23:06:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:06:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:06:00Z'
  commit: 1b191388
tags:
- quarantine
- uds
- broker
- metrics
sources:
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Type stubs for vertebra._vertebra
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
- id: vtb-cli
  resource: src/py/vertebra/cli/__init__.py::cli
  title: Root vtb Click command group
---

# Chapter 03: Quarantine Reclamation, Authenticated UDS Broker Attach, Session Affinity, and CLI

<!-- truth: start -->
- `LeaseState::{Free = 0, Leased = 1, Quarantined = 2}` ensures uncertain connections count against pool admission until `dispose_quarantined` completes ([#98](https://github.com/cofin/vertebra/issues/98)) [^core-broker].
- `BrokerAttachServer` and `BrokerAttachClient` enforce `0700` parent directory, `0600` Unix socket, peer UID match, constant-time secret verification, and `SCM_RIGHTS` SHM FD delivery ([#97](https://github.com/cofin/vertebra/issues/97)) [^py-broker].
- `BrokerHost` (`vtb broker serve` and supervisor mode) executes server-side `ROLLBACK` on `reclaim_worker_leases(pid)` before freeing slots and disposes quarantined connections on their owning thread [^core-broker] [^vtb-cli].
- `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` pin session affinity, stream `VTBARROW` / row chunks with `cancel()`, and expose `PoolMetrics` + quarantine counters on `BrokerMetrics` [^broker-transport] [^broker-plugin] [^vertebra-pyi].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, `ShmLeaseTable::reclaim_worker` frees dead-worker slots without running `ROLLBACK` on the physical connection, slots lack a `Quarantined` state ([#98](https://github.com/cofin/vertebra/issues/98)), workers attach by unauthenticated segment name ([#97](https://github.com/cofin/vertebra/issues/97)), and `BrokerMetrics` lacks per-pool and quarantine counters [^core-broker] [^broker-transport].

**Non-Goals:** SQLSpec provider protocol refactor (`ch04:t01`); Live Docker multi-process acceptance (`ch04:t02`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Quarantine & reclaim | `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs` [^core-broker] [^py-broker] | `quarantine`, `dispose_quarantined`, `reclaim_worker_leases` |
| UDS attach | `vertebra-core/src/broker_attach.rs`, `lib.rs` | `0700`/`0600`/UID check + constant-time secret + `SCM_RIGHTS` |
| Host & CLI | `extensions/broker/host.py`, `cli/__init__.py` [^vtb-cli] | `BrokerHost`, `ShutdownReport`, `vtb broker serve` |
| Affinity & metrics | `extensions/broker/{transport,plugin}.py` [^broker-transport] [^broker-plugin] | `PoolMetrics`, `BrokerMetrics`, session pinning, `VTBARROW` streaming |

**Knowledge Anchors:** [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [SHM Supervision and Recovery](../../../../knowledge/architecture/runtime/shm-supervision-and-recovery.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R11 / R19a**: Add `LeaseState::Quarantined`, `quarantine`, `dispose_quarantined`, `quarantined_count`, `ReclaimedLease`, and `reclaim_worker_leases` (`t01`).
- **R12a**: Implement pure-Rust `BrokerAttachServer` and `BrokerAttachClient` over permission-checked Unix domain sockets with `SCM_RIGHTS` FD passing (`t02`).
- **R12b / R13 / R19b**: Expose PyO3 `BrokerAttachServer`/`BrokerAttachClient` and implement `BrokerHost` with server-side crash `ROLLBACK`, owning-thread disposal, and `ShutdownReport` (`t03`).
- **R14 / R21**: Wire `vtb broker serve` CLI, session affinity pinning, `VTBARROW` / chunked streaming + `cancel()`, and per-pool `PoolMetrics` / quarantine counters (`t04`).

```rust
#[repr(u32)]
pub enum LeaseState { Free = 0, Leased = 1, Quarantined = 2 }
pub struct ReclaimedLease { pub slot_index: u32, pub generation: u64, pub pool_id: u16 }
impl ShmLeaseTable {
    pub fn quarantine(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn dispose_quarantined(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
    pub fn quarantined_count(&self) -> u32;
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> Vec<ReclaimedLease>;
}
```

```python
class PoolMetrics(msgspec.Struct, frozen=True, gc=False):
    pool_name: str
    active_leases: int
    quarantined: int
    total_quarantined: int
    total_disposed: int
    total_reclaimed: int

class ShutdownReport(msgspec.Struct, frozen=True, gc=False):
    undisposed: tuple[tuple[str, int, int], ...]

class BrokerHost:
    def __init__(self, config: BrokerConfig, *, socket_path: Path | None = None, secret: bytes | None = None) -> None: ...
    def start(self) -> None: ...
    def on_worker_exit(self, worker_pid: int) -> int: ...
    def stop(self, timeout: float = 10.0) -> ShutdownReport: ...
```

**Integration Flow:**
- External worker -> `BrokerAttachClient.attach(socket_path, secret)` -> `LeaseBroker.from_fd(received_fd)`.
- Dead worker `pid` -> `BrokerHost.on_worker_exit(pid)` -> `reclaim_worker_leases(pid)` -> owning thread executes `ROLLBACK` -> `release` on success or `quarantine` + `close` + `dispose_quarantined` on failure.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wrong UDS secret | HMAC digest mismatch | `BrokerAuthenticationError`; zero FD sent | Constant-time auth |
| Permissive socket dir | Directory mode `0755` or foreign UID | Refused before binding or sending secret | No secret leakage |
| Crash ROLLBACK fails | Broken backend socket during reclaim | Slot quarantined, physical connection closed, then disposed | Zero dirty re-lease |
| Disposal timeout | Backend close hangs past `stop(timeout)` | `ShutdownReport.undisposed` lists `(pool, slot, gen)` | Bounded shutdown |

## 3. Documentation & OKF Knowledge Plan

Security boundary and quarantine semantics are documented in `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Lease Quarantine State Machine and Dead-Worker Slot Reclamation | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `src/rs/vertebra-core/tests/it/broker.rs`, `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| `t02` | Implement Peer-Credential Authenticated UDS Broker Handshake and FD Passing | `src/rs/vertebra-core/src/broker_attach.rs`, `src/rs/vertebra-core/src/lib.rs` | `src/rs/vertebra-core/tests/it/shm.rs` |
| `t03` | Wire BrokerHost Background Reclaimer and Standalone Broker Daemon Lifecycle | `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi`, `src/py/vertebra/extensions/broker/host.py` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| `t04` | Implement Transaction Session Affinity, Streaming Cursors, Broker Metrics, and vtb broker CLI | `src/py/vertebra/extensions/broker/transport.py`, `src/py/vertebra/extensions/broker/plugin.py`, `src/py/vertebra/cli/__init__.py` | `src/py/tests/unit/extensions/test_shm_broker.py`, `src/py/tests/unit/extensions/test_broker_transport.py` |

## 5. Implementation Plan

- [x] [ae86568f] [t01-quarantine-and-reclaim-primitives.md](tasks/t01-quarantine-and-reclaim-primitives.md) — Add `LeaseState::Quarantined` and generation-fenced reclamation (`quarantine`, `dispose_quarantined`, `reclaim_worker_leases`) in `ShmLeaseTable`.
- [x] [ae86568f] [t02-uds-authenticated-attach.md](tasks/t02-uds-authenticated-attach.md) — Build Unix domain socket `SO_PEERCRED`/`LOCAL_PEERCRED` handshake and `SCM_RIGHTS` SHM file-descriptor passing.
- [x] [ae86568f] [t03-broker-host-reclamation-and-cli.md](tasks/t03-broker-host-reclamation-and-cli.md) — Expose UDS attach in `_vertebra` and implement `BrokerHost` reclamation and `ShutdownReport` in `host.py`.
- [x] [ae86568f] [t04-affinity-streaming-metrics-and-cli.md](tasks/t04-affinity-streaming-metrics-and-cli.md) — Pin active transactions to backend sessions, stream multi-batch `VTBARROW` frames, expose `PoolMetrics`, and wire `vtb broker serve` CLI commands.

## 6. Current State & Remaining Plan Snapshot

- **Completed (`ae86568f`)**: `state: completed`; `4/4` tasks completed and verified (`vertebra-core` unit/it/doc tests, `vertebra-mssql` lib tests, `87` Python pytest tests, `ruff`, `mypy`, `stubtest`, `clippy`, `make review-check`).
- **Active Invariants**: Quarantined slots count toward pool admission until disposed; attach secrets never appear in `repr`, logs, or metrics.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/extensions/test_broker_transport.py -q`

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^vtb-cli]: `src/py/vertebra/cli/__init__.py::cli` — Root vtb Click command group.
