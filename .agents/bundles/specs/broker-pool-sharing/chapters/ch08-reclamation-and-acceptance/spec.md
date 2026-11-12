---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch08-reclamation-and-acceptance
title: "Chapter 8: Reclamation and Acceptance"
description: PostgreSQL broker capsule, crash reclamation with server-side ROLLBACK and owning-thread disposal of quarantined connections, multi-process PostgreSQL and MySQL acceptance, and per-pool and quarantine metrics.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-engine-backend-registration
  - ch05-lease-safety-and-host
  - ch06-sqlspec-proxy
replan_trigger: null
current_task: "broker-pool-sharing:ch08:t01"
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
  - id: pg-module
    resource: src/rs/vertebra-postgres/src/lib.rs::_postgres
    title: PostgreSQL PyO3 module entry point
  - id: reclaim
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker
    title: Worker lease reclamation by PID
  - id: dialect-tags
    resource: src/rs/vertebra-core/src/broker.rs::DIALECT_TAG_POSTGRES
    title: Dialect tag constants
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.snapshot_metrics
    title: Broker metric snapshot for OpenTelemetry and ZPages
  - id: broker-metrics
    resource: src/py/vertebra/extensions/broker/transport.py::BrokerMetrics
    title: Broker metrics struct
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: pytest-databases PostgreSQL and MySQL service fixtures
tags:
  - broker
  - reclamation
  - acceptance
  - metrics
---

# Chapter 8: Reclamation and Acceptance

<!-- truth: start -->
- `ShmLeaseTable::reclaim_worker` bumps generation and frees every non-free slot owned by a dead PID without touching the physical connection, so an open server-side transaction survives reclamation [^reclaim].
- `DIALECT_TAG_POSTGRES = 1` exists in `vertebra-core`; `vertebra._postgres` (`src/rs/vertebra-postgres/src/lib.rs::_postgres`) exports no broker capsule at HEAD [^dialect-tags] [^pg-module].
- `BrokerPlugin.snapshot_metrics` reports one pool and has no quarantine counters; `BrokerMetrics` has no per-pool or quarantine fields [^broker-plugin] [^broker-metrics].
- No integration test starts multiple worker processes against a real server [^int-conftest].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Shared pools are only safe if a crashed worker's connections come back clean and uncertain connections leave the pool. This chapter registers the PostgreSQL backend, makes reclamation issue `ROLLBACK` and dispose quarantined connections on the thread that owns them, proves one server-side pool across four processes for PostgreSQL and MySQL, and exposes the counters operators need.

### 1.2 Non-Goals

- PostgreSQL wire protocol work (owned by `native-postgres-driver`).
- Docs (owned by `ch09`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| PostgreSQL module | `src/rs/vertebra-postgres/src/lib.rs::_postgres`; `src/py/vertebra/_postgres.pyi` [^pg-module] | Engine cdylib → `BrokerEngineCapsule` (`ch03 / t01`). |
| Reclamation | `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker`, `::active_leases_for_worker` [^reclaim] | Broker host owns physical connections. |
| Metrics | `extensions/broker/plugin.py::BrokerPlugin.snapshot_metrics`; `transport.py::BrokerMetrics` [^broker-plugin] [^broker-metrics] | ZPages and OpenTelemetry recorder. |
| Acceptance | `src/py/tests/integration/conftest.py::vtb_postgres_config`, `::vtb_mysql_config` [^int-conftest] | `pytest-databases` containers (remote-owned). |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).
- Skills: `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, `pytest-databases`, `.agents/skills/review-tests/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R10 (PostgreSQL)**: `vertebra._postgres.broker_engine_capsule()` returns a capsule with `dialect_tag == 1` (`t01`).
- **R19**: Reclamation issues server-side `ROLLBACK` before slots return; quarantined connections are disposed on the owning thread; bounded shutdown reports undisposed connections (`t02`).
- **R20**: Four worker processes share one server-side pool for PostgreSQL and MySQL; `SIGKILL` mid-transaction and forced rollback failure behave per contract (`t03`).
- **R21**: Per-pool and quarantine metrics in `snapshot_metrics` and ZPages (`t04`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
// vertebra-core/src/broker.rs
pub struct ReclaimedLease { pub slot_index: u32, pub generation: u64, pub pool_id: u16 }
impl ShmLeaseTable {
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> Vec<ReclaimedLease>;
}
```

```python
# vertebra._postgres
def broker_engine_capsule() -> object: ...
# vertebra.extensions.broker.transport
class PoolMetrics(msgspec.Struct, frozen=True, gc=False):
    pool_name: str
    active_leases: int
    quarantined: int
    total_quarantined: int
    total_disposed: int
    total_reclaimed: int
class BrokerMetrics(msgspec.Struct, frozen=True, gc=False):
    ...
    total_quarantined: int = 0
    total_disposed: int = 0
    pools: tuple[PoolMetrics, ...] = ()
# vertebra.extensions.broker.host (ch05 t03)
class ShutdownReport(msgspec.Struct, frozen=True, gc=False):
    undisposed: tuple[tuple[str, int, int], ...]
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Supervisor child-exit → `BrokerHost.on_worker_exit(pid)` → `ShmLeaseTable::reclaim_worker_leases(pid)` → per lease: owning connection thread runs `ROLLBACK` → success: slot freed; failure: `quarantine` → `close` → `dispose_quarantined`.
- `BrokerHost.shutdown(timeout)` → dispose all quarantined → `ShutdownReport.undisposed` for any left.
- `BrokerPlugin.on_app_init` → `BrokerEngineDiscovery` → `vertebra._postgres.broker_engine_capsule()` → `BrokerEngineRegistry.register`.
- `BrokerPlugin.snapshot_metrics` → `transport.metrics()` → `BrokerMetrics.pools` → `DbPoolMetricSample` per pool → ZPages `/debug/broker`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Worker SIGKILL in transaction | Lease held with open `BEGIN` | `ROLLBACK` runs before slot frees; `pg_stat_activity.state` returns to `idle` | No inherited transaction |
| ROLLBACK fails on reclaim | Socket broken | Slot quarantined, connection closed, disposed; capacity returns after dispose | No dirty re-lease |
| Disposal from wrong thread | Reclaim invoked on supervisor thread | Work is posted to the connection's owning thread | Driver thread affinity |
| Shutdown timeout | Disposal blocked past timeout | `ShutdownReport.undisposed` lists `(pool, slot, generation)` | Bounded shutdown |
| PostgreSQL wheel absent | `vertebra._postgres` import fails | Discovery skips; upstream SQLSpec path serves PostgreSQL | Optional engine |
| Capsule ABI mismatch | `abi_version != 1` | `RuntimeError` naming `vertebra._postgres` | Fail loud |
| Metrics with two pools | Two `pool_key`s | Two `PoolMetrics` entries, totals sum | Per-pool visibility |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given an unmodified `AsyncpgConfig` wrapped in `ProxyAsyncConfig` and 4 worker processes, when each opens a transaction, then `pg_stat_activity` shows connections from one pool and one worker `SIGKILL` mid-transaction leaves no open transaction after reclamation.
- Given the MySQL native wheel, when the broker starts, then `BrokerEngineRegistry.registered_tags()` contains `2` and `information_schema.PROCESSLIST` shows at most `pool_size + max_overflow` broker connections for 4 workers.
- Given a forced rollback failure, when the session releases, then that backend PID never appears in a later lease and `total_quarantined` increments.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Reclaim cleanup | `ROLLBACK` then free, quarantine on failure | Free immediately (HEAD): next lease inherits transaction state. |
| Disposal thread | Owning connection thread | Supervisor thread: native drivers are not thread-agnostic. |
| Acceptance DBs | PostgreSQL and MySQL containers | SQLite: no server-side connection view to count. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R10 (PostgreSQL) | [t01](tasks/t01-postgres-broker-capsule.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R19 | [t02](tasks/t02-crash-reclamation-and-disposal.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_lease_broker.py`, `src/rs/vertebra-core/tests/it/broker.rs` |
| R20 | [t03](tasks/t03-multi-process-acceptance.md) | `integration_acceptance` | `src/py/tests/integration/extensions/test_broker_pool_sharing.py` |
| R21 | [t04](tasks/t04-pool-and-quarantine-metrics.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch08:t01 — Export the PostgreSQL broker engine capsule (tasks/t01-postgres-broker-capsule.md)
- [ ] broker-pool-sharing:ch08:t02 — Reclaim crashed worker leases with ROLLBACK and dispose quarantined connections (tasks/t02-crash-reclamation-and-disposal.md)
- [ ] broker-pool-sharing:ch08:t03 — Prove multi-process pool sharing for PostgreSQL and MySQL (tasks/t03-multi-process-acceptance.md)
- [ ] broker-pool-sharing:ch08:t04 — Add per-pool and quarantine metrics (tasks/t04-pool-and-quarantine-metrics.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test -p vertebra-core --features tls,compression,gcp --test it broker && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_shm_lease_broker.py -q && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/extensions/test_broker_pool_sharing.py -q -rs` exits `0` with zero skips.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed). `t01` additionally waits on flow `native-postgres-driver`.
- **Active Invariants**: A reclaimed slot is free only after `ROLLBACK` succeeds or the connection is disposed; `vertebra-core` never depends on `vertebra-postgres`.

[^pg-module]: `src/rs/vertebra-postgres/src/lib.rs::_postgres` — PostgreSQL PyO3 module.
[^reclaim]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker` — Worker reclamation.
[^dialect-tags]: `src/rs/vertebra-core/src/broker.rs::DIALECT_TAG_POSTGRES` — Dialect tags.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.snapshot_metrics` — Metric snapshot.
[^broker-metrics]: `src/py/vertebra/extensions/broker/transport.py::BrokerMetrics` — Metrics struct.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Service fixtures.
