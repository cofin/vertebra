---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch04-engine-backend-registration
title: "Chapter 4: Engine Backend Registration"
description: Implement BrokerPoolBackend and export broker_engine_capsule() from the merged MySQL, Oracle, SQL Server, and DuckDB native wheels.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
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
  - id: mysql
    resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
    title: MySQL broker backend and normalize_mysql_placeholders
  - id: oracle
    resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
    title: Oracle native pool
  - id: mssql
    resource: src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders
    title: SQL Server placeholder rewriting
  - id: duckdb
    resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
    title: DuckDB named SQL normalization
  - id: tags
    resource: src/rs/vertebra-core/src/broker.rs::DIALECT_TAG_MYSQL
    title: Dialect tag constants
tags:
  - engines
  - registration
---

# Chapter 4: Engine Backend Registration

<!-- truth: start -->
- `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, and `vertebra._duckdb` each export `broker_engine_capsule() -> object` built by `vertebra_db::new_broker_engine_pycapsule` [^mysql] [^oracle] [^mssql] [^duckdb].
- Dialect tags: MySQL `2`, Oracle `4`, SQL Server `5`, DuckDB `6` [^tags].
- Each backend's `execute` decodes a `dbrpc` frame, runs it on the engine's native client/pool on a broker-owned thread, and encodes a `DbRpcResponse`; Arrow results spill to `VTBARROW` SHM.
- PostgreSQL registration lives in `ch08 / t01`; BigQuery and Spanner are excluded (root spec Non-Goals).
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

None of the merged engine wheels can serve broker-hosted pools: MySQL registers only a normalizer into its own static map, and Oracle, SQL Server, and DuckDB register nothing [^mysql]. This chapter gives each merged relational engine a `BrokerPoolBackend` and an exported capsule.

### 1.2 Non-Goals

- PostgreSQL (`ch08 / t01`), BigQuery/Spanner (excluded), SQLite (`native-sqlite-driver`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| MySQL | `src/rs/vertebra-mysql/src/lib.rs::{MySQLBrokerBackend,_mysql}`, `protocol.rs::normalize_mysql_placeholders`, `SyncMySQLClient` [^mysql] | Remote-owned database via native wire client. |
| Oracle | `src/rs/vertebra-oracle/src/lib.rs::_oracle`, `pool.rs::SyncOraclePool` [^oracle] | Remote-owned database. |
| SQL Server | `src/rs/vertebra-mssql/src/lib.rs::{_mssql,py_rewrite_mssql_placeholders,PySyncMSSQLClient}`, `params.rs::rewrite_named_mssql_placeholders` [^mssql] | Remote-owned database. |
| DuckDB | `src/rs/vertebra-duckdb/src/lib.rs::{_duckdb,normalize_named_sql,SyncDuckDBClient}` [^duckdb] | In-process engine. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md); `.agents/skills/pyo3-bridge/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R10**: Capsule export per engine (`t01` MySQL, `t02` Oracle, `t03` SQL Server, `t04` DuckDB).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
pub struct <Engine>BrokerBackend { pools: DashMap<u64, <EnginePool>>, next_id: AtomicU64 }
impl BrokerEngineBackend for <Engine>BrokerBackend { /* tag, name, normalize */ }
impl BrokerPoolBackend for <Engine>BrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>;
    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>;
    fn close_pool(&self, pool_id: u64);
}
#[pyfunction] fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>>;
```

```python
def broker_engine_capsule() -> object: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `BrokerEngineDiscovery.register_installed` → `vertebra._<engine>.broker_engine_capsule()` → `BrokerEngineRegistry.register`.
- Broker host `execute` frame → `BrokerEngineRegistry.execute(tag, pool_id, frame)` → `<Engine>BrokerBackend::execute` → native client.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Bad DSN | `open_pool("nope")` | `Err("invalid <engine> DSN: ...")` → `RuntimeError` | Fail loud |
| Unknown pool id | `execute(999, frame)` | `Err("unknown pool id 999")` | No silent default |
| Driver error | SQL error | `DbRpcResponse(status=Error, error_class="IntegrityError", ...)` | Typed errors preserved |
| Close twice | `close_pool(id)` twice | Second is no-op | Idempotent close |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given each wheel installed, when discovery runs, then `registered_tags()` contains `2, 4, 5, 6`.
- Given DuckDB registered, when a frame `execute("SELECT 42")` runs against `open_pool("duckdb:///:memory:", 1)`, then the response rows equal `[[42]]`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Execution thread | Engine-owned runtime thread per pool | Python-thread execution: re-enters the GIL per statement. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; the registration matrix is documented in `ch09 / t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R10 (MySQL) | [t01](tasks/t01-mysql-broker-capsule.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R10 (Oracle) | [t02](tasks/t02-oracle-broker-capsule.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R10 (SQL Server) | [t03](tasks/t03-mssql-broker-capsule.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |
| R10 (DuckDB) | [t04](tasks/t04-duckdb-broker-capsule.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_shm_broker.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch04:t01 — MySQL broker capsule (tasks/t01-mysql-broker-capsule.md)
- [ ] broker-pool-sharing:ch04:t02 — Oracle broker capsule (tasks/t02-oracle-broker-capsule.md)
- [ ] broker-pool-sharing:ch04:t03 — SQL Server broker capsule (tasks/t03-mssql-broker-capsule.md)
- [ ] broker-pool-sharing:ch04:t04 — DuckDB broker capsule (tasks/t04-duckdb-broker-capsule.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k capsule && make lint` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed).
- **Active Invariants**: Engine crates depend on `vertebra-db` only, never on `vertebra-py`.

[^mysql]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL backend.
[^oracle]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle pool.
[^mssql]: `src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders` — SQL Server placeholders.
[^duckdb]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB normalization.
[^tags]: `src/rs/vertebra-core/src/broker.rs::DIALECT_TAG_MYSQL` — Dialect tags.
