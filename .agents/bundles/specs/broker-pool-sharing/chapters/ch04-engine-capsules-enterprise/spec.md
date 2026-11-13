---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch04-engine-capsules-enterprise
title: "Chapter 04: Oracle, SQL Server, and PostgreSQL Broker Engine Capsules"
description: "Implement BrokerPoolBackend and export broker_engine_capsule() across vertebra-oracle, vertebra-mssql, and vertebra-postgres."
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
  - oracle
  - mssql
  - postgres
  - capsule
  - broker
sources:
  - id: oracle-rs
    resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
    title: "Oracle native Thin-mode pool"
  - id: mssql-rs
    resource: src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders
    title: "SQL Server native TDS parameter rewriter"
  - id: postgres-rs
    resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
    title: "PostgreSQL native Protocol 3.0 pool"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Chapter 04: Oracle, SQL Server, and PostgreSQL Broker Engine Capsules

<!-- truth: start -->
- `vertebra._oracle` (`DIALECT_TAG_ORACLE = 4`), `vertebra._mssql` (`DIALECT_TAG_MSSQL = 5`), and `vertebra._postgres` (`DIALECT_TAG_POSTGRES = 1`) each export `broker_engine_capsule() -> object` [^oracle-rs] [^mssql-rs] [^postgres-rs] [^core-broker].
- `PostgresBrokerBackend` wraps `vertebra_postgres::PostgresPool` already present on `main @ 18baadcd` [^postgres-rs].
- Every engine crate depends only on `vertebra-db` (`BrokerPoolBackend` + `new_broker_engine_pycapsule`), never on `vertebra-py` [^db-registry].
<!-- truth: end -->

## 1. Specification, Context & Provenance

With `BrokerEngineCapsule` and `BrokerEngineRegistry` established in `ch03`, the remaining merged enterprise relational wheels — Oracle (`vertebra-oracle`), SQL Server (`vertebra-mssql`), and PostgreSQL (`vertebra-postgres`) — need `BrokerPoolBackend` implementations so multi-worker deployments share one broker-owned server pool per `pool_key` [^oracle-rs] [^mssql-rs] [^postgres-rs].

**Non-Goals:** SQLite capsule (owned by `native-sqlite-driver`); BigQuery and Spanner (excluded per root spec).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Oracle | `src/rs/vertebra-oracle/src/lib.rs`, `src/py/vertebra/_oracle.pyi` [^oracle-rs] | `OracleBrokerBackend` (`tag=4`) over `SyncOraclePool` |
| SQL Server | `src/rs/vertebra-mssql/src/lib.rs`, `src/py/vertebra/_mssql.pyi` [^mssql-rs] | `MSSQLBrokerBackend` (`tag=5`) over TDS client pool |
| PostgreSQL | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` [^postgres-rs] | `PostgresBrokerBackend` (`tag=1`) over `PostgresPool` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PostgreSQL Native Driver](../../../../knowledge/domains/data/postgres-native-driver.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10 (Oracle)**: Implement `OracleBrokerBackend` (`DIALECT_TAG_ORACLE = 4`) and export `vertebra._oracle.broker_engine_capsule()` (`t01`).
- **R10 (SQL Server)**: Implement `MSSQLBrokerBackend` (`DIALECT_TAG_MSSQL = 5`) and export `vertebra._mssql.broker_engine_capsule()` (`t02`).
- **R10 (PostgreSQL)**: Implement `PostgresBrokerBackend` (`DIALECT_TAG_POSTGRES = 1`) over `PostgresPool` and export `vertebra._postgres.broker_engine_capsule()` (`t03`).

```rust
pub struct OracleBrokerBackend { /* DashMap<u64, Arc<SyncOraclePool>> */ }
pub struct MSSQLBrokerBackend { /* DashMap<u64, Arc<MssqlBrokerPool>> */ }
pub struct PostgresBrokerBackend { /* DashMap<u64, Arc<PostgresPool>> */ }

#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>>;
```

```python
# Declared in _oracle.pyi, _mssql.pyi, and _postgres.pyi:
def broker_engine_capsule() -> object: ...
```

**Integration Flow:**
- `BrokerEngineDiscovery.register_installed()` imports `_oracle`, `_mssql`, and `_postgres` and registers tags `4`, `5`, and `1` into `BrokerEngineRegistry`.
- Statement normalization delegates to Oracle `:1`, SQL Server `@P1`, and PostgreSQL `$1` placeholder rewriters.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid DSN | `open_pool("invalid://", 4)` | `Err` mapped to `RuntimeError` | Fail-loud pool open |
| Unknown `pool_id` | `execute(999, frame)` | `Err("unknown pool id 999")` | No default pool fallback |
| Double close | `close_pool(id)` called twice | Idempotent no-op | Clean teardown |

## 3. Documentation & OKF Knowledge Plan

Engine registration matrix updates execute in `ch08:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` (Oracle) | `t01` | `src/rs/vertebra-oracle/src/lib.rs`, `src/py/vertebra/_oracle.pyi` | `mypy.stubtest vertebra._oracle` |
| `R10` (SQL Server) | `t02` | `src/rs/vertebra-mssql/src/lib.rs`, `src/py/vertebra/_mssql.pyi` | `mypy.stubtest vertebra._mssql` |
| `R10` (PostgreSQL) | `t03` | `src/rs/vertebra-postgres/src/lib.rs`, `src/py/vertebra/_postgres.pyi` | `mypy.stubtest vertebra._postgres` |

## 5. Implementation Plan

- [ ] [t01-oracle-broker-capsule.md](tasks/t01-oracle-broker-capsule.md) — Implement OracleBrokerBackend (tag 4) over SyncOraclePool and export broker_engine_capsule() in _oracle.
- [ ] [t02-mssql-broker-capsule.md](tasks/t02-mssql-broker-capsule.md) — Implement MSSQLBrokerBackend (tag 5) with @P1 normalization and export broker_engine_capsule() in _mssql.
- [ ] [t03-postgres-broker-capsule.md](tasks/t03-postgres-broker-capsule.md) — Implement PostgresBrokerBackend (tag 1) over PostgresPool and export broker_engine_capsule() in _postgres.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: All three enterprise engine capsules expose ABI v1 and pass `mypy.stubtest`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle -p vertebra-mssql -p vertebra-postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py`

[^oracle-rs]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle native Thin-mode pool.
[^mssql-rs]: `src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders` — SQL Server native TDS parameter rewriter.
[^postgres-rs]: `src/rs/vertebra-postgres/src/pool.rs::PostgresPool` — PostgreSQL native Protocol 3.0 pool.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
