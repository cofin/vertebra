---
type: Task
id: broker-pool-sharing:ch02-engine-capsules-and-discovery:t02
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: Implement MySQL and DuckDB BrokerPoolBackend and broker_engine_capsule Exports
description: Implement MySQLBrokerBackend (tag 2) and DuckDBBrokerBackend (tag 6) and export broker_engine_capsule() in _mysql and _duckdb.
status: stable
state: completed
commit: 673051f1
completed_at: '2026-11-14T23:42:28Z'
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch02-engine-capsules-and-discovery:t01
files:
- src/rs/vertebra-mysql/src/lib.rs
- src/py/vertebra/_mysql.pyi
- src/rs/vertebra-duckdb/src/lib.rs
- src/py/vertebra/_duckdb.pyi
tests:
- src/py/tests/unit/extensions/test_shm_broker.py
read_set:
- src/rs/vertebra-mysql/src/lib.rs
- src/rs/vertebra-mysql/src/protocol.rs
- src/rs/vertebra-db/src/lib.rs
- src/py/vertebra/_mysql.pyi
- src/rs/vertebra-duckdb/src/lib.rs
- src/py/vertebra/_duckdb.pyi
estimated_loc: 220
tags:
- mysql
- capsule
- pyo3
- duckdb
- arrow
sources:
- id: mysql-rs
  resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
  title: MySQL PyO3 extension and broker backend
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: duckdb-rs
  resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
  title: DuckDB PyO3 extension
---

# Task `t02`: Implement MySQL and DuckDB BrokerPoolBackend and broker_engine_capsule Exports

## Objective & Scope Boundary

Upgrade `MySQLBrokerBackend` (`DIALECT_TAG_MYSQL = 2`) and implement `DuckDBBrokerBackend` (`DIALECT_TAG_DUCKDB = 6`) over `BrokerPoolBackend`, and export `broker_engine_capsule()` in `vertebra._mysql` and `vertebra._duckdb` per [`../spec.md` §2](../spec.md) [^mysql-rs] [^duckdb-rs] [^db-registry] [^core-broker].

## Bounded Read Manifest

- `src/rs/vertebra-mysql/src/lib.rs` — Current `MySQLBrokerBackend` and `register_mysql_broker_backend`.
- `src/rs/vertebra-mysql/src/protocol.rs` — `normalize_mysql_placeholders`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_mysql.pyi` — Type stubs for `vertebra._mysql`.
- `src/rs/vertebra-duckdb/src/lib.rs` — `SyncDuckDBClient`, `normalize_named_sql`, and Arrow SHM writer.
- `src/py/vertebra/_duckdb.pyi` — Type stubs for `vertebra._duckdb`.

## Contracts & Edge Cases

- Remove `register_mysql_broker_backend` static registration from `vertebra-mysql`; manage broker-owned `mysql_async::Pool` instances in `MySQLBrokerBackend` (`open_pool`, `execute`, `close_pool`).
- Host DuckDB connections inside `DuckDBBrokerBackend` so multiple Python workers share one single-writer DuckDB database through `dbrpc` frames and `VTBARROW` SHM.
- Export `#[pyfunction] fn broker_engine_capsule(py: Python<'_>)` in both extensions and declare `def broker_engine_capsule() -> object: ...` in `_mysql.pyi` and `_duckdb.pyi`.
- **Edge Case**: Malformed DSNs or unknown `pool_id` values in `open_pool` / `execute` return descriptive `Err` payloads mapped to `RuntimeError` without panicking.
- **Edge Case**: `close_pool` on an already-closed `pool_id` is an idempotent no-op.
- **Edge Case**: Large analytical DuckDB result sets spill to `VTBARROW` SHM (`arrow_shm` field in `DbRpcResponse`), while SQL errors map to typed `DbRpcResponse` error fields without panicking.

## Test Matrix

- Verify `2 in BrokerEngineRegistry.registered_tags()` and `normalize(2, "SELECT :id") == ("SELECT ?", 1)` plus unknown `pool_id` and invalid DSN error handling.
- Open an in-memory DuckDB broker pool (`duckdb:///:memory:`), execute DDL plus parameterized `SELECT 42` via `BrokerEngineRegistry.execute(6, ...)`, and assert row/`VTBARROW` output.
- `mypy.stubtest vertebra._mysql` and `mypy.stubtest vertebra._duckdb` pass.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._mysql vertebra._duckdb --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k "mysql or duckdb"
```

## Acceptance Criteria

- [x] `vertebra._mysql.broker_engine_capsule()` registers tag `2` in `_vertebra.BrokerEngineRegistry` and passes `stubtest`.
- [x] `vertebra._duckdb.broker_engine_capsule()` registers tag `6`, executes `dbrpc` queries end-to-end, and passes `stubtest`.
- [x] Zero regressions in the fast verification gate.

[^mysql-rs]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL PyO3 extension and broker backend.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^duckdb-rs]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB PyO3 extension.
