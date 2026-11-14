---
type: Task
id: broker-pool-sharing:ch02-engine-capsules-and-discovery:t02
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: Implement MySQL and DuckDB BrokerPoolBackend and broker_engine_capsule Exports
description: Implement MySQLBrokerBackend (tag 2) and DuckDBBrokerBackend (tag 6) and export broker_engine_capsule() in _mysql and _duckdb.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
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

Upgrade `MySQLBrokerBackend` (`DIALECT_TAG_MYSQL = 2`) to `BrokerPoolBackend` and export `broker_engine_capsule()` in `vertebra._mysql` per [`../spec.md` §2](../spec.md) [^mysql-rs] [^db-registry] [^core-broker].

Implement `DuckDBBrokerBackend` (`DIALECT_TAG_DUCKDB = 6`) and export `vertebra._duckdb.broker_engine_capsule()` per [`../spec.md` §2](../spec.md) [^duckdb-rs] [^db-registry] [^core-broker].

## Bounded Read Manifest

- `src/rs/vertebra-mysql/src/lib.rs` — Current `MySQLBrokerBackend` and `register_mysql_broker_backend`.
- `src/rs/vertebra-mysql/src/protocol.rs` — `normalize_mysql_placeholders`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_mysql.pyi` — Type stubs for `vertebra._mysql`.

- `src/rs/vertebra-duckdb/src/lib.rs` — `SyncDuckDBClient`, `normalize_named_sql`, and Arrow SHM writer.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_duckdb.pyi` — Type stubs for `vertebra._duckdb`.
- `src/py/tests/unit/extensions/test_shm_broker.py` — Broker capsule tests.

## Contracts & Edge Cases

- Remove `register_mysql_broker_backend` static registration; manage broker-owned `mysql_async::Pool` instances in `MySQLBrokerBackend` (`open_pool`, `execute`, `close_pool`).
- Export `#[pyfunction] fn broker_engine_capsule(py: Python<'_>)` and declare `def broker_engine_capsule() -> object: ...` in `_mysql.pyi`.
- **Edge Case**: Malformed MySQL DSN in `open_pool` returns `Err` mapped to `RuntimeError`.
- **Edge Case**: `close_pool` on an already-closed `pool_id` is an idempotent no-op.

- Host DuckDB connections inside `DuckDBBrokerBackend` so multiple Python workers share one single-writer DuckDB database through `dbrpc` frames and `VTBARROW` SHM.
- Export `broker_engine_capsule()` on `vertebra._duckdb` and update `src/py/vertebra/_duckdb.pyi`.
- **Edge Case**: Large analytical result sets spill to `VTBARROW` SHM (`arrow_shm` field in `DbRpcResponse`).
- **Edge Case**: SQL errors map to typed `DbRpcResponse` error fields without panicking.

## Test Matrix

- Verify `2 in BrokerEngineRegistry.registered_tags()` and `normalize(2, "SELECT :id") == ("SELECT ?", 1)`.
- Verify unknown `pool_id` and invalid DSN error handling plus `mypy.stubtest vertebra._mysql`.

- Open an in-memory DuckDB broker pool (`duckdb:///:memory:`), execute DDL + parameterized `SELECT 42` via `BrokerEngineRegistry.execute(6, ...)`, and assert row/Arrow output.
- `mypy.stubtest vertebra._duckdb` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._duckdb --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k duckdb
```

## Acceptance Criteria

- [ ] `vertebra._mysql.broker_engine_capsule()` registers tag `2` in `_vertebra.BrokerEngineRegistry`.
- [ ] `mypy.stubtest vertebra._mysql` passes.
- [ ] `vertebra._duckdb.broker_engine_capsule()` registers tag `6` and executes `dbrpc` queries end-to-end.
- [ ] `mypy.stubtest vertebra._duckdb` passes.
- [ ] Zero regressions in the fast verification gate.

[^mysql-rs]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL PyO3 extension and broker backend.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^duckdb-rs]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB PyO3 extension.
