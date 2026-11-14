---
type: Task
id: broker-pool-sharing:ch02-engine-capsules-and-discovery:t03
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: Implement Oracle and SQL Server BrokerPoolBackend and broker_engine_capsule Exports
description: Implement OracleBrokerBackend (tag 4) and wire MssqlBrokerBackend (tag 5) into BrokerPoolBackend, exporting broker_engine_capsule() in _oracle and _mssql.
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
- broker-pool-sharing:ch02-engine-capsules-and-discovery:t02
files:
- src/rs/vertebra-oracle/src/lib.rs
- src/py/vertebra/_oracle.pyi
- src/rs/vertebra-mssql/src/broker.rs
- src/rs/vertebra-mssql/src/lib.rs
- src/py/vertebra/_mssql.pyi
tests:
- src/py/tests/unit/extensions/test_shm_broker.py
read_set:
- src/rs/vertebra-oracle/src/lib.rs
- src/rs/vertebra-oracle/src/pool.rs
- src/rs/vertebra-db/src/lib.rs
- src/rs/vertebra-mssql/src/broker.rs
- src/rs/vertebra-mssql/src/lib.rs
- src/py/vertebra/_mssql.pyi
estimated_loc: 220
tags:
- oracle
- capsule
- pyo3
- mssql
sources:
- id: oracle-rs
  resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
  title: Oracle native Thin-mode pool
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
- id: mssql-rs
  resource: src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders
  title: SQL Server native TDS parameter rewriter
---

# Task `t03`: Implement Oracle and SQL Server BrokerPoolBackend and broker_engine_capsule Exports

## Objective & Scope Boundary

Implement `OracleBrokerBackend` (`DIALECT_TAG_ORACLE = 4`) over `SyncOraclePool` and wire `MssqlBrokerBackend` (`DIALECT_TAG_MSSQL = 5`, in `src/rs/vertebra-mssql/src/broker.rs`) into `BrokerPoolBackend`, exporting `broker_engine_capsule()` in `vertebra._oracle` and `vertebra._mssql` per [`../spec.md` §2](../spec.md) [^oracle-rs] [^mssql-rs] [^db-registry].

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/lib.rs` — `_oracle` PyO3 module definition (`SyncOraclePool` export and `PyO3` entry point).
- `src/rs/vertebra-oracle/src/pool.rs` — `SyncOraclePool` pool management API.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/rs/vertebra-mssql/src/broker.rs` — Existing `MssqlBrokerBackend` and TDS placeholder normalization.
- `src/rs/vertebra-mssql/src/lib.rs` — `_mssql` PyO3 module and `PySyncMSSQLClient`.
- `src/py/vertebra/_mssql.pyi` — `vertebra._mssql` type stubs (mirrored for `src/py/vertebra/_oracle.pyi`).

## Contracts & Edge Cases

- Implement `BrokerEngineBackend` (`normalize_statement` to `:1` Oracle binds) and `BrokerPoolBackend` over `SyncOraclePool` inside `OracleBrokerBackend`.
- Wire `rewrite_mssql_placeholders` (`@P1`..`@Pn`) in `normalize_statement` and manage bounded TDS connection pools in `MssqlBrokerBackend` (`src/rs/vertebra-mssql/src/broker.rs`).
- Export `broker_engine_capsule()` in `src/rs/vertebra-oracle/src/lib.rs` and `src/rs/vertebra-mssql/src/lib.rs`, declaring `def broker_engine_capsule() -> object: ...` in `src/py/vertebra/_oracle.pyi` and `src/py/vertebra/_mssql.pyi`.
- **Edge Case**: Invalid TNS/DSN connection strings or unknown `pool_id` values fail loudly with descriptive error messages without panicking.
- **Edge Case**: Repeated `close_pool` calls on an already-closed `pool_id` are idempotent no-ops.

## Test Matrix

- Assert `4 in BrokerEngineRegistry.registered_tags()` and verify `:1` Oracle placeholder normalization via `BrokerEngineRegistry.normalize(4, ...)`.
- Assert `5 in BrokerEngineRegistry.registered_tags()` and verify `@P1` SQL Server placeholder normalization via `BrokerEngineRegistry.normalize(5, ...)`.
- `mypy.stubtest vertebra._oracle` and `mypy.stubtest vertebra._mssql` pass.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._oracle vertebra._mssql --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k "oracle or mssql"
```

## Acceptance Criteria

- [x] `vertebra._oracle.broker_engine_capsule()` registers tag `4` and passes `stubtest`.
- [x] `vertebra._mssql.broker_engine_capsule()` registers tag `5` and passes `stubtest`.
- [x] Zero regressions in the fast verification gate.

[^oracle-rs]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle native Thin-mode pool.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^mssql-rs]: `src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders` — SQL Server native TDS parameter rewriter.
