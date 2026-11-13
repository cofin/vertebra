---
type: Task
id: "broker-pool-sharing:ch03:t04"
flow_id: broker-pool-sharing
chapter_id: ch03-engine-capsules-core
title: "DuckDB BrokerPoolBackend and broker_engine_capsule Export"
description: "Implement DuckDBBrokerBackend (tag 6) with broker-hosted single-writer pool and VTBARROW responses."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch03:t01"
  - "broker-pool-sharing:ch03:t02"
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 215
tags:
  - duckdb
  - capsule
  - arrow
sources:
  - id: duckdb-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
    title: "DuckDB PyO3 extension"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Task `t04`: DuckDB BrokerPoolBackend and broker_engine_capsule Export

## Objective & Scope Boundary

Implement `DuckDBBrokerBackend` (`DIALECT_TAG_DUCKDB = 6`) and export `vertebra._duckdb.broker_engine_capsule()` per [`../spec.md` §2](../spec.md) [^duckdb-rs] [^db-registry] [^core-broker].

## Bounded Read Manifest

- `src/rs/vertebra-duckdb/src/lib.rs` — `SyncDuckDBClient`, `normalize_named_sql`, and Arrow SHM writer.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_duckdb.pyi` — Type stubs for `vertebra._duckdb`.
- `src/py/tests/unit/extensions/test_shm_broker.py` — Broker capsule tests.

## Contracts & Edge Cases

- Host DuckDB connections inside `DuckDBBrokerBackend` so multiple Python workers share one single-writer DuckDB database through `dbrpc` frames and `VTBARROW` SHM.
- Export `broker_engine_capsule()` on `vertebra._duckdb` and update `src/py/vertebra/_duckdb.pyi`.
- **Edge Case**: Large analytical result sets spill to `VTBARROW` SHM (`arrow_shm` field in `DbRpcResponse`).
- **Edge Case**: SQL errors map to typed `DbRpcResponse` error fields without panicking.

## Test Matrix

- Open an in-memory DuckDB broker pool (`duckdb:///:memory:`), execute DDL + parameterized `SELECT 42` via `BrokerEngineRegistry.execute(6, ...)`, and assert row/Arrow output.
- `mypy.stubtest vertebra._duckdb` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._duckdb --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k duckdb
```

## Acceptance Criteria

- [ ] `vertebra._duckdb.broker_engine_capsule()` registers tag `6` and executes `dbrpc` queries end-to-end.
- [ ] `mypy.stubtest vertebra._duckdb` passes.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-rs]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB PyO3 extension.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
