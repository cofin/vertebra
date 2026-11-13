---
type: Task
id: "broker-pool-sharing:ch04:t02"
flow_id: broker-pool-sharing
chapter_id: ch04-engine-capsules-enterprise
title: "SQL Server BrokerPoolBackend and broker_engine_capsule Export"
description: "Implement MSSQLBrokerBackend (tag 5) with @P1 normalization and export broker_engine_capsule() in _mssql."
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
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/_mssql.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/rs/vertebra-mssql/src/lib.rs
  - src/rs/vertebra-mssql/src/params.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_mssql.pyi
estimated_loc: 210
tags:
  - mssql
  - capsule
  - pyo3
sources:
  - id: mssql-rs
    resource: src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders
    title: "SQL Server native TDS parameter rewriter"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
---

# Task `t02`: SQL Server BrokerPoolBackend and broker_engine_capsule Export

## Objective & Scope Boundary

Implement `MSSQLBrokerBackend` (`DIALECT_TAG_MSSQL = 5`) and export `vertebra._mssql.broker_engine_capsule()` per [`../spec.md` §2](../spec.md) [^mssql-rs] [^db-registry].

## Bounded Read Manifest

- `src/rs/vertebra-mssql/src/lib.rs` — `_mssql` PyO3 module and `PySyncMSSQLClient`.
- `src/rs/vertebra-mssql/src/params.rs` — `rewrite_named_mssql_placeholders`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_mssql.pyi` — `vertebra._mssql` type stubs.

## Contracts & Edge Cases

- Wire `rewrite_named_mssql_placeholders` (`@P1`..`@Pn`) in `normalize_statement` and manage bounded TDS connection pools in `MSSQLBrokerBackend`.
- Export `broker_engine_capsule()` in `src/rs/vertebra-mssql/src/lib.rs` and `src/py/vertebra/_mssql.pyi`.
- **Edge Case**: Bad connection strings or unknown `pool_id` values return descriptive errors without panicking.

## Test Matrix

- Assert `5 in BrokerEngineRegistry.registered_tags()` and `normalize(5, ...)` rewrites placeholders to `@P1`.
- `mypy.stubtest vertebra._mssql` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._mssql --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k mssql
```

## Acceptance Criteria

- [ ] `vertebra._mssql.broker_engine_capsule()` registers tag `5` and passes `stubtest`.
- [ ] Zero regressions in the chapter fast verification gate.

[^mssql-rs]: `src/rs/vertebra-mssql/src/params.rs::rewrite_named_mssql_placeholders` — SQL Server native TDS parameter rewriter.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
