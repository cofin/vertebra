---
type: Task
id: broker-pool-sharing:ch02-engine-capsules-and-discovery:t04
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: Implement PostgreSQL BrokerPoolBackend Over PostgresPool and broker_engine_capsule Export
description: Implement PostgresBrokerBackend (tag 1) over PostgresPool and export broker_engine_capsule() in _postgres.
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
- broker-pool-sharing:ch02-engine-capsules-and-discovery:t03
files:
- src/rs/vertebra-postgres/src/lib.rs
- src/py/vertebra/_postgres.pyi
tests:
- src/py/tests/unit/extensions/test_shm_broker.py
read_set:
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/src/pool.rs
- src/rs/vertebra-db/src/lib.rs
- src/py/vertebra/_postgres.pyi
estimated_loc: 220
tags:
- postgres
- capsule
- pyo3
sources:
- id: postgres-rs
  resource: src/rs/vertebra-postgres/src/pool.rs::PostgresPool
  title: PostgreSQL native Protocol 3.0 pool
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
---

# Task `t04`: Implement PostgreSQL BrokerPoolBackend Over PostgresPool and broker_engine_capsule Export

## Objective & Scope Boundary

Implement `PostgresBrokerBackend` (`DIALECT_TAG_POSTGRES = 1`) over `PostgresPool` and export `vertebra._postgres.broker_engine_capsule()` per [`../spec.md` §2](../spec.md) [^postgres-rs] [^db-registry].

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/lib.rs` — `_postgres` PyO3 module exports.
- `src/rs/vertebra-postgres/src/pool.rs` — `PostgresPool` and `PostgresPoolOptions`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_postgres.pyi` — `vertebra._postgres` type stubs.

## Contracts & Edge Cases

- Host `PostgresPool` instances inside `PostgresBrokerBackend`, supporting `dbrpc` execution, transaction control, and `VTBARROW` result frames.
- Export `broker_engine_capsule()` in `src/rs/vertebra-postgres/src/lib.rs` and `src/py/vertebra/_postgres.pyi`.
- **Edge Case**: Malformed PostgreSQL URLs or pool exhaustion map to typed `DbRpcResponse` / `RuntimeError` outcomes.

## Test Matrix

- Assert `1 in BrokerEngineRegistry.registered_tags()` and `$1` placeholder normalization via `normalize(1, ...)`.
- `mypy.stubtest vertebra._postgres` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k postgres
```

## Acceptance Criteria

- [x] `vertebra._postgres.broker_engine_capsule()` registers tag `1` and passes `stubtest`.
- [x] Zero regressions in the fast verification gate.

[^postgres-rs]: `src/rs/vertebra-postgres/src/pool.rs::PostgresPool` — PostgreSQL native Protocol 3.0 pool.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
