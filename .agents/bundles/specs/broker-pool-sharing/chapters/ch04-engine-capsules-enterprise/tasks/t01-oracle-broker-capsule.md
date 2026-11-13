---
type: Task
id: "broker-pool-sharing:ch04:t01"
flow_id: broker-pool-sharing
chapter_id: ch04-engine-capsules-enterprise
title: "Oracle BrokerPoolBackend and broker_engine_capsule Export"
description: "Implement OracleBrokerBackend (tag 4) over SyncOraclePool and export broker_engine_capsule() in _oracle."
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
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/rs/vertebra-oracle/src/lib.rs
  - src/rs/vertebra-oracle/src/pool.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_oracle.pyi
estimated_loc: 210
tags:
  - oracle
  - capsule
  - pyo3
sources:
  - id: oracle-rs
    resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
    title: "Oracle native Thin-mode pool"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
---

# Task `t01`: Oracle BrokerPoolBackend and broker_engine_capsule Export

## Objective & Scope Boundary

Implement `OracleBrokerBackend` (`DIALECT_TAG_ORACLE = 4`) and export `vertebra._oracle.broker_engine_capsule()` per [`../spec.md` §2](../spec.md) [^oracle-rs] [^db-registry].

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/lib.rs` — `_oracle` PyO3 module definition.
- `src/rs/vertebra-oracle/src/pool.rs` — `SyncOraclePool` API.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_oracle.pyi` — `vertebra._oracle` type stubs.

## Contracts & Edge Cases

- Implement `BrokerEngineBackend` (`normalize_statement` to `:1` binds) and `BrokerPoolBackend` over `SyncOraclePool`.
- Export `broker_engine_capsule()` in `src/rs/vertebra-oracle/src/lib.rs` and `src/py/vertebra/_oracle.pyi`.
- **Edge Case**: Invalid TNS/DSN strings fail loudly in `open_pool`.
- **Edge Case**: Repeated `close_pool` calls are idempotent.

## Test Matrix

- Assert `4 in BrokerEngineRegistry.registered_tags()` and verify Oracle placeholder normalization via `BrokerEngineRegistry.normalize(4, ...)`.
- `mypy.stubtest vertebra._oracle` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._oracle --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k oracle
```

## Acceptance Criteria

- [ ] `vertebra._oracle.broker_engine_capsule()` registers tag `4` and passes `stubtest`.
- [ ] Zero regressions in the chapter fast verification gate.

[^oracle-rs]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle native Thin-mode pool.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
