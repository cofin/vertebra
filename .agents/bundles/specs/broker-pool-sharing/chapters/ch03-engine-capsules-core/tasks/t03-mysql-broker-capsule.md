---
type: Task
id: "broker-pool-sharing:ch03:t03"
flow_id: broker-pool-sharing
chapter_id: ch03-engine-capsules-core
title: "MySQL BrokerPoolBackend and broker_engine_capsule Export"
description: "Implement BrokerPoolBackend for MySQL (tag 2) with real dbrpc/VTBARROW execution and export broker_engine_capsule()."
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
  - src/rs/vertebra-mysql/src/lib.rs
  - src/py/vertebra/_mysql.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/rs/vertebra-mysql/src/lib.rs
  - src/rs/vertebra-mysql/src/protocol.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_mysql.pyi
estimated_loc: 210
tags:
  - mysql
  - capsule
  - pyo3
sources:
  - id: mysql-rs
    resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
    title: "MySQL PyO3 extension and broker backend"
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Task `t03`: MySQL BrokerPoolBackend and broker_engine_capsule Export

## Objective & Scope Boundary

Upgrade `MySQLBrokerBackend` (`DIALECT_TAG_MYSQL = 2`) to `BrokerPoolBackend` and export `broker_engine_capsule()` in `vertebra._mysql` per [`../spec.md` §2](../spec.md) [^mysql-rs] [^db-registry] [^core-broker].

## Bounded Read Manifest

- `src/rs/vertebra-mysql/src/lib.rs` — Current `MySQLBrokerBackend` and `register_mysql_broker_backend`.
- `src/rs/vertebra-mysql/src/protocol.rs` — `normalize_mysql_placeholders`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_mysql.pyi` — Type stubs for `vertebra._mysql`.

## Contracts & Edge Cases

- Remove `register_mysql_broker_backend` static registration; manage broker-owned `mysql_async::Pool` instances in `MySQLBrokerBackend` (`open_pool`, `execute`, `close_pool`).
- Export `#[pyfunction] fn broker_engine_capsule(py: Python<'_>)` and declare `def broker_engine_capsule() -> object: ...` in `_mysql.pyi`.
- **Edge Case**: Malformed MySQL DSN in `open_pool` returns `Err` mapped to `RuntimeError`.
- **Edge Case**: `close_pool` on an already-closed `pool_id` is an idempotent no-op.

## Test Matrix

- Verify `2 in BrokerEngineRegistry.registered_tags()` and `normalize(2, "SELECT :id") == ("SELECT ?", 1)`.
- Verify unknown `pool_id` and invalid DSN error handling plus `mypy.stubtest vertebra._mysql`.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._mysql --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k mysql
```

## Acceptance Criteria

- [ ] `vertebra._mysql.broker_engine_capsule()` registers tag `2` in `_vertebra.BrokerEngineRegistry`.
- [ ] `mypy.stubtest vertebra._mysql` passes.
- [ ] Zero regressions in the chapter fast verification gate.

[^mysql-rs]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL PyO3 extension and broker backend.
[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
