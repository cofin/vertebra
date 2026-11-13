---
type: Task
id: "broker-pool-sharing:ch07:t03"
flow_id: broker-pool-sharing
chapter_id: ch07-standalone-dbapi-and-sqlalchemy
title: "SQLAlchemy and Advanced Alchemy BrokerPool Implementation"
description: "Implement BrokerPool, AsyncBrokerPool, and broker_creator leasing shared broker connections with quarantine-on-error."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch06:t01"
  - "broker-pool-sharing:ch07:t02"
files:
  - src/py/vertebra/extensions/sqlalchemy/pool.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
estimated_loc: 230
tags:
  - sqlalchemy
  - advanced-alchemy
  - broker
  - pool
sources:
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: "SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes"
---

# Task `t03`: SQLAlchemy and Advanced Alchemy BrokerPool Implementation

## Objective & Scope Boundary

Implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `src/py/vertebra/extensions/sqlalchemy/pool.py` and lazy-export them in `extensions/sqlalchemy/__init__.py` per [`../spec.md` §2](../spec.md) [^broker-transport] [^sa-dialects].

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — `SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport`.
- `src/py/vertebra/extensions/sqlspec/provider.py` — `SyncBrokerLeaseSource` and `AsyncBrokerLeaseSource`.
- `src/py/vertebra/db/dbapi/_sync.py` — `SyncDBAPIConnection`.
- `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — SQLAlchemy async DB-API wrapper.
- `src/py/vertebra/extensions/sqlalchemy/__init__.py` — Lazy package exports (`PY-06`).

## Contracts & Edge Cases

- Acquire SHM broker leases keyed by `config.pool_key()` on checkout (`_do_get`), execute rollback on return (`_do_return_conn`), and quarantine + raise `LeaseQuarantinedError` if rollback fails.
- Support both SQLAlchemy `create_engine` / `create_async_engine` and Advanced Alchemy `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig`.
- **Edge Case**: Constructing `BrokerPool` without a configured broker raises `ImproperConfigurationError` immediately.

## Test Matrix

- Verify two SQLAlchemy engines sharing one `pool_key` acquire from the same SHM partition and enforce `pool_size + max_overflow`.
- Forced rollback failure during connection return quarantines the lease and raises `LeaseQuarantinedError`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] `BrokerPool` and `AsyncBrokerPool` share broker pools cleanly across SQLAlchemy and Advanced Alchemy.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
