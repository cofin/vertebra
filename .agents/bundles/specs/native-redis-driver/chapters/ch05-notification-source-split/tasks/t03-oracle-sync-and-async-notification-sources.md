---
type: Task
id: "native-redis-driver:ch05:t03"
flow_id: native-redis-driver
chapter_id: ch05-notification-source-split
title: "Split OracleNotificationSource into SyncOracleNotificationSource and AsyncOracleNotificationSource"
description: "Add SyncOracleListener and split OracleNotificationSource into Sync*/Async* across oracle/{notify,driver,__init__}.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch05:t01"
files:
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
  - src/py/tests/integration/db/test_oracle_native.py
read_set:
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 240
tags:
  - oracle
  - notifications
  - aq
sources:
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: "Oracle AQ/TxEventQ notification source"
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver
    title: "Oracle driver notification_source factory"
---

# Task `t03`: Split OracleNotificationSource into SyncOracleNotificationSource and AsyncOracleNotificationSource

## Objective & Scope Boundary

Implement `SyncOracleListener`, `SyncOracleNotificationSource(SyncNotificationSource)`, and `AsyncOracleNotificationSource(AsyncNotificationSource)` across `oracle/{notify,driver,__init__}.py` per [`../spec.md` §2](../spec.md) [^ora-notify] [^ora-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/notify.py` — `OracleNotificationSource` and `OracleNotifyConfig`.
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver.notification_source`.
- `src/py/vertebra/db/adapters/oracle/__init__.py` — Lazy Oracle package exports.
- `src/py/tests/unit/db/test_oracle_adapter.py` — Oracle notification unit tests.

## Contracts & Edge Cases

- `SyncOracleDriver.notification_source()` returns `SyncOracleNotificationSource` (using `SyncOracleListener` over dedicated `SyncOracleClient` AQ dequeue); `AsyncOracleDriver.notification_source()` returns `AsyncOracleNotificationSource` (over `AsyncOracleListener`).
- Export `SyncOracleNotificationSource`, `AsyncOracleNotificationSource`, `SyncOracleListener`, and `OracleNotifyConfig` lazily in `oracle/__init__.py`; delete unprefixed `OracleNotificationSource` (`PY-07`).
- **Edge Case**: Transient ORA disconnect codes (`3113`, `3114`, `3135`, `12541`, etc.) apply bounded exponential reconnect backoff in both sync and async listeners.

## Test Matrix

- Update `test_oracle_adapter.py` and `test_oracle_native.py` to test both `SyncOracleNotificationSource` and `AsyncOracleNotificationSource`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k 'Notification or Listener' && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle
```

## Acceptance Criteria

- [ ] Oracle sync and async notification sources satisfy `SyncNotificationSource` and `AsyncNotificationSource` with zero aliases.
- [ ] Zero regressions in the chapter fast verification gate.

[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver` — Oracle driver notification_source factory.
