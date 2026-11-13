---
type: Task
id: "native-redis-driver:ch05:t01"
flow_id: native-redis-driver
chapter_id: ch05-notification-source-split
title: "Split NotificationSource into SyncNotificationSource and AsyncNotificationSource"
description: "Replace NotificationSource in capabilities.py, driver/__init__.py, and server/_channels.py with Sync*/Async* protocols."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/server/_channels.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/server/_channels.py
  - src/py/tests/unit/db/test_native_db_drivers.py
estimated_loc: 130
tags:
  - capabilities
  - notifications
  - channels
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "NotificationSource capability protocol in capabilities.py"
  - id: driver-init
    resource: src/py/vertebra/db/driver/__init__.py
    title: "Lazy capability exports in vertebra.db.driver"
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: "ChannelBus notification pump"
---

# Task `t01`: Split NotificationSource into SyncNotificationSource and AsyncNotificationSource

## Objective & Scope Boundary

Split `NotificationSource` into `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` across `capabilities.py`, `driver/__init__.py`, and `server/_channels.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^driver-init] [^channel-bus].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `NotificationSource` protocol definition.
- `src/py/vertebra/db/driver/__init__.py` — Lazy exports for `capabilities`.
- `src/py/vertebra/server/_channels.py` — `ChannelBus` `notification_source` typing.
- `src/py/tests/unit/db/test_native_db_drivers.py` — Capability and `ChannelBus` unit tests.

## Contracts & Edge Cases

- Delete `NotificationSource` with zero aliases (`PY-07`); export `SyncNotificationSource` and `AsyncNotificationSource` lazily in `driver/__init__.py` (`PY-06`).
- Update `ChannelBus` in `src/py/vertebra/server/_channels.py` to accept `AsyncNotificationSource | None`.
- **Edge Case**: `isinstance(obj, SyncNotificationSource)` and `isinstance(obj, AsyncNotificationSource)` validate protocol methods at runtime.

## Test Matrix

- Update `test_native_db_drivers.py` to verify both `SyncNotificationSource` and `AsyncNotificationSource` protocol conformance and `ChannelBus.pump_notifications`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py -q -k 'Notification or ChannelBus' && uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/server/_channels.py
```

## Acceptance Criteria

- [ ] `SyncNotificationSource` and `AsyncNotificationSource` replace `NotificationSource` cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
