---
type: Task
id: "native-redis-driver:ch06:t04"
flow_id: native-redis-driver
chapter_id: ch06-redis-notifications-and-resp3-cleanup
title: "Implement SyncValkeyNotificationSource and AsyncValkeyNotificationSource"
description: "Create valkey/notify.py defaulting ValkeyConfig and probing vertebra-core[valkey], lazy-exported in valkey/__init__.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch06:t03"
files:
  - src/py/vertebra/db/adapters/valkey/notify.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/vertebra/db/adapters/redis/notify.py
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
estimated_loc: 95
tags:
  - valkey
  - pubsub
  - notifications
sources:
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: "SyncValkeyDriver and AsyncValkeyDriver"
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "NotificationSource capability protocol in capabilities.py"
---

# Task `t04`: Implement SyncValkeyNotificationSource and AsyncValkeyNotificationSource

## Objective & Scope Boundary

Create `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `src/py/vertebra/db/adapters/valkey/notify.py` and lazy-export them in `valkey/__init__.py` per [`../spec.md` §2](../spec.md) [^valkey-drv] [^driver-caps].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/notify.py` — `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` from `t03`.
- `src/py/vertebra/db/adapters/valkey/config.py` — `ValkeyConfig` defaults.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy package exports (`PY-06`).

## Contracts & Edge Cases

- Default to `ValkeyConfig()` and probe `ensure_module("vertebra._redis", package="vertebra-core[valkey]", extra="valkey")`.
- **Edge Case**: Missing `vertebra._redis` raises `MissingDependencyError` naming `vertebra-core[valkey]`.

## Test Matrix

- Parameterized tests verify `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` protocol compliance, missing-wheel error hint, and loopback message delivery.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` pass unit and type checks.
- [ ] Zero regressions in the chapter fast verification gate.

[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
