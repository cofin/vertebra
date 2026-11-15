---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t04
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Implement Sync/Async Redis and Valkey Pub/Sub Notification Sources
description: Implement SyncRedisNotificationSource, AsyncRedisNotificationSource, SyncValkeyNotificationSource, and AsyncValkeyNotificationSource over vertebra._redis Pub/Sub.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch03-python-cutover-and-notifications:t03
files:
- src/py/vertebra/db/adapters/redis/notify.py
- src/py/vertebra/db/adapters/redis/__init__.py
- src/py/vertebra/db/adapters/valkey/notify.py
- src/py/vertebra/db/adapters/valkey/__init__.py
tests:
- src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/adapters/redis/driver.py
- src/py/vertebra/db/adapters/redis/__init__.py
- src/py/vertebra/_redis.pyi
- src/py/vertebra/db/adapters/redis/notify.py
- src/py/vertebra/db/adapters/valkey/config.py
estimated_loc: 220
tags:
- redis
- pubsub
- notifications
- valkey
sources:
- id: driver-caps
  resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
  title: NotificationSource capability protocol in capabilities.py
- id: redis-drv
  resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
  title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
- id: valkey-drv
  resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
  title: SyncValkeyDriver and AsyncValkeyDriver
---

# Task `t04`: Implement Sync/Async Redis and Valkey Pub/Sub Notification Sources

## Objective & Scope Boundary

Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` in `src/py/vertebra/db/adapters/redis/notify.py` and lazy-export them in `redis/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^redis-drv].

Create `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `src/py/vertebra/db/adapters/valkey/notify.py` and lazy-export them in `valkey/__init__.py` per [`../spec.md` §2](../spec.md) [^valkey-drv] [^driver-caps].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource`, `AsyncNotificationSource`, and `NotificationEvent`.
- `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver` and `AsyncRedisDriver`.
- `src/py/vertebra/db/adapters/redis/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_redis.pyi` — `SyncRedisListener` and `AsyncRedisListener`.

- `src/py/vertebra/db/adapters/redis/notify.py` — `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` from `t03`.
- `src/py/vertebra/db/adapters/valkey/config.py` — `ValkeyConfig` defaults.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy package exports (`PY-06`).

## Contracts & Edge Cases

- Support `publish`, `subscribe`, `psubscribe`, `ssubscribe` (Cluster sharded pub/sub), `unsubscribe`, `listen()`, `close()`/`aclose()`, and optional `ChangeFeed` envelope hydration.
- Record pattern/shard metadata on `NotificationEvent.metadata`.
- **Edge Case**: Breaking out of `listen()` closes the dedicated `SyncRedisListener` / `AsyncRedisListener` handle.

- Default to `ValkeyConfig()` and probe `ensure_module("vertebra._redis", package="vertebra-core[valkey]", extra="valkey")`.
- **Edge Case**: Missing `vertebra._redis` raises `MissingDependencyError` naming `vertebra-core[valkey]`.

## Test Matrix

- Loopback tests in `test_redis_valkey_resp3.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, channel/pattern/shard pub/sub, `ChangeFeed` hydration, and `ChannelBus` integration.

- Parameterized tests verify `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` protocol compliance, missing-wheel error hint, and loopback message delivery.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` pass unit and protocol checks.
- [ ] `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` pass unit and type checks.
- [ ] Zero regressions in the fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
