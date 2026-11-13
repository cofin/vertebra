---
type: Task
id: "native-redis-driver:ch06:t03"
flow_id: native-redis-driver
chapter_id: ch06-redis-notifications-and-resp3-cleanup
title: "Implement SyncRedisNotificationSource and AsyncRedisNotificationSource"
description: "Create redis/notify.py implementing SyncNotificationSource and AsyncNotificationSource over SyncRedisListener/AsyncRedisListener."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch04:t03"
  - "native-redis-driver:ch05:t01"
files:
  - src/py/vertebra/db/adapters/redis/notify.py
  - src/py/vertebra/db/adapters/redis/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/redis/__init__.py
  - src/py/vertebra/_redis.pyi
estimated_loc: 205
tags:
  - redis
  - pubsub
  - notifications
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "NotificationSource capability protocol in capabilities.py"
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: "Python socket/asyncio SyncRedisDriver and AsyncRedisDriver"
---

# Task `t03`: Implement SyncRedisNotificationSource and AsyncRedisNotificationSource

## Objective & Scope Boundary

Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` in `src/py/vertebra/db/adapters/redis/notify.py` and lazy-export them in `redis/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^redis-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource`, `AsyncNotificationSource`, and `NotificationEvent`.
- `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver` and `AsyncRedisDriver`.
- `src/py/vertebra/db/adapters/redis/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_redis.pyi` — `SyncRedisListener` and `AsyncRedisListener`.

## Contracts & Edge Cases

- Support `publish`, `subscribe`, `psubscribe`, `ssubscribe` (Cluster sharded pub/sub), `unsubscribe`, `listen()`, `close()`/`aclose()`, and optional `ChangeFeed` envelope hydration.
- Record pattern/shard metadata on `NotificationEvent.metadata`.
- **Edge Case**: Breaking out of `listen()` closes the dedicated `SyncRedisListener` / `AsyncRedisListener` handle.

## Test Matrix

- Loopback tests in `test_redis_valkey_resp3.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, channel/pattern/shard pub/sub, `ChangeFeed` hydration, and `ChannelBus` integration.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/redis
```

## Acceptance Criteria

- [ ] `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` pass unit and protocol checks.
- [ ] Zero regressions in the chapter fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
