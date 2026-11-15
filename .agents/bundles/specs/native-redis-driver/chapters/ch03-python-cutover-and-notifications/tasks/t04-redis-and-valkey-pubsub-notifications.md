---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t04
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Implement Sync/Async Redis Pub/Sub Notification Sources
description: Implement SyncRedisNotificationSource and AsyncRedisNotificationSource over vertebra._redis Pub/Sub.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch03-python-cutover-and-notifications:t03
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
estimated_loc: 180
tags:
  - redis
  - pubsub
  - notifications
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource capability protocol in capabilities.py
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: SyncRedisDriver and AsyncRedisDriver
verification_strategy: regression_tdd
---

# Task `t04`: Implement Sync/Async Redis Pub/Sub Notification Sources

## Objective & Scope Boundary

Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` (`src/py/vertebra/db/adapters/redis/notify.py`) over `vertebra._redis.SyncRedisListener` and `AsyncRedisListener`, lazy-exporting them from `src/py/vertebra/db/adapters/redis/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^redis-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource`, `AsyncNotificationSource`, and `NotificationEvent`.
- `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver` and `AsyncRedisDriver` (`vertebra._redis` client plumbing).
- `src/py/vertebra/db/adapters/redis/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_redis.pyi` — `SyncRedisListener` and `AsyncRedisListener` signatures.

## Contracts & Edge Cases

- Support `publish`, `subscribe`, `psubscribe`, `ssubscribe` (Cluster sharded pub/sub), `unsubscribe`, `listen()`, `close()`/`aclose()`, and optional `ChangeFeed` envelope hydration while recording pattern/shard metadata on `NotificationEvent.metadata`.
- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")`.
- **Edge Case**: Breaking out of `listen()` closes the dedicated `SyncRedisListener` / `AsyncRedisListener` handle deterministically.
- **Edge Case**: Missing `vertebra._redis` raises `MissingDependencyError` naming `vertebra-core[redis]`.

## Test Matrix

- Loopback tests in `test_redis_valkey_resp3.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, channel/pattern/shard pub/sub, `ChangeFeed` hydration, missing-wheel error hints, and `ChannelBus` integration.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis && uv run --no-sync mypy src/py/vertebra/db/adapters/redis
```

## Acceptance Criteria

- [ ] `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` pass unit and protocol checks.
- [ ] Zero regressions in the fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — SyncRedisDriver and AsyncRedisDriver.
