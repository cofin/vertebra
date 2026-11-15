---
type: Task
id: queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t04
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-stores-and-first-party-backends
title: Wire RedisQueueBackend and ValkeyQueueBackend Notification Wakeups & Result Persistence
description: Wire RedisQueueBackend (queues/redis.py over vertebra._redis) and ValkeyQueueBackend (queues/valkey.py over vertebra._valkey) notification wakeups, enqueue_sync, and TaskResultRecord persistence.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t03
files:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/valkey.py
  - src/py/vertebra/framework/queues/__init__.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/__init__.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/driver.py
estimated_loc: 225
tags:
  - queues
  - redis
  - valkey
  - streams
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: RedisQueueBackend and ValkeyQueueBackend
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: SyncRedisDriver and AsyncRedisDriver
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py
    title: SyncValkeyDriver and AsyncValkeyDriver
verification_strategy: regression_tdd
---

# Task `t04`: Wire RedisQueueBackend and ValkeyQueueBackend Notification Wakeups & Result Persistence

## Objective & Scope Boundary

Wire `RedisQueueBackend` (`src/py/vertebra/framework/queues/redis.py` over `vertebra._redis`) and `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py` over `vertebra._valkey`) with `Sync*NotificationSource`/`Async*NotificationSource` wakeups, `enqueue_sync`, and `TaskResultRecord` storage per [`../spec.md`](../spec.md) [^queue-redis] [^redis-drv] [^valkey-drv].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py`, `valkey.py`, & `__init__.py` — Redis and Valkey queue backends and public exports.
- `src/py/vertebra/db/adapters/{redis,valkey}/driver.py` — native `vertebra._redis` and `vertebra._valkey` sync/async drivers.

## Contracts & Edge Cases

- `RedisQueueBackend` executes over `vertebra._redis` (`SyncRedisNotificationSource` / `AsyncRedisNotificationSource`) and `ValkeyQueueBackend` executes over `vertebra._valkey` (`SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource`) with zero cross-imports between `queues/redis.py` and `queues/valkey.py`.
- Support sync `enqueue_sync`, async `enqueue`, cron slot leases, and `TaskResultRecord` persistence.

## Test Matrix

- Parametrized URL scheme (`redis://`, `rediss://`, `valkey://`, `valkeys://`, `unix://`), `MissingDependencyError`, stream claim/reclaim (`XAUTOCLAIM`), delayed sorted-set promotion, `enqueue_sync`, `TaskResultRecord`, and DLQ tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q
```

## Acceptance Criteria

- [ ] `RedisQueueBackend` (`vertebra._redis`) and `ValkeyQueueBackend` (`vertebra._valkey`) support `enqueue_sync`, notification wakeups, and `TaskResultRecord` storage with zero cross-imports.
- [ ] Zero regressions in the fast verification gate.

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py` — SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py` — SyncValkeyDriver and AsyncValkeyDriver.
