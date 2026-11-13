---
type: Task
id: "queues-tasks-and-channels:ch02-first-party-queue-backends-and-service:t03"
flow_id: queues-tasks-and-channels
chapter_id: ch02-first-party-queue-backends-and-service
title: "Wire Native RedisQueueBackend and ValkeyQueueBackend Over vertebra._redis Streams & Sorted Sets"
description: "Bind RedisQueueBackend and ValkeyQueueBackend to SyncRedisDriver/AsyncRedisDriver and SyncValkeyDriver/AsyncValkeyDriver streams, sorted sets, and pub/sub wakeups."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/framework/queues/redis.py
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
    title: "RedisQueueBackend and ValkeyQueueBackend"
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: "SyncRedisDriver and AsyncRedisDriver"
---

# Task `t03`: Wire Native RedisQueueBackend and ValkeyQueueBackend Over vertebra._redis Streams & Sorted Sets

## Objective & Scope Boundary

Wire `RedisQueueBackend` and `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/redis.py`) over `vertebra._redis` streams, sorted sets, and notification sources per [`../spec.md`](../spec.md) [^queue-redis] [^redis-drv].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` & `__init__.py` — Redis and Valkey queue backends and public exports.
- `src/py/vertebra/db/adapters/{redis,valkey}/driver.py` — native `vertebra._redis` sync/async drivers.

## Contracts & Edge Cases

- Execute immediate tasks over Redis/Valkey Streams (`XADD`, `XREADGROUP`, `XAUTOCLAIM`, `XACK`) and delayed/cron tasks over sorted-set `ZADD` + atomic Lua promotion into streams, with `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` wakeups and `TaskResultRecord` storage.
- Support `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` URLs via `vertebra-core[redis]` / `vertebra-core[valkey]` with zero `redis.asyncio` imports; support sync `enqueue_sync` and async `enqueue`.

## Test Matrix

- Parametrized URL scheme (`redis://`, `rediss://`, `valkey://`, `valkeys://`, `unix://`), `MissingDependencyError`, stream claim/reclaim (`XAUTOCLAIM`), delayed sorted-set promotion, and DLQ tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `RedisQueueBackend` and `ValkeyQueueBackend` run natively over `vertebra._redis` streams/sorted-sets with zero `redis.asyncio` imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py` — SyncRedisDriver and AsyncRedisDriver.
