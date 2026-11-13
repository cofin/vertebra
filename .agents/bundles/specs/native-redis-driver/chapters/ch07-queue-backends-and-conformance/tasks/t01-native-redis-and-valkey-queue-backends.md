---
type: Task
id: "native-redis-driver:ch07:t01"
flow_id: native-redis-driver
chapter_id: ch07-queue-backends-and-conformance
title: "Cut RedisQueueBackend and ValkeyQueueBackend Off redis.asyncio Onto vertebra._redis"
description: "Implement _RedisStreamAdapter over AsyncRedisClient in framework/queues/redis.py and support valkey/valkeys/unix URLs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch03:t04"
  - "native-redis-driver:ch04:t03"
files:
  - src/py/vertebra/framework/queues/redis.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/_redis.pyi
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 220
tags:
  - queues
  - streams
  - redis
  - valkey
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "RedisQueueBackend and ValkeyQueueBackend using redis.asyncio"
---

# Task `t01`: Cut RedisQueueBackend and ValkeyQueueBackend Off redis.asyncio Onto vertebra._redis

## Objective & Scope Boundary

Replace `redis.asyncio` in `src/py/vertebra/framework/queues/redis.py` with `_RedisStreamAdapter` wrapping `vertebra._redis.AsyncRedisClient` per [`../spec.md` §2](../spec.md) [^queue-redis].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` — `RedisQueueBackend`, `ValkeyQueueBackend`, and `RedisStreamClient`.
- `src/py/vertebra/_redis.pyi` — `AsyncRedisClient` stream and Lua script methods.
- `src/py/tests/unit/framework/test_distributed_queues.py` — Distributed queue unit tests.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` in `RedisQueueBackend` and `extra="valkey"` in `ValkeyQueueBackend`.
- Accept `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` while rejecting `memory://` and preserving `client: RedisStreamClient | None = None`.
- **Edge Case**: Never use a `Native*` class prefix (`_RedisStreamAdapter` complies with `PY-07`).

## Test Matrix

- `@pytest.mark.parametrize` covers all 5 accepted URL schemes, `memory://` rejection, missing-wheel `MissingDependencyError` hints, and loopback enqueue/consume/ack/autoclaim/DLQ.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/framework/queues/redis.py && uv run --no-sync mypy src/py/vertebra/framework/queues/redis.py
```

## Acceptance Criteria

- [ ] Zero `redis.asyncio` references remain in `src/py/vertebra/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
