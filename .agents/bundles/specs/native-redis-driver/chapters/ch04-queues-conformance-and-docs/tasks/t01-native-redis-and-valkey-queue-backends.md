---
type: Task
id: native-redis-driver:ch04-queues-conformance-and-docs:t01
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: Cut RedisQueueBackend Over to vertebra._redis Streams and ZSETs
description: Wire RedisQueueBackend onto native vertebra._redis stream consumer groups and delayed ZSETs.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch03-python-cutover-and-notifications:t04
files:
  - src/py/vertebra/framework/queues/redis.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/_redis.pyi
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 180
tags:
  - queues
  - streams
  - redis
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: RedisQueueBackend using redis.asyncio
verification_strategy: regression_tdd
---

# Task `t01`: Cut RedisQueueBackend Over to vertebra._redis Streams and ZSETs

## Objective & Scope Boundary

Replace `redis.asyncio` in `src/py/vertebra/framework/queues/redis.py` with `_RedisStreamAdapter` wrapping `vertebra._redis.AsyncRedisClient` per [`../spec.md` §2](../spec.md) [^queue-redis].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` — `RedisQueueBackend` and `RedisStreamClient`.
- `src/py/vertebra/_redis.pyi` — `AsyncRedisClient` stream and Lua script methods.
- `src/py/tests/unit/framework/test_distributed_queues.py` — Distributed queue unit tests.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` in `RedisQueueBackend`.
- Accept `redis://`, `rediss://`, and `unix://` while rejecting `memory://`, `valkey://`, and `valkeys://` and preserving `client: RedisStreamClient | None = None`.
- **Edge Case**: Never use a `Native*` class prefix (`_RedisStreamAdapter` complies with `PY-07`).

## Test Matrix

- `@pytest.mark.parametrize` covers `redis://`, `rediss://`, and `unix://`, `memory://` and `valkey://` rejection, missing-wheel `MissingDependencyError` hints, and loopback enqueue/consume/ack/autoclaim/DLQ.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/framework/queues/redis.py && uv run --no-sync mypy src/py/vertebra/framework/queues/redis.py
```

## Acceptance Criteria

- [ ] Zero `redis.asyncio` references remain in `src/py/vertebra/framework/queues/redis.py`.
- [ ] Zero regressions in the fast verification gate.

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend using redis.asyncio.
