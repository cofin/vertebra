---
type: Task
id: queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t03
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: Consolidate ValkeyStore, RedisStore & MemoryStore Over vertebra._redis with RESP3 Tracking
description: Consolidate framework/stores.py, extensions/litestar/store.py, and extensions/starlette/store.py over vertebra._redis preserving prefix-scoped SCAN+UNLINK.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t02
files:
- src/py/vertebra/framework/stores.py
- src/py/vertebra/extensions/litestar/store.py
- src/py/vertebra/extensions/starlette/store.py
tests:
- src/py/tests/unit/extensions/test_litestar_plugin.py
- src/py/tests/unit/extensions/test_starlette_plugin.py
read_set:
- src/py/vertebra/framework/stores.py
- src/py/vertebra/extensions/litestar/store.py
- src/py/vertebra/extensions/starlette/store.py
- src/py/vertebra/db/adapters/valkey/driver.py
- src/py/vertebra/db/adapters/redis/driver.py
estimated_loc: 225
tags:
- stores
- valkey
- redis
- litestar
sources:
- id: fw-stores
  resource: src/py/vertebra/framework/stores.py
  title: First-party KeyValueStore implementations
- id: ls-store
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Litestar Store implementations
- id: st-store
  resource: src/py/vertebra/extensions/starlette/store.py
  title: Starlette CacheStore calling flushdb()
---

# Task `t03`: Consolidate ValkeyStore, RedisStore & MemoryStore Over vertebra._redis with RESP3 Tracking

## Objective & Scope Boundary

Consolidate `ValkeyStore`, `RedisStore`, and `MemoryStore` across `framework/stores.py`, `extensions/litestar/store.py`, and `extensions/starlette/store.py` per [`../spec.md`](../spec.md) [^fw-stores] [^ls-store] [^st-store].

## Bounded Read Manifest

- `src/py/vertebra/framework/stores.py` — remove duplicate `KeyValueStore` socket/dict wrappers.
- `src/py/vertebra/extensions/litestar/store.py` & `starlette/store.py` — `litestar.stores.base.Store` and Starlette store wrappers.

## Contracts & Edge Cases

- Implement `ValkeyStore(litestar.stores.base.Store)` and `RedisStore(Store)` consuming `AsyncValkeyDriver` / `AsyncRedisDriver` (`vertebra._redis`) with millisecond `PX` TTLs, RESP3 `CLIENT TRACKING` L1 invalidation, and preserved prefix-scoped `SCAN` + `UNLINK` in `delete_all()` (retaining zero `flushdb()` calls across both Litestar and Starlette stores).
- Deduplicate `framework/stores.py` and `extensions/starlette/store.py` on top of the shared native store primitives.

## Test Matrix

- Parametrized `set`/`get`/`delete`/`expires_in`/prefix-isolated `delete_all` (asserting sibling prefixes survive) and RESP3 `CLIENT TRACKING` invalidation tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync pytest src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py
```

## Acceptance Criteria

- [ ] `ValkeyStore`, `RedisStore`, and `MemoryStore` share one native `vertebra._redis` foundation with RESP3 invalidation and zero `flushdb()` calls.
- [ ] Zero regressions in the fast verification gate.

[^fw-stores]: `src/py/vertebra/framework/stores.py` — First-party KeyValueStore implementations.
[^ls-store]: `src/py/vertebra/extensions/litestar/store.py` — Litestar Store implementations.
[^st-store]: `src/py/vertebra/extensions/starlette/store.py` — Starlette CacheStore calling flushdb().
