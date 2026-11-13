---
type: Task
id: "native-redis-driver:ch04:t03"
flow_id: native-redis-driver
chapter_id: ch04-python-adapter-cutover
title: "Cut SyncRedisDriver, AsyncRedisDriver, SyncValkeyDriver, and AsyncValkeyDriver Over to vertebra._redis"
description: "Replace Python socket/asyncio Redis and Valkey drivers with thin wrappers over vertebra._redis."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch04:t02"
files:
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
  - src/py/vertebra/framework/stores.py
  - src/py/vertebra/extensions/alchemy/cache.py
estimated_loc: 245
tags:
  - redis
  - valkey
  - driver
  - async
sources:
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: "Python socket/asyncio SyncRedisDriver and AsyncRedisDriver"
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: "SyncValkeyDriver and AsyncValkeyDriver"
---

# Task `t03`: Cut SyncRedisDriver, AsyncRedisDriver, SyncValkeyDriver, and AsyncValkeyDriver Over to vertebra._redis

## Objective & Scope Boundary

Rewrite `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, and `AsyncValkeyDriver` over `vertebra._redis` and prune removed exports from `valkey/__init__.py` per [`../spec.md` §2](../spec.md) [^redis-drv] [^valkey-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/driver.py` — 1,632-line Python socket/asyncio driver and store-facing methods.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy exports to prune `Resp3*` / `encode_*`.
- `src/py/vertebra/framework/stores.py` — `KeyValueStore` caller contract.
- `src/py/vertebra/extensions/alchemy/cache.py` — `SQLAlchemyCacheBackend` caller contract.

## Contracts & Edge Cases

- Probe `vertebra-core[redis]` (`extra="redis"`) in Redis drivers and `vertebra-core[valkey]` (`extra="valkey"`) in Valkey drivers; remove `socket_factory` parameter.
- Preserve `get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `pipeline()`, `add_invalidation_listener`, `cached_keys`, IAM `token_provider` refresh, and lazy dial.
- **Edge Case**: Constructing via `provide_driver` and calling `close()` without a running server succeeds without dialing TCP.

## Test Matrix

- Run `test_redis_valkey_driver.py`, `test_redis_valkey_resp3.py`, and downstream store/cache unit tests.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_litestar_starlette_stores.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] Zero Python `socket` or `asyncio.open_connection` code remains in `adapters/{redis,valkey}/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
