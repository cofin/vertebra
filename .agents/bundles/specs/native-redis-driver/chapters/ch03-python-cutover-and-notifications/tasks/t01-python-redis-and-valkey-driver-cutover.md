---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t01
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Cut Python Redis and Valkey Core and Drivers Over to vertebra._redis
description: Relocate test loopback wire parser to test helpers and rewrite RedisCore, ValkeyCore, Sync/AsyncRedisDriver, and Sync/AsyncValkeyDriver onto vertebra._redis.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- native-redis-driver:ch02-rust-clients-topologies-and-streams:t04
files:
- src/py/vertebra/db/adapters/redis/core.py
- src/py/vertebra/db/adapters/valkey/core.py
- src/py/vertebra/db/adapters/redis/__init__.py
- src/py/vertebra/db/adapters/redis/driver.py
- src/py/vertebra/db/adapters/valkey/driver.py
- src/py/vertebra/db/adapters/valkey/__init__.py
tests:
- src/py/tests/unit/conftest.py
- src/py/tests/unit/db/test_redis_valkey_driver.py
- src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
- src/py/tests/unit/conftest.py
- src/py/tests/unit/db/test_redis_valkey_driver.py
- src/py/tests/unit/db/test_redis_valkey_resp3.py
- src/py/vertebra/db/adapters/redis/core.py
- src/py/vertebra/db/adapters/valkey/core.py
- src/py/vertebra/db/adapters/redis/__init__.py
estimated_loc: 245
tags:
- redis
- tests
- resp3
- valkey
- core
- debloat
- driver
- async
sources:
- id: unit-conftest
  resource: src/py/tests/unit/conftest.py::Resp3FakeServer
  title: Resp3FakeServer loopback test fixture coupled to Resp3Parser
- id: redis-core
  resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
  title: Pure-Python Resp3Parser and RESP command encoders
- id: valkey-core
  resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
  title: ValkeyCore re-exporting Redis RESP3 helpers
- id: redis-drv
  resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
  title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
- id: valkey-drv
  resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
  title: SyncValkeyDriver and AsyncValkeyDriver
---

# Task `t01`: Cut Python Redis and Valkey Core and Drivers Over to vertebra._redis

## Objective & Scope Boundary

Eliminate `from vertebra.db.adapters.redis.core import Resp3Parser` in `src/py/tests/unit/conftest.py` BEFORE deleting `Resp3Parser` in `t02`, and migrate unit tests from `socket_factory` / `Resp3Error` onto loopback TCP and `DatabaseError` per [`../spec.md` §2](../spec.md) [^unit-conftest].

Delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` across `redis/core.py`, `valkey/core.py`, and `redis/__init__.py`, delegating `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` to `vertebra._redis` per [`../spec.md` §2](../spec.md) [^redis-core] [^valkey-core].

Rewrite `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, and `AsyncValkeyDriver` over `vertebra._redis` and prune removed exports from `valkey/__init__.py` per [`../spec.md` §2](../spec.md) [^redis-drv] [^valkey-drv].

## Bounded Read Manifest

- `src/py/tests/unit/conftest.py` — `Resp3FakeServer` top-level `from vertebra.db.adapters.redis.core import Resp3Parser`.
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — `socket_factory` and loopback driver tests.
- `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3 protocol and redirect tests.

- `src/py/vertebra/db/adapters/redis/core.py` — `Resp3Parser`, `Resp3Error`, `encode_*`, `crc16_xmodem`, and `parse_cluster_redirect`.
- `src/py/vertebra/db/adapters/valkey/core.py` — Re-exports from `redis.core`.
- `src/py/vertebra/db/adapters/redis/__init__.py` — Lazy exports (`PY-06`).
- `src/py/vertebra/_redis.pyi` — `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.

- `src/py/vertebra/db/adapters/redis/driver.py` — 1,632-line Python socket/asyncio driver and store-facing methods.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy exports to prune `Resp3*` / `encode_*`.
- `src/py/vertebra/framework/stores.py` — `KeyValueStore` caller contract.
- `src/py/vertebra/extensions/alchemy/cache.py` — `SQLAlchemyCacheBackend` caller contract.

## Contracts & Edge Cases

- Implement a compact test-only `_parse_wire_frame` helper inside `src/py/tests/unit/conftest.py` so `Resp3FakeServer` has zero imports from `vertebra.db.adapters.redis.core`.
- **Edge Case**: Ensure pytest collection across all of `src/py/tests/unit/` succeeds even when `Resp3Parser` is absent.

- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` with zero pure-Python fallback (`PY-07`).
- **Edge Case**: Update `valkey/core.py` in the same task so `from vertebra.db.adapters.valkey import ValkeyCore` never hits `ImportError`.

- Probe `vertebra-core[redis]` (`extra="redis"`) in Redis drivers and `vertebra-core[valkey]` (`extra="valkey"`) in Valkey drivers; remove `socket_factory` parameter.
- Preserve `get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `pipeline()`, `add_invalidation_listener`, `cached_keys`, IAM `token_provider` refresh, and lazy dial.
- **Edge Case**: Constructing via `provide_driver` and calling `close()` without a running server succeeds without dialing TCP.

## Test Matrix

- `@pytest.mark.parametrize` verifies `Resp3FakeServer` framing across bulk arrays, inline commands, and pipelined bursts.

- `@pytest.mark.parametrize` verifies `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` via `RedisCore` and `ValkeyCore`.

- Run `test_redis_valkey_driver.py`, `test_redis_valkey_resp3.py`, and downstream store/cache unit tests.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_litestar_starlette_stores.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `src/py/tests/unit/conftest.py` has zero `Resp3Parser` imports from production modules.
- [ ] Pure-Python `Resp3Parser` and command encoders are removed cleanly.
- [ ] Zero Python `socket` or `asyncio.open_connection` code remains in `adapters/{redis,valkey}/`.
- [ ] Zero regressions in the fast verification gate.

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
