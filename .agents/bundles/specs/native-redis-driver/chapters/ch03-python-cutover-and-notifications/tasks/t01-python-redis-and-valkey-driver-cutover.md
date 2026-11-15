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
plan_commit: 1b191388
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
- src/py/vertebra/db/adapters/redis/core.py
- src/py/vertebra/db/adapters/valkey/core.py
- src/py/vertebra/db/adapters/redis/driver.py
- src/py/vertebra/db/adapters/valkey/driver.py
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

Decouple `Resp3FakeServer` in `src/py/tests/unit/conftest.py` onto a test-only `_parse_wire_frame` helper, migrate tests off `socket_factory`/`Resp3Error`, delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `adapters/{redis,valkey}/{core,__init__}.py`, and rewrite `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, and `AsyncValkeyDriver` onto `vertebra._redis` per [`../spec.md` §2](../spec.md) [^unit-conftest] [^redis-core] [^valkey-core] [^redis-drv] [^valkey-drv].

## Bounded Read Manifest

- `src/py/tests/unit/conftest.py` — `Resp3FakeServer` top-level `from vertebra.db.adapters.redis.core import Resp3Parser`.
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — `socket_factory` and loopback driver tests.
- `src/py/vertebra/db/adapters/redis/core.py` — `Resp3Parser`, `Resp3Error`, `encode_*`, `crc16_xmodem`, and `parse_cluster_redirect`.
- `src/py/vertebra/db/adapters/valkey/core.py` — Re-exports from `redis.core`.
- `src/py/vertebra/db/adapters/redis/driver.py` — Pure-Python socket/asyncio Redis driver and store-facing methods.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.

## Contracts & Edge Cases

- Inline a self-contained test-only `_parse_wire_frame` helper in `src/py/tests/unit/conftest.py` so `Resp3FakeServer` never imports `vertebra.db.adapters.redis.core`.
- Delegate `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` to `vertebra._redis`, update `redis/__init__.py` and `valkey/__init__.py` lazy exports (`PY-06`), and remove all Python `socket` / `asyncio.open_connection` / `socket_factory` code with zero fallback (`PY-07`).
- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` in Redis drivers and `package="vertebra-core[valkey]", extra="valkey"` in Valkey drivers; preserve `get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `pipeline()`, `add_invalidation_listener`, `cached_keys`, IAM `token_provider` refresh, and lazy dial (`_vertebra_redis_completion_drainer` for async tasks).
- **Edge Case**: Pytest collection across all of `src/py/tests/unit/` stays intact once `Resp3Parser` is deleted.
- **Edge Case**: Constructing via `provide_driver` and calling `close()` offline succeeds without dialing TCP; `RedisConfig(url="memory://")` fails loudly with `ImproperConfigurationError`.

## Test Matrix

- `@pytest.mark.parametrize` verifies `Resp3FakeServer` framing across bulk arrays, inline commands, and pipelined bursts, plus `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` via `RedisCore` and `ValkeyCore`.
- `test_redis_valkey_driver.py`, `test_redis_valkey_resp3.py`, and `test_litestar_starlette_stores.py` verify store/cache integration and fail-loud missing-wheel hints.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_litestar_starlette_stores.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey && uv run --no-sync mypy src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `src/py/tests/unit/conftest.py` has zero `Resp3Parser` imports from production modules.
- [ ] Pure-Python `Resp3Parser`, command encoders, and `socket` / `asyncio.open_connection` transport code are removed cleanly.
- [ ] Store, cache, driver, and type checks pass with zero regressions.

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
