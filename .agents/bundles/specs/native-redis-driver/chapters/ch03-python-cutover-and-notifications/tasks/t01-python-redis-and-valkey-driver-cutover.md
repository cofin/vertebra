---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t01
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Decouple Valkey Adapter and Cut Python Redis Core and Drivers Over to vertebra._redis
description: Relocate test loopback wire parser to test helpers, remove adapters/redis imports from adapters/valkey, and rewrite RedisCore, SyncRedisDriver, and AsyncRedisDriver onto vertebra._redis.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch02-rust-clients-topologies-and-streams:t04
files:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/valkey/config.py
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
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/driver.py
estimated_loc: 1200
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
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig currently subclassing RedisConfig
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore re-exporting Redis RESP3 helpers
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver
verification_strategy: regression_tdd
---

# Task `t01`: Decouple Valkey Adapter and Cut Python Redis Core and Drivers Over to vertebra._redis

## Objective & Scope Boundary

Decouple `Resp3FakeServer` in `src/py/tests/unit/conftest.py` onto a test-only `_parse_wire_frame` helper, make `adapters/valkey/{config,core,driver,__init__}.py` self-contained with zero imports from `adapters/redis/*`, delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `adapters/redis/{core,__init__}.py`, and rewrite `SyncRedisDriver` and `AsyncRedisDriver` onto `vertebra._redis` per [`../spec.md` §2](../spec.md) [^unit-conftest] [^redis-core] [^valkey-cfg] [^valkey-core] [^redis-drv] [^valkey-drv].

## Bounded Read Manifest

- `src/py/tests/unit/conftest.py` — `Resp3FakeServer` top-level `from vertebra.db.adapters.redis.core import Resp3Parser`.
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — `socket_factory` and loopback driver tests.
- `src/py/vertebra/db/adapters/redis/core.py` — `Resp3Parser`, `Resp3Error`, `encode_*`, `crc16_xmodem`, and `parse_cluster_redirect`.
- `src/py/vertebra/db/adapters/valkey/config.py` — `ValkeyConfig` subclassing `RedisConfig`.
- `src/py/vertebra/db/adapters/valkey/core.py` — Re-exports from `redis.core`.
- `src/py/vertebra/db/adapters/redis/driver.py` — Pure-Python socket/asyncio Redis driver and store-facing methods.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.

## Contracts & Edge Cases

- Inline a self-contained test-only `_parse_wire_frame` helper in `src/py/tests/unit/conftest.py` so `Resp3FakeServer` never imports `vertebra.db.adapters.redis.core`.
- Make `ValkeyConfig` subclass `BaseDatabaseConfig` directly (owning its fields and `from_url`/`pool_key`/`build_sync_driver`/`build_async_driver` methods locally) and inline `ValkeyCore`/`SyncValkeyDriver`/`AsyncValkeyDriver` in `adapters/valkey/` with zero imports from `vertebra.db.adapters.redis` ahead of sibling `native-valkey-driver`.
- Delegate `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` in `redis/core.py` to `vertebra._redis`, update `redis/__init__.py` lazy exports (`PY-06`), and remove all Python `socket` / `asyncio.open_connection` / `socket_factory` code from `adapters/redis/` with zero fallback (`PY-07`).
- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` in `SyncRedisDriver`/`AsyncRedisDriver`; preserve `get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `pipeline()`, `add_invalidation_listener`, `cached_keys`, IAM `token_provider` refresh, and lazy dial (`_vertebra_redis_completion_drainer` for async tasks).
- **Edge Case**: Pytest collection across all of `src/py/tests/unit/` and existing Valkey tests stay green once `Resp3Parser` is deleted from `redis/core.py`.
- **Edge Case**: Constructing via `provide_driver` and calling `close()` offline succeeds without dialing TCP; `RedisConfig(url="memory://")` fails loudly with `ImproperConfigurationError`.

## Test Matrix

- `@pytest.mark.parametrize` verifies `Resp3FakeServer` framing across bulk arrays, inline commands, and pipelined bursts, plus `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` via `RedisCore`.
- `test_redis_valkey_driver.py`, `test_redis_valkey_resp3.py`, and `test_starlette_plugin.py` / `test_alchemy_cache_backend.py` verify store/cache integration, fail-loud missing-wheel hints, and zero `vertebra.db.adapters.redis` imports inside `src/py/vertebra/db/adapters/valkey/`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey && uv run --no-sync mypy src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `src/py/tests/unit/conftest.py` has zero `Resp3Parser` imports from production modules.
- [ ] `src/py/vertebra/db/adapters/valkey/` has zero imports from `vertebra.db.adapters.redis`.
- [ ] Pure-Python `Resp3Parser`, command encoders, and `socket` / `asyncio.open_connection` transport code are removed from `adapters/redis/`.
- [ ] Store, cache, driver, and type checks pass with zero regressions.

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig currently subclassing RedisConfig.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
