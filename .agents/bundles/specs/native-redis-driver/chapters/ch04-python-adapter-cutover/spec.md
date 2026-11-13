---
type: Spec
flow_id: native-redis-driver
chapter_id: ch04-python-adapter-cutover
title: "Chapter 04: Loopback Fixture Decoupling and Python Redis/Valkey Adapter Cutover"
description: "Decouple Resp3FakeServer in conftest.py onto a self-contained test framing helper BEFORE deleting pure-Python Resp3Parser, then cut Redis and Valkey Python adapters over to vertebra._redis."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - redis
  - valkey
  - driver
  - cutover
sources:
  - id: unit-conftest
    resource: src/py/tests/unit/conftest.py::Resp3FakeServer
    title: "Resp3FakeServer loopback test fixture coupled to Resp3Parser"
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: "Pure-Python Resp3Parser and RESP command encoders"
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: "ValkeyCore re-exporting Redis RESP3 helpers"
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: "Python socket/asyncio SyncRedisDriver and AsyncRedisDriver"
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: "SyncValkeyDriver and AsyncValkeyDriver"
---

# Chapter 04: Loopback Fixture Decoupling and Python Redis/Valkey Adapter Cutover

<!-- truth: start -->
- `src/py/tests/unit/conftest.py::Resp3FakeServer` moves onto a self-contained test-only `_parse_wire_frame` helper BEFORE `Resp3Parser` is deleted so pytest collection across `src/py/tests/unit/` never breaks [^unit-conftest] [^redis-core].
- `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` are removed from `redis/core.py`, `valkey/core.py`, `redis/__init__.py`, and `valkey/__init__.py` together [^redis-core] [^valkey-core].
- `SyncRedisDriver`/`AsyncRedisDriver` (`vertebra-core[redis]`) and `SyncValkeyDriver`/`AsyncValkeyDriver` (`vertebra-core[valkey]`) wrap `vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`) with zero Python `socket` / `asyncio.open_connection` transport code [^redis-drv] [^valkey-drv].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/tests/unit/conftest.py` imports `Resp3Parser` from `vertebra.db.adapters.redis.core` at module top level [^unit-conftest], and `valkey/core.py` re-exports `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `redis/core.py` [^valkey-core]. Decoupling `conftest.py` in `t01`, updating `redis/core.py` + `valkey/core.py` together in `t02`, and cutting both drivers over in `t03` eliminates 1,600+ lines of Python socket/parser code while keeping `KeyValueStore`, `MemoryStore`, `CacheStore`, and `SQLAlchemyCacheBackend` green.

**Non-Goals:** `NotificationSource` capability split (`ch05`); `vertebra-db::resp3` Rust removal and Redis/Valkey notification sources (`ch06`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Loopback fixture | `src/py/tests/unit/conftest.py::Resp3FakeServer` [^unit-conftest] | Self-contained test-only `_parse_wire_frame` |
| Core modules | `adapters/redis/{core,__init__}.py`, `adapters/valkey/core.py` [^redis-core] [^valkey-core] | `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect` delegated to `vertebra._redis` |
| Drivers | `adapters/redis/driver.py`, `adapters/valkey/{driver,__init__}.py` [^redis-drv] [^valkey-drv] | Thin native wrappers preserving store/cache contracts |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10**: Inline a self-contained test-only `_parse_wire_frame` helper into `src/py/tests/unit/conftest.py::Resp3FakeServer` and migrate unit tests off `Resp3Parser`/`Resp3Error`/`socket_factory` (`t01`).
- **R11**: Delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `redis/core.py`, `valkey/core.py`, and `redis/__init__.py`, delegating `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` to `vertebra._redis` (`t02`).
- **R12**: Rewrite `SyncRedisDriver`/`AsyncRedisDriver` (`extra="redis"`) and `SyncValkeyDriver`/`AsyncValkeyDriver` (`extra="valkey"`) over `vertebra._redis` + `spawn_rust_db_task_with_attr_and_cancel`, preserving all downstream store/cache methods (`t03`).

```python
class SyncRedisDriver:
    # Probes ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
    # Preserves get, set(ex=, px=, nx=, xx=), delete, unlink, scan, pttl, flushdb, pipeline(),
    # eval_script, discover_sentinel_master, add_invalidation_listener, cached_keys, close()

class AsyncRedisDriver:
    # Probes ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
    # Uses AsyncRedisClient + spawn_rust_db_task_with_attr_and_cancel ("_vertebra_redis_completion_drainer")

class SyncValkeyDriver(SyncRedisDriver): ...  # extra="valkey" -> vertebra-core[valkey]
class AsyncValkeyDriver(AsyncRedisDriver): ...  # extra="valkey" -> vertebra-core[valkey]
```

**Integration Flow:**
- `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and `SQLAlchemyCacheBackend` continue calling `SyncRedisDriver`/`AsyncRedisDriver` and `SyncValkeyDriver`/`AsyncValkeyDriver` unchanged.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wheel missing | `vertebra._redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` or `vertebra-core[valkey]` | Distinct extra hints |
| `memory://` DSN | `RedisConfig(url="memory://")` | Raises `ImproperConfigurationError` | No in-memory fake |

## 3. Documentation & OKF Knowledge Plan

Driver cutover docs execute in `ch07:t03` and `ch08`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` | `t01` | `src/py/tests/unit/conftest.py` | `pytest src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `R11` | `t02` | `adapters/redis/{core,__init__}.py`, `adapters/valkey/core.py` | `pytest src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `R12` | `t03` | `adapters/redis/driver.py`, `adapters/valkey/{driver,__init__}.py` | `pytest src/py/tests/unit/db/test_redis_valkey_driver.py` |

## 5. Implementation Plan

- [ ] [t01-decouple-test-loopback-wire-parser.md](tasks/t01-decouple-test-loopback-wire-parser.md) — Replace Resp3Parser import in src/py/tests/unit/conftest.py with a self-contained test framing helper.
- [ ] [t02-redis-and-valkey-core-cutover.md](tasks/t02-redis-and-valkey-core-cutover.md) — Remove Resp3Parser/Resp3Error/encode_* from redis/core.py, valkey/core.py, and redis/__init__.py; delegate to _redis.
- [ ] [t03-redis-and-valkey-driver-cutover.md](tasks/t03-redis-and-valkey-driver-cutover.md) — Replace Python socket/asyncio Redis and Valkey drivers with thin wrappers over vertebra._redis.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: Zero `import socket` or `asyncio.open_connection` calls remain in `src/py/vertebra/db/adapters/{redis,valkey}/`.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py`

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
