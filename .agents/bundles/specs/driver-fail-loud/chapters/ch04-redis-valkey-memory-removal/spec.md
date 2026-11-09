---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch04-redis-valkey-memory-removal
title: "Chapter 04: Redis and Valkey memory:// Removal"
description: RedisConfig drops the memory scheme so the registry rejects memory://, conformance cache cases run only against live servers, and Redis and Valkey drivers delete their in-process key store and raise ImproperConfigurationError for memory:// through every store and cache factory.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch03-remove-memory-fakes
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
last_verified_checkpoint: 20c535e3
last_reviewed_commit: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-09T03:27:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
sources:
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis sync and async RESP3 drivers with memory command emulation
  - id: redis-config
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis config whose url_schemes include memory
  - id: valkey-config
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: Valkey config subclassing RedisConfig
  - id: registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url
    title: Scheme dispatch raising ValueError for unregistered schemes
  - id: improper-config
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error subclassing VertebraError and ValueError
  - id: kv-store
    resource: src/py/vertebra/framework/stores.py::KeyValueStore
    title: Framework key-value store with from_valkey and from_redis factories
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore
    title: Litestar store with from_valkey and from_redis factories
  - id: starlette-store
    resource: src/py/vertebra/extensions/starlette/store.py::CacheStore
    title: Starlette cache store with from_valkey and from_redis factories
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url
    title: Advanced Alchemy cache whose from_valkey and from_redis build drivers and whose from_url memory:// selects the local L1 cache
  - id: conformance-cache
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config
    title: Conformance cache config resolution
  - id: integration-fixtures
    resource: src/py/tests/integration/conftest.py::vtb_redis_config
    title: pytest-databases Redis and Valkey config fixtures
tags:
  - database
  - drivers
  - fail-loud
  - redis
  - valkey
  - registry
  - conformance
---

# Chapter 04: Redis and Valkey memory:// Removal

<!-- truth: start -->
- **Registry drops the scheme**: `RedisConfig.url_schemes == ("redis", "rediss")`; `DatabaseRegistry.parse_url("memory://")` raises `ValueError("Unsupported database URL scheme: 'memory'")`.[^redis-config][^registry]
- **Redis and Valkey reject memory://**: `SyncRedisDriver.__init__` and `AsyncRedisDriver.__init__` raise `ImproperConfigurationError` for `memory://` URLs, so every Valkey subclass, every `from_valkey` / `from_redis` store factory, and `SQLAlchemyCacheBackend.from_valkey` / `from_redis` raise it too; without the guard a `memory://` URL resolves to `127.0.0.1:6379`.[^redis-driver][^improper-config][^kv-store][^litestar-store][^starlette-store][^alchemy-cache]
- **No in-process key store in the driver**: `_kv`, `_ttls`, `_scripts`, `_execute_memory_command`, `_eval_memory_script`, `_publish_memory`, and every `memory://` branch are deleted; all commands travel over a socket.[^redis-driver]
- **Tests use real seams**: `test_redis_valkey_resp3.py` and `test_native_db_drivers.py` Redis and Valkey cases use the `resp3_server` loopback fixture from `ch03-remove-memory-fakes`; conformance cache cases run only against `VERTEBRA_CONFORMANCE_<ENGINE>_DSN` or `VERTEBRA_CONFORMANCE_DOCKER=1` and skip otherwise.[^conformance-cache][^integration-fixtures]
- **Valkey accepts Redis URLs**: `ValkeyConfig.url_schemes` stays `("valkey", "valkeys")` and `ValkeyConfig(url="redis://...")` keeps working.[^valkey-config]
- **Preserved memory backends**: queue `MemoryQueueBackend` (`backend_url="memory://"`), object storage `memory://`, and `SQLAlchemyCacheBackend.from_url("memory://")` local L1 cache are unchanged.[^alchemy-cache]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`RedisConfig.url_schemes` includes `memory`, and the Redis driver emulates `GET`, `SET`, `DEL`, `PTTL`, `SCAN`, `UNLINK`, `PUBLISH`, `EVAL`, and `EVALSHA` against Python dictionaries across roughly 40 branches.[^redis-config][^redis-driver] Store factories (`KeyValueStore.from_valkey`, Litestar `MemoryStore.from_valkey`, Starlette `CacheStore.from_valkey`, Advanced Alchemy `SQLAlchemyCacheBackend.from_valkey`, and their `from_redis` twins) pass the URL straight to `ValkeyConfig(url=...)` or `RedisConfig(url=...)`, so `memory://` reaches the driver without passing through the registry.[^kv-store][^litestar-store][^starlette-store][^alchemy-cache]

This chapter removes `memory` from the registry, makes conformance cache cases live-only, deletes the driver emulation, and makes both Redis driver constructors raise `ImproperConfigurationError` (a `ValueError` subclass) for `memory://`.[^registry][^conformance-cache][^improper-config] `ValkeyConfig` inherits the behavior without its own scheme change.[^valkey-config] Preserved: queue and object storage `memory://` and `SQLAlchemyCacheBackend.from_url("memory://")`.

**Non-goals**: the `resp3_server` fixture and the consumer test retargets (owned by `ch03-remove-memory-fakes`); Redis queue `_InMemoryRedisStreamClient` fallback in `framework/queues/redis.py` (owned by `ch10-queue-backend-fail-loud`); RESP3 codec removal (owned by `native-redis-driver`); `test_driver_conformance.py::_resolve_live_config` (owned by `driver-fail-loud:ch09:t01`); documentation and OKF edits (owned by `ch11-docs-and-okf`).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Runs after `ch03-remove-memory-fakes`, which provides `src/py/tests/unit/conftest.py::resp3_server` with the key-value, pub/sub, shard pub/sub, and scripting surface and leaves no `memory://` Redis or Valkey construction in `test_redis_valkey_driver.py`, `test_alchemy_cache_backend.py`, `test_framework_subdomains.py`, or `test_starlette_plugin.py`.
- `driver-fail-loud:ch09:t01` edits `test_driver_conformance.py::_resolve_live_config` after `t01` edits the cache helpers in the same file.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Redis / Valkey drivers | `redis/driver.py::SyncRedisDriver`, `::AsyncRedisDriver`, `::_SyncSubscriptionStream`, `::_AsyncSubscriptionStream`[^redis-driver]; `valkey/driver.py` subclasses | External boundary (TCP); constructor guard runs before any socket, lock, or cache structure |
| Config and registry | `redis/config.py::RedisConfig`[^redis-config], `valkey/config.py::ValkeyConfig`[^valkey-config], `registry.py::DatabaseRegistry.parse_url`[^registry] | In-process; `SCHEME_REGISTRY` is built from `url_schemes` |
| Store consumers | `framework/stores.py::KeyValueStore`[^kv-store], `extensions/litestar/store.py::MemoryStore`[^litestar-store], `extensions/starlette/store.py::CacheStore`[^starlette-store], `extensions/alchemy/cache.py::SQLAlchemyCacheBackend`[^alchemy-cache] | Factories build configs directly and construct drivers; they inherit the driver guard unchanged |
| Conformance | `test_driver_conformance.py::_resolve_cache_config`[^conformance-cache], `integration/conftest.py::vtb_redis_config`, `::vtb_valkey_config`[^integration-fixtures] | Env DSN or `pytest-databases` containers; skip otherwise |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [python-framework skill](../../../../../skills/python-framework/SKILL.md)
- [review-tests skill](../../../../../skills/review-tests/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R7b (`t01`)**: `RedisConfig.url_schemes` excludes `memory`; `DatabaseRegistry.parse_url("memory://")` raises `ValueError`; the remaining `memory://` cache tests (`test_redis_valkey_resp3.py`, `test_native_db_drivers.py`, conformance cache helpers) use the loopback fixture or live-only resolution.
- **R7c (`t02`)**: Redis and Valkey drivers delete memory execution and raise `ImproperConfigurationError` for `memory://` URLs, which propagates through `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and Advanced Alchemy `SQLAlchemyCacheBackend` `from_valkey` / `from_redis`; `SQLAlchemyCacheBackend.from_url("memory://")` stays local.

`ch03-remove-memory-fakes` owns R6 and R7a.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
def _reject_memory_url(url: str) -> None:
    """Reject in-process memory:// URLs.

    Args:
        url: Configured Redis or Valkey URL.

    Raises:
        ImproperConfigurationError: When ``url`` starts with ``memory://`` (case-insensitive).
    """
```

| Guard | Exact message |
| --- | --- |
| `redis/driver.py::_reject_memory_url` | `Redis and Valkey drivers do not support memory:// URLs; pass a redis://, rediss://, valkey://, or valkeys:// URL` |

- `SyncRedisDriver.__init__` and `AsyncRedisDriver.__init__` call `_reject_memory_url(resolved.url)` before `super().__init__`.
- Unchanged public signatures: `SyncRedisDriver(config=None, *, broker=None, socket_factory=None)`, `AsyncRedisDriver(config=None, *, broker=None)`.
- `RedisConfig.url_schemes: ClassVar[tuple[str, ...]] = ("redis", "rediss")`; `ValkeyConfig.url_schemes` stays `("valkey", "valkeys")`.

Conformance contract (`src/py/tests/integration/db/test_driver_conformance.py`):

```python
class CacheConformanceSpec(FrozenBaseStruct, frozen=True, gc=False):
    engine: str
    config_cls: type[RedisConfig]
    sync_driver_cls: type[SyncRedisDriver]
    async_driver_cls: type[AsyncRedisDriver]
    tcp_url: str
    env_dsn_vars: tuple[str, ...] = ()
    container_fixture: str | None = None


def _resolve_cache_config(cache_spec: CacheConformanceSpec, request: pytest.FixtureRequest) -> RedisConfig: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `DatabaseRegistry.parse_url("memory://")` → `SCHEME_REGISTRY.get("memory")` is `None` → `ValueError`.
- `KeyValueStore.from_valkey("memory://")` → `ValkeyConfig(url="memory://")` → `AsyncValkeyDriver.__init__` → `AsyncRedisDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`.
- `MemoryStore.from_redis("memory://")` and `CacheStore.from_valkey("memory://")` follow the same chain.
- `SQLAlchemyCacheBackend.from_valkey("memory://")` → `ValkeyConfig(url="memory://")` → `SyncValkeyDriver.__init__` → `SyncRedisDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`; `SQLAlchemyCacheBackend.from_redis("memory://")` → `RedisConfig(url="memory://")` → `SyncRedisDriver.__init__` → same error.
- `SQLAlchemyCacheBackend.from_url("memory://")` → local L1 branch → no driver constructed.
- `test_cache_config_url_pool_key_and_provide_driver` → `_resolve_cache_config` → env DSN → `config_cls.from_url`; else `VERTEBRA_CONFORMANCE_DOCKER=1` → `request.getfixturevalue(container_fixture)`; else `pytest.skip`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Registry memory URL | `DatabaseRegistry.parse_url("memory://")` | `ValueError("Unsupported database URL scheme: 'memory'")` | Queue and object storage `memory://` unchanged |
| Registry network URLs | `parse_url("redis://localhost:6379/0")`, `parse_url("valkey://...")` | `RedisConfig`, `ValkeyConfig` | Zero eager driver imports |
| Direct Redis memory config | `SyncRedisDriver(RedisConfig(url="memory://"))` | `ImproperConfigurationError` before any socket | No silent `127.0.0.1:6379` connection |
| Uppercase memory URL | `AsyncRedisDriver(RedisConfig(url="MEMORY://"))` | `ImproperConfigurationError` | Case-insensitive match |
| Valkey store factory | `MemoryStore.from_valkey("memory://")` | `ImproperConfigurationError` (a `ValueError`) | Store factories need no own check |
| Alchemy driver-backed factories | `SQLAlchemyCacheBackend.from_valkey("memory://")`, `SQLAlchemyCacheBackend.from_redis("memory://")` | `ImproperConfigurationError` from the sync driver guard | `cache.py` needs no own check |
| Alchemy local cache | `SQLAlchemyCacheBackend.from_url("memory://")` | Backend with `sync_driver is None` and `async_driver is None` | Local L1 cache preserved |
| Valkey with Redis URL | `ValkeyConfig(url="redis://127.0.0.1:6379/0")` | Builds and drives normally | Guard rejects only `memory://` |
| Conformance cache, no live server | No `VERTEBRA_CONFORMANCE_REDIS_DSN`, no docker flag | `pytest.skip` | No fake engine in conformance |
| Conformance cache, whitespace env | `VERTEBRA_CONFORMANCE_REDIS_DSN="  "` | Treated as unset | Stripped value decides |
| Conformance cache, docker | `VERTEBRA_CONFORMANCE_DOCKER=1` | `vtb_redis_config` / `vtb_valkey_config` fixture | Live container only |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the registry, when `parse_url("memory://")` runs, then `ValueError` is raised.
- Given `KeyValueStore.from_redis("memory://")`, when called, then `ImproperConfigurationError` is raised.
- Given `SQLAlchemyCacheBackend.from_valkey("memory://")`, when called, then `ImproperConfigurationError` is raised.
- Given no conformance env DSN and no docker flag, when the cache roundtrip tests run, then they report `SKIPPED`.
- Given `QueueConfig(backend_url="memory://")`, when a queue service starts, then `MemoryQueueBackend` is used unchanged.

### 2.6 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| Where to reject `memory://` | Driver `__init__` guard in `redis/driver.py` | Config `__post_init__`: rejects earlier but blocks building configs for non-driver uses and touches more files; registry-only rejection misses store factories that build configs directly |
| Guard scope | Reject `memory://` only | Allow-list of schemes: would also reject `ValkeyConfig(url="redis://...")`, which `ValkeyConfig` documents as valid |
| Store and cache factories | Inherit the driver guard | Per-factory checks in four modules: duplicates one rule across eight factories |
| Conformance memory variants | Live-only with env DSN or docker, skip otherwise | Point conformance at the loopback fake: conformance would certify a test double |
| Task order | Registry and remaining test retargets (`t01`) before emulation deletion (`t02`) | Delete emulation first: `test_redis_valkey_resp3.py` and `test_native_db_drivers.py` would fail between commits |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch04]`
- In-scope docstrings change with code: `RedisConfig.url` and `RedisConfig.from_url` (`t01`), `ValkeyConfig.url` and `SyncRedisDriver.scan` / `AsyncRedisDriver.scan` (`t02`).
- `ch11-docs-and-okf` owns `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (URL dispatch bullet lists `redis`/`rediss`/`memory`) and `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (conformance paragraph states Redis and Valkey default to `memory://`). User-facing `docs/` and `README.md` describe `memory://` only for queues and object storage, which stay valid.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R7b | [t01](tasks/t01-registry-memory-scheme-and-conformance.md) | `behavior_tdd` | `src/py/tests/unit/db/test_native_db_drivers.py::test_registry_parse_url_rejects_memory_scheme` |
| R7c | [t02](tasks/t02-redis-valkey-memory-removal.md) | `behavior_tdd` | `src/py/tests/unit/db/test_redis_valkey_driver.py::test_redis_drivers_and_store_factories_reject_memory_url` |

## 5. Implementation Plan

- [x] [20c535e3] driver-fail-loud:ch04:t01 — Registry drops memory scheme; remaining cache tests use the fixture or live servers ([tasks/t01-registry-memory-scheme-and-conformance.md](tasks/t01-registry-memory-scheme-and-conformance.md))
- [x] [20c535e3] driver-fail-loud:ch04:t02 — Redis and Valkey drivers delete memory execution and reject memory:// ([tasks/t02-redis-valkey-memory-removal.md](tasks/t02-redis-valkey-memory-removal.md))
- **Chapter Recovery Checkpoint**: `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/integration/db/test_driver_conformance.py -q && make lint` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 20c535e3`; `state: completed`; depends on `ch03-remove-memory-fakes` for the `resp3_server` fixture and the consumer test retargets.
- **Remaining Tasks in Chapter**: none (`2/2` completed).
- **Documentation & OKF Sync**: `[0/0]` in this chapter; two OKF claims handed to `ch11`.
- **Active Invariants**: queue, object storage, and Advanced Alchemy `from_url("memory://")` local cache unchanged; `test_registry_parse_url_executes_zero_eager_driver_imports` stays green; no inline `#` comments in touched Python.

[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis sync and async RESP3 drivers with memory command emulation.
[^redis-config]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis config whose url_schemes include memory.
[^valkey-config]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — Valkey config subclassing RedisConfig.
[^registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url` — Scheme dispatch raising ValueError for unregistered schemes.
[^improper-config]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error subclassing VertebraError and ValueError.
[^kv-store]: `src/py/vertebra/framework/stores.py::KeyValueStore` — Framework key-value store with from_valkey and from_redis factories.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore` — Litestar store with from_valkey and from_redis factories.
[^starlette-store]: `src/py/vertebra/extensions/starlette/store.py::CacheStore` — Starlette cache store with from_valkey and from_redis factories.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url` — Advanced Alchemy cache whose from_valkey and from_redis build drivers and whose from_url memory:// selects the local L1 cache.
[^conformance-cache]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config` — Conformance cache config resolution.
[^integration-fixtures]: `src/py/tests/integration/conftest.py::vtb_redis_config` — pytest-databases Redis and Valkey config fixtures.
