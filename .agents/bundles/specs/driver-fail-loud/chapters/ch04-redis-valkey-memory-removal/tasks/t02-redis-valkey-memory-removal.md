---
type: Task
id: "driver-fail-loud:ch04:t02"
chapter_id: ch04-redis-valkey-memory-removal
title: Redis and Valkey drivers delete memory execution and reject memory://
description: Delete the in-process key store, script cache, memory publish, command emulation, and every memory:// branch from the Redis sync, async, and subscription-stream code, and raise ImproperConfigurationError for memory:// URLs from both driver constructors so Valkey drivers and store factories reject it.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch04:t01"
read_set:
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/vertebra/framework/stores.py
  - src/py/vertebra/extensions/starlette/store.py
  - src/py/vertebra/extensions/alchemy/cache.py
files:
  - src/py/vertebra/db/adapters/redis/driver.py
  - src/py/vertebra/db/adapters/valkey/config.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
estimated_loc: 250
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis sync and async drivers with memory emulation
  - id: valkey-config
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: Valkey config url docstring
  - id: kv-store
    resource: src/py/vertebra/framework/stores.py::KeyValueStore.from_valkey
    title: Framework store factories building configs directly
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore.from_valkey
    title: Litestar store factories building configs directly
  - id: starlette-store
    resource: src/py/vertebra/extensions/starlette/store.py::CacheStore.from_valkey
    title: Starlette store factories building configs directly
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_valkey
    title: Advanced Alchemy cache factories building sync and async drivers
tags:
  - redis
  - valkey
  - fail-loud
  - tdd
---

# Task t02: Redis and Valkey Drivers Delete Memory Execution and Reject `memory://`

## Objective & Scope Boundary

Delete every in-process emulation path from `src/py/vertebra/db/adapters/redis/driver.py` and add a constructor guard that raises `ImproperConfigurationError` for `memory://` URLs in `SyncRedisDriver.__init__` and `AsyncRedisDriver.__init__`.[^redis-driver] Valkey drivers inherit the guard, and `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and Advanced Alchemy `SQLAlchemyCacheBackend` `from_valkey` / `from_redis` raise it through the driver they construct, so the store and cache modules stay unchanged.[^kv-store][^litestar-store][^starlette-store][^alchemy-cache] `SQLAlchemyCacheBackend.from_url("memory://")` builds no driver and stays local. Remove `memory://` from the `ValkeyConfig.url` docstring.[^valkey-config]

Out of scope: `valkey/driver.py` (no memory code); store modules and `extensions/alchemy/cache.py` (no change needed); RESP3 codecs and `redis/core.py`; `framework/queues/redis.py`.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver.__slots__`, `__init__`, `cached_keys`, `get`, `set`, `delete`, `scan`, `unlink`, `pttl`, `flushdb`, `publish`, `subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `drain_messages`, `execute_command`, `execute_pipeline`, `_publish_memory`, `_execute_memory_command`, `_eval_memory_script`; `AsyncRedisDriver.__init__`, `get`, `set`, `delete`, `scan`, `unlink`, `pttl`, `flushdb`, `publish`, `unsubscribe`, `execute_command`, `execute_pipeline`; `_SyncSubscriptionStream._iter_messages`; `_AsyncSubscriptionStream._execute_subscribe`, `_stream`.[^redis-driver]
- `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig.url`.[^valkey-config]
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — imports and the tests retargeted onto `resp3_server` in `ch03-remove-memory-fakes`.
- `src/py/vertebra/framework/stores.py::KeyValueStore.from_valkey`, `::KeyValueStore.from_redis`.[^kv-store]
- `src/py/vertebra/extensions/litestar/store.py::MemoryStore.from_valkey(config_or_url: ValkeyConfig | str, *, key_prefix: str = "vertebra")`, `::MemoryStore.from_redis(config_or_url: RedisConfig | str, *, key_prefix: str = "vertebra")` — same shape as `KeyValueStore`; `MemoryStore` is already imported in the test file, so the module stays outside the read set.[^litestar-store]
- `src/py/vertebra/extensions/starlette/store.py::CacheStore.from_valkey`, `::CacheStore.from_redis`.[^starlette-store]
- `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_valkey` (builds `ValkeyConfig(url=...)`, then `SyncValkeyDriver(cfg)` and `AsyncValkeyDriver(cfg)`), `::SQLAlchemyCacheBackend.from_redis` (builds `RedisConfig(url=...)`, then `SyncRedisDriver(cfg)` and `AsyncRedisDriver(cfg)`), `::SQLAlchemyCacheBackend.from_url` (returns a local backend for `memory://` before any driver).[^alchemy-cache]

## Symbol & Signature Contracts

`src/py/vertebra/db/adapters/redis/driver.py`:

```python
def _reject_memory_url(url: str) -> None:
    """Reject in-process memory:// URLs.

    Args:
        url: Configured Redis or Valkey URL.

    Raises:
        ImproperConfigurationError: When ``url`` starts with ``memory://`` (case-insensitive).
    """
    if url.strip().lower().startswith("memory://"):
        msg = (
            "Redis and Valkey drivers do not support memory:// URLs; "
            "pass a redis://, rediss://, valkey://, or valkeys:// URL"
        )
        raise ImproperConfigurationError(msg)
```

- Place `_reject_memory_url` after `AsyncRedisDriver` and before `_AwaitableMessageList`.
- `SyncRedisDriver.__init__`: `resolved = config or RedisConfig()`, `_reject_memory_url(resolved.url)`, then `super().__init__(...)`. `AsyncRedisDriver.__init__`: same guard before `super().__init__`.
- Delete from `SyncRedisDriver`: `_kv`, `_ttls`, `_scripts` slots and assignments; the `memory://` blocks in `get`, `set`, `delete`, `scan`, `unlink`, `pttl`, `flushdb`, `publish`, `execute_command`, `execute_pipeline`; the methods `_publish_memory`, `_execute_memory_command`, `_eval_memory_script`. Negated checks become unconditional: `cached_keys` and `get` and `drain_messages` always call `self._drain_idle_sockets()`; `subscribe` / `psubscribe` / `ssubscribe` test only `if channels:` / `if patterns:` / `if shard_channels:`; `unsubscribe` always calls `self._send_pubsub_command("UNSUBSCRIBE", *channels)`.
- Delete from `AsyncRedisDriver`: the `memory://` blocks in `get`, `set`, `delete`, `scan`, `unlink`, `pttl`, `flushdb`, `publish`, `execute_command`, `execute_pipeline`; `unsubscribe` always awaits `_send_pubsub_command_async`.
- `_SyncSubscriptionStream._iter_messages` and `_AsyncSubscriptionStream._stream`: delete the `memory://` early return. `_AsyncSubscriptionStream._execute_subscribe`: `if self._channels:`.
- `SyncRedisDriver.scan` and `AsyncRedisDriver.scan` docstrings drop the `memory://` sentence and become `"""Run one ``SCAN`` step and return the next cursor and the matching keys."""`.
- Remove the `fnmatch` import (its only uses were the memory scan and memory publish).

`src/py/vertebra/db/adapters/valkey/config.py`: `url` docstring becomes `"""Valkey connection URI (`valkey://`, `valkeys://`, `redis://`, or `rediss://`)."""`.

Call-site wiring: `MemoryStore.from_valkey("memory://")` → `ValkeyConfig(url="memory://")` → `AsyncValkeyDriver.__init__` → `AsyncRedisDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`. `SQLAlchemyCacheBackend.from_valkey("memory://")` → `ValkeyConfig(url="memory://")` → `SyncValkeyDriver.__init__` → `SyncRedisDriver.__init__` → `_reject_memory_url` → `ImproperConfigurationError`; `from_redis` follows the same chain through `SyncRedisDriver`.

## Edge Cases & Error Semantics

- `memory://`, `MEMORY://`, and `memory://anything` raise `ImproperConfigurationError` before any socket, lock, or cache structure is created.
- `ImproperConfigurationError` subclasses `ValueError`, so callers catching `ValueError` from `parse_url` handle the direct-config path identically.
- Without the guard, `_default_node` resolves a `memory://` URL to `("127.0.0.1", 6379)` and connects to whatever server listens there; the guard prevents that.
- `ValkeyConfig(url="redis://...")` keeps working.
- `publish` returns the server's integer reply (no `max(1, delivered)` floor).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_redis_valkey_driver.py`. Dependency category: in-process constructor guard; wire behaviour stays covered by the `resp3_server` tests from `ch03-remove-memory-fakes`.
- New `test_redis_drivers_and_store_factories_reject_memory_url`, parametrized with ids:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `sync-redis` | `SyncRedisDriver(RedisConfig(url="memory://"))` | `pytest.raises(ImproperConfigurationError, match="memory://")` |
| `async-redis` | `AsyncRedisDriver(RedisConfig(url="MEMORY://"))` | `ImproperConfigurationError` |
| `sync-valkey` | `SyncValkeyDriver(ValkeyConfig(url="memory://"))` | `ImproperConfigurationError` |
| `async-valkey` | `AsyncValkeyDriver(ValkeyConfig(url="memory://"))` | `ImproperConfigurationError` |
| `kv-valkey` | `KeyValueStore.from_valkey("memory://")` | `ImproperConfigurationError` |
| `kv-redis` | `KeyValueStore.from_redis("memory://")` | `ImproperConfigurationError` |
| `litestar-valkey` | `MemoryStore.from_valkey("memory://")` | `ImproperConfigurationError` |
| `litestar-redis` | `MemoryStore.from_redis("memory://")` | `ImproperConfigurationError` |
| `starlette-valkey` | `CacheStore.from_valkey("memory://")` | `ImproperConfigurationError` |
| `starlette-redis` | `CacheStore.from_redis("memory://")` | `ImproperConfigurationError` |
| `alchemy-valkey` | `SQLAlchemyCacheBackend.from_valkey("memory://")` | `ImproperConfigurationError` |
| `alchemy-redis` | `SQLAlchemyCacheBackend.from_redis("memory://")` | `ImproperConfigurationError` |

- Parametrize over zero-argument callables (`lambda: SyncRedisDriver(RedisConfig(url="memory://"))`) so each case constructs inside `pytest.raises`. Import `ImproperConfigurationError` from `vertebra.typing`, `CacheStore` from `vertebra.extensions.starlette.store`, and `SQLAlchemyCacheBackend` from `vertebra.extensions.alchemy.cache`. `SQLAlchemyCacheBackend.from_url("memory://")` stays local and is asserted by `test_alchemy_cache_backend.py::test_alchemy_cache_memory_url_stays_local` from `ch03-remove-memory-fakes`.

## Verification

- Red: add the new test, then `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py -k reject_memory_url -q` fails for all twelve cases.
- Green + lint + typecheck: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/framework/test_framework_subdomains.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis/driver.py src/py/vertebra/db/adapters/valkey/config.py src/py/tests/unit/db/test_redis_valkey_driver.py && uv run --no-sync ruff format --check src/py/vertebra/db/adapters/redis/driver.py src/py/vertebra/db/adapters/valkey/config.py && uv run --no-sync mypy src/py/vertebra/db/adapters/redis/driver.py src/py/vertebra/db/adapters/valkey/config.py`
- Residue gate: `rg -n "memory://|_kv\b|_ttls|_scripts|_execute_memory_command|_eval_memory_script|_publish_memory|fnmatch" src/py/vertebra/db/adapters/redis/driver.py src/py/vertebra/db/adapters/valkey/` prints only lines inside `_reject_memory_url`: the docstring summary (`Reject in-process memory:// URLs.`), the `Raises:` docstring line, the `startswith("memory://")` condition, and the first message fragment (`"Redis and Valkey drivers do not support memory:// URLs; "`).
- Repository residue: `rg -n 'RedisConfig\(url="memory://"\)|ValkeyConfig\(url="memory://"' src/py/tests` prints only the new rejection test.

## Acceptance Criteria

- [ ] Both Redis driver constructors raise `ImproperConfigurationError` for `memory://`, and all eight store and cache factories surface it (R7c).
- [ ] No in-process key, TTL, script, or publish emulation remains in the Redis driver (R7c).
- [ ] `ValkeyConfig.url` docstring lists only network schemes.
- [ ] Every Redis/Valkey consumer unit test passes over the `resp3_server` fixture.
- [ ] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] The task runs after `driver-fail-loud:ch04:t01` and after chapter `ch03-remove-memory-fakes` so no test constructs a `memory://` Redis or Valkey config when the emulation is deleted.

[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis sync and async drivers with memory emulation.
[^valkey-config]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — Valkey config url docstring.
[^kv-store]: `src/py/vertebra/framework/stores.py::KeyValueStore.from_valkey` — Framework store factories building configs directly.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore.from_valkey` — Litestar store factories building configs directly.
[^starlette-store]: `src/py/vertebra/extensions/starlette/store.py::CacheStore.from_valkey` — Starlette store factories building configs directly.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_valkey` — Advanced Alchemy cache factories building sync and async drivers.
