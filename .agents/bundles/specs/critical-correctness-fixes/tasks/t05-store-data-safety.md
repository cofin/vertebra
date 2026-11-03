---
type: Task
id: critical-correctness-fixes:t05
title: Store prefix-scoped delete, millisecond TTLs, expires_in contract, and Litestar swap defaults
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/db/redis/driver.py
  - src/py/vertebra/framework/stores.py
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/starlette/store.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/litestar/config.py
  - docs/usage/queues-and-realtime.md
  - docs/usage/extensions.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/bundles/knowledge/domains/framework/subdomains.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/server/test_litestar_direct_fastpath.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/extensions/test_alchemy_cache_backend.py
plan_revision: 3
plan_commit: 24e2931
state_revision: 2
claimed_by: executor
claimed_at: 2026-11-03T16:50:15Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T17:42:00Z
last_verified_commit: 670fd6a
verification_evidence: "RED: 19 failed in 1.35s + 7 failed on SQLAlchemyCacheBackend prefix clear and millisecond TTLs; GREEN: 84 passed across store and SQLAlchemy cache suites, ruff check/format and mypy passed"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T17:42:00Z
commit: 670fd6a
tags: [bugfix, data-loss, stores, redis, litestar]
---

# t05 — Store prefix-scoped delete, millisecond TTLs, `expires_in` contract, and Litestar swap defaults

## Objective

`KeyValueStore`, Litestar `MemoryStore`, and Starlette `CacheStore` delete only their own keys and send valid TTLs for sub-second expiries; `KeyValueStore` and `MemoryStore` return `None` from `expires_in` for keys without expiry; and Litestar apps keep their own channels backend and response cache unless the user opts into the swap.

## Context

- **Strategy**: `regression_tdd`. Each defect reproduces at `142cc49` with the Redis driver's `memory://` mode (local-substitutable) and a real Litestar app.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/framework/stores.py::KeyValueStore.delete_all` and `src/py/vertebra/extensions/litestar/store.py::MemoryStore.delete_all` call `await self._driver.flushdb()`, which deletes every key in the database.
  - Both `set` methods send `ex=int(ttl_s)`. `expires_in=timedelta(milliseconds=250)` sends `EX 0`, which Redis rejects; the `memory://` driver stores an already expired key.
  - Both `expires_in` methods return `-1` for keys without expiry (local entry or driver `PTTL == -1`). Litestar's `Store.expires_in` contract returns `None` when the key does not exist or has no expiry.
  - `src/py/vertebra/extensions/litestar/config.py::LitestarConfig` defaults `swap_memory_channels=True` and `swap_memory_response_cache=True`, so `LitestarPlugin._finalize_app_acceleration` replaces the app's `MemoryChannelsBackend` and response-cache `MemoryStore` without the user asking. A planning probe confirmed the swap with the default config.
  - The stores have no namespace, so no prefix-scoped delete is possible. The driver (`src/py/vertebra/db/redis/driver.py::SyncRedisDriver` / `::AsyncRedisDriver`) has no `scan`, `unlink`, or `px` support.
  - `src/py/vertebra/extensions/starlette/store.py::CacheStore.delete_all` and `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.clear` / `clear_async` also call `flushdb()` and truncate sub-second TTLs (`_compute_expiry` / `_resolve_driver_ex`). Both get the same prefix-scoped `SCAN` + `UNLINK` and millisecond `PX` TTL fix (`CacheStore` defaults to `key_prefix="vertebra"`; `SQLAlchemyCacheBackend` defaults to `key_prefix="vtb:aa:"`, matching `create_cache_config(key_prefix="vtb:aa:")`).
- **Decisions** (spec §2.2):
  - New keyword `key_prefix: str = "vertebra"` on both store constructors and on `from_valkey` / `from_redis`. Driver keys are `f"{key_prefix}:{key}"`. The name avoids a clash with the existing `KeyValueStore.namespace(prefix)` method. `key_prefix` is validated with `validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)`, which rejects glob characters (`*`, `?`, `[`) that would widen `SCAN MATCH`.
  - The local `_entries` dict is keyed by the full prefixed key, so Valkey invalidation pushes (which carry server keys) evict entries without translation.
  - TTLs are always sent as `PX` milliseconds, `math.ceil(seconds * 1000)`. `expires_in <= 0` raises `ValueError("expires_in must be a positive duration")` before any state changes.
  - `delete_all` clears the local entries, then loops `scan(cursor, match=f"{key_prefix}:*", count=500)` + `unlink(*keys)` until the cursor returns to `0`. Redis Cluster per-node scanning is out of scope.
- **Existing test that changes**: `src/py/tests/unit/db/test_redis_valkey_driver.py::test_valkey_resp3_client_tracking_and_litestar_integration` pushes an invalidation for `"k1"`; the server key is now `"vertebra:k1"`.
- **Canonical test modules**: `src/py/tests/unit/db/test_redis_valkey_driver.py` (imports `AsyncRedisDriver`, `RedisConfig`, `KeyValueStore`, Litestar `MemoryStore`), `src/py/tests/unit/server/test_litestar_direct_fastpath.py` (holds the Channels/Store swap test and the Litestar primitives `Litestar`, `ChannelsPlugin`, `MemoryChannelsBackend`, `ResponseCacheConfig`, `LitestarMemoryStore`).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.

## Steps

### 1. Write failing tests (RED)

1. `src/py/tests/unit/db/test_redis_valkey_driver.py`:
   - Add `from datetime import timedelta` and `from typing import Any` to the standard-library imports.
   - In `test_valkey_resp3_client_tracking_and_litestar_integration`, change `vdriver.handle_invalidation_push(["k1"])` to `vdriver.handle_invalidation_push(["vertebra:k1"])` and `assert "k1" not in store._entries` to `assert "vertebra:k1" not in store._entries`.
   - Append:

   ```python
   _STORE_CLASSES: list[type[Any]] = [KeyValueStore, MemoryStore]
   _STORE_IDS = ["framework", "litestar"]


   @pytest.mark.anyio
   async def test_redis_driver_scan_unlink_and_px_in_memory_mode() -> None:
       """The memory:// driver supports PX TTLs, prefix SCAN, and UNLINK."""
       driver = AsyncRedisDriver(RedisConfig(url="memory://"))
       await driver.set("cache:a", b"1", px=250)
       await driver.set("cache:b", b"2")
       await driver.set("other:c", b"3")
       assert 0 < await driver.pttl("cache:a") <= 250
       cursor, keys = await driver.scan(0, match="cache:*")
       assert cursor == 0
       assert sorted(keys) == ["cache:a", "cache:b"]
       assert await driver.unlink("cache:a", "cache:b") == 2
       assert await driver.get("cache:a") is None
       assert await driver.get("other:c") == b"3"
       with pytest.raises(ValueError, match="ex or px"):
           await driver.set("k", b"v", ex=1, px=1000)
       await driver.close()


   @pytest.mark.anyio
   @pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
   async def test_store_delete_all_removes_only_its_prefix(store_cls: type[Any]) -> None:
       """delete_all removes the store's prefixed keys and leaves other keys in the database."""
       driver = AsyncRedisDriver(RedisConfig(url="memory://"))
       await driver.set("other:keep", b"x")
       store = store_cls(driver=driver)
       await store.set("a", "1")
       await store.set("b", "2", expires_in=60)
       await store.delete_all()
       assert await driver.get("other:keep") == b"x"
       assert await driver.get("vertebra:a") is None
       assert await store.get("a") is None
       assert await store.get("b") is None
       await driver.close()


   @pytest.mark.anyio
   @pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
   async def test_store_sends_sub_second_ttl_as_milliseconds(store_cls: type[Any]) -> None:
       """A 250 ms expiry reaches the driver as a PX TTL instead of EX 0."""
       driver = AsyncRedisDriver(RedisConfig(url="memory://"))
       store = store_cls(driver=driver)
       await store.set("short", "v", expires_in=timedelta(milliseconds=250))
       assert 0 < await driver.pttl("vertebra:short") <= 250
       assert await store.get("short") == b"v"
       await driver.close()


   @pytest.mark.anyio
   @pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
   @pytest.mark.parametrize("backed", [False, True], ids=["local", "driver"])
   async def test_store_expires_in_returns_none_without_expiry(store_cls: type[Any], backed: bool) -> None:
       """expires_in returns None for keys without expiry and for missing keys."""
       driver = AsyncRedisDriver(RedisConfig(url="memory://")) if backed else None
       writer = store_cls(driver=driver)
       await writer.set("forever", "v")
       reader = store_cls(driver=driver) if backed else writer
       assert await reader.expires_in("forever") is None
       assert await reader.expires_in("missing") is None
       if driver is not None:
           await driver.close()


   @pytest.mark.anyio
   @pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
   @pytest.mark.parametrize("expires_in", [0, -5, timedelta(0)], ids=["zero", "negative", "zero-delta"])
   async def test_store_rejects_non_positive_ttl(store_cls: type[Any], expires_in: int | timedelta) -> None:
       """A zero or negative expiry raises before the key is written."""
       store = store_cls()
       with pytest.raises(ValueError, match="expires_in must be a positive duration"):
           await store.set("k", "v", expires_in=expires_in)
       assert await store.get("k") is None


   @pytest.mark.parametrize("store_cls", _STORE_CLASSES, ids=_STORE_IDS)
   def test_store_rejects_invalid_key_prefix(store_cls: type[Any]) -> None:
       """Key prefixes containing glob characters are rejected."""
       with pytest.raises(ValueError, match="key_prefix"):
           store_cls(key_prefix="cache*")
   ```

2. `src/py/tests/unit/server/test_litestar_direct_fastpath.py`:
   - Add `from vertebra.extensions.litestar.plugin import LitestarPlugin` to the first-party imports.
   - Append after `test_swap_channels_backend_and_response_cache_store`:

   ```python
   def test_litestar_plugin_keeps_memory_channels_and_response_cache_by_default() -> None:
       """The default LitestarConfig leaves Litestar's channels backend and response cache store in place."""
       channels_plugin = ChannelsPlugin(backend=MemoryChannelsBackend(history=10), channels=["alerts"])
       plugin = LitestarPlugin()
       app = Litestar(
           route_handlers=[],
           plugins=[channels_plugin, plugin],
           response_cache_config=ResponseCacheConfig(store="response_cache"),
           stores={"response_cache": LitestarMemoryStore()},
       )
       plugin.ensure_finalized(app)
       assert type(channels_plugin._backend) is MemoryChannelsBackend
       assert type(app.stores.get("response_cache")) is LitestarMemoryStore
   ```

3. `src/py/tests/unit/extensions/test_starlette_plugin.py`:
   - Add `from datetime import timedelta` to the standard-library imports and `from vertebra.db.redis import AsyncRedisDriver, RedisConfig` to the first-party imports.
   - Append:

   ```python
   @pytest.mark.anyio
   async def test_cache_store_delete_all_removes_only_its_prefix() -> None:
       """Starlette CacheStore.delete_all removes its prefixed keys and leaves other keys in place."""
       driver = AsyncRedisDriver(RedisConfig(url="memory://"))
       await driver.set("other:keep", b"x")
       cache = CacheStore(async_driver=driver)
       await cache.set("a", "1", expires_in=timedelta(milliseconds=250))
       assert 0 < await driver.pttl("vertebra:a") <= 250
       await cache.delete_all()
       assert await driver.get("other:keep") == b"x"
       assert await driver.get("vertebra:a") is None
       await driver.close()
   ```

4. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/server/test_litestar_direct_fastpath.py::test_litestar_plugin_keeps_memory_channels_and_response_cache_by_default src/py/tests/unit/extensions/test_starlette_plugin.py::test_cache_store_delete_all_removes_only_its_prefix -q
   ```

   Expected: `test_redis_driver_scan_unlink_and_px_in_memory_mode` fails with `TypeError` (unexpected keyword `px`); `test_store_delete_all_removes_only_its_prefix` fails (`other:keep` deleted); `test_store_sends_sub_second_ttl_as_milliseconds` fails (`pttl == -2`); the `expires_in` cases fail (`-1 is None`); the non-positive TTL cases fail with `DID NOT RAISE`; the key-prefix cases fail with `TypeError`; the Litestar test fails because the backend is the Vertebra `ChannelsBackend`; the Starlette test fails (`pttl == -2` for the unprefixed key).

### 2. Implement (GREEN)

1. `src/py/vertebra/db/redis/driver.py`, class `SyncRedisDriver`:
   - Replace `set` with:

   ```python
   def set(self, key: str, value: bytes | str, *, ex: int | None = None, px: int | None = None) -> None:
       """Set a key value with an optional TTL and update the client-side cache.

       Args:
           key: Key to write.
           value: Value bytes or text.
           ex: Expiry in whole seconds.
           px: Expiry in milliseconds.

       Raises:
           ValueError: When both ``ex`` and ``px`` are given.
       """
       if ex is not None and px is not None:
           raise ValueError("set() accepts ex or px, not both")
       raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
       ttl_seconds = float(ex) if ex is not None else (px / 1000.0 if px is not None else None)
       if self.config.effective_tracking_enabled:
           self._cache_put(key, raw, ttl_seconds=ttl_seconds)
       if self.config.url.startswith("memory://"):
           with self._lock:
               self._kv[key] = raw
               if ttl_seconds is not None:
                   self._ttls[key] = time.monotonic() + ttl_seconds
               else:
                   self._ttls.pop(key, None)
           return
       if ex is not None:
           self.execute_command("SET", key, raw, "EX", ex)
       elif px is not None:
           self.execute_command("SET", key, raw, "PX", px)
       else:
           self.execute_command("SET", key, raw)
   ```

   - Add after `delete`:

   ```python
   def scan(self, cursor: int = 0, *, match: str | None = None, count: int = 500) -> tuple[int, list[str]]:
       """Run one ``SCAN`` step and return the next cursor and the matching keys.

       In ``memory://`` mode all matching keys are returned with cursor ``0``.
       """
       if self.config.url.startswith("memory://"):
           with self._lock:
               keys = [k for k in self._kv if match is None or fnmatch.fnmatchcase(k, match)]
           return 0, keys
       parts: list[str | int] = ["SCAN", cursor]
       if match is not None:
           parts.extend(["MATCH", match])
       parts.extend(["COUNT", count])
       reply = self.execute_command(*parts)
       return int(RedisCore.decode_part(reply[0])), [RedisCore.decode_part(k) for k in reply[1]]

   def unlink(self, *keys: str) -> int:
       """Remove ``keys`` with ``UNLINK`` and evict them from the client-side cache."""
       if not keys:
           return 0
       for key in keys:
           self._client_cache.pop(key, None)
       if self.config.url.startswith("memory://"):
           removed = 0
           with self._lock:
               for key in keys:
                   self._ttls.pop(key, None)
                   if self._kv.pop(key, None) is not None:
                       removed += 1
           return removed
       res = self.execute_command("UNLINK", *keys)
       return int(res) if isinstance(res, int) else 0
   ```

   - In `_execute_memory_command`, extend the `SET` branch to parse `PX`: after the existing `EX` check add `px = int(parts[4]) if len(parts) >= 5 and RedisCore.decode_part(parts[3]).upper() == "PX" else None` and call `self.set(k, v, ex=ex, px=px)`.
   - Confirm `RedisCore.decode_part` is imported from `vertebra.db.redis.core` (it is used by `_execute_memory_command`); `fnmatch` and `time` are already imported.
2. Same file, class `AsyncRedisDriver`:
   - Replace `set` with the async equivalent: same signature and `ValueError`; memory mode delegates `self._sync.set(key, raw, ex=ex, px=px)` after the cache update; network mode awaits `execute_command` with `EX`, `PX`, or neither. Client-cache TTL uses the same `ttl_seconds` computation.
   - Add `async def scan(self, cursor: int = 0, *, match: str | None = None, count: int = 500) -> tuple[int, list[str]]` (memory mode: `return self._sync.scan(cursor, match=match, count=count)`; network: same command building as sync with `await self.execute_command(*parts)`).
   - Add `async def unlink(self, *keys: str) -> int` (memory mode: `return self._sync.unlink(*keys)`; network: evict from `self._client_cache`, then `await self.execute_command("UNLINK", *keys)`).
3. `src/py/vertebra/framework/stores.py::KeyValueStore` (apply the identical change to `src/py/vertebra/extensions/litestar/store.py::MemoryStore`):
   - Imports: add `import math` and `from vertebra.utils.text import validate_identifier`.
   - `__slots__ = ("_driver", "_entries", "_key_prefix")`.
   - `__init__(self, *, driver: AsyncValkeyDriver | AsyncRedisDriver | None = None, key_prefix: str = "vertebra") -> None`: first line `self._key_prefix = validate_identifier(key_prefix, "key_prefix", allow_hyphens=True)`; rest unchanged. Docstring documents `driver`, `key_prefix` ("Namespace prepended to every key as `<key_prefix>:<key>`; `delete_all` removes only keys under it."), and `Raises: ValueError`.
   - `from_valkey(cls, config_or_url, *, key_prefix: str = "vertebra")` and `from_redis(...)` pass `key_prefix=key_prefix`.
   - Add property after `driver`: `def key_prefix(self) -> str: """Return the namespace prepended to every stored key.""" return self._key_prefix`.
   - `get`, `delete`, `exists` (via `get`), `expires_in`, `set`: compute `full_key = self._make_key(key)` and use it for `_entries` and every driver call.
   - `set`: replace the TTL block with

   ```python
   ttl_ms = self._ttl_milliseconds(expires_in)
   expires_at = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0
   self._entries[full_key] = (raw, expires_at)
   if self._driver is not None:
       await self._driver.set(full_key, raw, px=ttl_ms)
   ```

   (compute `ttl_ms` before writing `_entries` so a `ValueError` leaves no state).
   - `delete_all`:

   ```python
   async def delete_all(self) -> None:
       """Delete every key under this store's ``key_prefix`` with ``SCAN`` + ``UNLINK``."""
       self._entries.clear()
       if self._driver is None:
           return
       pattern = f"{self._key_prefix}:*"
       cursor = 0
       while True:
           cursor, keys = await self._driver.scan(cursor, match=pattern, count=500)
           if keys:
               await self._driver.unlink(*keys)
           if cursor == 0:
               break
   ```

   - `expires_in`: local entry with `expires_at is None` returns `None`; driver `pttl_ms in (-2, -1)` returns `None`; otherwise unchanged (`int(remaining)` / `max(0, pttl_ms // 1000)`). Docstring: "Return remaining TTL in seconds, or None when the key is missing, expired, or has no expiry."
   - Private methods at the end of the class (before or after `_on_driver_invalidate`):

   ```python
   def _make_key(self, key: str) -> str:
       """Return the driver key ``<key_prefix>:<key>``."""
       return f"{self._key_prefix}:{key}"

   @staticmethod
   def _ttl_milliseconds(expires_in: int | timedelta | None) -> int | None:
       """Convert ``expires_in`` to whole milliseconds, rounding up.

       Raises:
           ValueError: When ``expires_in`` is zero or negative.
       """
       if expires_in is None:
           return None
       seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
       if seconds <= 0:
           raise ValueError("expires_in must be a positive duration")
       return math.ceil(seconds * 1000)
   ```

   - `get` with `renew_for`: write the renewed entry under `full_key` (local only, unchanged behavior otherwise).
4. `src/py/vertebra/extensions/starlette/store.py::CacheStore`:
   - Imports: add `import math` and `from vertebra.utils.text import validate_identifier`.
   - `__slots__ = ("_async_driver", "_entries", "_key_prefix", "_sync_driver")`; `__init__` gains `key_prefix: str = "vertebra"` (validated exactly as in step 3); `from_valkey` / `from_redis` gain `key_prefix: str = "vertebra"` and pass it through; add the `key_prefix` property.
   - `get_sync`, `set_sync`, `delete_sync`, `get`, `set`, `delete`: use `full_key = self._make_key(key)` for `_entries` and driver calls.
   - `set_sync` / `set`: `ttl_ms = self._ttl_milliseconds(expires_in)`, `expires_at = None if ttl_ms is None else time.monotonic() + ttl_ms / 1000.0`, driver call `set(full_key, raw, px=ttl_ms)`.
   - `delete_all`: the same `SCAN` + `UNLINK` loop as step 3 over `self._async_driver` (return after clearing `_entries` when `self._async_driver is None`).
   - Add the same `_make_key` and `_ttl_milliseconds` private methods; delete the module-level `_compute_expiry` function.
5. `src/py/vertebra/extensions/litestar/config.py::LitestarConfig`: set `swap_memory_channels: bool = False` and `swap_memory_response_cache: bool = False`. Document every field with a per-field attribute docstring (a string literal directly under the field, the convention used by `src/py/vertebra/db/rls.py::TenantScopeConfig`); do not add an `Attributes:` section to the class docstring. Use these texts:
   - `offload_compression`: "Compress responses in the native server instead of Litestar middleware."
   - `offload_cors`: "Answer CORS in the native server instead of Litestar middleware."
   - `offload_static_files`: "Serve static file routes from the native server."
   - `offload_openapi`: "Serve the generated OpenAPI schema from the native server."
   - `swap_memory_channels`: "Replace Litestar's `MemoryChannelsBackend` with Vertebra's `ChannelsBackend` during finalization. Off by default."
   - `swap_memory_response_cache`: "Replace the response cache `MemoryStore` with Vertebra's `MemoryStore` during finalization. Off by default."
   - `enable_direct_handler_fastpath`: "Compile Litestar route handlers into the direct dispatch fastpath."
   - `enable_fastpath_bypass`: "Route eligible requests around the Litestar ASGI stack."
   - `enable_health_endpoint`: "Register a native health check route."
   - `health_path`: "Path served by the health check route."
   - `mcp_config`: "Optional MCP configuration for the agent integration."
   - `a2a_config`: "Optional A2A configuration for the agent integration."
   - `agent_config`: "Optional agent harness configuration."
6. Docs:
   - `docs/usage/queues-and-realtime.md`, section `## Key-Value Stores & Transactional Email`: after the first paragraph add: `` Stores backed by Redis or Valkey prefix every key with `key_prefix` (default `"vertebra"`), and `delete_all()` removes only keys under that prefix. Expiries are sent in milliseconds, and `expires_in()` returns `None` for keys without an expiry. `` Change the example line to `cache = KeyValueStore(key_prefix="app-cache")`.
   - `docs/usage/extensions.md`, section `## Litestar Integration & Middleware Offload`: after the first code block add: `` `LitestarConfig(swap_memory_channels=True, swap_memory_response_cache=True)` opts into replacing Litestar's in-memory channels backend and response cache store with Vertebra's implementations. Both are off by default. ``
7. Knowledge:
   - `.agents/bundles/knowledge/domains/extensions/litestar.md`, `LitestarPlugin.ensure_finalized` bullet: replace "to swap in `ChannelsBackend` (`swap_channels_backend`) and `MemoryStore` (`swap_response_cache_store`)" with "to swap in `ChannelsBackend` (`swap_channels_backend`) and `MemoryStore` (`swap_response_cache_store`) when `LitestarConfig.swap_memory_channels` / `swap_memory_response_cache` are enabled (both default `False`)".
   - Same file, `ChannelsBackend & MemoryStore` bullet: append "`MemoryStore` prefixes keys with `key_prefix` (default `vertebra`), deletes with `SCAN` + `UNLINK`, sends `PX` TTLs, and returns `None` from `expires_in` for keys without expiry."
   - `.agents/bundles/knowledge/domains/framework/subdomains.md`, the `queues`/`channels.py`/`stores.py`/`email` bullet: after "`MemoryStore` and `NamespacedStore`" insert "(`KeyValueStore` alias `MemoryStore`; `key_prefix`-scoped `delete_all`, `PX` TTLs)".
8. Changelog `docs/changelog/index.md`, `## 0.3.0`:
   - `### Changed`: `` - `LitestarConfig.swap_memory_channels` and `LitestarConfig.swap_memory_response_cache` default to `False`; `KeyValueStore`, the Litestar `MemoryStore`, and the Starlette `CacheStore` prefix Redis/Valkey keys with `key_prefix` (default `"vertebra"`). ``
   - `### Fixed`: `` - `KeyValueStore.delete_all`, Litestar `MemoryStore.delete_all`, and Starlette `CacheStore.delete_all` delete only their prefix with `SCAN` + `UNLINK` instead of `FLUSHDB`; sub-second expiries are sent as `PX` milliseconds; `expires_in` returns `None` for keys without expiry. `SyncRedisDriver` / `AsyncRedisDriver` gain `scan`, `unlink`, and `set(px=...)`. ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/db/redis/driver.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar/store.py src/py/vertebra/extensions/starlette/store.py src/py/vertebra/extensions/litestar/config.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_starlette_plugin.py docs/usage/queues-and-realtime.md docs/usage/extensions.md .agents/bundles/knowledge/domains/extensions/litestar.md .agents/bundles/knowledge/domains/framework/subdomains.md docs/changelog/index.md
   git commit -S -m "fix(stores): scope delete_all to the key prefix and send millisecond TTLs"
   ```

## Verification

- **RED**: the Step 1.4 command exits `1` with the failures listed.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/agent/test_runtime_and_workflows.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py -q
  uv run ruff check src/py/vertebra/db/redis/driver.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar/store.py src/py/vertebra/extensions/starlette/store.py src/py/vertebra/extensions/litestar/config.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_starlette_plugin.py
  uv run ruff format --check src/py/vertebra/db/redis/driver.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar/store.py src/py/vertebra/extensions/starlette/store.py src/py/vertebra/extensions/litestar/config.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/extensions/test_starlette_plugin.py
  uv run mypy src/py/vertebra/db/redis/driver.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar/store.py src/py/vertebra/extensions/starlette/store.py src/py/vertebra/extensions/litestar/config.py
  ```

  All exit `0`.
- **Phase gate**: Phase 2 checkpoint in `spec.md` (`make lint-all`, `make py-test`).

## Acceptance Criteria

- [x] R8: `delete_all` and `SQLAlchemyCacheBackend.clear` / `clear_async` remove only `<key_prefix>:*` across all four stores/caches; foreign keys survive; no `flushdb` call remains in `framework/stores.py`, `extensions/litestar/store.py`, `extensions/starlette/store.py`, or `extensions/alchemy/cache.py`.
- [x] R8: Sub-second TTLs reach the driver as `PX`; non-positive TTLs raise `ValueError`; `expires_in` returns `None` for missing and non-expiring keys.
- [x] R8: Driver `scan`, `unlink`, and `set(px=...)` work in `memory://` and network modes; `ex` with `px` raises.
- [x] R9: Default `LitestarConfig` leaves `MemoryChannelsBackend` and the response cache `MemoryStore` in place.
- [x] Docs, knowledge chapters, and changelog updated; targeted checks pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning probe: with `LitestarPlugin()` and default config, `ensure_finalized` replaced the app's `MemoryChannelsBackend` and response-cache `MemoryStore`.
- [2026-11-03 15:50] Dependents grep (`KeyValueStore`, `framework.stores`, `litestar.store`, `CacheStore(`, `ResponseCacheStore`, `.delete_all()` in `src/py/vertebra`): no production code reads store keys directly from the driver. `extensions/litestar/mcp/tasks.py` builds a local-only `ResponseCacheStore()` (inherits the prefix change, no driver). `extensions/sqlspec/spanner.py` `delete_all`/`expires_in` are the Spanner sqlspec store, unrelated to these classes. The only test that inspects raw keys is the Valkey invalidation test updated in Step 1.1. Remaining `flushdb()` callers after this task: `extensions/alchemy/cache.py` (owned by `sqlalchemy-and-django-integration`) and the driver's own `flushdb` method.
