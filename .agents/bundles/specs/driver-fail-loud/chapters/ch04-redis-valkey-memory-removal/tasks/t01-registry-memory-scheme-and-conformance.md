---
type: Task
id: "driver-fail-loud:ch04:t01"
chapter_id: ch04-redis-valkey-memory-removal
title: Registry drops the memory scheme and remaining cache tests use the fixture or live servers
description: Remove memory from RedisConfig.url_schemes so DatabaseRegistry.parse_url rejects memory://, update RedisConfig docstrings, retarget the RESP3 sync pubsub and native Valkey tests to the loopback fixture, and make conformance cache cases live-only.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/redis/config.py
  - src/py/tests/unit/conftest.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conftest.py
files:
  - src/py/vertebra/db/adapters/redis/config.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 110
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-09T03:20:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:27:00Z"
last_verified_commit: 20c535e3
verification_evidence: "pytest test_native_db_drivers.py test_redis_valkey_resp3.py test_driver_conformance.py passed; ruff and mypy exited 0"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T03:27:00Z"
commit: 20c535e3
sources:
  - id: redis-config
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis config url_schemes and from_url
  - id: registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url
    title: Scheme dispatch over SCHEME_REGISTRY
  - id: conformance-cache
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config
    title: Conformance cache config resolution
  - id: integration-fixtures
    resource: src/py/tests/integration/conftest.py::vtb_redis_config
    title: pytest-databases Redis and Valkey config fixtures
tags:
  - redis
  - valkey
  - registry
  - conformance
  - tdd
---

# Task t01: Registry Drops the `memory` Scheme; Remaining Cache Tests Go Live

## Objective & Scope Boundary

Set `RedisConfig.url_schemes = ("redis", "rediss")` so `SCHEME_REGISTRY` no longer maps `memory` and `DatabaseRegistry.parse_url("memory://")` raises `ValueError("Unsupported database URL scheme: 'memory'")`.[^redis-config][^registry] Update `RedisConfig.url` and `RedisConfig.from_url` docstrings. Retarget the last unit tests that construct `memory://` cache configs onto the `resp3_server` fixture from `ch03-remove-memory-fakes`, and make the conformance cache cases resolve only live servers.[^conformance-cache][^integration-fixtures]

Out of scope: driver memory branches (`driver-fail-loud:ch04:t02`); `ValkeyConfig` (its `url_schemes` already exclude `memory`; its docstring changes in `driver-fail-loud:ch04:t02`); the `resp3_server` fixture itself (owned by `ch03-remove-memory-fakes`); `_resolve_live_config` (owned by `driver-fail-loud:ch09:t01`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/config.py::RedisConfig.url_schemes`, `::RedisConfig.url`, `::RedisConfig.from_url`.[^redis-config]
- `src/py/tests/unit/conftest.py::resp3_server` — fixture from `ch03-remove-memory-fakes` (`redis_url`, `valkey_url`, and the `SUBSCRIBE` / `PSUBSCRIBE` / `SSUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH` / `SPUBLISH` surface).
- `src/py/tests/unit/db/test_native_db_drivers.py::test_polymorphic_registry_parse_url_roundtrip`, `::test_valkey_resp3_client_tracking_and_litestar_integration`, `::test_database_registry_url_schemes`.
- `src/py/tests/unit/db/test_redis_valkey_resp3.py::test_sync_multiplexed_pubsub_and_cluster_moved_ask`, `::_RecordingBroker`.
- `src/py/tests/integration/db/test_driver_conformance.py::CacheConformanceSpec`, `::CACHE_ENGINE_SPECS`, `::test_cache_config_url_pool_key_and_provide_driver`, `::_resolve_cache_config`, `::test_sync_cache_memory_roundtrip_and_pipeline`, `::test_async_cache_memory_roundtrip_and_pipeline`.[^conformance-cache]
- `src/py/tests/integration/conftest.py::vtb_redis_config`, `::vtb_valkey_config`.[^integration-fixtures]

## Symbol & Signature Contracts

`src/py/vertebra/db/adapters/redis/config.py`:

- `url_schemes: ClassVar[tuple[str, ...]] = ("redis", "rediss")`.
- `url` attribute docstring: `"""Redis connection URI (`redis://` or `rediss://`)."""`.
- `from_url` docstring: `"""Construct a `RedisConfig` from a `redis://` or `rediss://` URI."""`.

`src/py/tests/integration/db/test_driver_conformance.py`:

```python
class CacheConformanceSpec(FrozenBaseStruct, frozen=True, gc=False):
    engine: str
    config_cls: type[RedisConfig]
    sync_driver_cls: type[SyncRedisDriver]
    async_driver_cls: type[AsyncRedisDriver]
    tcp_url: str
    env_dsn_vars: tuple[str, ...] = ()
    """Environment variables holding a live server URL, checked in order."""
    container_fixture: str | None = None
```

- `CACHE_ENGINE_SPECS`: Redis gains `env_dsn_vars=("VERTEBRA_CONFORMANCE_REDIS_DSN",)`; Valkey gains `env_dsn_vars=("VERTEBRA_CONFORMANCE_VALKEY_DSN",)`.
- `_resolve_cache_config(cache_spec, request) -> RedisConfig`: return `cache_spec.config_cls.from_url(value)` for the first non-empty stripped env var; else return the container fixture when `VERTEBRA_CONFORMANCE_DOCKER == "1"`; else `pytest.skip(f"{cache_spec.engine} conformance requires {' or '.join(cache_spec.env_dsn_vars)} or VERTEBRA_CONFORMANCE_DOCKER=1")`. Docstring: `"""Resolve a live `RedisConfig` or `ValkeyConfig` from an env DSN or `pytest-databases` container, skipping otherwise."""`.
- Rename `test_sync_cache_memory_roundtrip_and_pipeline` → `test_sync_cache_roundtrip_and_pipeline` and `test_async_cache_memory_roundtrip_and_pipeline` → `test_async_cache_roundtrip_and_pipeline`; bodies unchanged.
- `test_cache_config_url_pool_key_and_provide_driver`: delete `mem_cfg` and its `pool_key` assertion; call `provide_driver(parsed, ...)` for the three driver resolutions. Driver construction opens no socket, so the case stays offline.

## Edge Cases & Error Semantics

- `DatabaseRegistry.parse_url("memory://")` raises `ValueError` whose message is `Unsupported database URL scheme: 'memory'`.
- `DatabaseRegistry.parse_url("redis://localhost:6379/0")` and `"rediss://..."` still return `RedisConfig`; `"valkey://..."` still returns `ValkeyConfig`.
- `test_registry_parse_url_executes_zero_eager_driver_imports` stays green (no import change).
- `RedisConfig(url="memory://")` built directly still constructs in this task; `driver-fail-loud:ch04:t02` makes the driver reject it.
- Env var set to whitespace counts as unset.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_redis_valkey_resp3.py`, `src/py/tests/integration/db/test_driver_conformance.py`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| New `test_registry_parse_url_rejects_memory_scheme` | `DatabaseRegistry.parse_url("memory://")` | `pytest.raises(ValueError, match="Unsupported database URL scheme: 'memory'")` |
| Roundtrip parametrize | delete the `("memory://", RedisConfig)` row from `test_polymorphic_registry_parse_url_roundtrip` | remaining rows pass |
| `test_native_db_drivers.py::test_valkey_resp3_client_tracking_and_litestar_integration(resp3_server)` | `mem_vcfg` uses `url=resp3_server.valkey_url`; `MemoryStore.from_valkey(resp3_server.valkey_url)` | `vdriver.cached_keys == ("user:1",)` after `set`; `handle_invalidation_push(["user:1"]) == 1`; `await store.get("k1") == b"v1"` |
| `test_redis_valkey_resp3.py::test_sync_multiplexed_pubsub_and_cluster_moved_ask(resp3_server)` | `SyncRedisDriver(RedisConfig(url=resp3_server.redis_url), broker=broker)`; the shard message is sent with `driver.execute_command("SPUBLISH", "shard:1", b"msg-s")` in place of `driver.publish("shard:1", b"msg-s")` because `SSUBSCRIBE` subscribers receive `smessage` only from `SPUBLISH`; the test declares `resp3_server: Resp3FakeServer` with the `TYPE_CHECKING` import from `tests.unit.conftest` (same pattern in `test_native_db_drivers.py`) | `drain_messages() == [PubSubMessage(kind="message", channel="alpha", payload=b"msg-a", pattern=None), PubSubMessage(kind="pmessage", channel="beta:42", payload=b"msg-b", pattern="beta:*"), PubSubMessage(kind="smessage", channel="shard:1", payload=b"msg-s", pattern=None)]`; `("alpha", b"msg-a") in broker.published`; after `unsubscribe("alpha")` and `publish("alpha", "ignored")`, `drain_messages() == []` |
| Conformance local | no env DSN, no docker flag | both cache roundtrip tests report `SKIPPED` with reason naming `VERTEBRA_CONFORMANCE_REDIS_DSN` / `VERTEBRA_CONFORMANCE_VALKEY_DSN` |
| Conformance pool key | `test_cache_config_url_pool_key_and_provide_driver` | `parsed.pool_key() == (engine, tcp_url)`; `sync_first is sync_second` |

## Verification

- Red: add `test_registry_parse_url_rejects_memory_scheme`, then `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py -k rejects_memory_scheme -q` fails (returns `RedisConfig`).
- Green + lint + typecheck: `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/integration/db/test_driver_conformance.py -q -rs && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis/config.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/integration/db/test_driver_conformance.py && uv run --no-sync ruff format --check src/py/vertebra/db/adapters/redis/config.py && uv run --no-sync mypy src/py/vertebra/db/adapters/redis/config.py`; the `-rs` summary lists the two cache roundtrip cases per engine as skipped.
- Residue gate: `rg -n '"memory://"' src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/integration/db/test_driver_conformance.py src/py/vertebra/db/adapters/redis/config.py | rg -v 'dsn="memory://"|reject_memory_dsn'` prints only the `parse_url("memory://")` line of the new rejection test. The `rg -v` filter excludes the one-line `pytest.param(lambda: SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://")), id="sync-postgres")` rows and the test name of `test_postgres_and_mysql_drivers_reject_memory_dsn`, whose literal shape `driver-fail-loud:ch03:t01` pins.

## Acceptance Criteria

- [x] `RedisConfig.url_schemes == ("redis", "rediss")` and `parse_url("memory://")` raises `ValueError` (R7b).
- [x] No unit test in the three files constructs a `memory://` cache config (R7b).
- [x] Conformance cache cases run only against env DSN or docker servers and skip otherwise (R7b).
- [x] Zero inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] `driver-fail-loud:ch09:t01` edits `_resolve_live_config` in the same conformance module; this task leaves that function untouched.

[^redis-config]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis config url_schemes and from_url.
[^registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url` — Scheme dispatch over SCHEME_REGISTRY.
[^conformance-cache]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config` — Conformance cache config resolution.
[^integration-fixtures]: `src/py/tests/integration/conftest.py::vtb_redis_config` — pytest-databases Redis and Valkey config fixtures.
