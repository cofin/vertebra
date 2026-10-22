---
type: Spec
flow_id: db-redis-valkey-and-analytics
title: "Chapter 3: Native RESP3 Redis/Valkey drivers, Stores/Channels adapters, ChangeFeed Parquet spill, and DataFusion analytics"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Implement native RESP3 sync/async socket and TLS connection pools for Redis and Valkey with pipelining, pub/sub, and Lua EVALSHA caching, wire KeyValueBackend and ChannelBus adapters, and deliver ChangeFeed ObjectStore Parquet spill and DataFusionEngine vectorized Arrow SQL execution."
tags: [feature, database, redis, valkey, cdc, datafusion, arrow]
parent_prd: vertebra-native-db-drivers
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 3: Native RESP3 Redis/Valkey drivers, Stores/Channels adapters, ChangeFeed Parquet spill, and DataFusion analytics

**Flow ID:** `db-redis-valkey-and-analytics`

## Specification

### Parent Context

This chapter is Chapter 3 of [`vertebra-native-db-drivers`](../vertebra-native-db-drivers/spec.md) and depends on Chapter 2 (`db-mysql-and-oracle-drivers`). It owns requirements **R6** (native RESP3 sync/async socket + TLS connection pools for Redis and Valkey with pipelining, pub/sub, client-side cache invalidation push parsing, and Lua `EVALSHA` caching), **R7** (live `KeyValueBackend` and `ChannelBus` adapters in `vertebra.framework.{stores,channels}` and `vertebra.extensions.litestar.{store,channels}`), **R8** (live PostgreSQL `ChangeFeed` consumer with `ObjectStore` Parquet spill, single-RTT `QueryPipeline.execute`, and `DataFusionEngine` vectorized Arrow SQL execution behind `vertebra[datafusion]`), and **R9** (Redis, Valkey, CDC, and DataFusion unit and integration acceptance suite).

### Code Analysis Summary

- **`src/py/vertebra/db/redis.py` (141 lines) & `valkey.py` (161 lines):**
  - `SyncRedisDriver` (`redis.py:38-108`) and `SyncValkeyDriver` (`valkey.py:61-119`) only read and write an in-process `self._kv: dict[str, bytes]` dictionary (`FW-02`) and have no RESP3 wire parser, TCP/TLS socket pool, pipelined response reader, pub/sub subscriber loop, or Lua `EVALSHA` script cache.
  - `SyncValkeyDriver` (`valkey.py:61-119`) and `AsyncValkeyDriver` (`valkey.py:121-161`) duplicate `SyncRedisDriver` and `AsyncRedisDriver`, and `SyncRedisDriver.encode_hello3_handshake` (`redis.py:69-85`) accepts duplicate `auth_username`/`auth_password` and `username`/`password` parameters (`FW-57`).
- **`src/py/vertebra/framework/stores/__init__.py` (47 lines), `channels/__init__.py` (13 lines), `extensions/litestar/store.py` (164 lines), & `channels.py` (173 lines):**
  - `vertebra.framework.stores` (`:1-47`) only exports `MemoryStore`, `NamespacedStore`, and `swap_litestar_response_cache_store`; `vertebra.framework.channels` (`:1-13`) only re-exports `ChannelsBackend` and `swap_litestar_channels_backend`.
  - `MemoryStore` (`extensions/litestar/store.py:59-122`) caches every `set()` indefinitely in `self._entries` alongside `self._driver`, never propagates `delete_all()` or `expires_in()` (`PTTL`) to `self._driver`, and never registers a RESP3 client-side cache invalidation callback when backed by `AsyncValkeyDriver`.
  - `ChannelsBackend` (`extensions/litestar/channels.py:66-123`) calls `await self._driver.publish(channel, payload)` (`:83`), but `subscribe()` (`:93-97`) and `stream_events()` (`:105-123`) never open a remote Redis/Valkey pub/sub subscription stream.
- **`src/py/vertebra/db/cdc.py` (118 lines), `pipeline.py` (129 lines), & `datafusion.py` (84 lines):**
  - `ChangeFeed` (`cdc.py:39-118`) stores spilled payloads (`>7,500` bytes) only in an in-memory `_spill_store: dict[int, dict[str, Any]]` (`:45`), grows `self._history` without bound (`:47,99`), and silently returns `{}` when a spilled `event_id` is missing (`:107`, `FW-59`).
  - `QueryPipeline` (`pipeline.py:48-129`) has duplicate `chunk_size` and `max_pairs_per_call` parameters in `build_jsonb_object` (`:63-87`), silently overwrites colliding step parameter names in `compile()` (`:115`), does not validate SQL identifiers in `RelOverlay` or `CteStep` (`FW-59`), and has no `execute` / `execute_async` methods to run the compiled CTE statement against `SyncPostgresDriver` / `AsyncPostgresDriver`.
  - `DataFusionEngine` (`datafusion.py:46-84`) only returns a static dictionary from `explain_plan` (`:76-84`) and has no `execute_arrow`, `execute_sql`, or `stream_arrow_ipc` implementation (`FW-10`).

#### Findings carried in this chapter

- `FW-02`: Replace fake in-memory KV in `SyncRedisDriver`/`AsyncRedisDriver` and `SyncValkeyDriver`/`AsyncValkeyDriver` with a real RESP3 parser and TCP/TLS connection pool (`memory://` retained explicitly for unit tests).
- `FW-10`: Implement `DataFusionEngine` vectorized SQL execution over Arrow/Parquet/ObjectStore tables behind `vertebra[datafusion]`.
- `FW-57`: Deduplicate `SyncValkeyDriver`/`AsyncValkeyDriver` onto `SyncRedisDriver`/`AsyncRedisDriver` and remove `auth_username`/`auth_password` parameter duplication.
- `FW-59`: Bound `ChangeFeed._history`, spill large CDC payloads and WAL batches to `ObjectStore` Parquet/JSON objects, raise `KeyError` on missing spilled payloads, and validate SQL identifiers and parameter collisions in `QueryPipeline` and `RelOverlay`.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, zero inline `#` comments, `vertebra.utils.serializers` for JSON encoding/decoding, and explicit optional dependency boundaries (`vertebra[datafusion]`).

### Requirements

| ID | Requirement |
| --- | --- |
| C3-R1 | Implement a complete RESP3 wire protocol parser (`Resp3Parser`) and pooled sync (`socket` / `ssl.SSLSocket`) + async (`asyncio.StreamReader` / `StreamWriter`) transport in `src/py/vertebra/db/redis.py` and `valkey.py` supporting `HELLO 3`, `CLIENT TRACKING` push invalidations (`>2\r\n$10\r\ninvalidate...`), pipelining (`execute_pipeline`), pub/sub (`subscribe` / `listen`), and Lua `EVALSHA` caching (`eval_script`), while deduplicating `ValkeyDriver` onto `RedisDriver`. |
| C3-R2 | Implement `KeyValueBackend` in `src/py/vertebra/framework/stores.py` and `ChannelBus` in `src/py/vertebra/framework/channels.py`, and wire remote `PTTL` (`expires_in`), `FLUSHDB` (`delete_all`), Valkey RESP3 push invalidation hooks, and background Redis/Valkey pub/sub fan-in tasks in `src/py/vertebra/extensions/litestar/{store,channels}.py`. |
| C3-R3 | Add `datafusion` optional extra (`datafusion>=43.0`, `pyarrow>=18.0`) in `pyproject.toml`, implement `ObjectStore` Parquet/JSON spill and `LISTEN`/WAL consumption in `ChangeFeed` (`src/py/vertebra/db/cdc.py`), add identifier validation, parameter collision checks, and `execute`/`execute_async` in `QueryPipeline` (`src/py/vertebra/db/pipeline.py`), and implement vectorized SQL execution and Arrow IPC streaming in `DataFusionEngine` (`src/py/vertebra/db/datafusion.py`). |
| C3-R4 | Add unit and integration acceptance tests for RESP3 Redis/Valkey drivers, `KeyValueBackend`/`ChannelBus`, `ChangeFeed` Parquet spill, and `DataFusionEngine` in `src/py/tests/unit/db/test_redis_valkey_and_analytics.py`. |

### Target Layout

```text
pyproject.toml                                          add vertebra[datafusion] extra
src/py/vertebra/
  db/
    redis.py                                            Resp3Parser + pooled SyncRedisDriver / AsyncRedisDriver (TCP/TLS, pipeline, pub/sub, EVALSHA)
    valkey.py                                           SyncValkeyDriver / AsyncValkeyDriver subclassing Redis drivers with RESP3 CLIENT TRACKING push cache
    cdc.py                                              ChangeFeed bounded history + ObjectStore Parquet/JSON spill + Postgres LISTEN consumer
    pipeline.py                                         RelOverlay/CteStep identifier validation + QueryPipeline parameter collision check + execute/execute_async
    datafusion.py                                       DataFusionEngine SessionContext binding + Arrow/Parquet registration + execute_arrow / stream_arrow_ipc
  framework/
    stores.py                                           KeyValueBackend + MemoryStore + NamespacedStore
    channels.py                                         ChannelBus + ChannelsBackend
  extensions/
    litestar/
      store.py                                          MemoryStore with PTTL, delete_all, and Valkey invalidation push hook
      channels.py                                       ChannelsBackend with background Redis/Valkey pub/sub subscriber fan-in
src/py/tests/unit/data/
  test_redis_valkey_driver.py                           focused unit tests for RESP3 parser, pipelining, pub/sub, and EVALSHA
  test_pg_gateway.py                                    focused unit tests for ChangeFeed Parquet spill, QueryPipeline validation, and DataFusionEngine
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Interleaved RESP3 push frames (`>`) arriving mid-command on a tracked Valkey connection corrupt the command reply parser. | `Resp3Parser.read_response` detects leading `>` push frames, routes `["invalidate", keys]` frames immediately to the registered invalidation callback, and reads the next wire frame for the awaiting command caller. |
| Background pub/sub listener tasks in `ChannelsBackend` leak on shutdown if not cancelled. | `ChannelsBackend.on_shutdown` cancels and awaits the background reader task with `contextlib.suppress(asyncio.CancelledError)` before draining local subscriber queues. |

## Implementation Plan

### Phase 1: Native RESP3 Drivers, Stores/Channels, CDC Parquet Spill, and DataFusion
- [x] 3.1 Native RESP3 sync/async socket and TLS connection pool for `RedisDriver` and `ValkeyDriver` with pipelining, pub/sub, and Lua `EVALSHA` caching ([worksheet](tasks/3.1.md))
- [x] 3.2 Live `KeyValueBackend` and `ChannelBus` Redis/Valkey adapters ([worksheet](tasks/3.2.md))
- [x] 3.3 Live `ChangeFeed` consumer with `ObjectStore` Parquet spill, `QueryPipeline.execute`, and `DataFusionEngine` vectorized Arrow SQL ([worksheet](tasks/3.3.md))
- [x] 3.4 Unit and integration acceptance tests for RESP3 Redis/Valkey drivers, `ChangeFeed` Parquet spill, and `DataFusionEngine` ([worksheet](tasks/3.4.md))
- [x] Chapter gate: `make lint-all && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C3-R1 | 3.1 | `uv run pytest src/py/tests/unit/data/test_redis_valkey_driver.py -k "redis or valkey" -q` |
| C3-R2 | 3.2 | `uv run pytest src/py/tests/unit/data/test_redis_valkey_driver.py -k "valkey_resp3_client_tracking" -q` |
| C3-R3 | 3.3 | `uv run pytest src/py/tests/unit/data/test_pg_gateway.py -q` |
| C3-R4 | 3.4 | `uv run pytest src/py/tests/unit/data/test_redis_valkey_driver.py src/py/tests/unit/data/test_pg_gateway.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| FW-02 | 3.1 |
| FW-57 | 3.1 |
| FW-10 | 3.3 |
| FW-59 | 3.3 |

## Continuity Snapshot

- Active flow: `db-redis-valkey-and-analytics`, state `planned`. Current task: none. Next step: `/flow:implement db-redis-valkey-and-analytics` after Chapter 2 completes.
- Plan identity: `plan_revision: 2`, `plan_commit: ""`. State identity: `state_revision: 0`.
- Blockers: depends on Chapter 2 (`db-mysql-and-oracle-drivers`).
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`.
