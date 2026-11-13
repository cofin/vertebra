---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch06-distributed-channels-and-valkey-stores
title: "Chapter 06: Distributed ChannelsBackends (Memory, Redis, Valkey, PostgreSQL, Oracle) & ValkeyStore"
description: "Wire SyncNotificationSource and AsyncNotificationSource into ChannelBus, Litestar ChannelsPlugin, Starlette, and Flask, and consolidate ValkeyStore over vertebra._redis."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - channels
  - stores
  - valkey
  - redis
  - litestar
sources:
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: "In-process/SHM ChannelBus"
  - id: ls-channels
    resource: src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend
    title: "Litestar channels monkeypatch"
  - id: fw-stores
    resource: src/py/vertebra/framework/stores.py
    title: "Framework stores"
  - id: ls-store
    resource: src/py/vertebra/extensions/litestar/store.py
    title: "Litestar Store adapters"
---

# Chapter 06: Distributed ChannelsBackends (Memory, Redis, Valkey, PostgreSQL, Oracle) & ValkeyStore

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `ChannelBus` is single-node only, `extensions/litestar/channels.py` uses `swap_channels_backend` monkeypatching, and `stores.py` / `starlette/store.py` duplicate KV logic and call `flushdb()` [^channel-bus] [^ls-channels] [^fw-stores].
- **Target Architecture**: `ChannelBus`, Vertebra `ChannelsBackend`, and Litestar `ChannelsPlugin` bind `SyncNotificationSource` / `AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle without monkeypatching; `ValkeyStore` consolidates onto `vertebra._redis` with RESP3 tracking and `SCAN`+`UNLINK` [^ls-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter wires distributed pub/sub notifications (`SyncNotificationSource` / `AsyncNotificationSource` from `native-redis-driver:ch05`) into Vertebra, Litestar `ChannelsPlugin`, Starlette, and Flask channels, and consolidates `ValkeyStore` / `RedisStore` onto `vertebra._redis` [^channel-bus] [^ls-store].

## 2. Requirements, Concrete Contracts & Architecture

- **R10 (Distributed `ChannelsBackend` Across 5 Backends)**: Support Memory, Redis, Valkey, PostgreSQL, and Oracle in `ChannelBus`, Vertebra `ChannelsBackend`, Litestar `ChannelsPlugin` (deleting `swap_channels_backend`), Starlette, and Flask.
- **R11 (`ValkeyStore` Consolidation)**: `ValkeyStore(litestar.stores.base.Store)` over `vertebra._redis` with RESP3 `CLIENT TRACKING` invalidation and prefix-scoped `SCAN`+`UNLINK`.

```python
class ChannelBus:
    def __init__(
        self,
        *,
        notification_source: AsyncNotificationSource | None = None,
        sync_notification_source: SyncNotificationSource | None = None,
        node_id: str | None = None,
    ) -> None: ...

class ValkeyStore(Store):
    __slots__ = ("_client_tracking", "_driver", "_key_prefix", "_l1_cache")
    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None: ...
    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None: ...
    async def delete(self, key: str) -> None: ...
    async def delete_all(self) -> None: ...
```

## 3. Documentation & OKF Knowledge Plan

Realtime channels and KV store documentation land in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` `ChannelBus` & `ChannelsBackend` | `t01` | `server/_channels.py`, `framework/channels.py` | `pytest test_realtime_backpressure.py` |
| `R10` Litestar/Starlette/Flask channels | `t02` | `extensions/{litestar,starlette,flask}/channels.py` | `pytest test_litestar_plugin.py` |
| `R11` `ValkeyStore` & KV consolidation | `t03` | `framework/stores.py`, `extensions/{litestar,starlette}/store.py` | `pytest test_litestar_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-channel-bus-notification-fanout.md](tasks/t01-channel-bus-notification-fanout.md) — Bridge `ChannelBus` and `framework/channels.py` to `SyncNotificationSource` / `AsyncNotificationSource` across all 5 backends.
- [ ] [t02-litestar-and-starlette-channels-backends.md](tasks/t02-litestar-and-starlette-channels-backends.md) — Wire Litestar `ChannelsPlugin`, Starlette, and Flask channels backends and delete `swap_channels_backend`.
- [ ] [t03-valkey-and-redis-kv-store-consolidation.md](tasks/t03-valkey-and-redis-kv-store-consolidation.md) — Consolidate `ValkeyStore`, `RedisStore`, and `MemoryStore` over `vertebra._redis` with RESP3 tracking and `SCAN`+`UNLINK`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch06-distributed-channels-and-valkey-stores` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync pytest src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py
```

[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^ls-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels monkeypatch.
[^fw-stores]: `src/py/vertebra/framework/stores.py` — Framework stores.
[^ls-store]: `src/py/vertebra/extensions/litestar/store.py` — Litestar Store adapters.
