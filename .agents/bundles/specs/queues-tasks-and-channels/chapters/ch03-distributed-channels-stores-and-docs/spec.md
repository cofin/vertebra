---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: 'Chapter 03: Distributed Channels Across 5 Backends, Django Cache/Channels, ValkeyStore & Docs/OKF'
description: Wire Django native cache/channels, bridge Sync/AsyncNotificationSource into ChannelBus and Litestar/Starlette/Flask channels across 5 backends, consolidate ValkeyStore over vertebra._redis, and synchronize docs and OKF chapters.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
depends_on_chapters:
- ch02-workers-litestar-queues-and-django-tasks
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- django
- tasks
- queues
- channels
- cache
- stores
- valkey
- redis
sources:
- id: dj-queues
  resource: src/py/vertebra/extensions/django/queues.py
  title: Legacy Django deque queue module
- id: dj-cache
  resource: src/py/vertebra/extensions/django/cache.py
  title: Django cache backend
- id: dj-channels
  resource: src/py/vertebra/extensions/django/channels.py
  title: Django channel layer
- id: channel-bus
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: In-process/SHM ChannelBus
- id: ls-channels
  resource: src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend
  title: Litestar channels monkeypatch
- id: fw-stores
  resource: src/py/vertebra/framework/stores.py
  title: Framework stores
- id: ls-store
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Litestar Store adapters
- id: doc-qr
  resource: docs/usage/queues-and-realtime.md
  title: Queues and realtime usage guide
- id: okf-queues
  resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  title: Distributed queues OKF chapter
- id: okf-litestar
  resource: .agents/bundles/knowledge/domains/extensions/litestar.md
  title: Litestar OKF chapter
- id: okf-srv-lim
  resource: .agents/bundles/knowledge/domains/server/server-runtime-limitations.md
  title: Server runtime limitations OKF chapter
---

# Chapter 03: Distributed Channels Across 5 Backends, Django Cache/Channels, ValkeyStore & Docs/OKF

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/django/{cache,channels}.py` rely on in-process dicts, `ChannelBus` is single-node only, `extensions/litestar/channels.py` uses `swap_channels_backend` monkeypatching, `stores.py` / `starlette/store.py` duplicate KV logic (while prefix-scoped `SCAN`+`UNLINK` already replaced `flushdb()` in `364c7cbe`), and docs/`distributed-queues-and-email.md` still describe fail-loud `executor=` baselines and unprefixed protocols [^dj-cache] [^dj-channels] [^channel-bus] [^ls-channels] [^fw-stores] [^doc-qr] [^okf-queues].
- **Target Architecture**: Django `CacheBackend`/`ChannelLayer` run on native `vertebra._redis` and `ChannelBus`; `ChannelBus`, Vertebra `ChannelsBackend`, Litestar `ChannelsPlugin`, Starlette, and Flask channels bind `SyncNotificationSource`/`AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle without monkeypatching; `ValkeyStore`/`RedisStore` consolidate onto `vertebra._redis` with RESP3 `CLIENT TRACKING` and `SCAN`+`UNLINK`; docs and `<= 200`-line OKF chapters match present reality [^ls-store] [^okf-litestar] [^okf-srv-lim].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter upgrades Django `CacheBackend` and `ChannelLayer` onto native drivers, bridges `SyncNotificationSource`/`AsyncNotificationSource` (`native-redis-driver:ch03`) into `ChannelBus` and Litestar/Starlette/Flask channels (deleting `swap_channels_backend`), consolidates `ValkeyStore`/`RedisStore` onto `vertebra._redis`, and synchronizes all public usage/reference docs and OKF v0.2 knowledge chapters [^dj-cache] [^channel-bus] [^ls-store] [^doc-qr] [^okf-queues].

## 2. Requirements, Concrete Contracts & Architecture

- **R8 (Django `6.0+` `TaskBackend` & Worker Command)**: `enqueue` uses `SyncQueueStore`, `aenqueue` uses `AsyncQueueStore`, `vertebra_worker` injects `Container.scope()` dependencies, and `django/queues.py` is deleted.
- **R9 (Django Native Cache & Channels)**: Prefix-safe `SCAN`+`UNLINK` Valkey/Redis `CacheBackend` and distributed `ChannelLayer`.

```python
class TaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def enqueue(self, task: Task, args: Sequence[Any], kwargs: Mapping[str, Any]) -> TaskResult: ...
    async def aenqueue(self, task: Task, args: Sequence[Any], kwargs: Mapping[str, Any]) -> TaskResult: ...
    def get_result(self, result_id: str) -> TaskResult: ...
    async def aget_result(self, result_id: str) -> TaskResult: ...
```

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

- **R12 (Documentation & OKF Synchronization)**: Update `docs/usage/{queues-and-realtime,extensions,databases,analytics-and-cdc}.md`, `docs/reference/{queues,database-capabilities}.md`, `distributed-queues-and-email.md`, `litestar.md`, and `server-runtime-limitations.md`.

```text
Documentation & OKF Target Matrix:
  docs/usage/queues-and-realtime.md          — QueueBackendSettings, native stores, ChannelBus
  docs/usage/extensions.md                   — litestar-queues 0.14+, Django 6.0+ TASKS, ValkeyStore
  docs/usage/{databases,analytics-and-cdc}.md — SyncNotificationSource / AsyncNotificationSource
  docs/reference/{queues,database-capabilities}.md — SyncQueueStore / AsyncQueueStore APIs
  distributed-queues-and-email.md (<=200L)   — live PG/Oracle/Redis/Valkey queue truth block
  litestar.md & server-runtime-limitations.md (<=200L) — ChannelsPlugin & delivery caveats
```

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/{queues-and-realtime,extensions,databases,analytics-and-cdc}.md`, `docs/reference/{queues,database-capabilities}.md`, [distributed-queues-and-email.md](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [litestar.md](../../../../knowledge/domains/extensions/litestar.md), and [server-runtime-limitations.md](../../../../knowledge/domains/server/server-runtime-limitations.md) in `t04` (`CACHES`, `TASKS`, `CHANNEL_LAYERS`, realtime channels, and KV stores), keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Wire Django Native Cache/Channels and Bridge ChannelBus Notification Fanout Across 5 Backends | `src/py/vertebra/extensions/django/cache.py`, `src/py/vertebra/extensions/django/channels.py`, `src/py/vertebra/server/_channels.py` (+1 more) | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/server/test_realtime_backpressure.py` |
| `t02` | Wire Litestar ChannelsPlugin, Starlette, and Flask Channels Backends Without Monkeypatching | `src/py/vertebra/extensions/litestar/channels.py`, `src/py/vertebra/extensions/starlette/channels.py`, `src/py/vertebra/extensions/flask/channels.py` | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/extensions/test_starlette_plugin.py` |
| `t03` | Consolidate ValkeyStore, RedisStore & MemoryStore Over vertebra._redis with RESP3 Tracking | `src/py/vertebra/framework/stores.py`, `src/py/vertebra/extensions/litestar/store.py`, `src/py/vertebra/extensions/starlette/store.py` | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/extensions/test_starlette_plugin.py` |
| `t04` | Synchronize Public Queues, Realtime, Extensions & Databases Docs and OKF Chapters | `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, `docs/reference/database-capabilities.md` (+6 more) | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py`, `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |

## 5. Implementation Plan

- [ ] [t01-django-cache-channels-and-channelbus-fanout.md](tasks/t01-django-cache-channels-and-channelbus-fanout.md) — Implement native Valkey/Redis Django CacheBackend + ChannelLayer and bridge ChannelBus / framework/channels.py to SyncNotificationSource and AsyncNotificationSource.
- [ ] [t02-litestar-and-starlette-channels-backends.md](tasks/t02-litestar-and-starlette-channels-backends.md) — Connect Litestar ChannelsPlugin, Starlette, and Flask channels to native notification sources and delete swap_channels_backend.
- [ ] [t03-valkey-and-redis-kv-store-consolidation.md](tasks/t03-valkey-and-redis-kv-store-consolidation.md) — Consolidate framework/stores.py, extensions/litestar/store.py, and extensions/starlette/store.py over vertebra._redis preserving prefix-scoped SCAN+UNLINK.
- [ ] [t04-queues-channels-docs-and-okf-sync.md](tasks/t04-queues-channels-docs-and-okf-sync.md) — Update queues-and-realtime, extensions, databases, and analytics docs plus reference pages, and sync distributed-queues-and-email.md, litestar.md, and server-runtime-limitations.md.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-distributed-channels-stores-and-docs` (`t04`) is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/django src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/django src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync python -m tools.docs.scan_public_docstrings && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

[^dj-queues]: `src/py/vertebra/extensions/django/queues.py` — Legacy Django deque queue module.
[^dj-cache]: `src/py/vertebra/extensions/django/cache.py` — Django cache backend.
[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^ls-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels monkeypatch.
[^fw-stores]: `src/py/vertebra/framework/stores.py` — Framework stores.
[^ls-store]: `src/py/vertebra/extensions/litestar/store.py` — Litestar Store adapters.
[^doc-qr]: `docs/usage/queues-and-realtime.md` — Queues and realtime usage guide.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar OKF chapter.
[^okf-srv-lim]: `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` — Server runtime limitations OKF chapter.
