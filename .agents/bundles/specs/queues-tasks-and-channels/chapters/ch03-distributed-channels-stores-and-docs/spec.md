---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: 'Chapter 03: Distributed Channels Across 5 Backends, Django ChannelLayer, ValkeyStore/RedisStore & Docs/OKF'
description: Wire Django ChannelLayer, bridge Sync/AsyncNotificationSource into ChannelBus and Litestar/Starlette/Flask channels across 5 backends, consolidate ValkeyStore (vertebra._valkey) and RedisStore (vertebra._redis), and synchronize docs and OKF chapters.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
depends_on_chapters:
  - ch02-workers-litestar-queues-and-django-tasks
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
tags:
  - django
  - tasks
  - queues
  - channels
  - stores
  - valkey
  - redis
sources:
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
current_task: "queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t01"
---

# Chapter 03: Distributed Channels Across 5 Backends, Django ChannelLayer, ValkeyStore/RedisStore & Docs/OKF

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `extensions/django/channels.py` relies on in-process dicts, `ChannelBus` bridges one async `NotificationSource` and an `AsyncChangeSource` but has no sync source, no origin-node de-duplication, and no per-backend wiring, `src/py/vertebra/framework/channels.py` only re-exports `ChannelBus`, `extensions/litestar/channels.py` uses `swap_channels_backend` monkeypatching, `KeyValueStore` (`framework/stores.py`), `MemoryStore` (`extensions/litestar/store.py`), and `CacheStore` (`extensions/starlette/store.py`) duplicate KV logic with no `ValkeyStore` or `RedisStore` (while prefix-scoped `SCAN`+`UNLINK` already replaced `flushdb()` in `364c7cbe`), and docs/`distributed-queues-and-email.md` still describe fail-loud `executor=` baselines and unprefixed protocols [^dj-channels] [^channel-bus] [^ls-channels] [^fw-stores] [^doc-qr] [^okf-queues].
- **Target Architecture**: Django `ChannelLayer` (`extensions/django/channels.py`) runs on `ChannelBus` + `SyncNotificationSource`/`AsyncNotificationSource` (while Django `CacheBackend` is delivered in `sqlalchemy-and-django-integration:ch03:t03`); `ChannelBus`, a new first-party `ChannelsBackend` in `src/py/vertebra/framework/channels.py`, Litestar `ChannelsPlugin`, Starlette, and Flask channels bind `SyncNotificationSource`/`AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle without monkeypatching; new `ValkeyStore` (`vertebra._valkey`) and `RedisStore` (`vertebra._redis`) run on native drivers with RESP3 `CLIENT TRACKING` and `SCAN`+`UNLINK`; docs and `<= 200`-line OKF chapters match present reality [^ls-store] [^okf-litestar] [^okf-srv-lim].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter upgrades Django `ChannelLayer` (`extensions/django/channels.py`) onto `ChannelBus` and native notification sources, bridges `SyncNotificationSource`/`AsyncNotificationSource` (`native-redis-driver` and `native-valkey-driver`) into `ChannelBus` and Litestar/Starlette/Flask channels (deleting `swap_channels_backend`), introduces `ValkeyStore` (`vertebra._valkey`) and `RedisStore` (`vertebra._redis`), and synchronizes all public usage/reference docs and OKF v0.2 knowledge chapters [^dj-channels] [^channel-bus] [^ls-store] [^doc-qr] [^okf-queues].

## 2. Requirements, Concrete Contracts & Architecture

- **R9–R10 (Django `ChannelLayer` & Distributed `ChannelBus`)**: Route `send`, `receive`, `new_channel`, `group_add`, `group_discard`, and `group_send` in `extensions/django/channels.py::ChannelLayer` over `ChannelBus` + `AsyncNotificationSource` (`publish_sync` via `SyncNotificationSource`).
- **R10 (Distributed `ChannelsBackend` Across 5 Backends)**: Support Memory, Redis (`vertebra._redis`), Valkey (`vertebra._valkey`), PostgreSQL (`vertebra._postgres`), and Oracle (`vertebra._oracle`) in `ChannelBus`, the new first-party `ChannelsBackend`, Litestar `ChannelsPlugin` (deleting `swap_channels_backend`), Starlette, and Flask.
- **R11 (New `ValkeyStore` & `RedisStore`)**: `ValkeyStore(litestar.stores.base.Store)` over `AsyncValkeyDriver` (`vertebra._valkey`) and `RedisStore(Store)` over `AsyncRedisDriver` (`vertebra._redis`) with RESP3 `CLIENT TRACKING` invalidation and prefix-scoped `SCAN`+`UNLINK`.

```python
class ChannelBus:
    def __init__(
        self,
        *,
        backend: str = "memory",
        history: int = 0,
        change_source: AsyncChangeSource | None = None,
        change_feed: str | None = None,
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

- **R12 (Documentation & OKF Synchronization)**: Update `docs/usage/{queues-and-realtime,extensions,databases,analytics-and-cdc}.md`, `docs/reference/{queues,database-capabilities,db}.md`, `distributed-queues-and-email.md`, `litestar.md`, and `server-runtime-limitations.md`.

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/{queues-and-realtime,extensions,databases,analytics-and-cdc}.md`, `docs/reference/{queues,database-capabilities,db}.md`, [distributed-queues-and-email.md](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [litestar.md](../../../../knowledge/domains/extensions/litestar.md), and [server-runtime-limitations.md](../../../../knowledge/domains/server/server-runtime-limitations.md) in `t04`, keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Wire Django ChannelLayer and Bridge ChannelBus Notification Fanout Across 5 Backends | `src/py/vertebra/extensions/django/channels.py`, `src/py/vertebra/server/_channels.py`, `src/py/vertebra/framework/channels.py` | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/server/test_realtime_backpressure.py` |
| `t02` | Wire Litestar ChannelsPlugin, Starlette, and Flask Channels Backends Without Monkeypatching | `src/py/vertebra/extensions/litestar/channels.py`, `src/py/vertebra/extensions/starlette/channels.py`, `src/py/vertebra/extensions/flask/channels.py` | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/extensions/test_starlette_plugin.py` |
| `t03` | Introduce ValkeyStore and RedisStore and Consolidate MemoryStore, KeyValueStore & CacheStore | `src/py/vertebra/framework/stores.py`, `src/py/vertebra/extensions/litestar/store.py`, `src/py/vertebra/extensions/starlette/store.py` | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/extensions/test_starlette_plugin.py` |
| `t04` | Synchronize Public Queues, Realtime, Extensions & Databases Docs and OKF Chapters | `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, `docs/reference/database-capabilities.md` (+7 more) | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py`, `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |

## 5. Implementation Plan

- [ ] [t01-django-channels-and-channelbus-fanout.md](tasks/t01-django-channels-and-channelbus-fanout.md) — Implement distributed Django `ChannelLayer` (`extensions/django/channels.py`) and bridge `ChannelBus` / `framework/channels.py` to `SyncNotificationSource` and `AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle.
- [ ] [t02-litestar-and-starlette-channels-backends.md](tasks/t02-litestar-and-starlette-channels-backends.md) — Connect Litestar `ChannelsPlugin`, Starlette, and Flask channels to native notification sources and delete `swap_channels_backend`.
- [ ] [t03-valkey-and-redis-kv-store-consolidation.md](tasks/t03-valkey-and-redis-kv-store-consolidation.md) — Introduce `ValkeyStore` (`vertebra._valkey`) and `RedisStore` (`vertebra._redis`) and deduplicate `framework/stores.py`, `extensions/litestar/store.py`, and `extensions/starlette/store.py` on them, preserving prefix-scoped `SCAN`+`UNLINK`.
- [ ] [t04-queues-channels-docs-and-okf-sync.md](tasks/t04-queues-channels-docs-and-okf-sync.md) — Update `queues-and-realtime`, `extensions`, `databases`, and `analytics` docs plus reference pages, and sync `distributed-queues-and-email.md`, `litestar.md`, and `server-runtime-limitations.md`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-distributed-channels-stores-and-docs` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-redis-driver`, `native-valkey-driver`, and `sqlalchemy-and-django-integration`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/django/channels.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/django/channels.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync python -m tools.docs.scan_public_docstrings && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py -q
```

[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^ls-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels monkeypatch.
[^fw-stores]: `src/py/vertebra/framework/stores.py` — Framework stores.
[^ls-store]: `src/py/vertebra/extensions/litestar/store.py` — Litestar Store adapters.
[^doc-qr]: `docs/usage/queues-and-realtime.md` — Queues and realtime usage guide.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar OKF chapter.
[^okf-srv-lim]: `.agents/bundles/knowledge/domains/server/server-runtime-limitations.md` — Server runtime limitations OKF chapter.
