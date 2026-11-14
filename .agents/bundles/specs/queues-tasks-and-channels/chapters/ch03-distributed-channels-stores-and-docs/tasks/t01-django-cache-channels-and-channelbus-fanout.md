---
type: Task
id: queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t01
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: Wire Django Native Cache/Channels and Bridge ChannelBus Notification Fanout Across 5 Backends
description: Implement native Valkey/Redis Django CacheBackend + ChannelLayer and bridge ChannelBus / framework/channels.py to SyncNotificationSource and AsyncNotificationSource.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t04
files:
- src/py/vertebra/extensions/django/cache.py
- src/py/vertebra/extensions/django/channels.py
- src/py/vertebra/server/_channels.py
- src/py/vertebra/framework/channels.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/server/test_realtime_backpressure.py
read_set:
- src/py/vertebra/extensions/django/cache.py
- src/py/vertebra/extensions/django/channels.py
- src/py/vertebra/db/adapters/valkey/driver.py
- src/py/vertebra/server/_channels.py
- src/py/vertebra/framework/channels.py
- src/py/vertebra/db/driver/capabilities.py
estimated_loc: 220
tags:
- django
- cache
- channels
- valkey
- notifications
- pubsub
sources:
- id: dj-cache
  resource: src/py/vertebra/extensions/django/cache.py
  title: Django cache backend
- id: dj-channels
  resource: src/py/vertebra/extensions/django/channels.py
  title: Django channel layer
- id: channel-bus
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: In-process/SHM ChannelBus
- id: fw-channels
  resource: src/py/vertebra/framework/channels.py
  title: First-party Vertebra ChannelsBackend
---

# Task `t01`: Wire Django Native Cache/Channels and Bridge ChannelBus Notification Fanout Across 5 Backends

## Objective & Scope Boundary

Upgrade `extensions/django/cache.py` and `extensions/django/channels.py` onto native `SyncValkeyDriver`/`AsyncValkeyDriver` (`vertebra._redis`) and `SyncNotificationSource`/`AsyncNotificationSource` per [`../spec.md`](../spec.md) [^dj-cache] [^dj-channels].

Wire `SyncNotificationSource` and `AsyncNotificationSource` (from upstream `native-redis-driver:ch03`) into `ChannelBus` (`server/_channels.py`) and `ChannelsBackend` (`framework/channels.py`) across Memory, Redis, Valkey, PostgreSQL, and Oracle per [`../spec.md`](../spec.md) [^channel-bus] [^fw-channels].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/cache.py` & `channels.py` — existing stubs.
- `src/py/vertebra/db/adapters/valkey/driver.py` & `server/_channels.py` — native Valkey/Redis drivers and `ChannelBus`.

- `src/py/vertebra/server/_channels.py` — `ChannelBus` local/SHM subscriber fan-out and backpressure.
- `src/py/vertebra/framework/channels.py` — `ChannelsBackend` configuration and backend factory.
- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource` and `AsyncNotificationSource`.

## Contracts & Edge Cases

- `CacheBackend(BaseCache)` executes `add`, `get`, `set`, `touch`, `delete`, `get_many`, `set_many`, `delete_many`, `incr`, `decr`, `clear` (prefix-scoped `SCAN`+`UNLINK`), plus `aget`/`aset`/`adelete` over `SyncValkeyDriver`/`AsyncValkeyDriver` or `SyncRedisDriver`/`AsyncRedisDriver` (or explicit SHM cache), with zero silent dict fallback.
- `ChannelLayer` routes `send`, `receive`, `new_channel`, `group_add`, `group_discard`, and `group_send` over `ChannelBus` + `AsyncNotificationSource` (plus sync `publish_sync` via `SyncNotificationSource`).

- Attach optional `AsyncNotificationSource` (and `SyncNotificationSource` for synchronous publishers/CLI listeners) to `ChannelBus` and `ChannelsBackend`, supporting `memory`, `redis`, `valkey`, `postgres`, and `oracle` backends.
- Stamp cross-node envelopes with `origin_node_id` so local subscribers receive each message exactly once while remote nodes fan incoming frames into their local subscriber rings with bounded backpressure and clean `aclose()` / `close()` teardown.

## Test Matrix

- Parametrized sync/async Django `CacheBackend` TTL/atomic `incr`/prefix `clear` tests and `ChannelLayer` group fan-out tests.

- Parametrized multi-node `ChannelBus` fan-out, origin-node deduplication, bounded queue backpressure, and sync/async `NotificationSource` tests across Memory, Redis, Valkey, PostgreSQL, and Oracle sources.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync pytest src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py
```

## Acceptance Criteria

- [ ] `extensions/django/cache.py` and `extensions/django/channels.py` execute over native drivers and notification sources without dict fallbacks.
- [ ] `ChannelBus` and `framework/channels.py` fan out cleanly across Memory, Redis, Valkey, PostgreSQL, and Oracle `SyncNotificationSource` / `AsyncNotificationSource` backends.
- [ ] Zero regressions in the fast verification gate.

[^dj-cache]: `src/py/vertebra/extensions/django/cache.py` — Django cache backend.
[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^fw-channels]: `src/py/vertebra/framework/channels.py` — First-party Vertebra ChannelsBackend.
