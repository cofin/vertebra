---
type: Task
id: queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t01
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: Wire Django ChannelLayer and Bridge ChannelBus Notification Fanout Across 5 Backends
description: Implement distributed Django ChannelLayer and bridge ChannelBus / framework/channels.py to SyncNotificationSource and AsyncNotificationSource across Memory, Redis, Valkey, PostgreSQL, and Oracle.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t04
files:
  - src/py/vertebra/extensions/django/channels.py
  - src/py/vertebra/server/_channels.py
  - src/py/vertebra/framework/channels.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/server/test_realtime_backpressure.py
read_set:
  - src/py/vertebra/extensions/django/channels.py
  - src/py/vertebra/server/_channels.py
  - src/py/vertebra/framework/channels.py
  - src/py/vertebra/db/driver/capabilities.py
estimated_loc: 200
tags:
  - django
  - channels
  - valkey
  - redis
  - notifications
  - pubsub
sources:
  - id: dj-channels
    resource: src/py/vertebra/extensions/django/channels.py
    title: Django channel layer
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: In-process/SHM ChannelBus
  - id: fw-channels
    resource: src/py/vertebra/framework/channels.py
    title: ChannelBus re-export receiving the new first-party ChannelsBackend
verification_strategy: regression_tdd
---

# Task `t01`: Wire Django ChannelLayer and Bridge ChannelBus Notification Fanout Across 5 Backends

## Objective & Scope Boundary

Upgrade `src/py/vertebra/extensions/django/channels.py::ChannelLayer` onto `ChannelBus` and `SyncNotificationSource`/`AsyncNotificationSource`, and wire `SyncNotificationSource` and `AsyncNotificationSource` (from `native-redis-driver` and `native-valkey-driver`) into `ChannelBus` (`server/_channels.py`) and `ChannelsBackend` (`framework/channels.py`) across Memory, Redis, Valkey, PostgreSQL, and Oracle per [`../spec.md`](../spec.md) [^dj-channels] [^channel-bus] [^fw-channels]. (Django `CacheBackend(BaseCache)` in `extensions/django/cache.py` is owned by `sqlalchemy-and-django-integration:ch03:t03`.)

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/channels.py` — existing in-memory `ChannelLayer` stub.
- `src/py/vertebra/server/_channels.py` — `ChannelBus` local/SHM subscriber fan-out and backpressure.
- `src/py/vertebra/framework/channels.py` — `ChannelBus` re-export; receives the new `ChannelsBackend` configuration and backend factory.
- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource` and `AsyncNotificationSource` protocols.

## Contracts & Edge Cases

- `ChannelLayer` routes `send`, `receive`, `new_channel`, `group_add`, `group_discard`, and `group_send` over `ChannelBus` + `AsyncNotificationSource` (plus sync `publish_sync` via `SyncNotificationSource`).
- Attach optional `AsyncNotificationSource` (and `SyncNotificationSource` for synchronous publishers/CLI listeners) to `ChannelBus` and `ChannelsBackend` across `memory`, `redis` (`vertebra._redis`), `valkey` (`vertebra._valkey`), `postgres` (`vertebra._postgres`), and `oracle` (`vertebra._oracle`), stamping cross-node envelopes with `origin_node_id` so local subscribers receive each message once while remote nodes fan incoming frames cleanly with bounded backpressure and `aclose()` / `close()` teardown.

## Test Matrix

- Parametrized Django `ChannelLayer` group fan-out tests alongside parametrized multi-node `ChannelBus` fan-out, origin-node deduplication, bounded queue backpressure, and `SyncNotificationSource`/`AsyncNotificationSource` tests across Memory, Redis, Valkey, PostgreSQL, and Oracle sources.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django/channels.py src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py && uv run --no-sync mypy src/py/vertebra/extensions/django/channels.py src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/server/test_realtime_backpressure.py -q
```

## Acceptance Criteria

- [ ] `extensions/django/channels.py` executes over `ChannelBus` and native notification sources without dict fallbacks.
- [ ] `ChannelBus` and `framework/channels.py` fan out cleanly across Memory, Redis, Valkey, PostgreSQL, and Oracle `SyncNotificationSource` / `AsyncNotificationSource` backends.
- [ ] Zero regressions in the fast verification gate.

[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^fw-channels]: `src/py/vertebra/framework/channels.py` — ChannelBus re-export receiving the new first-party ChannelsBackend.
