---
type: Task
id: "queues-tasks-and-channels:ch05-django-tasks-and-cache-integration:t03"
flow_id: queues-tasks-and-channels
chapter_id: ch05-django-tasks-and-cache-integration
title: "Upgrade Django CacheBackend and ChannelLayer onto vertebra._redis & NotificationSources"
description: "Replace in-process dict stubs in extensions/django/cache.py and channels.py with native Valkey/Redis drivers and NotificationSources."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/extensions/django/cache.py
  - src/py/vertebra/extensions/django/channels.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/cache.py
  - src/py/vertebra/extensions/django/channels.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/server/_channels.py
estimated_loc: 220
tags:
  - django
  - cache
  - channels
  - valkey
sources:
  - id: dj-cache
    resource: src/py/vertebra/extensions/django/cache.py
    title: "Django cache backend"
  - id: dj-channels
    resource: src/py/vertebra/extensions/django/channels.py
    title: "Django channel layer"
---

# Task `t03`: Upgrade Django CacheBackend and ChannelLayer onto vertebra._redis & NotificationSources

## Objective & Scope Boundary

Upgrade `extensions/django/cache.py` and `extensions/django/channels.py` onto native `SyncValkeyDriver`/`AsyncValkeyDriver` (`vertebra._redis`) and `SyncNotificationSource`/`AsyncNotificationSource` per [`../spec.md`](../spec.md) [^dj-cache] [^dj-channels].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/cache.py` & `channels.py` — existing stubs.
- `src/py/vertebra/db/adapters/valkey/driver.py` & `server/_channels.py` — native Valkey/Redis drivers and `ChannelBus`.

## Contracts & Edge Cases

- `CacheBackend(BaseCache)` executes `add`, `get`, `set`, `touch`, `delete`, `get_many`, `set_many`, `delete_many`, `incr`, `decr`, `clear` (prefix-scoped `SCAN`+`UNLINK`), plus `aget`/`aset`/`adelete` over `SyncValkeyDriver`/`AsyncValkeyDriver` or `SyncRedisDriver`/`AsyncRedisDriver` (or explicit SHM cache), with zero silent dict fallback.
- `ChannelLayer` routes `send`, `receive`, `new_channel`, `group_add`, `group_discard`, and `group_send` over `ChannelBus` + `AsyncNotificationSource` (plus sync `publish_sync` via `SyncNotificationSource`).

## Test Matrix

- Parametrized sync/async Django `CacheBackend` TTL/atomic `incr`/prefix `clear` tests and `ChannelLayer` group fan-out tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `extensions/django/cache.py` and `extensions/django/channels.py` execute over native drivers and notification sources without dict fallbacks.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-cache]: `src/py/vertebra/extensions/django/cache.py` — Django cache backend.
[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
