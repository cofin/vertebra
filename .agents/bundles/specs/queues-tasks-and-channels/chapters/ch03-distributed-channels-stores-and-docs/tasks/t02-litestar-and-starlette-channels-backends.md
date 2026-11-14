---
type: Task
id: queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t02
flow_id: queues-tasks-and-channels
chapter_id: ch03-distributed-channels-stores-and-docs
title: Wire Litestar ChannelsPlugin, Starlette, and Flask Channels Backends Without Monkeypatching
description: Connect Litestar ChannelsPlugin, Starlette, and Flask channels to native notification sources and delete swap_channels_backend.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch03-distributed-channels-stores-and-docs:t01
files:
- src/py/vertebra/extensions/litestar/channels.py
- src/py/vertebra/extensions/starlette/channels.py
- src/py/vertebra/extensions/flask/channels.py
tests:
- src/py/tests/unit/extensions/test_litestar_plugin.py
- src/py/tests/unit/extensions/test_starlette_plugin.py
read_set:
- src/py/vertebra/extensions/litestar/channels.py
- src/py/vertebra/extensions/starlette/channels.py
- src/py/vertebra/extensions/flask/channels.py
- src/py/vertebra/server/_channels.py
estimated_loc: 210
tags:
- channels
- litestar
- starlette
- flask
sources:
- id: ls-channels
  resource: src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend
  title: Litestar channels backend monkeypatching slated for removal
- id: st-channels
  resource: src/py/vertebra/extensions/starlette/channels.py
  title: Starlette channels backend
- id: fl-channels
  resource: src/py/vertebra/extensions/flask/channels.py
  title: Flask synchronous channels integration
---

# Task `t02`: Wire Litestar ChannelsPlugin, Starlette, and Flask Channels Backends Without Monkeypatching

## Objective & Scope Boundary

Replace `swap_channels_backend` monkeypatching in `extensions/litestar/channels.py` with a first-class Litestar `ChannelsBackend` (`ChannelsPlugin` integration) and wire Starlette and Flask channels onto `AsyncNotificationSource` / `SyncNotificationSource` per [`../spec.md`](../spec.md) [^ls-channels] [^st-channels] [^fl-channels].

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/channels.py` — `ChannelsBackend(litestar.channels.backends.base.ChannelsBackend)` and `ChannelsPlugin` builder.
- `src/py/vertebra/extensions/starlette/channels.py` & `flask/channels.py` — ASGI and synchronous WSGI channel adapters.

## Contracts & Edge Cases

- Delete `swap_channels_backend` completely; provide a clean `ChannelsBackend` + `create_channels_plugin(...)` supporting Memory, Redis, Valkey, PostgreSQL, and Oracle `AsyncNotificationSource` backends (`on_startup`, `on_shutdown`, `publish`, `subscribe`, `unsubscribe`, `stream_events`, bounded history).
- Wire `extensions/starlette/channels.py` (`AsyncNotificationSource`) and `extensions/flask/channels.py` (`SyncNotificationSource` blocking `publish` / `listen()` without an event loop).

## Test Matrix

- Parametrized Litestar `ChannelsPlugin`, Starlette `ChannelsBackend`, and Flask synchronous `SyncNotificationSource` pub/sub tests across all 5 backend kinds, plus assertion that `swap_channels_backend` is removed.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync pytest src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py
```

## Acceptance Criteria

- [ ] Litestar `ChannelsPlugin`, Starlette, and Flask channels run over `AsyncNotificationSource` / `SyncNotificationSource` and `swap_channels_backend` is deleted.
- [ ] Zero regressions in the fast verification gate.

[^ls-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels backend monkeypatching slated for removal.
[^st-channels]: `src/py/vertebra/extensions/starlette/channels.py` — Starlette channels backend.
[^fl-channels]: `src/py/vertebra/extensions/flask/channels.py` — Flask synchronous channels integration.
