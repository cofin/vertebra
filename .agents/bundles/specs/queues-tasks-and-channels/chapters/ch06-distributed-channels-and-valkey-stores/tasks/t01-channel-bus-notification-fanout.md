---
type: Task
id: "queues-tasks-and-channels:ch06-distributed-channels-and-valkey-stores:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch06-distributed-channels-and-valkey-stores
title: "Wire SyncNotificationSource / AsyncNotificationSource into ChannelBus & Framework ChannelsBackend"
description: "Bridge server/_channels.py::ChannelBus and framework/channels.py across Memory, Redis, Valkey, PostgreSQL, and Oracle notification sources."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-queue-store-capabilities-and-native-stores:t01"
files:
  - src/py/vertebra/server/_channels.py
  - src/py/vertebra/framework/channels.py
tests:
  - src/py/tests/unit/server/test_realtime_backpressure.py
read_set:
  - src/py/vertebra/server/_channels.py
  - src/py/vertebra/framework/channels.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 215
tags:
  - channels
  - notifications
  - pubsub
sources:
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: "In-process/SHM ChannelBus"
  - id: fw-channels
    resource: src/py/vertebra/framework/channels.py
    title: "First-party Vertebra ChannelsBackend"
---

# Task `t01`: Wire SyncNotificationSource / AsyncNotificationSource into ChannelBus & Framework ChannelsBackend

## Objective & Scope Boundary

Wire `SyncNotificationSource` and `AsyncNotificationSource` (from upstream `native-redis-driver:ch05`) into `ChannelBus` (`server/_channels.py`) and `ChannelsBackend` (`framework/channels.py`) across Memory, Redis, Valkey, PostgreSQL, and Oracle per [`../spec.md`](../spec.md) [^channel-bus] [^fw-channels].

## Bounded Read Manifest

- `src/py/vertebra/server/_channels.py` — `ChannelBus` local/SHM subscriber fan-out and backpressure.
- `src/py/vertebra/framework/channels.py` — `ChannelsBackend` configuration and backend factory.
- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource` and `AsyncNotificationSource`.

## Contracts & Edge Cases

- Attach optional `AsyncNotificationSource` (and `SyncNotificationSource` for synchronous publishers/CLI listeners) to `ChannelBus` and `ChannelsBackend`, supporting `memory`, `redis`, `valkey`, `postgres`, and `oracle` backends.
- Stamp cross-node envelopes with `origin_node_id` so local subscribers receive each message exactly once while remote nodes fan incoming frames into their local subscriber rings with bounded backpressure and clean `aclose()` / `close()` teardown.

## Test Matrix

- Parametrized multi-node `ChannelBus` fan-out, origin-node deduplication, bounded queue backpressure, and sync/async `NotificationSource` tests across Memory, Redis, Valkey, PostgreSQL, and Oracle sources.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync mypy src/py/vertebra/server/_channels.py src/py/vertebra/framework/channels.py src/py/vertebra/framework/stores.py src/py/vertebra/extensions/litestar src/py/vertebra/extensions/starlette src/py/vertebra/extensions/flask/channels.py && uv run --no-sync pytest src/py/tests/unit/server/test_realtime_backpressure.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py
```

## Acceptance Criteria

- [ ] `ChannelBus` and `framework/channels.py` fan out cleanly across Memory, Redis, Valkey, PostgreSQL, and Oracle `SyncNotificationSource` / `AsyncNotificationSource` backends.
- [ ] Zero regressions in the chapter fast verification gate.

[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — In-process/SHM ChannelBus.
[^fw-channels]: `src/py/vertebra/framework/channels.py` — First-party Vertebra ChannelsBackend.
