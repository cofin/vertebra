---
type: Task
id: db-engine-primitives:t03
title: Delete vertebra/db/channels.py and move engine-neutral pub/sub fan-out into vertebra/server/_channels.py::ChannelBus
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t02
files:
  - src/py/vertebra/db/channels.py
  - src/py/vertebra/server/_channels.py
  - src/py/vertebra/framework/channels.py
  - src/py/vertebra/extensions/litestar/channels.py
  - src/py/vertebra/extensions/starlette/channels.py
  - docs/usage/extensions.md
  - .agents/bundles/knowledge/domains/extensions/litestar.md
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - src/py/tests/unit/extensions/test_starlette_plugin.py
  - src/py/tests/unit/extensions/test_fastapi_plugin.py
  - src/py/tests/unit/extensions/test_flask_plugin.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 4
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:47:00Z
updated_at: 2026-11-04T01:12:00Z
commit: null
tags: [db, channels, server, framework, litestar, starlette, clean-break]
---

## Objective

Delete `src/py/vertebra/db/channels.py` (`DatabaseChannelBackend`), implement the engine-neutral `ChannelBus` pub/sub substrate in `src/py/vertebra/server/_channels.py` (re-exported in `src/py/vertebra/framework/channels.py`), and subclass `ChannelBus` in `src/py/vertebra/extensions/{litestar,starlette}/channels.py` with zero `vertebra.framework.*` or `vertebra.db.adapters.*` imports.

## Context

- **Verification strategy (`behavior_tdd`)**: `ChannelBus` publish/subscribe/history/stream lifecycle, `NotificationSource` + `AsyncChangeSource` + `ChangeFeed` fan-in, Litestar/Starlette/FastAPI/Flask `ChannelsBackend` compatibility (`on_startup`, `on_shutdown`, `stream_events`, `swap_channels_backend`, `attach_channels`), and complete removal of `vertebra.db.channels` are directly observable in unit tests.
- **Architectural invariants**:
  - `.agents/bundles/knowledge/domains/extensions/litestar.md` forbids any `vertebra.extensions.*` module from importing `vertebra.framework.*`, sharing runtime substrates via `vertebra.server._*` (`_routing.py`, `_fastpath.py`). Placing `ChannelBus` in `src/py/vertebra/server/_channels.py` and re-exporting it in `src/py/vertebra/framework/channels.py` satisfies both the extension isolation boundary and the `vertebra/db` downward dependency rule (`server -> db` protocols only).
  - `ChannelBus` imports only engine-agnostic capability protocols (`NotificationSource`, `AsyncChangeSource`) and `ChangeFeed` — zero `vertebra.db.adapters.*` imports, zero `pg_notify` SQL, and zero `DBMS_AQ.ENQUEUE` PL/SQL.
  - Expose protected `_register_stream_queue(self) -> asyncio.Queue[tuple[str, bytes] | None]` and `_unregister_stream_queue(self, queue: asyncio.Queue[tuple[str, bytes] | None]) -> None` hooks on `ChannelBus` so `vertebra.extensions.litestar.channels.ChannelsBackend.stream_events` shields queue cleanup with `with anyio.CancelScope(shield=True): self._unregister_stream_queue(queue)` without reaching into private slot attributes.

## Steps

1. **Write failing `ChannelBus` decoupling and `vertebra.db.channels` removal tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py` and `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`:
     - Assert `importlib.import_module("vertebra.db.channels")` raises `ModuleNotFoundError` and `hasattr(vertebra.db, "DatabaseChannelBackend") is False`.
     - Replace `DatabaseChannelBackend` tests with `ChannelBus` tests (`from vertebra.framework.channels import ChannelBus` and `from vertebra.server._channels import ChannelBus`, asserting identity `is`) verifying `on_startup`/`on_shutdown`, bounded `history` replay (`get_history`), multi-subscriber fan-out (`publish`, `subscribe`, `unsubscribe`, `stream_events`), `NotificationSource` forwarding (`await source.publish(channel, payload)` and `pump_notifications`), `AsyncChangeSource` pump (`pump_change_source`), `ChangeFeed` oversized payload hydration (`"PUBLISH"` `data_b64` envelope), and single-delivery deduplication when `ChannelBus` and `PostgresNotificationSource` share the same `ChangeFeed` instance.
   - In `src/py/tests/unit/extensions/test_litestar_plugin.py` and `src/py/tests/unit/extensions/test_starlette_plugin.py`:
     - Assert `issubclass(ChannelsBackend, ChannelBus)` is `True`, `swap_channels_backend` and `attach_channels` accept `ChannelsBackend | ChannelBus | None`, and neither `litestar/channels.py` nor `starlette/channels.py` imports `vertebra.framework` or `vertebra.db.channels`.

2. **Implement `vertebra/server/_channels.py::ChannelBus`, update channel extensions, and delete `db/channels.py` (GREEN)**:
   - Create `src/py/vertebra/server/_channels.py` defining `ChannelBus` with `__slots__` (`_change_feed`, `_change_source`, `_history`, `_max_history`, `_notification_source`, `_queues`, `_reader_task`, `_running`, `_subscribed_channels`, `_subscribers`) and methods `__init__`, `backend`, `channels`, `notification_source`, `change_source`, `change_feed`, `on_startup`, `on_shutdown`, `publish`, `subscribe`, `unsubscribe`, `stream_events`, `get_history`, `pump_notifications`, `pump_change_source`, `_register_stream_queue`, and `_unregister_stream_queue`. Decode `"PUBLISH"` `ChangeEvent` envelopes (`base64.b64decode(payload["data_b64"])`) and `"b64:"`-prefixed `NotificationEvent` payloads (`pump_notifications` hydrating JSON `ChangeFeed` envelopes via `self._change_feed.hydrate_notification` when `self._change_feed is not None`). When `change_feed` is provided and exposes `add_listener`, attach local fan-in automatically, and in `ChannelBus.publish` skip separate local `_fan_in_local` dispatch whenever `self._notification_source is not None` and `getattr(self._notification_source, "change_feed", None) is self._change_feed` so shared-`ChangeFeed` publishes deliver once rather than twice.
   - Update `src/py/vertebra/framework/channels.py` to re-export `ChannelBus` from `vertebra.server._channels` (`__all__ = ("ChannelBus",)`).
   - Update `src/py/vertebra/extensions/litestar/channels.py` (`ChannelsBackend(ChannelBus)`, `swap_channels_backend(app, backend: ChannelsBackend | ChannelBus | None = None)`, `_load_channels_types` registering `ChannelBus` and `ChannelsBackend`) and `src/py/vertebra/extensions/starlette/channels.py` (`ChannelsBackend(ChannelBus)`, `attach_channels(app, backend: ChannelsBackend | ChannelBus | None = None)`).
   - Delete `src/py/vertebra/db/channels.py` (`git rm`).
   - Update `docs/usage/extensions.md` (replacing `DatabaseChannelBackend` with `ChannelBus` / `ChannelsBackend`) and `.agents/bundles/knowledge/domains/extensions/litestar.md` (updating `sources:` and prose from `vertebra.db.channels` to `vertebra.server._channels.ChannelBus`).

3. **Refactor & verify**:
   - Run `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/docs/test_examples.py` and `make lint-all`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py -k channel` -> fails while `vertebra.db.channels` still exists.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [ ] R3: `src/py/vertebra/db/channels.py` is deleted (`import vertebra.db.channels` raises `ModuleNotFoundError`), `ChannelBus` lives in `src/py/vertebra/server/_channels.py` (re-exported in `vertebra.framework.channels`), `vertebra.extensions.{litestar,starlette,fastapi,flask}.channels` pass all unit tests with zero `vertebra.framework.*` or `vertebra.db.adapters.*` imports, and `make lint-all` (`make review-check`) exits `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
