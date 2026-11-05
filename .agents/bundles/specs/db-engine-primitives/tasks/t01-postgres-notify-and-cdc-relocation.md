---
type: Task
id: db-engine-primitives:t01
title: Extract PostgreSQL logical message and NOTIFY primitives to vertebra/db/adapters/postgres/notify.py and bound ChangeFeed
description: "Keep `src/py/vertebra/db/cdc.py` engine-neutral (`CDCConfig`, `ChangeEvent`, `ChangeFeed`) with preserved default values, normalized `spill_prefix` object keys, bounded in-memory spill LRU eviction, and fail-loud `SyncObjectStore.get()` hydration, remove `build_emit_message_sql` from `ChangeFeed` and `extensions/sqlspec/events.py`, and extract PostgreSQL logical replication / `NOTIFY` primitives into `src/py/vertebra/db/adapters/postgres/notify.py` (`PostgresNotifyConfig`, `PostgresNotificationSource(NotificationSource)`)."
state: "completed"
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
  - src/py/vertebra/db/cdc.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/extensions/sqlspec/events.py
  - docs/usage/analytics-and-cdc.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_pg_gateway.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 8
plan_commit: "b41aae46213761f36af0709c0a3ce6d26b2793e0"
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-05T01:51:01Z"
last_verified_commit: "6c07c8fe1d072d8096dea60c8df87f3b8eb1860b"
verification_evidence: "Behavior TDD RED observed; canonical t01 files: 93 passed; Ruff/format/Mypy and make lint-all exit 0; contemporaneous CDC knowledge validated."
created_at: 2026-11-03T21:47:00Z
updated_at: "2026-11-05T02:59:14Z"
commit: "6c07c8fe1d072d8096dea60c8df87f3b8eb1860b"
tags: [db, cdc, postgres, notify, spill]
---

## Objective

Keep `src/py/vertebra/db/cdc.py` engine-neutral (`CDCConfig`, `ChangeEvent`, `ChangeFeed`) with preserved default values, normalized `spill_prefix` object keys, bounded in-memory spill LRU eviction, and fail-loud `SyncObjectStore.get()` hydration, remove `build_emit_message_sql` from `ChangeFeed` and `extensions/sqlspec/events.py`, and extract PostgreSQL logical replication / `NOTIFY` primitives into `src/py/vertebra/db/adapters/postgres/notify.py` (`PostgresNotifyConfig`, `PostgresNotificationSource(NotificationSource)`).

## Context

- **Verification strategy (`behavior_tdd`)**: `CDCConfig` field separation and default preservation, `OrderedDict` LRU eviction on `_spill_store`, exception propagation when `SyncObjectStore.get` fails, `ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend` event lifecycle, and `PostgresNotificationSource` SQL generation and `NotificationSource` conformance are directly testable at public Python APIs.
- **Architectural invariants**:
  - `CDCConfig` (`channel: str = "vertebra_events"`, `spill_threshold_bytes: int = 7500`, `spill_prefix: str = "cdc/spill"`, `max_history_size: int = 1024`, `max_memory_spills: int = 1024`) and `PostgresNotifyConfig` (`channel: str = "vertebra_events"`, `spill_threshold_bytes: int = 7500`, `spill_table: str = "vertebra_notify_spill"`, `spill_prefix: str = "cdc/spill"`, `replication_slot_name: str = "vertebra_cdc_slot"`, `publication_name: str = "vertebra_pub"`, `max_slot_wal_keep_size_mb: int = 4096`) inherit `FrozenBaseStruct` (`frozen=True, gc=False, kw_only=True`) with inline PEP 257 field docstrings (never `Attributes:` sections in class docstrings) and `validate_identifier` checks on SQL identifiers.
  - Normalize `prefix = self._config.spill_prefix.rstrip("/")` when constructing `SyncObjectStore` spill keys (`f"{prefix}/{event_id}.json"` if `prefix` else `f"{event_id}.json"`) so trailing slashes never produce `"//"`.
  - Never swallow `SyncObjectStore.get` errors in `ChangeFeed.hydrate_notification`.
  - Use `@pytest.mark.parametrize` for identifier validation and spill boundary test cases (zero `for` loops over test inputs).

## Steps

1. **Write failing CDC engine-separation, bounded-spill, error-propagation, event-backend, and `PostgresNotificationSource` tests (RED)**:
   - In `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` and `src/py/tests/unit/db/test_pg_gateway.py`:
     - Assert `CDCConfig` exposes only `channel` (`"vertebra_events"`), `spill_threshold_bytes` (`7500`), `spill_prefix` (`"cdc/spill"`), `max_history_size` (`1024`), and `max_memory_spills` (`1024`), rejecting legacy `replication_slot_name`, `publication_name`, `max_slot_wal_keep_size_mb`, and `spill_table` keyword arguments with `TypeError`, and normalizing trailing slashes on `spill_prefix="cdc/spill/"` to `"cdc/spill/1.json"`.
     - Assert `ChangeFeed` has no `build_emit_message_sql` attribute.
     - Assert `ChangeFeed(CDCConfig(spill_threshold_bytes=16, max_memory_spills=2))` evicts spill ID `1` after publishing 3 oversized payloads without a `SyncObjectStore`, and `hydrate_notification` on the evicted envelope raises `KeyError` while spill IDs `2` and `3` hydrate intact.
     - Assert `ChangeFeed(store=failing_store).hydrate_notification(spilled_envelope)` propagates the `RuntimeError` raised by `store.get(key)` instead of returning `ChangeEvent(..., payload=None)`.
     - Assert `PostgresNotifyConfig` validates `channel`, `spill_table`, `replication_slot_name`, and `publication_name` via `validate_identifier` (`@pytest.mark.parametrize` over invalid SQL identifiers raising `ValueError`), `PostgresNotificationSource.build_emit_message_sql` returns `("SELECT pg_logical_emit_message($1, $2, $3)", (transactional, prefix, payload_bytes))`, `PostgresNotificationSource.build_notify_sql` returns `("SELECT pg_notify($1, $2)", (channel, payload))`, `isinstance(PostgresNotificationSource(driver), NotificationSource)` is `True`, `await source.publish(channel, payload)` executes parameterized `SELECT pg_notify($1, $2)` (wrapping through `change_feed.publish_event` when `change_feed` is provided), `subscribe`/`unsubscribe` validate channel identifiers, and iterating `source.listen()` delegates to `driver.listen_notifications(...)` when present and raises `NotSupportedError` otherwise.
   - In `src/py/tests/unit/extensions/test_data_storage_integrations.py`:
     - Add unit tests for `ChangeFeedSyncEventBackend` and `ChangeFeedAsyncEventBackend` covering `publish`, `publish_many`, `dequeue` (including `SyncObjectStore` spill hydration), `ack`, `nack` redelivery, `shutdown`, and `hasattr(backend, "build_emit_message_sql") is False`.

2. **Refactor `cdc.py`, `extensions/sqlspec/events.py`, and implement `adapters/postgres/notify.py` (GREEN)**:
   - In `src/py/vertebra/db/cdc.py`:
     - Inherit `ChangeEvent` and `CDCConfig` from `FrozenBaseStruct` (`frozen=True, gc=False, kw_only=True`) with inline PEP 257 field docstrings; remove `replication_slot_name`, `publication_name`, `max_slot_wal_keep_size_mb`, and `spill_table` from `CDCConfig`; add `max_memory_spills: int = 1024` (validating `max_memory_spills >= 1` in `__post_init__`).
     - Normalize `prefix = self._config.spill_prefix.rstrip("/")` when building `spill_key` in `publish_event` and `hydrate_notification`.
     - Replace `self._spill_store: dict[int, dict[str, Any]] = {}` in `ChangeFeed` with `collections.OrderedDict[int, dict[str, Any]]()`, evicting `self._spill_store.popitem(last=False)` whenever `len(self._spill_store) > self._config.max_memory_spills`.
     - In `ChangeFeed.hydrate_notification`, let `self._store.get(spill_key)` propagate exceptions unmasked, and raise `KeyError(f"Evicted or unknown in-memory CDC spill id: {event_id}")` when `event_id` is absent from `self._spill_store`.
     - Delete `ChangeFeed.build_emit_message_sql`. (Leave `SpannerChildPartition`, `SpannerChangeStreamRecord`, `SyncSpannerChangeStreamSource`, and `AsyncSpannerChangeStreamSource` in place solely until `t02` moves them.)
   - In `src/py/vertebra/extensions/sqlspec/events.py`, remove `build_emit_message_sql` from `ChangeFeedSyncEventBackend` and `ChangeFeedAsyncEventBackend`.
   - Create `src/py/vertebra/db/adapters/postgres/notify.py` defining `PostgresNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` and `PostgresNotificationSource`:
     - `__init__(self, driver: AsyncPostgreSQLDriver | SyncPostgreSQLDriver, config: PostgresNotifyConfig | None = None, *, change_feed: ChangeFeed | None = None) -> None` and `@property def change_feed(self) -> ChangeFeed | None` returning `self._change_feed`.
     - `publish(self, channel: str, payload: bytes | str) -> None` validating `channel` via `validate_identifier`, formatting the wire payload (via the canonical `ChangeFeed` envelope `self._change_feed.publish_event("PUBLISH", {"channel": channel, "data_b64": base64.b64encode(raw_bytes).decode("ascii")})[0]` where `raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)` when `self._change_feed is not None`, or passing `payload` directly for `str` and `"b64:" + base64.b64encode(bytes(payload)).decode("ascii")` for `bytes` when `self._change_feed is None`), and executing `build_notify_sql` via `await self._driver.execute(...)` when `isinstance(self._driver, AsyncDatabaseDriver)` else `await asyncio.to_thread(self._driver.execute, ...)`.
     - `subscribe(self, channels: Sequence[str]) -> None` and `unsubscribe(self, channels: Sequence[str]) -> None` validating channel identifiers and updating `self._subscribed_channels`.
     - `listen(self) -> AsyncIterator[NotificationEvent]` delegating to `self._driver.listen_notifications(tuple(self._subscribed_channels))` when exposed and raising `NotSupportedError("Live PostgreSQL LISTEN streaming requires the native Rust PostgreSQL driver")` otherwise.
     - Re-export `PostgresNotifyConfig` and `PostgresNotificationSource` via `lazy_exports` / `__all__` in `src/py/vertebra/db/adapters/postgres/__init__.py`.
   - Update the `CDCConfig` / PostgreSQL notification example in `docs/usage/analytics-and-cdc.md` so `CDCConfig` uses only engine-neutral fields and PostgreSQL replication/notify fields use `PostgresNotifyConfig`.
   - Update `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md::Change Feed` in the same task: document the five engine-neutral `CDCConfig` defaults, bounded `OrderedDict` spill eviction, normalized object keys, propagated object-store errors, and `KeyError` for evicted or unknown memory spills; replace the `ChangeFeed.build_emit_message_sql` row with adapter-owned `PostgresNotifyConfig` and `PostgresNotificationSource` rows; add the `adapters/postgres/notify.py` source resource and provenance footnote; and remove the claim that spill storage and history grow without bound. Keep the existing Spanner location accurate until `t02` moves the readers.

3. **Refactor & verify**:
   - Run `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/docs/test_examples.py` and `make lint-all`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -k notify` -> fails (`ModuleNotFoundError: No module named 'vertebra.db.adapters.postgres.notify'`).
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_data_storage_integrations.py` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [x] R1: `CDCConfig` and `ChangeFeed` preserve existing defaults while containing zero PostgreSQL-specific fields or SQL strings, normalize `spill_prefix` keys, enforce `max_memory_spills` LRU eviction, and propagate `SyncObjectStore.get` errors.
- [x] R1: `ChangeFeedSyncEventBackend` and `ChangeFeedAsyncEventBackend` omit `build_emit_message_sql` and pass unit tests for `publish`, `publish_many`, `dequeue` (with `SyncObjectStore` spill hydration), `ack`, `nack`, and `shutdown`.
- [x] R1: `PostgresNotifyConfig` and `PostgresNotificationSource` live in `src/py/vertebra/db/adapters/postgres/notify.py`, satisfy `isinstance(..., NotificationSource)`, and validate SQL identifiers.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->

- [2026-11-05T01:49:53Z] Behavior-TDD RED: missing `postgres.notify` import and failing CDC bounds/defaults/key/error/emitter behaviors before implementation. GREEN: four declared canonical test modules `93 passed`; focused Ruff/format/Mypy and `make lint-all` exit `0`. Spanner implementation remains unchanged for t02. Revised-surface plan review approves revision 5, including contemporaneous knowledge and precise future relocation/gate targets.
