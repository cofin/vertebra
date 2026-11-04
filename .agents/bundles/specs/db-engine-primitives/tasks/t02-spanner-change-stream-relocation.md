---
type: Task
id: db-engine-primitives:t02
title: Relocate Spanner change stream readers to vertebra/db/adapters/spanner/change_stream.py with SyncChangeSource and AsyncChangeSource conformance
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t01
files:
  - src/py/vertebra/db/cdc.py
  - src/py/vertebra/db/channels.py
  - src/py/vertebra/db/adapters/spanner/change_stream.py
  - src/py/vertebra/db/adapters/spanner/__init__.py
tests:
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
plan_revision: 4
plan_commit: "679a981"
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
tags: [db, spanner, cdc, change-stream, async]
---

## Objective

Move `SpannerChildPartition`, `SpannerChangeStreamRecord`, `SyncSpannerChangeStreamSource`, and `AsyncSpannerChangeStreamSource` out of `src/py/vertebra/db/cdc.py` into `src/py/vertebra/db/adapters/spanner/change_stream.py`, bind stream defaults on construction so `SyncSpannerChangeStreamSource.stream()` conforms statically and at runtime to `SyncChangeSource` and `AsyncSpannerChangeStreamSource.stream()` conforms to `AsyncChangeSource`, and eliminate `inspect.isawaitable` / `asyncio.iscoroutine` duck-typing.

## Context

- **Verification strategy (`behavior_tdd`)**: Partition record parsing across GoogleSQL (`SELECT * FROM READ_<stream>(...)`) and Spangres (`SELECT * FROM spanner.read_json_<stream>(...)`), synchronous `poll_partition` + `SyncChangeSource.stream()`, and zero-argument asynchronous `AsyncChangeSource.stream()` child-partition fan-out are directly verifiable against deterministic driver doubles in `test_spanner_queue_and_cdc.py`.
- **Architectural invariants**:
  - Zero `asyncio.iscoroutine` or `inspect.isawaitable` runtime branching: `SyncSpannerChangeStreamSource` accepts a synchronous driver (`select(...) -> list[dict[str, Any]]`) and `AsyncSpannerChangeStreamSource` accepts an asynchronous driver (`async def select(...) -> list[dict[str, Any]]`).
  - `SyncChangeSource.stream(self) -> Iterator[ChangeEvent]` and `AsyncChangeSource.stream(self) -> AsyncIterator[ChangeEvent]` (`src/py/vertebra/db/driver/capabilities.py`) take zero required arguments. Therefore both `SyncSpannerChangeStreamSource.__init__` and `AsyncSpannerChangeStreamSource.__init__` accept optional `start_timestamp: str | None = None`, `end_timestamp: str | None = None`, `partition_token: str | None = None`, and `heartbeat_milliseconds: int = 10_000` (plus `poll_interval_seconds: float = 0.5` on `AsyncSpannerChangeStreamSource`), and both `stream(...)` methods default every parameter (resolving `start_timestamp or self._start_timestamp or datetime.now(UTC).isoformat()`) so Mypy structural protocol typing and `isinstance(source, SyncChangeSource)` / `isinstance(source, AsyncChangeSource)` succeed.
  - `src/py/vertebra/db/cdc.py` retains only `CDCConfig`, `ChangeEvent`, and `ChangeFeed`.

## Steps

1. **Write failing Spanner change-stream relocation and `SyncChangeSource` / `AsyncChangeSource` conformance tests (RED)**:
   - In `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`:
     - Update imports to `from vertebra.db.adapters.spanner.change_stream import AsyncSpannerChangeStreamSource, SpannerChangeStreamRecord, SpannerChildPartition, SyncSpannerChangeStreamSource` (and re-exports on `vertebra.db.adapters.spanner`).
     - Assert `hasattr(vertebra.db.cdc, "SyncSpannerChangeStreamSource") is False`, `hasattr(vertebra.db.cdc, "AsyncSpannerChangeStreamSource") is False`, `hasattr(vertebra.db.cdc, "SpannerChangeStreamRecord") is False`, and `hasattr(vertebra.db.cdc, "SpannerChildPartition") is False`.
     - Add tests assigning `sync_source: SyncChangeSource = SyncSpannerChangeStreamSource(sync_driver, "orders_stream", start_timestamp="2026-11-03T00:00:00Z")`, verifying `isinstance(sync_source, SyncChangeSource) is True`, `sync_source.poll_partition(...)` and zero-argument `list(sync_source.stream())` execute synchronously across GoogleSQL and Spangres TVF dialects, and `validate_identifier` rejects invalid SQL `stream_name` strings (`@pytest.mark.parametrize`).
     - Add async tests assigning `source: AsyncChangeSource = AsyncSpannerChangeStreamSource(async_driver, "orders_stream", start_timestamp="2026-11-03T00:00:00Z")`, asserting `isinstance(source, AsyncChangeSource)` is `True`, `await source.poll_partition(...)` awaits `async_driver.select(...)` directly, and zero-argument `async for event in source.stream():` yields `ChangeEvent` records across root and promoted child partitions cleanly.

2. **Create `adapters/spanner/change_stream.py` and clean `cdc.py` (GREEN)**:
   - Create `src/py/vertebra/db/adapters/spanner/change_stream.py` containing:
     - `SpannerChildPartition(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` with inline PEP 257 field docstrings.
     - `SpannerChangeStreamRecord(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` with inline PEP 257 field docstrings.
     - Shared `_BaseSpannerChangeStreamSource` partition-state, TVF query builder, and record parser logic validating `stream_name` via `validate_identifier`.
     - `SyncSpannerChangeStreamSource` implementing `SyncChangeSource` (synchronous `poll_partition`, `stream` with all optional parameters, `parse_raw_row`, `ingest_records`, `schedule_partition`, `complete_partition`).
     - `AsyncSpannerChangeStreamSource` implementing `AsyncChangeSource` (`async def poll_partition` awaiting `self._driver.select(...)` directly without `inspect.isawaitable` and `async def stream` with all optional parameters).
   - Re-export `AsyncSpannerChangeStreamSource`, `SpannerChangeStreamRecord`, `SpannerChildPartition`, and `SyncSpannerChangeStreamSource` in `src/py/vertebra/db/adapters/spanner/__init__.py`.
   - Remove `SpannerChildPartition`, `SpannerChangeStreamRecord`, `_BaseSpannerChangeStreamSource`, `SyncSpannerChangeStreamSource`, and `AsyncSpannerChangeStreamSource` (plus unused `inspect` / `Mapping` / `Literal` imports) from `src/py/vertebra/db/cdc.py`, and update `src/py/vertebra/db/channels.py`'s Spanner change-stream import to `vertebra.db.adapters.spanner.change_stream` until `t03` deletes `channels.py`.

3. **Refactor & verify**:
   - Run `uv run pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py` and `make lint-all`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py` -> fails (`ModuleNotFoundError: No module named 'vertebra.db.adapters.spanner.change_stream'`).
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [ ] R2: `SpannerChildPartition`, `SpannerChangeStreamRecord`, `SyncSpannerChangeStreamSource`, and `AsyncSpannerChangeStreamSource` live in `src/py/vertebra/db/adapters/spanner/change_stream.py`, `SyncSpannerChangeStreamSource` satisfies `SyncChangeSource` and `AsyncSpannerChangeStreamSource` satisfies `AsyncChangeSource` both statically (`Mypy`) and via `isinstance(...)` with zero-argument `stream()` and zero `inspect.isawaitable` branching, and `src/py/vertebra/db/cdc.py` contains zero Spanner symbols.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
