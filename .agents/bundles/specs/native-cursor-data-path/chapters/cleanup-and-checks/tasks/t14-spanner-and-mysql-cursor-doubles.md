---
type: Task
id: "native-cursor-data-path:cleanup:t14"
chapter_id: cleanup-and-checks
title: "Spanner channel and MySQL dialect doubles gain the cursor surface"
description: "Give the Spanner client double in the channel bus test and the two MySQL protocol driver doubles the cursor factory and cursor members the drivers call, so seven failing tests pass with no production change."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - "src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py::test_channel_bus_spanner_source_delivers_once_per_output_channel"
  - "src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py:1-20"
  - "src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerCursorProtocol"
  - "src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerClientProtocol"
  - "src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerDriver.cursor"
  - "src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerDriver.execute_with_description"
  - "src/py/vertebra/db/adapters/spanner/change_stream.py::AsyncSpannerChangeStreamSource.poll_partition"
  - "src/py/tests/unit/db/test_mysql_driver.py::_FakeMySQLSyncProtocolDriver"
  - "src/py/tests/unit/db/test_mysql_driver.py::_FakeMySQLAsyncProtocolDriver"
  - "src/py/tests/unit/db/test_mysql_driver.py::test_mysql_sync_and_async_engine_initialize_and_isolation"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::_FakeMSSQLProtocolCursor"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::_FakeMSSQLAsyncProtocolCursor"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection.cursor"
files: []
tests:
  - src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py
  - src/py/tests/unit/db/test_mysql_driver.py
estimated_loc: 130
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: spanner-protocol
    resource: src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerClientProtocol
    title: "Client protocol the Spanner driver calls, including cursor()"
  - id: channel-test
    resource: src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py::test_channel_bus_spanner_source_delivers_once_per_output_channel
    title: "Channel bus test with a Spanner client double"
  - id: mysql-double
    resource: src/py/tests/unit/db/test_mysql_driver.py::_FakeMySQLSyncProtocolDriver
    title: "MySQL protocol driver double used under the SQLAlchemy dialect"
  - id: mssql-cursor-double
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::_FakeMSSQLProtocolCursor
    title: "Existing cursor double to model the MySQL one on"
tags: [testing, database, spanner, mysql]
---

# t14 — Spanner channel and MySQL dialect doubles gain the cursor surface

## Objective & Scope Boundary

Seven tests pass again: the six cases of `test_channel_bus_spanner_source_delivers_once_per_output_channel` and `test_mysql_sync_and_async_engine_initialize_and_isolation`.[^channel-test][^mysql-double]

Both failures come from stale doubles. Neither is a production defect:

- `AsyncSpannerDriver.select` runs on a cursor from `client.cursor(...)`. `AsyncSpannerClientProtocol` declares that method with the keywords the driver passes, and the native client has it. The test's `Client` double only has the removed `query` method.[^spanner-protocol]
- A DB-API connection takes its cursors from `driver.cursor(lazy_begin=...)` and raises `InterfaceError` at construction for a driver without `cursor()`. The MySQL protocol driver doubles only have `execute_with_description`.

In scope: the two test files. Out of scope: every production file, `AsyncSpannerDriver`, the DB-API module, and every other test in the two files.

## Bounded Read Manifest

- The Spanner channel test, and the import block of its file.
- `AsyncSpannerCursorProtocol`, `AsyncSpannerClientProtocol.cursor`, `AsyncSpannerDriver.cursor`, and `AsyncSpannerDriver.execute_with_description` for the calls the double must answer.
- `AsyncSpannerChangeStreamSource.poll_partition` for how rows are consumed.
- The two MySQL driver doubles and the test that uses them.
- `_FakeMSSQLProtocolCursor` and `_FakeMSSQLAsyncProtocolCursor` in the enterprise driver test file, as the model for the MySQL cursor doubles. Read only; t13 owns that file.[^mssql-cursor-double]
- `SyncDBAPIConnection.cursor` for the factory call.

## Contracts & Edge Cases

### Spanner client double

Add one module-level class above the test in `test_db_drivers_channels_and_dialers.py`:

```python
class _SpannerChangeStreamCursor:
    """Async cursor double that records one change-stream read on its owning client double."""

    def __init__(
        self,
        calls: list[tuple[str, str | None]],
        param_encoder: Callable[[Any], str | None] | None,
    ) -> None: ...

    description: tuple[tuple[Any, ...], ...] | None
    rowcount: int
    lastrowid: Any | None
    arraysize: int

    async def execute(self, sql: str, params: Any = ()) -> _SpannerChangeStreamCursor: ...
    async def executemany(self, sql: str, seq_of_params: Any) -> _SpannerChangeStreamCursor: ...
    async def fetchone(self) -> tuple[Any, ...] | None: ...
    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    async def fetchall(self) -> list[tuple[Any, ...]]: ...
    async def close(self) -> None: ...
```

- `__init__` sets `description = None`, `rowcount = -1`, `lastrowid = None`, `arraysize = 1`, and an empty row buffer.
- `execute` appends `(sql, param_encoder(params) if param_encoder is not None else None)` to `calls`. It sets `description` to `(("data_change_record", None, None, None, None, None, None),)`, buffers the single row `([{"table_name": "orders", "mod_type": "INSERT", "mods": [{"keys": {"id": 1}}]}],)`, sets `rowcount = 1`, and returns `self`.
- `executemany` clears the buffer, sets `description = None` and `rowcount = len(seq_of_params)`, and returns `self`.
- `fetchone` pops the first buffered row or returns `None`. `fetchmany` returns up to `size` rows, or `arraysize` rows when `size` is `None`. `fetchall` returns and clears the buffer.
- `close` clears the buffer.

Change the nested `Client` class in the test:

- Delete its `query` method. Nothing calls it.
- Add:

```python
def cursor(
    self,
    *,
    staleness_mode: str = "strong",
    staleness_value: int = 0,
    request_tag: str | None = None,
    transaction_tag: str | None = None,
    param_encoder: Callable[[Any], str | None] | None = None,
    lazy_begin: bool = False,
) -> _SpannerChangeStreamCursor:
```

- The method discards the five keywords it does not use with one tuple assignment to `_`, and returns `_SpannerChangeStreamCursor(self.calls, param_encoder)`.
- `calls` and `close` stay as they are.

The test body and its assertions do not change. `client.calls[0][1]` is the JSON text the driver's encoder produced, so `decode_json(raw_params)["start_ts"]` still reads the start timestamp. `Callable` and `Any` are already imported at the top of the file.

### MySQL protocol driver doubles

Add two module-level classes above `_FakeMySQLSyncProtocolDriver` in `test_mysql_driver.py`:

```python
class _FakeMySQLProtocolCursor:
    """Minimal PEP 249 sync cursor backed by _FakeMySQLSyncProtocolDriver."""

    def __init__(self, driver: _FakeMySQLSyncProtocolDriver) -> None: ...
    def execute(self, sql: str, parameters: Any = ()) -> _FakeMySQLProtocolCursor: ...
    def executemany(self, sql: str, seq_of_parameters: Any) -> _FakeMySQLProtocolCursor: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...


class _FakeMySQLAsyncProtocolCursor:
    """Minimal PEP 249 async cursor delegating to _FakeMySQLProtocolCursor."""

    def __init__(self, sync_cursor: _FakeMySQLProtocolCursor) -> None: ...
```

- The sync cursor declares `description: tuple[tuple[Any, ...], ...] | None`, `rowcount: int`, `lastrowid: int | None`, and `arraysize: int` at class level, and sets them to `None`, `-1`, `None`, and `1` in `__init__`.
- `execute` calls `self._driver.execute_with_description(sql, parameters)`, stores the rows and the description, resets the read position, sets `rowcount = len(rows)`, and returns `self`.
- `executemany` calls `execute` once per parameter set, then sets `description = None` and `rowcount` to the number of sets.
- The fetch methods and `close` behave as `_FakeMSSQLProtocolCursor` does. There is no `nextset`.
- The async cursor exposes `description`, `rowcount`, `lastrowid`, and `arraysize` as read-only properties over the sync cursor. Its `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, and `close` are coroutines that call the sync cursor. `execute` and `executemany` return the async cursor.

Add to `_FakeMySQLSyncProtocolDriver`:

```python
def cursor(self, *, lazy_begin: bool = False) -> _FakeMySQLProtocolCursor:
```

Add to `_FakeMySQLAsyncProtocolDriver`:

```python
def cursor(self, *, lazy_begin: bool = False) -> _FakeMySQLAsyncProtocolCursor:
```

- Both discard `lazy_begin`. The sync one returns `_FakeMySQLProtocolCursor(self)`. The async one returns `_FakeMySQLAsyncProtocolCursor(_FakeMySQLProtocolCursor(self._sync))`.
- `execute_with_description` stays on both doubles. The cursor doubles call it.

The test body and its assertions do not change.

Rules for both files:

- No inline comment. Every new class and public method has a one-line docstring.
- No production file changes. When a double still fails after these edits because a driver calls a member this worksheet does not list, stop and report the member and the calling line.

## Test Matrix

| Test | Expected |
| --- | --- |
| `test_channel_bus_spanner_source_delivers_once_per_output_channel`, six cases | Pass; `client.calls` holds one entry whose SQL contains `READ_orders_stream` and whose JSON text holds `start_ts` |
| `test_mysql_sync_and_async_engine_initialize_and_isolation` | Passes; server version `(8, 0, 36)`, schema `appdb`, isolation level read and set, `SELECT 1` returns `1`, sync and async |
| Every other test in the two files | Unchanged result |
| `mypy` | No error in the two files |
| `ruff check` on the two files | No finding |

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py src/py/tests/unit/db/test_mysql_driver.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py src/py/tests/unit/db/test_mysql_driver.py
uv run --no-sync --frozen mypy | grep "test_db_drivers_channels_and_dialers.py\|test_mysql_driver.py"
git diff --stat -- src/py/vertebra src/rs
```

The first command reports no failure. The `grep` and the last command print nothing.

## Acceptance Criteria

- [ ] The seven tests pass with their assertions unchanged.
- [ ] The Spanner client double has `cursor()` with the six keywords of `AsyncSpannerClientProtocol.cursor` and no `query` method.
- [ ] Both MySQL driver doubles have `cursor(*, lazy_begin=False)`, and their cursor doubles have the members listed above.
- [ ] No production file has a diff from this task.

[^spanner-protocol]: `src/py/vertebra/db/adapters/spanner/_async.py::AsyncSpannerClientProtocol`
[^channel-test]: `src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py::test_channel_bus_spanner_source_delivers_once_per_output_channel`
[^mysql-double]: `src/py/tests/unit/db/test_mysql_driver.py::_FakeMySQLSyncProtocolDriver`
[^mssql-cursor-double]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::_FakeMSSQLProtocolCursor`
