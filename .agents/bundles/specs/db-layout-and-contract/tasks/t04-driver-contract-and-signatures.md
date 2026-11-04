---
type: Task
id: db-layout-and-contract:t04
title: Unify SyncDatabaseDriver / AsyncDatabaseDriver method signatures and eliminate type-ignore overrides
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-layout-and-contract:t01
  - db-layout-and-contract:t02
  - db-layout-and-contract:t03
files:
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/db/driver/_async.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/datafusion/driver.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/db/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - docs/examples/storage_and_db.py
tests:
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/docs/test_examples.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
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
created_at: 2026-11-03T21:40:00Z
updated_at: 2026-11-04T01:08:00Z
commit: null
tags: [db, driver, contract, typing, dbapi]
---

## Objective

Standardize `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`, all eight SQL/analytical adapter drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`), and Spanner snapshot/transaction handles (`SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction`, `AsyncSpannerTransaction`) on one method, parameter-name, transaction, error-mapping, and return-type contract with zero `# type: ignore[override]` annotations and zero legacy alias methods, updating PEP 249 `SyncDBAPIConnection` / `AsyncDBAPICursor`, GCP SQLSpec callers, and `docs/examples/storage_and_db.py` accordingly.

## Context

- **Verification strategy (`behavior_tdd`)**: Method parameter names (`parameters`, `threshold_rows`), return types (`execute -> int`, `execute_with_description -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`, `select_one -> raises NotFoundError`, `select_value -> NotFoundError on 0 rows / None on SQL NULL`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `transaction()`, `cancel()`), `SQLite` typed `DatabaseError` mapping and parameter normalization, removal of legacy driver alias methods, and strict Mypy Liskov substitutability without `# type: ignore[override]` are directly testable across all eight adapters.
- **Architectural invariants**:
  - Standardize the second positional/keyword parameter name `parameters: Sequence[Any] | Mapping[str, Any] | None = None` across all eight adapters (replacing `*params: Any` in `postgres`/`mysql`/`sqlite` and renaming `params` -> `parameters` in `oracle`/`mssql`/`duckdb`), rename `shm_threshold_rows` -> `threshold_rows` on `select_to_arrow` in `sqlite`, `oracle`, `mssql`, and `duckdb`, and delete the legacy driver alias methods (`executemany`, `fetch`, `fetchrow`, `fetchval`, `fetch_arrow`, `insert_arrow`) on `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `SyncBigQueryDriver`/`AsyncBigQueryDriver`, and `SyncSpannerDriver`/`AsyncSpannerDriver`, updating callers in `bigquery/driver.py`, `spanner/driver.py`, `datafusion/driver.py` (`register_bigquery_table`, `register_bigquery_query`, `register_spanner_table`, `register_spanner_query`), and `extensions/sqlspec/{bigquery,spanner}.py` to call `select_to_arrow` and `load_from_arrow` directly.
  - `execute(...)` always returns affected rowcount `int`; row-tuple queries use `execute_tuples(...)` or `select*(...)`.
  - `execute_with_description(...)` always returns a 2-tuple `(rows: list[tuple[Any, ...]], column_names: tuple[str, ...])`. `SyncDBAPIConnection.execute_with_description` and `AsyncDBAPICursor.execute` in `src/py/vertebra/extensions/sqlalchemy/dialects.py` project `column_names` into 7-element PEP 249 tuples `tuple((col, None, None, None, None, None, None) for col in column_names) if column_names else None`.
  - `select_one(...)` and `select_value(...)` raise `NotFoundError` (which subclasses `(DataError, ValueError)`) strictly when `len(rows) == 0` across every engine, including BigQuery, Spanner, `SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction` (`select_one` and `query_one`), and `AsyncSpannerTransaction` (`select_one` and `query_one`). When `select_value(...)` receives 1 row whose first column is `SQL NULL` (`rows[0][0] is None`), it returns `None` without raising `NotFoundError`.
  - `BigQuerySyncDriver` / `BigQueryAsyncDriver` in `src/py/vertebra/extensions/sqlspec/bigquery.py` delegate `select_one` to `self._core.select_one(...)` (`NotFoundError`) and `select_one_or_none` to `self._core.select_one_or_none(...)` (`None`). `SpannerSyncDriver.select_one_or_none` / `SpannerAsyncDriver.select_one_or_none` in `spanner.py` preserve `self._active_snapshot` `StalenessBound` routing via `self.select(...)` (returning `rows[0] if rows else None`).
  - `SyncPostgreSQLDriver.stream_arrow_batches(self, sql, parameters=None, *, batch_size=65_536, columns=None, **kwargs) -> Iterator[Any]` and `AsyncPostgreSQLDriver.stream_arrow_batches(...) -> AsyncIterator[Any]` (async generator) match the base driver signatures, and `postgres`/`mysql` `select_to_arrow(...)` construct `ArrowCursor.from_rows(..., threshold_rows=threshold_rows)`.
  - `SyncSQLiteDriver` translates `sqlite3.Error` subclasses (`IntegrityError`, `ProgrammingError`, `DataError`, `OperationalError`, `InterfaceError`, `NotSupportedError`, `InternalError`, `DatabaseError`) into `vertebra.db.driver.DatabaseError` subclasses via `_translate_sqlite_error` and normalizes `Decimal` (`str`), `datetime`/`date`/`time` (`isoformat()`), `UUID` (`str`), and `dict`/`list` JSON bind parameters (`encode_json` via `vertebra.utils.serializers`) so `sqlite` satisfies the typed-error and parameter conformance contract prior to `native-embedded-and-cache-drivers`.
  - **Transaction contract (`begin`, `commit`, `rollback`, `transaction`)**:
    - `transaction()` returns an `AbstractContextManager[Self]` (sync) / `AbstractAsyncContextManager[Self]` (async) calling `begin()`, yielding `self`, calling `commit()` on clean exit, and calling `rollback()` before re-raising on exception.
    - `sqlite`, `oracle`, `mssql`, and `duckdb`: track `self._in_transaction: bool = False`; `begin()` executes `"BEGIN"` (`"BEGIN TRANSACTION"` on native `duckdb`) and sets `self._in_transaction = True`; per-statement `self._conn.commit()` in `execute`, `execute_many`, and `load_from_arrow` runs only when `not self._in_transaction`; `commit()` / `rollback()` commit or roll back the connection and reset `self._in_transaction = False`.
    - `postgres` and `mysql`: pin a checked-out connection `self._tx_conn` across `begin()`..`commit()`/`rollback()` when backed by a connection pool, and snapshot/restore `_in_memory_rows` in `memory://` / `executor=` unit-test harnesses.
    - `bigquery`: `begin()`, `commit()`, and `rollback()` execute `"BEGIN TRANSACTION"`, `"COMMIT TRANSACTION"`, and `"ROLLBACK TRANSACTION"` via `self.execute(...)` over `self._session_id`.
    - `spanner`: while `self._in_transaction` is `True`, `execute`/`execute_many` append DML `(sql, parameters)` entries to `self._tx_dml_buffer` and `apply_mutations`/`load_from_arrow` append `Mutation` items to `self._tx_mutation_buffer`; `commit()` flushes `_tx_dml_buffer` atomically via `execute_batch_dml` and `_tx_mutation_buffer` via `apply_mutations` before clearing `_in_transaction`, while `rollback()` clears both buffers without issuing RPCs (`run_in_transaction(fn)` remains Spanner's retryable read-write transaction API).
  - `cancel()` (sync) / `async def cancel()` (async) interrupts an active query when supported (`sqlite3.Connection.interrupt()`) and is an idempotent no-op when no query is active.

## Steps

1. **Write failing parameterized driver-contract tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add `@pytest.mark.parametrize` contract tests across SQLite, DuckDB, Oracle, MSSQL, PostgreSQL, MySQL, BigQuery, and Spanner drivers asserting:
     - `execute()` returns `int`, `execute_with_description()` returns `(list[tuple], tuple[str, ...])` of length 2, `select_to_arrow(..., threshold_rows=...)` returns an `ArrowCursor`, and legacy aliases (`executemany`, `fetch`, `fetchrow`, `fetchval`, `fetch_arrow`, `insert_arrow`) are absent (`hasattr(driver, alias) is False`).
     - `select_one()` and `select_value()` raise `NotFoundError` (`isinstance(exc, (DatabaseError, ValueError))`) when 0 rows match; `select_one_or_none()` returns `None`; and `select_value("SELECT NULL")` returns `None` without raising `NotFoundError`.
     - `SyncSQLiteDriver` raises `IntegrityError` (`isinstance(exc, DatabaseError)`) on duplicate primary keys, raises `OperationalError` / `ProgrammingError` (`DatabaseError`) on invalid SQL, and binds `Decimal`, timezone-aware `datetime`, and `dict`/`list` JSON values cleanly.
     - `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `SyncSpannerSnapshot`, and `AsyncSpannerSnapshot` raise `NotFoundError` from `select_one()` (and `query_one()` on transactions) on 0 rows and return `None` from `select_one_or_none()`.
     - `with sync_driver.transaction():` and `async with async_driver.transaction():` commit on normal exit and roll back on raised exception across all 8 adapters (including Spanner buffered batch DML/mutation flush on `commit()` and buffer discard on `rollback()`), `stream_arrow_batches()` yields Arrow record batches synchronously on sync drivers and via `async for` on async drivers, and `cancel()` is callable synchronously on sync drivers and asynchronously on async drivers.
     - Zero `# type: ignore[override]` annotations exist in `src/py/vertebra/db/adapters/*/driver.py` and `src/py/vertebra/db/driver/*.py`.
   - In `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, add tests asserting PEP 249 `SyncDBAPICursor.description` and `AsyncDBAPICursor.description` expose 7-element `(name, None, ...)` tuples for both sync and async `+vtb` DB-API cursors after `execute_with_description` returns `tuple[str, ...]`.

2. **Align base classes, all 8 adapter drivers, DB-API / GCP SQLSpec callers, and `docs/examples/storage_and_db.py` (GREEN)**:
   - Update `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` and `src/py/vertebra/db/driver/_async.py::{AsyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver}` with the complete unified signatures (`execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `begin`, `commit`, `rollback`, `transaction`, `cancel`, `close`), deleting legacy alias methods (`executemany`, `fetch`, `fetchrow`, `fetchval`, `fetch_arrow`, `insert_arrow`), raising `NotFoundError` in `select_one` and `select_value` strictly when `len(rows) == 0`, and returning `None` when `rows[0][0] is None`.
   - Update `postgres/driver.py`, `mysql/driver.py`, and `sqlite/driver.py`: replace `*params: Any` with `parameters: Sequence[Any] | Mapping[str, Any] | None = None`, return integer rowcount from `execute()`, return `(rows, tuple[str, ...])` from `execute_with_description()`, rename `shm_threshold_rows` -> `threshold_rows` on `sqlite/driver.py::select_to_arrow`, return `ArrowCursor.from_rows(..., threshold_rows=threshold_rows)` from `postgres` and `mysql` `select_to_arrow()`, convert `AsyncPostgreSQLDriver.stream_arrow_batches` to an `AsyncIterator[Any]` async generator with optional `columns: Sequence[str] | None = None`, add `_in_transaction` / `_tx_conn` / in-memory snapshot transaction handling across all three adapters, add `_translate_sqlite_error` and `Decimal`/`datetime`/`UUID`/JSON bind normalization in `sqlite`, and implement `sqlite` `cancel()` via `self._conn.interrupt()`.
   - Update `oracle/driver.py`, `mssql/driver.py`, and `duckdb/driver.py`: rename `params` -> `parameters` and `shm_threshold_rows` -> `threshold_rows`, return a 2-tuple `(rows, col_names)` from `execute_with_description()`, implement `_in_transaction` gating (`begin`, `commit`, `rollback`), and raise `NotFoundError` on 0 rows in `select_one()` / `select_value()`.
   - Update `bigquery/driver.py`, `spanner/driver.py` (`Sync*`, `Async*`, `SyncSpannerSnapshot`, `AsyncSpannerSnapshot`, `SyncSpannerTransaction`, `AsyncSpannerTransaction`), and `datafusion/driver.py`: remove `fetch_arrow` and `insert_arrow` in favor of `select_to_arrow` and `load_from_arrow` (updating `export_to_object_store`, `load_from_object_store`, and `DataFusionEngine.register_{bigquery,spanner}_{table,query}` callers), raise `NotFoundError` in `select_one()`, `query_one()`, and `select_value()` when `len(rows) == 0`, return `None` on 0 rows in `select_one_or_none()` and on 1-row `SQL NULL` in `select_value()`, implement BigQuery multi-statement `BEGIN`/`COMMIT`/`ROLLBACK TRANSACTION` and Spanner buffered `_tx_dml_buffer` / `_tx_mutation_buffer` `begin`/`commit`/`rollback`/`transaction()`, and expose `stream_arrow_batches` and `cancel()`.
   - Update `src/py/vertebra/extensions/sqlalchemy/dialects.py` (`SyncDBAPIConnection.execute_with_description`, `AsyncDBAPICursor.execute`) to unpack `(rows, col_names)` and synthesize 7-item PEP 249 ` description` tuples, update `src/py/vertebra/extensions/sqlspec/bigquery.py` (`select_one` -> `self._core.select_one`, `select_one_or_none` -> `self._core.select_one_or_none`, `fetch_arrow`/`insert_arrow` -> `select_to_arrow`/`load_from_arrow`) and `spanner.py` (`select_one_or_none` preserving `self._active_snapshot` via `self.select`, `fetch_arrow`/`insert_arrow` -> `select_to_arrow`/`load_from_arrow`), and update `docs/examples/storage_and_db.py` (`db.execute("INSERT INTO reports (key) VALUES (?)", ("reports/q2.json",))`).
   - Update existing unit tests in `test_{postgres_driver,mysql_driver,enterprise_olap_drivers,bigquery,spanner,sqlspec_bigquery,sqlspec_spanner}.py` (including `select_to_arrow(..., shm_threshold_rows=...)` -> `threshold_rows=` call sites in `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `fetch_arrow`/`insert_arrow` -> `select_to_arrow`/`load_from_arrow` on `_SimulatedSyncCoreDriver` and `_SimulatedAsyncCoreDriver` in `test_sqlspec_bigquery.py`, and `driver.execute("SELECT ...")` -> `driver.execute_tuples(...)` / `driver.select(...)`), and remove every `# type: ignore[override]` in `src/py/vertebra/db/`.

3. **Refactor & verify**:
   - Run `mypy`, `ruff`, and the full unit test suite to confirm strict override conformance across all eight adapters.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` -> fails on `execute` return types, 3-tuple `execute_with_description`, BigQuery/Spanner `select_one() -> None`, and `# type: ignore[override]` presence.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [ ] R6: All eight SQL/analytical sync and async drivers (plus `SyncSpannerSnapshot`/`AsyncSpannerSnapshot` and `SyncSpannerTransaction`/`AsyncSpannerTransaction`) implement identical method signatures, parameter names (`parameters`, `threshold_rows`), and return types (`execute -> int`, `execute_with_description -> (rows, col_names)`, `select_one -> NotFoundError`, `select_value -> NotFoundError on 0 rows / None on SQL NULL`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `transaction()`, `cancel()`, `close()`).
- [ ] Zero `# type: ignore[override]` comments remain in `src/py/vertebra/db/`.
- [ ] SQLAlchemy PEP 249 `SyncDBAPICursor.description` / `AsyncDBAPICursor.description` and BigQuery/Spanner SQLSpec drivers pass all unit tests cleanly.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
