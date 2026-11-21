---
type: Task
id: "native-cursor-data-path:wire:t04"
chapter_id: wire-parity
title: "PostgreSQL broker one-shot path, SQLite bound cursor, and Oracle allow-list"
description: "Route broker-leased PostgreSQL one-shot queries through the pool single-call path, bind SyncSQLiteDriver.cursor directly to the native client method, and add the user-approved Oracle allow-list entries citing oracle/rust-oracledb#101."
worksheet_format: compact
status: stable
state: completed
priority: P1
verification_strategy: targeted_unit
depends_on:
  - "native-cursor-data-path:wire:t02"
read_set:
  - "src/py/vertebra/db/adapters/postgres/_async.py:925-995"
  - "src/py/vertebra/db/adapters/postgres/_sync.py:854-920"
  - "src/py/vertebra/db/driver/_common.py:150-220"
  - "src/py/vertebra/db/driver/_common.py:280-305"
  - "src/py/vertebra/db/adapters/sqlite/_sync.py:40-170"
  - "tools/benchmarks/adapters/sql.py:66-100"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:425-445"
  - "src/py/tests/unit/tools/test_adapter_benchmarks_sql.py:548-615"
files:
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/sqlite/_sync.py
  - tools/benchmarks/adapters/sql.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_sqlite_driver.py
  - src/py/tests/unit/db/test_dbapi_standalone.py
  - src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
estimated_loc: 110
plan_revision: 5
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T20:40:00Z
sources:
  - id: pg-async-driver
    resource: src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver._single_call_checked
    title: "Async PostgreSQL single-call checkout path"
  - id: pg-sync-driver
    resource: src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._single_call_checked
    title: "Sync PostgreSQL single-call checkout path"
  - id: sqlite-sync-driver
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver._bind_client
    title: "SQLite direct native method binding"
  - id: bench-sql
    resource: tools/benchmarks/adapters/sql.py::COMPARATOR_ALLOW_LIST
    title: "Benchmark comparator allow-list"
tags: [performance, postgres, sqlite, oracle, benchmarks]
---

# t04 — PostgreSQL broker one-shot path, SQLite bound cursor, and Oracle allow-list

## Objective & Scope Boundary

Close the remaining Python-layer regressions surfaced in `single:t27` and record the user-approved allow-list entry for upstream `oracle/rust-oracledb#101`:

1. **PostgreSQL broker-leased one-shot path**: `PostgresAsyncConfig` defaults to `share_pool=True`, attaching the default broker and setting `_lease_at_checkout=True`. Previously `_single_call_checked` forced `_lease_at_checkout` queries through `_acquire_session_client` -> `client.fetch_rows` -> `client.close()` (three Tokio hops plus a `DISCARD ALL` reset round trip, costing `356 us` vs `83.86 us`). Bracket the pool single-call method with broker lease acquire and release instead, reserving `_acquire_session_client` for custom `client_factory` and pinned sessions.[^pg-async-driver][^pg-sync-driver]
2. **SQLite direct bound `cursor`**: bind `SyncSQLiteDriver.cursor` to `client.cursor` in `_bind_client` (and to `_raise_closed` on `close()`), removing the two Python wrapper frames (`cursor` and `_require_client`) that sit on every SQLAlchemy `sqlite+vtb` statement.[^sqlite-sync-driver]
3. **Oracle allow-list entry (user-approved)**: add `vertebra-oracle-sync` and `vertebra-oracle-async` to `COMPARATOR_ALLOW_LIST` citing upstream `oracle/rust-oracledb#101` (`post_connect` resizes `statement_cache` to `0` when `AUTH_MAX_OPEN_CURSORS` is absent), and update `test_adapter_benchmarks_sql.py` accordingly.[^bench-sql]

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/postgres/_async.py:925-995` and `_sync.py:854-920` for `_single_call_checked`.
- `src/py/vertebra/db/driver/_common.py:150-220`, `280-305` for `_BrokerLeasePlan`.
- `src/py/vertebra/db/adapters/sqlite/_sync.py:40-170` for `_bind_client`.
- `tools/benchmarks/adapters/sql.py:66-100` and `test_adapter_benchmarks_sql.py:425-445`, `548-615` for `COMPARATOR_ALLOW_LIST`.

## Contracts & Edge Cases

1. **PostgreSQL broker-leased one-shot**:
   - When `self._client_factory is not None`: keep the existing `_acquire_session_client` / `_release_pooled_client` branch unchanged.
   - When `self._client_factory is None` and `self._lease_at_checkout` is `True`: acquire one broker lease (`_acquire_broker_lease_async` / `_acquire_broker_lease`), run the pool single-call method (`pick(pool)(sql, params, ...)` with IAM token refresh on auth rejection), and release the broker lease in `finally` via `_release_lease(self._broker, self._lease_plan(), lease)`.
   - Skip `_ensure_pool_async()` / `_ensure_pool()` lookup overhead when `self._pool is not None` and `self.config.token_provider is None`.
   - Broker lease counters remain identical (`1` acquire and `1` release per unpinned statement, `0` leaks on error or cancellation).
2. **SQLite bound `cursor`**:
   - Add `"cursor"` to `SyncSQLiteDriver.__slots__`, bind `self.cursor = client.cursor` in `_bind_client`, and swap `self.cursor = _raise_closed` when `client is None`.
   - `SyncSQLiteDriver` continues to satisfy `SyncSQLDriverProtocol` under `mypy` and `pyright`.
   - `SyncDBAPIConnection.cursor` binds `partial(driver.cursor, lazy_begin=True)` (or `driver.cursor` in autocommit mode), keeping `conn.cursor.func == driver.cursor` intact with zero Python wrapper frames.
3. **Oracle allow-list**:
   - Add `"vertebra-oracle-sync"` and `"vertebra-oracle-async"` to `COMPARATOR_ALLOW_LIST` with single-line reason referencing `oracle/rust-oracledb#101`.
   - Update `test_sql_cli_check_exit_codes` to use a non-allow-listed driver for the `[FAIL]` check and add parametrized tests verifying both Oracle entries emit `allowed`.

## Test Matrix

| Check | Expected |
| --- | --- |
| Broker-leased PostgreSQL one-shot query | Calls pool single-call directly with 1 lease acquire and 1 release |
| `SyncSQLiteDriver.cursor` lifecycle | Bound to `client.cursor` while open and `_raise_closed` after close |
| Oracle entries in `COMPARATOR_ALLOW_LIST` | Both `vertebra-oracle-sync` and `vertebra-oracle-async` return `allowed` |
| Targeted pytest, ruff, and mypy checks | Zero errors and zero failures |

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/vertebra/db/adapters/postgres/_async.py src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/sqlite/_sync.py tools/benchmarks/adapters/sql.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
uv run --no-sync --frozen mypy src/py/vertebra/db/adapters/postgres/_async.py src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/sqlite/_sync.py tools/benchmarks/adapters/sql.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py
```

## Acceptance Criteria

- [x] Broker-leased one-shot PostgreSQL queries run through the pool single-call path with one lease acquire and one release and no `DISCARD ALL` reset round trip.
- [x] `SyncSQLiteDriver.cursor` is bound directly to `client.cursor` when open and `_raise_closed` when closed.
- [x] `COMPARATOR_ALLOW_LIST` holds both Oracle driver entries citing `oracle/rust-oracledb#101`.
- [x] All targeted pytest, ruff, and mypy checks pass.

[^pg-async-driver]: `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver._single_call_checked`
[^pg-sync-driver]: `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver._single_call_checked`
[^sqlite-sync-driver]: `src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver._bind_client`
[^bench-sql]: `tools/benchmarks/adapters/sql.py::COMPARATOR_ALLOW_LIST`
