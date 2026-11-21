---
type: Task
id: "native-cursor-data-path:network:t17"
chapter_id: network-engines
title: "SQL Server native drivers on the native cursor with the lease at connect"
description: "SyncMSSQLDriver and AsyncMSSQLDriver query methods call the native cursor, _prepare_sql is deleted, and the broker lease follows the client."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t16"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/mssql/_sync.py:31-165"
  - "src/py/vertebra/db/adapters/mssql/_sync.py:165-437"
  - "src/py/vertebra/db/adapters/mssql/_async.py::AsyncMSSQLDriver"
  - "src/py/vertebra/db/adapters/mssql/core.py:170-200"
  - "src/py/vertebra/db/adapters/mssql/core.py:318-324"
  - "src/py/vertebra/db/adapters/mssql/pipeline.py:1-60"
  - "src/py/vertebra/db/driver/_common.py:148-199"
  - "src/py/tests/unit/db/test_mssql_driver.py"
files:
  - "src/py/vertebra/db/adapters/mssql/_sync.py"
  - "src/py/vertebra/db/adapters/mssql/_async.py"
  - "src/py/vertebra/db/adapters/mssql/core.py"
tests:
  - "src/py/tests/unit/db/test_mssql_driver.py"
estimated_loc: 240
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: mssql-sync-driver
    resource: "src/py/vertebra/db/adapters/mssql/_sync.py::SyncMSSQLDriver"
    title: "Native driver query path"
tags: [mssql, python, driver, lease]
---

# SQL Server native drivers on the native cursor with the lease at connect

## Objective & Scope Boundary

The SQL Server drivers stop rewriting placeholders in Python and stop consulting the shared-memory statement cache for every statement. Query methods run one native cursor call. The broker lease is taken when the driver connects and returned when it closes.

Out of scope:

- `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` beyond removing their `_prepare_sql` calls.
- The SQLSpec adapter (t18).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/mssql/_sync.py:165-357` holds the query methods; each starts with `_prepare_sql`.
- `src/py/vertebra/db/adapters/mssql/_sync.py:401-437` holds `_run_with_client` and `_prepare_sql`.
- `src/py/vertebra/db/adapters/mssql/core.py:175-200` is `MSSQLCore.rewrite_placeholders`; `:322` is the module-level alias.

## Contracts & Edge Cases

- `cursor` takes keyword-only `lazy_begin: bool = False` and passes it to the client cursor. The driver's own query methods use the default.
- Add `cursor(self) -> SyncMSSQLCursor` and the async form returning `AsyncMSSQLCursor`. Both return `self._client.cursor()` after the open check the other methods use.
- Add `_execute_cursor(self, sql, parameters)` that runs `client.cursor().execute(sql, parameters or ())` inside `_run_with_client`, so the token refresh retry still applies.
- `execute` returns `max(cursor.rowcount, 0)`. `execute_with_description` returns `(cursor.fetchall(), names)` or `([], ())`. `execute_tuples` returns the rows. The `hasattr(client, 'execute_with_description')` probe and the `client.query` dict branch are removed.
- `select`, `select_one`, `select_one_or_none`, `select_value`, and `select_with_total` are rewritten on `_execute_cursor` and keep their return types.
- `execute_many` calls `client.cursor().executemany(sql, list(parameter_sets))` and returns `max(cursor.rowcount, 0)`.
- Arrow and bulk-load methods pass the caller's SQL to their native call unchanged. The native side rewrites placeholders (t15).
- Delete `_prepare_sql`, the `_last_rewritten_sql` attribute, and the `last_rewritten_sql` property. Tests that read `last_rewritten_sql` are deleted; the rewrite is covered by the Rust tests of t15.
- Lease: `connect` calls `self._attach_checkout_lease(client)` once after the client is ready. The async driver awaits its acquire helper and calls `client.attach_lease(...)`. The driver stores no lease. `close` calls `client.release_lease()`. A refused acquire attaches nothing and the driver proceeds. A driver dropped without `close()` releases through the client's guard. The lease is held for the life of the client, so an idle open connection occupies a broker slot. `_run_with_client` loses `use_broker_lease` and both lease calls. When the driver has no broker, no lease code runs.
- Delete `MSSQLCore.rewrite_placeholders` and the module function `rewrite_mssql_placeholders` from `core.py` when `git grep` shows no caller under `src/py` outside tests. The extension function `vertebra._mssql.rewrite_mssql_placeholders` stays; the cleanup chapter audits it.
- `statement_cache_stats` keeps returning the broker statistics it returns today. It does not register statements.
- Tests that assert a `vertebra._vertebra` error class from a query method stay unchanged.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select dicts | `driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Tuples and names | `execute_with_description('SELECT 1 AS a, 2 AS b')` | `([(1, 2)], ('a', 'b'))` |
| Row count | `UPDATE` on two rows | `2` |
| Positional parameters | `select('SELECT ? AS a', (5,))` | `[{'a': 5}]` |
| execute_many | Three sets | `3` |
| No broker | Ten statements with `broker=None` | Recording broker double never called |
| One lease per connection | Ten statements with a recording broker | One acquire at connect; one release at close |
| Acquire refused at connect | Broker double whose `acquire` raises `RuntimeError`; connect, then run a statement | The statement succeeds; no `attach_lease` call; zero releases at `close()` |
| Driver dropped without close | Recording broker; connect, run a statement, `del driver`, `gc.collect()` | One release recorded |
| Token refresh | Token provider and one rejection | One retry, then success |
| Error class | Key violation through `execute` | `IntegrityError` |
| Async | The same cases through `AsyncMSSQLDriver` | Same results |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_mssql_driver.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/mssql/_sync.py src/py/vertebra/db/adapters/mssql/_async.py src/py/vertebra/db/adapters/mssql/core.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/mssql/_sync.py src/py/vertebra/db/adapters/mssql/_async.py src/py/vertebra/db/adapters/mssql/core.py`
- `test -z "$(git grep -n '_prepare_sql' -- src/py/vertebra/db/adapters/mssql)"`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/mssql)"`
- `test -z "$(git grep -n 'use_broker_lease' -- src/py/vertebra/db/adapters/mssql)"`

## Acceptance Criteria

- [x] No file under the SQL Server adapter package calls `_prepare_sql` or `_lookup_or_cache_statement`.
- [x] Lease tests use a recording broker double and assert call counts.
- [x] Arrow and bulk-load tests pass unchanged.

## Notes & Discoveries

- `MSSQLCore.rewrite_placeholders` and `rewrite_mssql_placeholders` in `src/py/vertebra/db/adapters/mssql/core.py` were retained because `git grep` showed non-test callers under `src/py` (`src/py/vertebra/db/adapters/mssql/config.py`, `src/py/vertebra/db/adapters/mssql/__init__.py`, and `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py`).
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (outside `t17` declared `tests:`) still references `last_rewritten_sql` on `SyncMSSQLDriver` / `AsyncMSSQLDriver`.
- `uv run --no-sync` required `UV_CACHE_DIR=/tmp/uv-cache` in the sandbox because `~/.cache/uv` is read-only.
