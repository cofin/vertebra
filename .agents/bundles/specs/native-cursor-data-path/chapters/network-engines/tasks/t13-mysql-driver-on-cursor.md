---
type: Task
id: "native-cursor-data-path:network:t13"
chapter_id: network-engines
title: "MySQL native drivers on the native cursor with the lease at connect"
description: "SyncMySQLDriver and AsyncMySQLDriver query methods call the native cursor, per-statement compile and statement-cache calls are deleted, and the broker lease follows the client."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t12"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/mysql/_sync.py:37-140"
  - "src/py/vertebra/db/adapters/mysql/_sync.py:239-560"
  - "src/py/vertebra/db/adapters/mysql/_sync.py:560-620"
  - "src/py/vertebra/db/adapters/mysql/_async.py::AsyncMySQLDriver"
  - "src/py/vertebra/db/adapters/mysql/core.py:111-210"
  - "src/py/vertebra/db/adapters/mysql/core.py:322-453"
  - "src/py/vertebra/db/adapters/mysql/pipeline.py:1-60"
  - "src/py/vertebra/db/driver/_common.py:148-199"
  - "src/py/tests/unit/db/test_mysql_driver.py"
files:
  - "src/py/vertebra/db/adapters/mysql/_sync.py"
  - "src/py/vertebra/db/adapters/mysql/_async.py"
  - "src/py/vertebra/db/adapters/mysql/core.py"
tests:
  - "src/py/tests/unit/db/test_mysql_driver.py"
estimated_loc: 280
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: mysql-sync-driver
    resource: "src/py/vertebra/db/adapters/mysql/_sync.py::SyncMySQLDriver"
    title: "Native driver query path"
tags: [mysql, python, driver, lease]
---

# MySQL native drivers on the native cursor with the lease at connect

## Objective & Scope Boundary

Every row-returning and row-count method on the two MySQL drivers runs one native cursor call. `compile_for_driver`, the SQL encode, and the twelve `_lookup_or_cache_statement` calls per driver are gone. The broker lease is taken when the driver obtains its client and returned when the client is closed or replaced.

Out of scope:

- `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `execute_script`, and `fetch_all_sets`. They keep their native calls and their `MySQLCore.compile_for_driver` call, and lose only the statement-cache call.
- `pipeline.py`.
- The SQLSpec adapter (t14).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/mysql/_sync.py:239-560` holds the query methods; each has the `compile_for_driver` plus `_lookup_or_cache_statement` pair.
- `src/py/vertebra/db/adapters/mysql/_sync.py:560-620` holds `_get_client`, `_refresh_client_after_auth_error`, and `_run_with_client`.
- `src/py/vertebra/db/adapters/mysql/core.py:142-178` and `:322-420` hold the compile helpers.

## Contracts & Edge Cases

- `cursor` takes keyword-only `lazy_begin: bool = False` and passes it to the client cursor. The driver's own query methods use the default.
- Add `cursor(self) -> SyncMySQLCursor` on the sync driver and `cursor(self) -> AsyncMySQLCursor` on the async driver. Both call `_ensure_open()` and return `self._get_client().cursor()`.
- Add `_execute_cursor(self, sql, parameters)` that runs `client.cursor().execute(sql, parameters or ())` inside `_run_with_client`, so the authentication retry still applies. The async form awaits it.
- `execute` returns `max(cursor.rowcount, 0)`. `execute_with_description` returns `(cursor.fetchall(), names)` with names from `cursor.description`, or `([], ())` when it is `None`. `execute_tuples` returns `cursor.fetchall()` or `[]`.
- `select` hydrates through `self.hydrate_rows(rows, names, schema_type)` for every `schema_type`, including `None`. The separate dict-row branch and its `client.fetch_all` call are removed.
- `select_one`, `select_one_or_none`, `select_many`, `select_value`, `select_value_or_none`, and `select_with_total` are rewritten on `_execute_cursor` and keep their return types and their `NotFoundError` behavior.
- `execute_many` calls `client.cursor().executemany(sql, parameter_sets)` and returns `max(cursor.rowcount, 0)`.
- The `lastrowid` property reads the last cursor's `lastrowid` stored on the driver after each `execute`, in place of the client attribute.
- Lease: `_get_client` calls `self._attach_checkout_lease(client)` once, right after it creates or refreshes the client. The async driver awaits its acquire helper and calls `client.attach_lease(...)`. The driver stores no lease. `close` and `_refresh_client_after_auth_error` call `client.release_lease()`. A refused acquire attaches nothing and the driver proceeds. A driver dropped without `close()` releases through the client's guard. The lease is held for the life of the client, so an idle open connection occupies a broker slot. `_run_with_client` loses its `use_broker_lease` parameter and both lease calls. When the driver has no broker, no lease code runs.
- `_run_with_client` no longer wraps errors with `MySQLCore.map_exception` for cursor calls: the cursor already raises a `DatabaseError` subclass, which `map_exception` returns unchanged. The wrap stays for the non-cursor native calls listed under out of scope.
- Delete every `_lookup_or_cache_statement` call in both files, including those in the out-of-scope methods.
- `MySQLCore.compile_for_driver(sql, parameters)` keeps its signature. Its body becomes one call to the native `compile_binds` resolved through `MySQLCore.require_native_module()`. `None` or empty parameters still return `(sql, ())` without calling the extension.
- Delete `MySQLCore._compile_named_mysql_binds`, `MySQLCore._rewrite_placeholders_outside_literals`, `MySQLCore._consume_literal_or_comment`, and `MySQLCore.coerce_param`, plus the module function `coerce_mysql_param`.
- `MySQLCore.compile_statement` and the module function `compile_mysql_statement` offer the `format` target style. Run `git grep -n 'compile_statement\|compile_mysql_statement' -- src/py`. With no caller outside `core.py` and tests, delete both. With a caller, keep both, rewrite `compile_statement` so the `qmark` branch calls `compile_for_driver` and the `format` branch replaces `?` with `%s` in the returned text, and name the caller in the commit message.
- Tests that asserted statement-cache registration or a deleted private helper are deleted. Tests on `compile_for_driver` output stay. Tests that assert a `vertebra._vertebra` error class from a query method stay unchanged.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select dicts | `driver.select('SELECT 1 AS a')` | `[{'a': 1}]` |
| Select schema | `select(..., schema_type=Row)` | List of `Row` |
| Tuples and names | `execute_with_description('SELECT 1 AS a, 2 AS b')` | `([(1, 2)], ('a', 'b'))` |
| Empty result names | `execute_with_description` on a zero-row select | `([], ('a', 'b'))` |
| Row count | `execute` of an `UPDATE` on two rows | `2` |
| lastrowid | `execute` of an `INSERT`, then `driver.lastrowid` | New id |
| Named parameters | `select('SELECT :a AS a', {'a': 5})` | `[{'a': 5}]` |
| execute_many | Three sets | `3` |
| No broker | Ten statements with `broker=None` | Recording broker double never called |
| One lease per client | Ten statements with a recording broker | One acquire; one release at `close()` |
| Lease across auth refresh | Token provider and one rejection | Release then acquire around the refresh; statement succeeds |
| Acquire refused at connect | Broker double whose `acquire` raises `RuntimeError`; connect, then run a statement | The statement succeeds; no `attach_lease` call; zero releases at `close()` |
| Driver dropped without close | Recording broker; connect, run a statement, `del driver`, `gc.collect()` | One release recorded |
| Error class | Duplicate key through `execute` | `IntegrityError` |
| Async | The same cases through `AsyncMySQLDriver` | Same results |

## Verification

- `uv run --no-sync pytest src/py/tests/unit/db/test_mysql_driver.py -q`
- `uv run --no-sync ruff check src/py/vertebra/db/adapters/mysql/_sync.py src/py/vertebra/db/adapters/mysql/_async.py src/py/vertebra/db/adapters/mysql/core.py`
- `uv run --no-sync mypy src/py/vertebra/db/adapters/mysql/_sync.py src/py/vertebra/db/adapters/mysql/_async.py src/py/vertebra/db/adapters/mysql/core.py`
- `test -z "$(git grep -n '_lookup_or_cache_statement' -- src/py/vertebra/db/adapters/mysql)"`
- `test -z "$(git grep -n 'use_broker_lease' -- src/py/vertebra/db/adapters/mysql)"`
- `test -z "$(git grep -n '_compile_named_mysql_binds' -- src/py)"`

## Acceptance Criteria

- [ ] No file under the MySQL adapter package calls `_lookup_or_cache_statement`.
- [ ] `core.py` holds no placeholder scanning code.
- [ ] Lease tests use a recording broker double and assert call counts.
- [ ] Arrow, script, and multi-result-set tests pass unchanged.

## Notes & Discoveries

- **Blocked by upstream contract gap in `native-cursor-data-path:network:t12` (`src/rs/vertebra-mysql/src/cursor.rs`)**: `extract_binds` (`src/rs/vertebra-mysql/src/cursor.rs:480-494, 525-532`) returns `Cow::Borrowed(sql)` for positional sequences (`tuple`/`list`/iterable) without rewriting `%s` placeholders to `?`. Previously, `SyncMySQLClient` and `AsyncMySQLClient` called `normalize_mysql_placeholders(sql)` (`src/rs/vertebra-mysql/src/client.rs:297`), which rewrote `%s`, `%(name)s`, and `:name` to `?`. Moving `SyncMySQLDriver` and `AsyncMySQLDriver` onto `client.cursor().execute(sql, bound)` and delegating `MySQLCore.compile_for_driver` to `vertebra._mysql.compile_binds` therefore causes 17 existing tests in `src/py/tests/unit/db/test_mysql_driver.py` that pass `%s` positional placeholders to fail with `OperationalError: MySQL driver error: Statement takes 0 parameters but N was supplied.` (plus the `%s` case of `test_compile_for_driver_preserves_literals_comments_and_backticks`). All 293 other tests in `test_mysql_driver.py` (including all 6 new t13 cursor query matrix, broker lease lifecycle, and auth-refresh lease tests using `?`/`:name`) pass, and `ruff`, `mypy`, and all three `git grep` checks pass.
- **Unblock condition**: Update `src/rs/vertebra-mysql/src/cursor.rs` (`extract_binds` / `compile_binds` / positional plan cache) to rewrite `%s` placeholders outside literals and comments to `?` (matching `normalize_mysql_placeholders in protocol.rs`), rebuild `_mysql`, and re-run `pytest src/py/tests/unit/db/test_mysql_driver.py -q`.
- **Minor scope note**: Deleting `coerce_mysql_param` and `compile_mysql_statement` from `src/py/vertebra/db/adapters/mysql/core.py` also required removing their re-exports from `src/py/vertebra/db/adapters/mysql/__init__.py` so `mypy` and `pyright` pass.
