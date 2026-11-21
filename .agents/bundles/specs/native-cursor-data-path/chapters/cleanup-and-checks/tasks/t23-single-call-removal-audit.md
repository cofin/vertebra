---
type: Task
id: "native-cursor-data-path:cleanup:t23"
chapter_id: cleanup-and-checks
title: "Removal audit after the single-call work"
description: "Run the R9 checks again over what single-call-paths supersedes: confirm the helpers its worksheets delete are gone, delete native client and pool methods that lost their last production caller, and record why each remaining SQLSpec hook stays."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-cursor-data-path:single:t27"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t04-python-removal-audit.md:150-235"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t05-native-removal-audit.md:60-183"
  - "src/py/vertebra/_sqlite.pyi::SyncSQLiteClient"
  - "src/py/vertebra/_sqlite.pyi::AsyncSQLiteClient"
  - "src/py/vertebra/_duckdb.pyi::SyncDuckDBClient"
  - "src/py/vertebra/_duckdb.pyi::AsyncDuckDBClient"
  - "src/py/vertebra/_postgres.pyi::SyncPostgresClient"
  - "src/py/vertebra/_postgres.pyi::AsyncPostgresClient"
  - "src/py/vertebra/_postgres.pyi::SyncPostgresPool"
  - "src/py/vertebra/_postgres.pyi::AsyncPostgresPool"
  - "src/py/vertebra/_oracle.pyi::SyncOracleClient"
  - "src/py/vertebra/_oracle.pyi::AsyncOracleClient"
  - "src/py/vertebra/_oracle.pyi::SyncOraclePool"
  - "src/py/vertebra/_oracle.pyi::AsyncOraclePool"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver"
  - "src/py/vertebra/extensions/sqlspec/_driver.py::BaseAsyncDriver"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection"
  - "src/py/vertebra/db/dbapi/_async.py::AsyncDBAPIConnection"
  - "Makefile:240-242"
files:
  - src/py/vertebra/_sqlite.pyi
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/py/vertebra/_duckdb.pyi
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/python_pool.rs
  - src/py/vertebra/_oracle.pyi
  - src/rs/vertebra-oracle/src/client.rs
  - src/rs/vertebra-oracle/src/pool.rs
  - src/py/vertebra/extensions/sqlspec/_driver.py
tests:
  - src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py
estimated_loc: 200
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T17:07:19Z
sources:
  - id: single-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md
    title: "Chapter that moves four drivers onto the single-call methods"
  - id: native-audit
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t05-native-removal-audit.md
    title: "Decision rule and format for the native method audit"
  - id: python-audit
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t04-python-removal-audit.md
    title: "Decision rule and format for the Python audit"
  - id: purity-tests
    resource: src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py::test_native_cursor_superseded_symbols_removed
    title: "Parametrized removed-symbol test that t04 added"
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec adapter base with the hook methods and the cache-hit executor"
tags: [cleanup, pyo3, stubs, dead-code]
---

# t23 — Removal audit after the single-call work

## Objective & Scope Boundary

After `single-call-paths`, nothing it superseded is left. The helpers its worksheets delete are gone. A native client or pool method that no production code calls any more is deleted with its stub entry. Each SQLSpec hook that stays has a recorded caller.[^single-spec]

This is the R9 audit of t04 and t05, run once more over a later change. It uses their decision rule and their notes format.[^native-audit][^python-audit]

Every edit is a deletion, plus rows added to one existing parametrized test. The `files:` list names what the audit may delete from. It is longer than three entries because one rule is applied to four extensions; a file with no unused candidate is not touched.

Out of scope: the MySQL, SQL Server, Spanner, and BigQuery native clients and drivers, whose query methods still run on the cursor; the cursor classes; the four single-call methods; Redis, Valkey, storage, and bundler extensions; knowledge pages (t21, t22); and any file not listed in `files:` or `tests:`. A leftover found in another file is recorded in Notes and reported, not edited.

## Bounded Read Manifest

- The `single-call-paths` chapter spec and its 31 worksheets under `tasks/`. In each worksheet read the heading "Deleted in this task" where it exists, and every line that says a name is deleted, removed, or must print nothing. Build the name list from them.
- `t04` lines `150-235` and `t05` lines `60-183` for the decision rule, the review rule for the dead-code scan, and the notes format.
- The twelve class bodies in the four stubs listed in `read_set`.
- `BaseSyncDriver` and `BaseAsyncDriver` for the hook methods and the executor registration.[^sqlspec-base]
- `SyncDBAPIConnection` and `AsyncDBAPIConnection` for the bound cursor factory.
- `Makefile:240-242` for the stubtest command.
- In each Rust file, only the `#[pymethods]` function a deletion targets. Locate it with `grep -n "fn <name>"`.

## Symbol & Signature Contracts

No new symbol.

### Must be gone

Each command prints nothing. A match is deleted when it sits in a file under `files:`. A match elsewhere stops the task with the path, because the owning `single-call-paths` task did not finish.

| Names | Command scope | Deleted by |
| --- | --- | --- |
| `_execute_cursor`, `_execute_on_pool` | `src/py/vertebra/db/adapters/postgres` | `single:t13`, `single:t14` |
| `_execute_cursor` | `src/py/vertebra/db/adapters/oracle` | `single:t17`, `single:t18` |
| `_lastrowid` | `src/py/vertebra/db/adapters/sqlite` | `single:t06` |
| `fn ready_awaitable` | `src/rs` | `single:t05`, `single:t08`, `single:t16`, `single:t19`, `single:t21`, `single:t22` |
| The `Run` variant of `WorkerTask`, `end_op`, `ensure_open_and_not_cancelled` | `src/rs/vertebra-sqlite/src`, with the pattern from the `single:t03` worksheet | `single:t03` |
| `sql_owned` | `src/rs/vertebra-sqlite/src/py_client.rs` | `single:t02` |
| `_native_cursor` | `src/py/vertebra/db/dbapi` | `single:t25`, `single:t26` |
| `def cursor` | `src/py/vertebra/db/dbapi/_sync.py`, `src/py/vertebra/db/dbapi/_async.py`, inside the two connection classes | `single:t25`, `single:t26` |

Add every further name the 31 worksheets say they delete to this table in Notes, with its scope and its worksheet, and run the same check for it.

`_execute_cursor` stays in the MySQL, SQL Server, Spanner, and BigQuery adapters. Those scopes are not searched.

### Delete when unused: native client and pool methods

Classes: `SyncSQLiteClient`, `AsyncSQLiteClient`, `SyncDuckDBClient`, `AsyncDuckDBClient`, `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`.

Candidates: every public method in each stub class body that runs a statement and returns rows, a row, a value, or a count. The list is read from the stub when the task starts.

Never a candidate: `cursor`, `fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`, `executemany` and other batch methods, transaction control, lease methods, `close`, `connect`, `acquire`, `release`, Arrow, COPY, pipeline, portal, listener, and notification methods, and properties.

For each candidate, run `git grep -nw -- <name> -- 'src/py/vertebra/**/*.py' 'tools/**/*.py'` and keep only hits whose receiver is that extension's native client or pool object.

Decision rule per candidate, as in t05:

1. A production Python call site exists: keep the method. Record `Class.method -> path::function` in Notes.
2. No production Python call site: delete the `#[pymethods]` function and its `.pyi` entry on both the sync and the async class. Delete Rust tests that exercise only that method through Python.
3. A Rust helper that becomes unused after step 2 is deleted when `cargo clippy` reports it as dead code. A helper the cursor or a single-call method still calls stays.

### Keep and record: SQLSpec hook path

The cache-hit executor serves cached statements. Upstream SQLSpec still calls the hook methods by name on every other path.

| Name on `BaseSyncDriver` and `BaseAsyncDriver` | Rule |
| --- | --- |
| `with_cursor`, `collect_rows`, `resolve_rowcount`, `dispatch_execute`, `handle_database_exceptions` | Keep. Record in Notes the upstream function that calls each one, found with `grep -rn "<name>" .venv/lib/python3.12/site-packages/sqlspec/driver/` |
| Any other private helper in `_driver.py` that only the hook methods called before the executor | Delete when `git grep -nw` finds no reference outside its definition and tests |
| An adapter subclass override of a hook under `src/py/vertebra/extensions/sqlspec/adapters/` | Keep. Record it. These files are not in `files:` |

### Removed-symbol test

Extend the parametrized test `test_native_cursor_superseded_symbols_removed` with one row per must-be-gone Python name that is an attribute of a class or module. The rows are `_execute_cursor` and `_execute_on_pool` on both PostgreSQL drivers, and `_execute_cursor` on both Oracle drivers. Add `_native_cursor` in the `__slots__` of both DB-API connection classes. Use the row shape the test already has. Add no loop and no new test function.[^purity-tests]

### Stubtest rule

No file named `*allowlist*` exists outside `.venv`, and no `Makefile` line passes `--allowlist` or `--ignore-missing-stub`. `mypy.stubtest` runs on nine modules: `vertebra._vertebra`, `_sqlite`, `_duckdb`, `_postgres`, `_mysql`, `_mssql`, `_oracle`, `_spanner`, and `_bigquery`.

## Edge Cases & Error Semantics

- A method name such as `execute` or `query` is common. A hit counts only when the receiver is the native client or pool object of that extension. When the receiver type is unclear, read the enclosing function.
- A method used by `tools/benchmarks/` counts as a production call site.
- A PostgreSQL client `query` method backs Arrow, COPY, pipelines, and metadata. It is kept whenever one such caller remains, and the caller is recorded.
- Test files under `src/py/tests/` that call a deleted native method directly are changed to call the matching single-call method or the cursor and assert the same rows. If more than three test files need that change for one extension, stop, mark the task blocked, and record the files in Notes.
- Deleting a method never adds a replacement, alias, re-export, or deprecation shim. No fallback branch is added.
- A test double that still defines a deleted method is left alone unless a test calls it. Unused double methods are not this task's work.
- Free-threaded builds: a deletion changes no lock and no `Send` or `Sync` bound. If the compiler reports otherwise, revert that deletion and record it in Notes.
- The dead-code scan reports hook methods called by name. Each is explained in Notes with one of the three reasons t04 uses: protocol or interface member, framework hook called by name, or documented public API.

## Public-Seam Test Blueprint

| Check | Expected |
| --- | --- |
| Each must-be-gone command | Prints nothing |
| `test_native_cursor_superseded_symbols_removed` | Passes for every row, including the new ones |
| `ruff check --select F401,F811,F841` on the listed paths | No finding |
| `mypy.stubtest` on the nine modules | Success, with no allow-list |
| `cargo clippy` with warnings denied | No dead-code warning in the four crates |
| Dead-code scan | Every item in a file this spec edited is deleted or explained in Notes |
| Driver test files for the four engines | Pass unchanged, except tests updated under the three-file rule |

`mypy.stubtest` fails on the seeded state where a method is deleted from Rust and left in the stub. Confirm this once on the first deletion by running stubtest before editing the `.pyi`, then again after. When the audit deletes nothing, record that and skip the seeded check.

## Verification

```bash
make dev
git grep -n -e '_execute_cursor' -e '_execute_on_pool' -- src/py/vertebra/db/adapters/postgres
git grep -n '_execute_cursor' -- src/py/vertebra/db/adapters/oracle
git grep -n '_lastrowid' -- src/py/vertebra/db/adapters/sqlite
git grep -n 'fn ready_awaitable' -- src/rs
git grep -n 'sql_owned' -- src/rs/vertebra-sqlite/src/py_client.rs
git grep -n '_native_cursor' -- src/py/vertebra/db/dbapi
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py -p no:xdist -q
uv run --no-sync --frozen ruff check --select F401,F811,F841 src/py/vertebra/db src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/tests tools/benchmarks
uvx vulture src/py/vertebra/db src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy --min-confidence 80
uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._sqlite vertebra._duckdb vertebra._postgres vertebra._mysql vertebra._mssql vertebra._oracle vertebra._spanner vertebra._bigquery
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --features tls,http3,compression,gcp -- -D warnings
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite -p vertebra-duckdb -p vertebra-postgres -p vertebra-oracle
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/extensions/test_sqlspec_driver_dispatch.py -p no:xdist -q
uv run --no-sync --frozen mypy
find . -name '*allowlist*' -not -path './.venv*' -not -path '*/node_modules/*' -not -path '*/target*'
grep -n -- '--allowlist\|--ignore-missing-stub' Makefile
make lint-all
```

The six `git grep` lines, the `find`, and the last `grep` print nothing. Run the `WorkerTask` check with the command the `single:t03` worksheet gives.

## Acceptance Criteria

- [ ] Every must-be-gone check prints nothing, including the names added from the worksheets.
- [ ] Each native candidate is deleted with its stub entry, or kept with its call site in Notes.
- [ ] Each SQLSpec hook that stays has its upstream caller in Notes.
- [ ] `mypy.stubtest` passes on the nine modules with no allow-list, and `ruff`, `cargo clippy`, and `mypy` report nothing.
- [ ] Every dead-code scan item in a file this spec edited is deleted or explained.
- [ ] No alias, re-export, or fallback was added, and no file outside `files:` and `tests:` has a diff from this task.

## Notes & Discoveries

### Must-be-gone checks

All must-be-gone symbols returned zero matches in their target scopes:

- `_execute_cursor`, `_execute_on_pool` in `src/py/vertebra/db/adapters/postgres`: 0 matches.
- `_execute_cursor` in `src/py/vertebra/db/adapters/oracle`: 0 matches.
- `_lastrowid` in `src/py/vertebra/db/adapters/sqlite`: 0 matches.
- `fn ready_awaitable` in `src/rs`: 0 matches (only unit test functions `ready_awaitable_can_be_awaited_twice` and `ready_awaitable_works_with_wait_for` in `src/rs/vertebra-core/src/py_async.rs`).
- `WorkerTask::Run`, `fn end_op`, `fn ensure_open_and_not_cancelled` in `src/rs/vertebra-sqlite/src`: 0 matches.
- `sql_owned` in sync SQLite paths (`SyncSQLiteCursor::execute`, `SyncSQLiteClient::run_once`): 0 matches (remaining occurrences in `src/rs/vertebra-sqlite/src/py_client.rs` belong to `tokio::task::spawn_blocking` async closures and batch/Arrow workers requiring `'static` `String`).
- `_native_cursor` and connection `def cursor` in `src/py/vertebra/db/dbapi/`: 0 matches.
- Removed-symbol purity test `test_native_cursor_superseded_symbols_removed` extended with 8 rows (`SyncPostgreSQLDriver._execute_cursor`, `SyncPostgreSQLDriver._execute_on_pool`, `AsyncPostgreSQLDriver._execute_cursor`, `AsyncPostgreSQLDriver._execute_on_pool`, `SyncOracleDriver._execute_cursor`, `AsyncOracleDriver._execute_cursor`, `SyncDBAPIConnection._native_cursor`, `AsyncDBAPIConnection._native_cursor`).

### Native client and pool method audit

- `SyncDuckDBClient.execute` / `AsyncDuckDBClient.execute`: kept; called by `src/py/vertebra/db/adapters/duckdb/_sync.py:598` and `_async.py:626` (`_apply_secret_ddl`).
- `SyncPostgresClient.query`, `AsyncPostgresClient.query`, `SyncPostgresClient.query_multi`, `AsyncPostgresClient.query_multi`: kept as documented public native client APIs (exercised across 11 native PostgreSQL test suites including command deadlines, COPY, portals, pipelines, listeners, and Arrow streams).
- `SyncSQLiteClient.execute`, `AsyncSQLiteClient.execute`: kept as documented public native client APIs (exercised by `src/py/tests/unit/db/test_sqlite_driver.py` for direct native `last_insert_rowid` and statement execution).
- `SyncOracleClient.execute`, `AsyncOracleClient.execute`: kept as documented public native client APIs (exercised by `test_oracle_native.py` PL/SQL + out-bind tests and `test_oracle_adapter.py`).
- `SyncPostgresPool`, `AsyncPostgresPool`, `SyncOraclePool`, `AsyncOraclePool`: zero superseded statement-running methods remain; only `fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`, and lifecycle/cursor methods are exposed.
- No method was deleted in this pass, so no seeded stubtest failure check was required.

### SQLSpec hook callers (`sqlspec/driver/`)

All five hook methods on `BaseSyncDriver` and `BaseAsyncDriver` remain required for uncached, multi-statement, and custom-override paths called by upstream `sqlspec`:

- `with_cursor`: called by `sqlspec/driver/_sync.py::_dispatch_statement_with_cursor` (lines 263, 1806) and `sqlspec/driver/_async.py`.
- `collect_rows`: called by `sqlspec/driver/_sync.py` (line 1987) and `sqlspec/driver/_async.py` (line 457).
- `resolve_rowcount`: called by `sqlspec/driver/_sync.py` (line 1999) and `sqlspec/driver/_async.py` (line 476).
- `dispatch_execute`: called by `sqlspec/driver/_sync.py` (lines 350, 385) and `sqlspec/driver/_async.py`.
- `handle_database_exceptions`: called by `sqlspec/driver/_sync.py` (lines 260, 1801, 1843, 1887) and `sqlspec/driver/_async.py`.

### Static and stubtest checks

- `mypy.stubtest` passed across all 13 extensions (`vertebra._vertebra` through `vertebra._valkey`) with zero allow-lists in the repository or `Makefile`.
- `ruff check --select F401,F811,F841` reported zero unused imports, redefinitions, or unused variables.

[^single-spec]: `.agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md`
[^native-audit]: `.agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t05-native-removal-audit.md`
[^python-audit]: `.agents/bundles/specs/native-cursor-data-path/chapters/cleanup-and-checks/tasks/t04-python-removal-audit.md`
[^purity-tests]: `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py::test_native_cursor_superseded_symbols_removed`
[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver`
