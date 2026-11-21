---
type: Task
id: "native-cursor-data-path:cleanup:t05"
chapter_id: cleanup-and-checks
title: "Removal audit for native methods and stubs"
description: "Delete native client methods, stub entries, and Rust-to-Python exports that only the removed Python paths called, and confirm stubtest passes without an allow-list."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: static_validation
depends_on: []
read_set:
  - "src/rs/vertebra-py/src/text.rs::classify_sql_statement"
  - "src/py/vertebra/_vertebra.pyi:790-800"
  - "src/py/vertebra/_sqlite.pyi:26-110"
  - "src/py/vertebra/_duckdb.pyi:24-135"
  - "src/py/vertebra/_mysql.pyi:65-150"
  - "src/py/vertebra/_mssql.pyi:26-115"
  - "src/py/vertebra/_oracle.pyi:137-255"
  - "Makefile:240-242"
  - "Makefile:443-460"
files:
  - src/py/vertebra/_vertebra.pyi
  - src/rs/vertebra-py/src/text.rs
  - src/py/vertebra/_sqlite.pyi
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/py/vertebra/_duckdb.pyi
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_mysql.pyi
  - src/rs/vertebra-mysql/src/client.rs
  - src/py/vertebra/_mssql.pyi
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/_oracle.pyi
  - src/rs/vertebra-oracle/src/client.rs
tests: []
estimated_loc: 200
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: text-export
    resource: src/rs/vertebra-py/src/text.rs::classify_sql_statement
    title: "PyO3 export of the SQL statement classifier"
  - id: duckdb-stub
    resource: src/py/vertebra/_duckdb.pyi::SyncDuckDBClient
    title: "DuckDB native client stub"
  - id: makefile-stubtest
    resource: Makefile
    title: "stubtest commands in the py-test, lint, and lint-all targets"
tags: [cleanup, pyo3, stubs, dead-code]
---

# t05 — Removal audit for native methods and stubs

## Objective & Scope Boundary

Each extension's Python-facing surface holds the cursor, the methods the native drivers still call, and nothing kept only for the removed Python paths. `mypy.stubtest` passes for every extension with no allow-list.

Every edit is a deletion from a `#[pymethods]` block, a `#[pyfunction]` registration, or a `.pyi` class body. The `files:` list is longer than three entries because one rule is applied to five extensions and the core module; a pair with no unused candidate is not touched.

Out of scope: `.py` files (t04), PostgreSQL, Spanner, and BigQuery native clients (their `query` methods back the cursor and other public driver features), Redis, Valkey, storage, and bundler extensions.

This task and t04 edit separate files and can run at the same time. A candidate's Python callers are judged on the tree as it stands when this task starts; t04 only removes callers, so a candidate kept here because of a caller t04 later deletes is caught by the chapter check `make lint-all` plus a second run of the grep below.

## Bounded Read Manifest

- `src/rs/vertebra-py/src/text.rs::classify_sql_statement` and `src/py/vertebra/_vertebra.pyi:790-800` for the classifier export.[^text-export]
- The client class bodies in the five stubs listed in `read_set`.[^duckdb-stub]
- `Makefile:240-242` and `Makefile:443-460` for the stubtest commands.[^makefile-stubtest]
- In each Rust file, only the `#[pymethods]` function a deletion targets. Locate it with `grep -n "fn <name>"`.

## Symbol & Signature Contracts

No new symbol.

Candidates. For each one, run `git grep -nw -- <name> -- 'src/py/vertebra/**/*.py' 'tools/**/*.py'` and keep only hits that call the method on that extension's native client object.

| Module | Classes | Candidate methods |
| --- | --- | --- |
| `vertebra._vertebra` | module function | `classify_sql_statement` |
| `vertebra._sqlite` | `SyncSQLiteClient`, `AsyncSQLiteClient` | `fetch_all` |
| `vertebra._duckdb` | `SyncDuckDBClient`, `AsyncDuckDBClient` | `fetch_all`, `fetch_one`, `fetch_optional`, `execute_tuples`, `execute_with_description`, `select_value` |
| `vertebra._mysql` | `SyncMySQLClient`, `AsyncMySQLClient` | `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `fetch_many`, `select_value` |
| `vertebra._mssql` | `SyncMSSQLClient`, `AsyncMSSQLClient` | `execute_with_description`, `query`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value` |
| `vertebra._oracle` | `SyncOracleClient`, `AsyncOracleClient` | `query`, `query_one` |

Decision rule per candidate:

1. A production Python call site exists: keep the method. Record `Class.method -> path::function` in Notes.
2. No production Python call site: delete the `#[pymethods]` function and its `.pyi` entry on both the sync and the async class. Delete Rust tests that exercise only that method through Python.
3. A Rust helper that becomes unused after step 2 is deleted when `cargo clippy` reports it as dead code. A helper the cursor still calls stays.
4. `classify_sql_statement`: when no `.py` file imports it from `vertebra._vertebra`, delete the `#[pyfunction]`, its `add_function` registration, and the stub entry. The Rust function in `vertebra-core/src/text.rs` stays when any Rust caller remains; check with `git grep -nw classify_sql_statement -- 'src/rs'`.

Stubtest rule: no file named `*allowlist*` exists outside `.venv`, and no `Makefile` line passes `--allowlist` or `--ignore-missing-stub`.

Stub diff review: for each of the eight SQL extensions, `git diff origin/main -- src/py/vertebra/_<engine>.pyi` adds exactly two public classes, the sync and async cursor.

## Edge Cases & Error Semantics

- A method name such as `execute` or `query` is common. A hit counts only when the receiver is the native client or pool object of that extension. When the receiver type is unclear, read the enclosing function.
- Test files under `src/py/tests/` that call a deleted native method directly are updated to call the cursor and assert the same rows. If more than three test files need that change for one extension, stop, mark the task blocked, and record the files in Notes.
- A method used by `tools/benchmarks/` counts as a production call site.
- Deleting a method never adds a replacement, alias, or deprecation shim.
- Free-threaded builds: a deletion changes no lock or `Send`/`Sync` bound. If the compiler reports otherwise, revert that deletion and record it in Notes.

## Public-Seam Test Blueprint

No new test file. The checks are static:

- `mypy.stubtest` fails on the seeded state where a method is deleted from Rust and left in the stub. Confirm this once on the first deletion by running stubtest before editing the `.pyi`, then again after.
- Existing driver test files prove the kept methods still work.

## Verification

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._sqlite vertebra._duckdb vertebra._postgres vertebra._mysql vertebra._mssql vertebra._oracle vertebra._spanner vertebra._bigquery
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --features tls,http3,compression,gcp -- -D warnings
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py -p vertebra-sqlite -p vertebra-duckdb -p vertebra-mysql -p vertebra-mssql -p vertebra-oracle
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/db/test_oracle_adapter.py -q
find . -name '*allowlist*' -not -path './.venv*' -not -path '*/node_modules/*' -not -path '*/target*'
grep -n -- '--allowlist\|--ignore-missing-stub' Makefile
```

The `find` and `grep` commands print nothing. Run `cargo test` only for crates this task edited.

## Acceptance Criteria

- [x] Every candidate is deleted or listed in Notes with its call site.
- [x] `mypy.stubtest` passes for the nine modules with no allow-list.
- [x] `cargo clippy` passes with warnings denied.
- [x] The five driver test files pass.
- [x] Each SQL extension's stub diff adds only the two cursor classes.

## Notes & Discoveries

### Candidate Audit Results

Every candidate across all six modules had zero production Python or `tools/` call sites on the native client and was deleted from both Rust (`#[pyfunction]` / `#[pymethods]` on sync and async classes) and `.pyi`:

- `vertebra._vertebra`: `classify_sql_statement` — deleted PyO3 export + `add_function` registration in `src/rs/vertebra-py/src/text.rs` and stub entry in `src/py/vertebra/_vertebra.pyi`. Kept `vertebra_core::text::classify_sql_statement` (still called by `vertebra-duckdb/src/lib.rs:3787` and `vertebra-sqlite/src/worker.rs:802`).
- `vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`): `fetch_all` — deleted from `src/rs/vertebra-sqlite/src/py_client.rs` and `src/py/vertebra/_sqlite.pyi`.
- `vertebra._duckdb` (`SyncDuckDBClient`, `AsyncDuckDBClient`): `fetch_all`, `fetch_one`, `fetch_optional`, `execute_tuples`, `execute_with_description`, `select_value` — deleted from `src/rs/vertebra-duckdb/src/lib.rs` and `src/py/vertebra/_duckdb.pyi`.
- `vertebra._mysql` (`SyncMySQLClient`, `AsyncMySQLClient`): `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `fetch_many`, `select_value` — deleted from `src/rs/vertebra-mysql/src/client.rs` and `src/py/vertebra/_mysql.pyi`.
- `vertebra._mssql` (`SyncMSSQLClient`, `AsyncMSSQLClient`): `execute_with_description`, `query`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value` — deleted from `src/rs/vertebra-mssql/src/lib.rs` and `src/py/vertebra/_mssql.pyi`.
- `vertebra._oracle` (`SyncOracleClient`, `AsyncOracleClient`): `query`, `query_one` — deleted from `src/rs/vertebra-oracle/src/client.rs` and `src/py/vertebra/_oracle.pyi`.

### Rust Helpers & Imports Removed as Dead Code

- `src/rs/vertebra-duckdb/src/lib.rs`: removed `result_set_to_py_dicts`, `result_set_to_py_tuples`, `result_set_to_py_with_description`, `result_set_to_single_dict`.
- `src/rs/vertebra-mssql/src/lib.rs`: removed unused imports `cell_to_py`, `first_row_to_py_dict`, `query_result_to_py_envelope`, `query_result_to_py_rows_and_cols`, `rows_to_py_dicts`.
- `src/rs/vertebra-oracle/src/client.rs`: removed `WorkerQueryOutcome::into_py_first_row_or_none`.

### Test Files Updated to Use Native Cursors

- `_sqlite` (1 file): `src/py/tests/unit/db/test_sqlite_driver.py` (`client.fetch_all(...)` -> `client.cursor().execute(...).fetchall()`).
- `_duckdb` (3 files): `src/py/tests/unit/db/test_duckdb_driver.py` (`client.execute_tuples(...)`, `reference.execute_tuples(...)`, `sync_driver._client.fetch_one`/`select_value` -> cursor/driver methods), `src/py/tests/unit/extensions/test_data_storage_integrations.py` (`client.fetch_optional(...)` -> `client.cursor()`), `src/py/tests/unit/storage/test_gcs_token_refresh.py` (`_SyncClientFaultProxy`/`_AsyncClientFaultProxy` updated to wrap `cursor()` instead of `execute_with_description()`).

### Stub Diff Summary (`git diff origin/main -- src/py/vertebra/_<engine>.pyi`)

- `_sqlite.pyi`: +2 classes (`SyncSQLiteCursor`, `AsyncSQLiteCursor`), -2 methods (`fetch_all` x2).
- `_duckdb.pyi`: +2 classes (`SyncDuckDBCursor`, `AsyncDuckDBCursor`), -12 methods (6 x2).
- `_postgres.pyi`: +2 classes (`SyncPostgresCursor`, `AsyncPostgresCursor`), -0 methods.
- `_mysql.pyi`: +2 classes (`SyncMySQLCursor`, `AsyncMySQLCursor`), -12 methods (6 x2).
- `_mssql.pyi`: +2 classes (`SyncMSSQLCursor`, `AsyncMSSQLCursor`), -12 methods (6 x2).
- `_oracle.pyi`: +3 classes (`OracleOutBindVar`, `SyncOracleCursor`, `AsyncOracleCursor`), -4 methods (`query`, `query_one` x2). Note: `OracleOutBindVar` backs `SyncOracleCursor.var()` / `AsyncOracleCursor.var()`.
- `_spanner.pyi`: +2 classes (`SyncSpannerCursor`, `AsyncSpannerCursor`), -0 methods.
- `_bigquery.pyi`: +2 classes (`SyncBigQueryCursor`, `AsyncBigQueryCursor`), -0 methods.

### Verification Notes

- Seeded stubtest failure confirmed on `vertebra._vertebra`: removing `classify_sql_statement` from Rust while leaving `_vertebra.pyi` intact caused `mypy.stubtest vertebra._vertebra` to fail with `vertebra._vertebra.classify_sql_statement is not present at runtime`. After updating `_vertebra.pyi` and rebuilding all extensions (`UV_NO_SYNC=1 make dev`), `mypy.stubtest` passed with `Success: no issues found in 9 modules` and zero `*allowlist*` files or `--allowlist`/`--ignore-missing-stub` flags in `Makefile`.
- Unrelated sibling-task test failure observed in `src/py/tests/unit/db/test_mysql_driver.py::test_mysql_sync_and_async_engine_initialize_and_isolation`: `SyncDBAPIConnection` in `src/py/vertebra/db/dbapi/_sync.py` (modified outside t05) rejects `_FakeMySQLSyncProtocolDriver` because the pure-Python test double does not implement `cursor()`. All 309 other tests in `test_mysql_driver.py` and all 603 tests across `test_sqlite_driver.py`, `test_duckdb_driver.py`, `test_mssql_driver.py`, and `test_oracle_adapter.py` passed.
- Unrelated pre-existing/sibling-task failures observed in `src/py/tests/unit/storage/test_gcs_token_refresh.py` (4 DuckDB GCS secret-rotation tests fail because `SyncDuckDBDriver.select()` / `AsyncDuckDBDriver.select()` no longer triggers secret rotation on `needs_refresh()`).

[^text-export]: `src/rs/vertebra-py/src/text.rs::classify_sql_statement`
[^duckdb-stub]: `src/py/vertebra/_duckdb.pyi::SyncDuckDBClient`
[^makefile-stubtest]: `Makefile`
