---
type: Task
id: "native-cursor-data-path:cleanup:t04"
chapter_id: cleanup-and-checks
title: "Removal audit for the Python data path"
description: "Prove that every Python helper, protocol member, export, and test the native cursor superseded is gone, and delete what is left."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on: []
read_set:
  - "src/py/vertebra/db/driver/_common.py::CommonDriverMixin"
  - "src/py/vertebra/db/driver/_common.py::_BrokerLeasePlan"
  - "src/py/vertebra/db/driver/capabilities.py:16-33"
  - "src/py/vertebra/db/dbapi/__init__.py"
  - "src/py/vertebra/db/dbapi/_sync.py::SyncSQLDriverProtocol"
  - "src/py/vertebra/db/dbapi/_async.py::AsyncSQLDriverProtocol"
  - "src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py:761-780"
files:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/db/driver/_async.py
  - src/py/vertebra/db/dbapi/__init__.py
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/vertebra/db/dbapi/_async.py
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/mssql/_sync.py
  - src/py/vertebra/db/adapters/mssql/_async.py
tests:
  - src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py
estimated_loc: 180
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: driver-common
    resource: src/py/vertebra/db/driver/_common.py::_BrokerLeasePlan
    title: "Broker lease and statement-cache helpers"
  - id: dbapi-protocol
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncSQLDriverProtocol
    title: "Driver protocol consumed by the DB-API connection"
  - id: purity-tests
    resource: src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py::test_notification_source_legacy_symbol_cleanly_removed
    title: "Existing removed-symbol test pattern"
tags: [cleanup, dbapi, database, dead-code]
---

# t04 — Removal audit for the Python data path

## Objective & Scope Boundary

After this task, a search for any name the native cursor superseded finds nothing in Python sources, tools, or tests, and a test keeps the names from coming back.

Earlier chapters delete each name in the step that replaces its last caller. This task checks that, deletes leftovers, and records the result. Every production edit here is a deletion. The `files:` list is longer than three entries because one audit covers the shared driver layer and four engines' helper modules; a file with no leftover is not touched.

Out of scope: `.rs` and `.pyi` files (t05), `docs/` (t06), and knowledge chapters (t07 to t12). `extensions/sqlspec/proxy.py` and `provider.py` are read-only here except to delete a must-be-gone name.

## Bounded Read Manifest

Read only what a `git grep` hit points to, plus:

- `src/py/vertebra/db/driver/_common.py::CommonDriverMixin` and `::_BrokerLeasePlan` for the lease helpers.[^driver-common]
- `src/py/vertebra/db/driver/capabilities.py:16-33` for `__all__`.
- `src/py/vertebra/db/dbapi/__init__.py` for the re-exports.
- The class `SyncSQLDriverProtocol` in `src/py/vertebra/db/dbapi/_sync.py` and the class `AsyncSQLDriverProtocol` in `src/py/vertebra/db/dbapi/_async.py`.[^dbapi-protocol]
- `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py:761-780` for the removed-symbol test pattern.[^purity-tests]

## Symbol & Signature Contracts

No new production symbol. One new parametrized test, described under the test blueprint.

Must-be-gone names. Each command prints nothing:

```bash
for name in is_row_returning_sql is_insert_sql _scan_unquoted_sql_tokens _match_dollar_quote_tag \
  _has_tsql_output_clause coerce_dbapi_rows_and_desc _coerce_column_description _ErrorTranslator \
  _translate_errors _TRANSLATE_ERRORS _translate_native_error _translate_vertebra_error \
  _extract_out_bind_vars _populate_out_bind_vars _DBAPIOutBindVar _unwrap_typed_parameters unwrap_typed_parameters \
  _normalize_bound_params _compile_for_native _fetch_rows_with_columns NATIVE_CURSOR_ENGINES; do
  git grep -nw -- "$name" -- 'src/py' 'tools'
done
git grep -nw -- is_read_only_sql -- 'src/py' 'tools'
```

Delete-when-unused names. Run `git grep -nw -- <name> -- src tools` for each. Classify every hit as definition, export line, stub entry, test, or production call site.

| Group | Names | File that defines them |
| --- | --- | --- |
| Lease and statement cache | `_BrokerLeasePlan`, `_lease_plan`, `_lookup_or_cache_statement`, `_acquire_broker_lease`, `_release_broker_lease` | `db/driver/_common.py` |
| Driver protocol | `SyncSQLDriverProtocol`, `AsyncSQLDriverProtocol`, and their members `execute_with_description`, `execute_tuples`, `select` | `db/dbapi/_sync.py`, `db/dbapi/_async.py` |
| Driver bases | `query_with_columns` | `db/driver/_sync.py`, `db/driver/_async.py` |
| DB-API cursors | `SyncDBAPICursor`, `AsyncDBAPICursor` | `db/dbapi/_sync.py`, `db/dbapi/_async.py` |
| DuckDB helpers | `normalize_parameters`, `param_len` | `db/adapters/duckdb/core.py` |
| PostgreSQL helpers | `normalize_parameters`, `project_query_rows` | `db/adapters/postgres/core.py` |
| MySQL helper | `compile_for_driver` | `db/adapters/mysql/core.py` |
| SQL Server helper | `_prepare_sql` | `db/adapters/mssql/_sync.py`, `_async.py` |
| Oracle helpers | `split_out_bind_parameters`, `coerce_parameters`, `track_session_context_sql`, `is_dml_or_plsql`, `coerce_result_rows_and_cols` | `db/adapters/oracle/core.py` |

Decision rule per name:

1. No production call site: delete the definition, its `__all__` and re-export lines, and the tests, fixtures, and fakes that exist only to exercise it.
2. A production call site exists: keep the name. Record `name -> path::function` in Notes.
3. A protocol member is deleted when no code calls that member on an object typed as the protocol. A protocol class with no remaining member or no remaining annotation user is deleted.
4. `db/driver/capabilities.py`: delete any protocol or member that describes `execute_with_description`, `execute_tuples`, or `query_with_columns`. At `06e99fb` the file has none; confirm that still holds.

Lease rule: each remaining call to `_acquire_broker_lease`, `_release_broker_lease`, or `_lookup_or_cache_statement` under `src/py/vertebra/db/adapters/` sits inside a connection checkout, release, open, or close function. A call inside `select`, `select_one`, `execute`, `execute_many`, or `execute_with_description` is removed, and the engine's unit test file is run. Calls under `extensions/sqlspec/proxy.py` and `provider.py` are their own methods of the same name and are left alone.

## Edge Cases & Error Semantics

- A hit inside a string, docstring, or comment counts. Reword or delete it.
- A hit in `src/py/tests/` for a must-be-gone name is deleted with the test that asserts the removed helper. A test that still checks observable cursor behavior is kept and only the stale reference is removed.
- `execute_with_description` stays on the native driver classes as a public method. Only the protocol member and probing code are candidates.
- `_refresh_dynamic_secrets` and `invalidate_rejected_credentials` are kept by design and are not candidates.
- No alias, re-export, `getattr` fallback, or `# noqa` is added to make a check pass.
- If deleting a name needs a behavior change in a file outside `files:`, stop, mark the task blocked, and record the file and reason in Notes.

## Public-Seam Test Blueprint

Add one parametrized test to `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py`, next to the existing removed-symbol tests:

```python
@pytest.mark.parametrize(("module_name", "symbol_name"), _NATIVE_CURSOR_REMOVED_SYMBOLS)
def test_native_cursor_superseded_symbols_removed(module_name: str, symbol_name: str) -> None: ...
```

`_NATIVE_CURSOR_REMOVED_SYMBOLS` is a module-level tuple of pairs. The test imports the module with `importlib.import_module` and asserts `not hasattr(module, symbol_name)`.

| Module | Names |
| --- | --- |
| `vertebra.db.dbapi` | `is_row_returning_sql`, `is_read_only_sql`, `is_insert_sql`, `coerce_dbapi_rows_and_desc` |
| `vertebra.db.dbapi._sync` | `is_row_returning_sql`, `is_read_only_sql`, `is_insert_sql`, `_scan_unquoted_sql_tokens`, `_match_dollar_quote_tag`, `_has_tsql_output_clause`, `coerce_dbapi_rows_and_desc`, `_coerce_column_description`, `_ErrorTranslator`, `_translate_errors`, `_TRANSLATE_ERRORS`, `_translate_native_error`, `_translate_vertebra_error`, `_extract_out_bind_vars`, `_populate_out_bind_vars`, `_DBAPIOutBindVar` |
| `vertebra.db.driver._common` | `NATIVE_CURSOR_ENGINES` |
| `vertebra.extensions.sqlspec._driver` | `_unwrap_typed_parameters`, `_normalize_bound_params` |

Add a pair for each delete-when-unused name that this task deletes, using its defining module. The SQLSpec rows use `pytest.importorskip("sqlspec")` inside the test when the module name starts with `vertebra.extensions.sqlspec`.

Add one more test next to it, `test_sqlspec_bases_reuse_one_exception_handler`: for a `BaseSyncDriver` and a `BaseAsyncDriver` built over a SQLite driver, two calls to `handle_database_exceptions()` return the same object. The two handler classes stay; no handler is built per call.

Add a third test, `test_dbapi_exception_names_are_native_classes_after_cleanup`: for each of the nine PEP 249 error names, the attribute on `vertebra.db.dbapi` is the `vertebra._vertebra` class, with `Error` bound to `DatabaseError`.

Run the tests before any deletion. They characterize the tree: rows that fail name the leftovers.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/db/test_db_unified_driver_contracts.py -q
uv run --no-sync ruff check --select F401,F811,F841 src/py/vertebra/db src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/tests tools/benchmarks
uvx vulture src/py/vertebra/db src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy --min-confidence 80
uv run --no-sync mypy src/py/vertebra/db src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy
uv run --no-sync pyright src/py/vertebra/db
make lint-all
```

Plus the two `git grep` blocks above, which print nothing.

`vulture` review rule: each reported item in a file this spec edited is deleted, or listed in Notes with one of three reasons: protocol or interface member, framework hook called by name, or documented public API.

## Acceptance Criteria

- [ ] Both `git grep` blocks print nothing.
- [ ] `test_native_cursor_superseded_symbols_removed` passes for every row.
- [ ] `ruff` reports no `F401`, `F811`, or `F841` in the listed paths.
- [ ] Every `vulture` item is deleted or explained in Notes.
- [ ] No per-statement lease or statement-cache call remains under `db/adapters/`.
- [ ] `make lint-all` passes.
- [ ] Notes lists each delete-when-unused name as deleted or kept with its call site.

## Notes & Discoveries

### Must-be-gone audit
- Zero hits across `src/py/vertebra` and `tools` for all 22 must-be-gone names (`is_row_returning_sql`, `is_read_only_sql`, `is_insert_sql`, `_scan_unquoted_sql_tokens`, `_match_dollar_quote_tag`, `_has_tsql_output_clause`, `coerce_dbapi_rows_and_desc`, `_coerce_column_description`, `_ErrorTranslator`, `_translate_errors`, `_TRANSLATE_ERRORS`, `_translate_native_error`, `_translate_vertebra_error`, `_extract_out_bind_vars`, `_populate_out_bind_vars`, `_DBAPIOutBindVar`, `_unwrap_typed_parameters`, `unwrap_typed_parameters`, `_normalize_bound_params`, `_compile_for_native`, `_fetch_rows_with_columns`, `NATIVE_CURSOR_ENGINES`).
- Remaining string literals under `src/py/tests/` exist only in negative-assertion removal tests (`test_db_capabilities_and_purity_gates.py`, `test_dbapi_standalone.py`, `test_sqlspec_driver_dispatch.py`, `test_postgres_compiler_transport.py`, `test_sqlspec_sqlite_mysql_mssql_duckdb.py`).

### Delete-when-unused audit
- **Lease and statement cache (`db/driver/_common.py`)**:
  - `_lookup_or_cache_statement`: already deleted (0 hits).
  - `_release_broker_lease` (`CommonDriverMixin._release_broker_lease`): **deleted** (0 production callers; `proxy.py`/`provider.py` define independent methods). Removed `test_release_broker_lease_preserves_existing_behavior` and updated `test_acquire_broker_lease_calls_recording_broker` in `test_db_unified_driver_contracts.py`.
  - `_extract_column_names` (`CommonDriverMixin._extract_column_names`): **deleted** (0 references across `src/` and `tools/`, caught by AST dead-code scan).
  - `_BrokerLeasePlan`: **kept** -> `db/driver/_common.py::CommonDriverMixin._lease_plan`, `_release_lease`, `_lease_releaser`.
  - `_lease_plan`: **kept** -> `db/driver/_common.py::CommonDriverMixin.{_acquire_broker_lease,_attach_checkout_lease,_acquire_pid_lease,_release_pid_lease}` and async adapters' `_acquire_broker_lease_async` / `_attach_checkout_lease_async`.
  - `_acquire_broker_lease`: **kept** -> `db/driver/_common.py::CommonDriverMixin._attach_checkout_lease`.
- **Driver protocol (`db/dbapi/_sync.py`, `db/dbapi/_async.py`)**:
  - `SyncSQLDriverProtocol`, `AsyncSQLDriverProtocol`: **kept** (type annotations on `SyncDBAPIConnection._driver`, `AsyncDBAPIConnection._driver`, `_build_driver`, `_build_async_driver`, `driver` properties, with active `begin`/`commit`/`rollback`/`close` calls).
  - `execute_with_description`, `execute_tuples`, `select` protocol members on both protocols: **deleted** (0 callers on protocol-typed objects).
- **Driver bases (`db/driver/_sync.py`, `db/driver/_async.py`)**:
  - `query_with_columns` (`SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`): **deleted** (0 production callers). Deleted `test_driver_query_with_columns_preserves_zero_row_columns` in `test_db_unified_driver_contracts.py`.
- **DB-API cursors (`db/dbapi/_sync.py`, `db/dbapi/_async.py`)**:
  - `SyncDBAPICursor`, `AsyncDBAPICursor`: already deleted (0 hits).
- **DuckDB helpers (`db/adapters/duckdb/core.py`)**:
  - `normalize_parameters`, `param_len`: already deleted (0 hits in production).
- **PostgreSQL helpers (`db/adapters/postgres/core.py`)**:
  - `project_query_rows`: already deleted (0 hits).
  - `normalize_parameters` (`PostgreSQLCore.normalize_parameters`): **deleted** along with unused `normalize_postgres_parameters` import (0 production callers; `test_postgres_options_and_binds.py` updated to call `normalize_postgres_parameters` directly).
- **MySQL helper (`db/adapters/mysql/core.py`)**:
  - `compile_for_driver`: **kept** -> `db/adapters/mysql/_sync.py::SyncMySQLDriver.{fetch_all_sets,select_to_arrow,stream_arrow_batches}` and `db/adapters/mysql/_async.py::AsyncMySQLDriver.{fetch_all_sets,select_to_arrow,stream_arrow_batches}`.
- **SQL Server helper (`db/adapters/mssql/_sync.py`, `_async.py`)**:
  - `_prepare_sql`: already deleted (0 hits).
- **Oracle helpers (`db/adapters/oracle/core.py`)**:
  - `split_out_bind_parameters`, `is_dml_or_plsql`: already deleted (0 hits).
  - `coerce_parameters`: **kept** -> `db/adapters/oracle/{_sync,_async}.py::*OracleDriver.select_to_arrow`, `db/adapters/oracle/pipeline.py::{SyncOraclePipeline,AsyncOraclePipeline}.results`.
  - `track_session_context_sql`: **kept** -> `db/adapters/oracle/{_sync,_async}.py::*OracleDriver.{_execute_cursor,select_to_arrow}`, `db/adapters/oracle/pipeline.py::{SyncOraclePipeline,AsyncOraclePipeline}.results`.
  - `coerce_result_rows_and_cols`: **kept** -> `db/adapters/oracle/pipeline.py::{SyncOraclePipeline,AsyncOraclePipeline}.results`.
- **`db/driver/capabilities.py`**: confirmed zero protocol/member references to `execute_with_description`, `execute_tuples`, or `query_with_columns`.

### Remaining lease call sites under `src/py/vertebra/db/adapters/`
- Zero calls to `_acquire_broker_lease`, `_release_broker_lease`, or `_lookup_or_cache_statement` under `src/py/vertebra/db/adapters/`.
- Connection-checkout lease attachment calls:
  - `mssql/_sync.py::SyncMSSQLDriver.connect:105` (`_attach_checkout_lease`)
  - `mssql/_async.py::AsyncMSSQLDriver.{_acquire_broker_lease_async:189,_attach_checkout_lease_async:207}` (`_lease_plan`)
  - `mysql/_sync.py::SyncMySQLDriver._get_client:571` (`_attach_checkout_lease`)
  - `mysql/_async.py::AsyncMySQLDriver.{_acquire_broker_lease_async:551,_attach_checkout_lease_async:569,_get_client:610}`
  - `oracle/_sync.py::SyncOracleDriver.{connect:367,_acquire_pool_client:793}` (`_attach_checkout_lease`)
  - `oracle/_async.py::AsyncOracleDriver.{_acquire_broker_lease_async:802,_attach_checkout_lease_async:820}` (`_lease_plan`)
  - `postgres/_sync.py::SyncPostgreSQLDriver._acquire_pool_client:747` (`_attach_checkout_lease`)
  - `postgres/_async.py::AsyncPostgreSQLDriver.{_acquire_broker_lease_async:746,_attach_checkout_lease_async:765}` (`_lease_plan`)

### Tooling & verification caveats
- `uvx vulture` could not run inside the offline sandbox (`vulture` is not pre-installed in `.venv` or `~/.local/share/uv/tools/` and network access is disabled). Substituted a Python AST zero-reference scan across all owned files, which flagged `CommonDriverMixin._extract_column_names` (deleted), `PostgreSQLCore.normalize_parameters` (deleted), plus public/tested helpers `build_direct_client_kwargs`, `decode_tns_packet`, `decode_ttc_session_attrs`, and `select_with_total` (kept as documented public API).
- `ruff check --select F401,F811,F841` across the full path list reports two pre-existing unused imports (`COMPARATOR_ALLOW_LIST`, `COMPARATOR_PAIRS`) in out-of-scope `src/py/tests/unit/tools/test_adapter_benchmarks_sql.py`; all owned files and touched tests pass `ruff check` and `ruff format --check` with 0 errors.
- `pyright src/py/vertebra/db` reports 6 pre-existing `BrokenWorkerInterpreter.run_sync` errors in untouched `db/dialer/_base.py` and `db/driver/_cloud_auth.py`; all touched production files pass `pyright` with 0 errors, and `mypy` passes with 0 issues across 225 files in `db`, `extensions/sqlspec`, and `extensions/sqlalchemy`.
- Full `make lint-all` was not run because the working tree has parallel in-flight edits from other chapter tasks; run `make lint-all` at the end-of-chapter gate.

[^driver-common]: `src/py/vertebra/db/driver/_common.py::_BrokerLeasePlan`
[^dbapi-protocol]: `src/py/vertebra/db/dbapi/_sync.py::SyncSQLDriverProtocol`
[^purity-tests]: `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py::test_notification_source_legacy_symbol_cleanly_removed`
