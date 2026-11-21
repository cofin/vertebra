---
type: Task
id: "native-cursor-data-path:cleanup:t13"
chapter_id: cleanup-and-checks
title: "Enterprise driver tests on the cursor contract"
description: "Rewrite or delete the eleven failing tests in the enterprise driver test file that pin per-statement leases, shared-memory statement cache entries, and last_rewritten_sql, and remove the doubles only they used."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py:1-16"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py:34-37"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py:94-103"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py:185-270"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::_MockMSSQLClient"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_sqlite_initialization_pragmas_and_broker_coordination"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_sqlalchemy_dbapi_broker_forwarding"
  - "src/py/tests/unit/db/test_enterprise_olap_drivers.py:1400-1640"
  - "src/py/tests/unit/db/test_sqlite_driver.py::test_sync_sqlite_driver_queries_take_no_lease_or_statement_cache_entry"
  - "src/py/tests/unit/db/test_duckdb_driver.py::test_duckdb_sync_driver_queries_take_no_lease_and_cache_no_statement"
  - "src/py/tests/unit/db/test_oracle_adapter.py::test_sync_oracle_driver_lease_unpooled_lifecycle_refusal_and_gc"
  - "src/py/tests/unit/db/test_mssql_driver.py::test_sync_mssql_driver_checkout_lease_lifecycle"
  - "src/py/tests/unit/db/test_mssql_driver.py::test_sync_mssql_driver_native_cursor_query_matrix"
files: []
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 290
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: olap-tests
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: "Enterprise and analytical driver unit tests"
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec: lease contract and cursor contract"
  - id: mssql-lease-tests
    resource: src/py/tests/unit/db/test_mssql_driver.py::test_sync_mssql_driver_checkout_lease_lifecycle
    title: "SQL Server lease tests on the current contract"
tags: [testing, database, lease, cleanup]
---

# t13 — Enterprise driver tests on the cursor contract

## Objective & Scope Boundary

`test_enterprise_olap_drivers.py` passes and has no `mypy` error. Eleven of its tests fail today. They assert behavior that the lease contract in root spec section 2 removed: a lease per statement, a shared-memory statement cache entry per SQL text, and the `last_rewritten_sql` attribute.[^olap-tests][^root-spec]

In scope: that one test file. Out of scope: every production file, every other test file, and the tests in this file that pass today.

No production change is made. No double gains a method here: each double this task touches is deleted, because the tests that needed it are deleted or no longer use it.

## Bounded Read Manifest

- Lines `1-16`, `34-37`, and `94-103` of the test file for the imports this task removes.
- Lines `185-270` for `_MockLeaseTable`, `_MockStatementCache`, and `_MockBroker`, which stay unchanged.
- `_MockMSSQLClient` and the three tests named in `read_set`.
- Lines `1400-1640` for the Oracle, DuckDB, and SQL Server lease tests and their doubles.
- The five tests in other files named in `read_set`. They cover the current contract and are not edited.[^mssql-lease-tests]

## Contracts & Edge Cases

Each change below is complete. Nothing else in the file changes.

Rewrite, three tests:

1. `test_sqlite_initialization_pragmas_and_broker_coordination` becomes `test_sqlite_initialization_pragmas_and_no_broker_lease`.
   - Docstring: `Verify SQLite WAL initialization pragmas, zero broker leases on the query path, and the Arrow SHM threshold.`
   - Replace `assert broker.lease_table.acquired == 4` and `assert broker.lease_table.released == 4` with `== 0` for both.
   - Add the same two `== 0` assertions after the `select_to_arrow` assertions and before `driver.close()`.
   - The pragma, row, and Arrow assertions stay.
2. `test_mssql_tds_framing_and_placeholder_rewriter` keeps its name.
   - Docstring: `Verify single-pass @P1 placeholder rewriting skips string literals, quoted identifiers, and comments.`
   - Keep everything through `assert count == 3`. Delete the rest of the body: the `_MockBroker`, the `SyncMSSQLDriver` built on `_MockMSSQLClient`, the two `execute` calls, the `last_rewritten_sql` assertion, the `select` assertion, the `statement_cache().hits` assertion, and `close()`.
3. `test_sqlalchemy_dbapi_broker_forwarding` becomes `test_sqlalchemy_dbapi_embedded_engines_take_no_broker_lease`.
   - Parameters: `"sqlite+vtb:///:memory:"` and `"duckdb+vtb:///:memory:"`. The SQL Server parameter is removed.
   - Docstring: `Verify embedded DB-API connections run statements on the native cursor and take no broker lease.`
   - Delete the `injected_driver` expression. Build the connection with `SyncDBAPIConnection(url, broker=broker)`.
   - Keep the three `cur.execute` calls, the `fetchone` assertion, `cur.close()`, and `conn.close()`.
   - Replace the two `>= 2` assertions with `broker.lease_table.acquired == 0` and `broker.lease_table.released == 0`.
   - The `pytest.importorskip` for DuckDB stays.

Delete, six tests. Each pinned behavior that is gone, and the named tests cover what replaced it:

| Deleted test | Covered by |
| --- | --- |
| `test_sync_oracle_driver_uses_common_mixin_broker_and_bytes_statement_cache` | `src/py/tests/unit/db/test_oracle_adapter.py::test_sync_oracle_driver_lease_unpooled_lifecycle_refusal_and_gc` |
| `test_async_oracle_driver_uses_common_mixin_broker_and_bytes_statement_cache` | `src/py/tests/unit/db/test_oracle_adapter.py::test_async_oracle_driver_lease_unpooled_lifecycle_refusal_and_gc` |
| `test_sync_duckdb_driver_uses_real_lease_broker_and_releases_on_error` | `src/py/tests/unit/db/test_duckdb_driver.py::test_duckdb_sync_driver_queries_take_no_lease_and_cache_no_statement` |
| `test_async_duckdb_driver_uses_real_lease_broker_and_releases_on_error` | `src/py/tests/unit/db/test_duckdb_driver.py::test_duckdb_async_driver_queries_take_no_lease_and_cache_no_statement` |
| `test_sync_mssql_driver_uses_common_mixin_broker_and_bytes_statement_cache` | `src/py/tests/unit/db/test_mssql_driver.py::test_sync_mssql_driver_checkout_lease_lifecycle` and `test_sync_mssql_driver_native_cursor_query_matrix` |
| `test_async_mssql_driver_uses_common_mixin_broker_and_bytes_statement_cache` | `src/py/tests/unit/db/test_mssql_driver.py::test_async_mssql_driver_checkout_lease_lifecycle` and `test_async_mssql_driver_native_cursor_query_matrix` |

The SQL Server case removed from the DB-API test is covered by `test_sync_mssql_driver_native_cursor_query_matrix`, which calls `driver.cursor(lazy_begin=True)` as the DB-API connection does, and by `test_sync_mssql_driver_checkout_lease_lifecycle`.

Delete, four doubles with no remaining user: `_MockMSSQLClient`, `_AsyncMSSQLBrokerTestClient`, `_SyncOracleLeaseTestClient`, `_AsyncOracleLeaseTestClient`.

Delete, imports with no remaining user:

- `import re` and `import sqlite3`.
- `LeaseBroker` from the `vertebra._vertebra` import. `ProgrammingError` stays.
- The whole `from vertebra.db.driver._common import (DUCKDB_DIALECT_TAG, MSSQL_DIALECT_TAG, ORACLE_DIALECT_TAG)` statement.

Keep unchanged: `_MockLeaseGrant`, `_MockLeaseTable`, `_MockStatementEntry`, `_MockStatementCache`, `_MockBroker`, `test_core_probe_helpers_removed`, `test_mssql_and_sqlite_probe_helpers_removed`, and the `_FakeMSSQL*Protocol*` doubles.

Edge cases:

- Before deleting a double or an import, run `git grep -n "<name>" -- src/py/tests/unit/db/test_enterprise_olap_drivers.py`. When a use remains outside the deleted tests, stop and report it. Do not keep a deleted test alive to hold the name.
- No other file imports the four deleted doubles. Confirm with `git grep -n "_MockMSSQLClient\|_AsyncMSSQLBrokerTestClient\|_SyncOracleLeaseTestClient\|_AsyncOracleLeaseTestClient" -- src/py/tests`. A match outside this file stops the task.
- The four `mypy` errors in this file all name `last_rewritten_sql` and disappear with the lines above.

## Test Matrix

| Test | Expected |
| --- | --- |
| `test_sqlite_initialization_pragmas_and_no_broker_lease` | Passes; leases acquired and released are both `0` before and after the Arrow read |
| `test_mssql_tds_framing_and_placeholder_rewriter` | Passes; asserts the rewritten SQL and a count of `3` |
| `test_sqlalchemy_dbapi_embedded_engines_take_no_broker_lease` with the SQLite URL | Passes; row `(1, "vtb")`; zero leases |
| Same with the DuckDB URL | Passes, or skips when `vertebra._duckdb` is not built |
| The six deleted test names | Not collected |
| `last_rewritten_sql` in the file | No match |
| `mypy` | No error in this file |
| `ruff check` on the file | No finding |

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_sqlite_driver.py -p no:xdist -q -k "lease or native_cursor_query_matrix"
uv run --no-sync --frozen ruff check src/py/tests/unit/db/test_enterprise_olap_drivers.py
uv run --no-sync --frozen mypy | grep "test_enterprise_olap_drivers.py"
git grep -n "last_rewritten_sql\|_MockMSSQLClient\|LeaseTestClient\|_DIALECT_TAG" -- src/py/tests/unit/db/test_enterprise_olap_drivers.py
git diff --stat -- src/py/vertebra src/rs
```

The first two commands report no failure. The `grep` after `mypy` and the `git grep` print nothing. The last command prints nothing. Set `UV_CACHE_DIR` to a writable directory when the default cache is read-only.

## Acceptance Criteria

- [ ] The file has no failing test and no `mypy` error.
- [ ] The three rewritten tests assert zero leases or the rewriter output as specified.
- [ ] The six tests, four doubles, and listed imports are gone, and each covering test named above still passes.
- [ ] No production file and no other test file has a diff from this task.

[^olap-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py`
[^root-spec]: `.agents/bundles/specs/native-cursor-data-path/spec.md`
[^mssql-lease-tests]: `src/py/tests/unit/db/test_mssql_driver.py::test_sync_mssql_driver_checkout_lease_lifecycle`
