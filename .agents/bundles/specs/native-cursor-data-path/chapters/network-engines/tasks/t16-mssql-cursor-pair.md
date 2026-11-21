---
type: Task
id: "native-cursor-data-path:network:t16"
chapter_id: network-engines
title: "SyncMSSQLCursor and AsyncMSSQLCursor"
description: "Add the SQL Server cursor pair, created by client.cursor(), over the existing TDS execution path."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t42"
  - "native-cursor-data-path:network:t15"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-mssql/src/lib.rs:1-30"
  - "src/rs/vertebra-mssql/src/lib.rs:250-270"
  - "src/rs/vertebra-mssql/src/lib.rs:492-545"
  - "src/rs/vertebra-mssql/src/lib.rs:745-765"
  - "src/rs/vertebra-mssql/src/lib.rs:1007-1070"
  - "src/rs/vertebra-mssql/src/lib.rs:1330-1340"
  - "src/rs/vertebra-mssql/src/cursor.rs"
  - "src/py/vertebra/_mssql.pyi:26-60"
  - "src/py/vertebra/_mssql.pyi:128-160"
  - "src/py/tests/unit/db/test_mssql_driver.py:1-80"
files:
  - "src/rs/vertebra-mssql/src/cursor.rs"
  - "src/rs/vertebra-mssql/src/lib.rs"
  - "src/rs/vertebra-mssql/src/transport.rs"
  - "src/rs/vertebra-mssql/src/client.rs"
  - "src/rs/vertebra-mssql/src/params.rs"
  - "src/py/vertebra/_mssql.pyi"
tests:
  - "src/py/tests/unit/db/test_mssql_driver.py"
estimated_loc: 260
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
  - id: mssql-client
    resource: "src/rs/vertebra-mssql/src/lib.rs::PySyncMSSQLClient"
    title: "Client classes that create the cursors"
tags: [mssql, rust, pyo3, dbapi]
---

# SyncMSSQLCursor and AsyncMSSQLCursor

## Objective & Scope Boundary

`SyncMSSQLClient.cursor()` and `AsyncMSSQLClient.cursor()` return PEP 249 cursors that run on the client's existing connection and broker path.

Out of scope:

- Python driver changes (t17).
- Removing existing client methods.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `execute_with_description` on `PySyncMSSQLClient` and `::execute_many` in `lib.rs` show the call into `MssqlClient`, GIL release, and error conversion to reuse.
- `src/rs/vertebra-mssql/src/cursor.rs::register` is called from the module init since t15. This task fills its body.

## Contracts & Edge Cases

- `cursor` also takes keyword-only `lazy_begin: bool = False`, in the Rust signature and in the stub. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when the session has none open, then run the statement. A failed begin clears the result and raises. Test rows: a cursor created with `lazy_begin=True` leaves `in_transaction` true after an insert and `rollback()` removes the row; the same cursor after `commit()` opens a new transaction on its next statement; a default cursor opens none.
- Lease methods, on each client class this task edits: `attach_lease(self, release: Callable[[], None]) -> None` and `release_lease(self) -> None`, both plain methods, with stub entries. The client holds a `Mutex<Option<LeaseGuard>>` using the guard from t42. `attach_lease` releases a guard that is already held, then stores the new one. `release_lease` releases now and is idempotent. The client's `close` releases the guard, and dropping the client releases it through the guard's drop. Test rows: `attach_lease` with a counting callable then `release_lease` twice gives one call; `attach_lease` then `del client` and `gc.collect()` gives one call; a second `attach_lease` releases the first callable.
- Classes `SyncMSSQLCursor` and `AsyncMSSQLCursor`, module `vertebra._mssql`, defined in `cursor.rs`. `register` in `cursor.rs` adds both with `m.add_class`. Each holds the same shared client handle the client class holds and a `std::sync::Mutex<CursorState>`.
- `cursor(&self)` is added to both client `#[pymethods]` blocks. Neither does I/O.
- `execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> Self` calls `bind_positional` for a sequence and `prepare_sql_and_params` for a mapping, runs the statement through the same client function `execute_with_description` uses, and stores tuple rows, the description, and the row count.
- `executemany` uses `prepare_many_sql_and_params` and the function `execute_many` uses. `rowcount` is the total. `description` is `None`.
- A statement that returns several result sets stores the first one that has column metadata. Later sets are discarded. `rowcount` for such a statement is the row count of the stored set.
- DML with an `OUTPUT` clause sets both `description` and `rowcount`, from the reply. No SQL text is inspected for `OUTPUT`.
- Async methods return `RustDbTaskAwaitable`. Preparation runs on the calling thread.
- Fetch methods, properties, `arraysize`, `close`, and the failed-execute and concurrent-execute rules follow chapter spec section 2. Engine errors leave through `mssql_error_to_py`, unchanged; `TypeError` and `ValueError` from parameters become `InterfaceError` through `cursor_interface_err`.
- Remove the `allow(dead_code)` attribute from `cursor.rs`.
- Stub: two cursor classes and one `cursor` method on each client class.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `SELECT 1 AS a, N'x' AS b` | `[(1, 'x')]`; names `a`, `b` |
| Command | `CREATE TABLE #t (id INT)` | `description` is `None` |
| DML count | `INSERT` of three rows | `rowcount` 3 |
| OUTPUT clause | `INSERT ... OUTPUT inserted.id` | `description` present; rows returned |
| Word output in text | `SELECT 'output' AS output` | Treated as a plain select |
| Empty set | `SELECT 1 WHERE 1 = 0` | `[]`; description present |
| Positional | `SELECT ? + ?` with `(1, 2)` | `[(3,)]` |
| executemany | Three sets | `rowcount` 3 |
| Key violation | Duplicate primary key | `IntegrityError` |
| Syntax error | `SELEC 1` | `ProgrammingError` |
| Closed | `close()` then `execute` | `InterfaceError` |
| Async | The same cases through `AsyncMSSQLCursor` | Same results |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._mssql`
- `uv run --no-sync pytest src/py/tests/unit/db/test_mssql_driver.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mssql cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mssql --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-mssql/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` and `test_async_cursor_*` use the fixtures the existing driver tests use and cover every matrix case.
- [ ] The stub diff adds two classes and two methods.

## Notes & Discoveries

- `src/rs/vertebra-mssql/src/transport.rs`, `client.rs`, and `params.rs` are in scope for this task so the cursor query path can use tiberius `Command` (`sp_executesql`) to preserve `DONE` row counts, keep only the first result set (`result_index() == 0`), and handle `OUTPUT` clauses without SQL inspection, plus unwrap `TypedParameter` via `.value` on the slow path of `py_any_to_mssql_param`.
