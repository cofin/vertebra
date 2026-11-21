---
type: Task
id: "native-cursor-data-path:network:t12"
chapter_id: network-engines
title: "SyncMySQLCursor and AsyncMySQLCursor"
description: "Add the MySQL cursor pair, created by client.cursor(), over the existing prepared-statement execution path."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t42"
  - "native-cursor-data-path:network:t11"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-mysql/src/client.rs:541-560"
  - "src/rs/vertebra-mysql/src/client.rs:655-840"
  - "src/rs/vertebra-mysql/src/client.rs:1199-1240"
  - "src/rs/vertebra-mysql/src/client.rs:1368-1447"
  - "src/rs/vertebra-mysql/src/client.rs:1684-1725"
  - "src/rs/vertebra-mysql/src/client.rs:1853-1935"
  - "src/rs/vertebra-mysql/src/cursor.rs"
  - "src/py/vertebra/_mysql.pyi:27-37"
  - "src/py/vertebra/_mysql.pyi:65-110"
  - "src/py/vertebra/_mysql.pyi:172-215"
  - "src/py/tests/unit/db/test_mysql_driver.py:1-80"
files:
  - "src/rs/vertebra-mysql/src/cursor.rs"
  - "src/rs/vertebra-mysql/src/client.rs"
  - "src/py/vertebra/_mysql.pyi"
tests:
  - "src/py/tests/unit/db/test_mysql_driver.py"
estimated_loc: 260
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
  - id: mysql-client
    resource: "src/rs/vertebra-mysql/src/client.rs::MySQLClientCore"
    title: "Execution core both cursors call"
tags: [mysql, rust, pyo3, dbapi]
---

# SyncMySQLCursor and AsyncMySQLCursor

## Objective & Scope Boundary

`SyncMySQLClient.cursor()` and `AsyncMySQLClient.cursor()` return PEP 249 cursors. One `execute` binds parameters from the cached plan, runs `execute_result_set` on the client core, and stores tuple rows, the description, `rowcount`, and `lastrowid` from the server reply.

Out of scope:

- Python driver changes (t13).
- Removing `fetch_all`, `execute_with_description`, or other client methods. The cleanup chapter audits native methods that lost their last caller.
- `lib.rs`.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-mysql/src/client.rs:655-840` holds `execute_result_set` and `execute_many_batch` on the core.
- `src/rs/vertebra-mysql/src/client.rs:1368-1447` shows how the sync client releases the GIL and maps errors; `:1853-1935` shows how the async client builds its awaitable.
- `src/rs/vertebra-mysql/src/cursor.rs::register` is called from the module init since t11. This task fills its body.

## Contracts & Edge Cases

- `cursor` also takes keyword-only `lazy_begin: bool = False`, in the Rust signature and in the stub. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when the session has none open, then run the statement. A failed begin clears the result and raises. Test rows: a cursor created with `lazy_begin=True` leaves `in_transaction` true after an insert and `rollback()` removes the row; the same cursor after `commit()` opens a new transaction on its next statement; a default cursor opens none.
- Lease methods, on each client class this task edits: `attach_lease(self, release: Callable[[], None]) -> None` and `release_lease(self) -> None`, both plain methods, with stub entries. The client holds a `Mutex<Option<LeaseGuard>>` using the guard from t42. `attach_lease` releases a guard that is already held, then stores the new one. `release_lease` releases now and is idempotent. The client's `close` releases the guard, and dropping the client releases it through the guard's drop. Test rows: `attach_lease` with a counting callable then `release_lease` twice gives one call; `attach_lease` then `del client` and `gc.collect()` gives one call; a second `attach_lease` releases the first callable.
- Classes `SyncMySQLCursor` and `AsyncMySQLCursor`, module `vertebra._mysql`, defined in `cursor.rs`. Each holds `Arc<MySQLClientCore>` and a `std::sync::Mutex<CursorState>`.
- `client.rs` adds `pub fn cursor(&self) -> SyncMySQLCursor` and `pub fn cursor(&self) -> AsyncMySQLCursor` to the two `#[pymethods]` blocks. Neither does I/O.
- `register` in `cursor.rs` adds both classes with `m.add_class` and the function `compile_binds` with `m.add_function`.
- `#[pyfunction] compile_binds(sql: &str, parameters: Option<&Bound<PyAny>> = None) -> PyResult<(String, Py<PyTuple>)>` returns the SQL and values produced by `bind_parameters` plus `coerce_value`. The Arrow and multi-result-set paths of the Python driver call it in t13, so one implementation of named-parameter rewriting remains.
- `execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> Self`: `ensure_open`, `bind_parameters`, `coerce_value` on each value, convert to the core's `Params` the way `SyncMySQLClient.execute` does, run `execute_result_set` with the GIL released, then `store`.
- `executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> Self` binds each set with the same plan and runs `execute_many_batch`. `rowcount` is the total affected rows. `description` is `None`. An empty sequence sets `rowcount` to `0` and sends nothing.
- The async class returns `RustDbTaskAwaitable` from `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, and `close`. Binding and coercion run on the calling thread before the task starts.
- Fetch methods, properties, `arraysize`, `close`, the failed-execute rule, and the concurrent-execute rule are those of chapter spec section 2.
- Engine errors leave through `to_py_err` on `MySQLError`, unchanged. `TypeError` and `ValueError` from parameter conversion are raised as `InterfaceError` through `cursor_interface_err`.
- Remove the `allow(dead_code)` attribute from `cursor.rs`.
- Stub: two cursor classes, one `cursor` method on each client class, and `def compile_binds(sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[str, tuple[Any, ...]]: ...`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| compile_binds | `compile_binds('SELECT :a', {'a': 1})` | `('SELECT ?', (1,))` |
| Select | `SELECT 1 AS a, 'x' AS b` | `[(1, 'x')]`; description names `a`, `b` |
| Command | `CREATE TEMPORARY TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)` | `description` is `None` |
| Insert | One-row `INSERT` | `rowcount` 1; `lastrowid` is the new id |
| Update count | `UPDATE` touching two rows | `rowcount` 2; `lastrowid` is `None` |
| Empty set | `SELECT 1 FROM t WHERE 1 = 0` | `[]`; description present |
| Positional | `SELECT ? + ?` with `(1, 2)` | `[(3,)]` |
| Named | `SELECT :a + :b` with a mapping | `[(3,)]` |
| executemany | Three sets | `rowcount` 3 |
| Duplicate key | Second insert of the same key | `IntegrityError` |
| Syntax error | `SELEC 1` | `ProgrammingError` |
| Closed | `close()` then `execute` | `InterfaceError` |
| Async | The same cases through `AsyncMySQLCursor` | Same results |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._mysql`
- `uv run --no-sync pytest src/py/tests/unit/db/test_mysql_driver.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-mysql/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` and `test_async_cursor_*` run against the existing wire test server fixture and cover every matrix case.
- [ ] The stub diff adds two classes, two methods, and one function.
