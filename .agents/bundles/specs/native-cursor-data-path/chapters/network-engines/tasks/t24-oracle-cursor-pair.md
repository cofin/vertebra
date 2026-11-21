---
type: Task
id: "native-cursor-data-path:network:t24"
chapter_id: network-engines
title: "SyncOracleCursor and AsyncOracleCursor"
description: "Add the Oracle cursor pair, created by client.cursor(), with value coercion and statement kind decided in the worker."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t42"
  - "native-cursor-data-path:network:t21"
  - "native-cursor-data-path:network:t22"
  - "native-cursor-data-path:network:t23"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-oracle/src/client.rs:273-336"
  - "src/rs/vertebra-oracle/src/client.rs:478-540"
  - "src/rs/vertebra-oracle/src/client.rs:1591-1700"
  - "src/rs/vertebra-oracle/src/client.rs:2531-2700"
  - "src/rs/vertebra-oracle/src/client.rs:3110-3260"
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/py/vertebra/_oracle.pyi:137-180"
  - "src/py/vertebra/_oracle.pyi:279-322"
  - "src/py/tests/unit/db/oracle_test_doubles.py"
files:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/py/vertebra/_oracle.pyi"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 280
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
  - id: oracle-worker
    resource: "src/rs/vertebra-oracle/src/client.rs::OracleWorkerHandle"
    title: "Worker handle both cursors call"
tags: [oracle, rust, pyo3, dbapi]
---

# SyncOracleCursor and AsyncOracleCursor

## Objective & Scope Boundary

`SyncOracleClient.cursor()` and `AsyncOracleClient.cursor()` return PEP 249 cursors. One `execute` coerces binds, picks the query or execute worker command from the cached statement facts, and stores tuple rows that already carry the configured UUID, decimal, vector, and JSON handling.

Out of scope:

- Out-bind variables, `var`, and the `out_binds` property (t25).
- Python driver changes (t26, t27).
- Pooled clients. A pool lease is a client object and gets `cursor()` through the same class.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-oracle/src/client.rs:2531-2700` are the sync `query`, `execute`, and `execute_many` methods; `:3110-3260` are the async forms. The cursor sends the same worker commands.
- `src/rs/vertebra-oracle/src/client.rs:273-336` is `WorkerQueryOutcome`, the reply the cursor reads.
- `src/rs/vertebra-oracle/src/client.rs:478-540` is `extract_bind_args`.

## Contracts & Edge Cases

- `cursor` also takes keyword-only `lazy_begin: bool = False`, in the Rust signature and in the stub. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when the session has none open, then run the statement. A failed begin clears the result and raises. Test rows: a cursor created with `lazy_begin=True` leaves `in_transaction` true after an insert and `rollback()` removes the row; the same cursor after `commit()` opens a new transaction on its next statement; a default cursor opens none.
- Lease methods, on each client class this task edits: `attach_lease(self, release: Callable[[], None]) -> None` and `release_lease(self) -> None`, both plain methods, with stub entries. The client holds a `Mutex<Option<LeaseGuard>>` using the guard from t42. `attach_lease` releases a guard that is already held, then stores the new one. `release_lease` releases now and is idempotent. The client's `close` releases the guard, and dropping the client releases it through the guard's drop. Test rows: `attach_lease` with a counting callable then `release_lease` twice gives one call; `attach_lease` then `del client` and `gc.collect()` gives one call; a second `attach_lease` releases the first callable.
- Classes `SyncOracleCursor` and `AsyncOracleCursor`, module `vertebra._oracle`, defined in `cursor.rs`. `register` in `cursor.rs` adds both. Each holds the client's worker handle, an `OracleValueOptions`, a `lowercase: bool`, and a `std::sync::Mutex<CursorState>`.
- `cursor(self, *, enable_uuid_binary: bool = True, fetch_decimals: bool = False, vector_return_format: str = 'list', enable_lowercase_column_names: bool = True)` is added to both client `#[pymethods]` blocks. It validates `vector_return_format` and does no I/O.
- `execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = ()) -> Self`: `ensure_open`; coerce each bind with `coerce_bind_from_py`; a mapping key has one leading colon removed; read `statement_facts(sql)`; send the query command for `Query` and the execute command for `DmlOrPlsql`; convert each cell with `coerce_cell_to_py` using the column's Oracle type name; `store`.
- A `DmlOrPlsql` statement whose reply carries rows (DML with a returning clause that the worker rewrites today) sets both `description` and `rowcount`.
- `executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> Self` coerces every set and sends the existing execute-many command. `rowcount` is the total. `description` is `None`.
- A read-only property `touches_session_context: bool` returns the flag from the statement facts of the last `execute`, and `False` before any.
- Async methods return `RustDbTaskAwaitable`. Bind coercion runs on the calling thread.
- Fetch methods, properties, `arraysize`, `close`, and the failed-execute and concurrent-execute rules follow chapter spec section 2. Engine errors leave through `to_py_err` on `OracleError`, unchanged; `TypeError` and `ValueError` from binds become `InterfaceError` through `cursor_interface_err`.
- Remove the `allow(dead_code)` attributes added in t22 and t23.
- Stub: two cursor classes and one `cursor` method on each client class.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `SELECT 1 AS a, 'x' AS b FROM dual` | `[(1, 'x')]`; names `a`, `b` |
| Upper-case names | Cursor created with `enable_lowercase_column_names=False` | Names `A`, `B` |
| Integer result | `SELECT 5 FROM dual` with defaults | `int` 5 |
| Decimal result | Same with `fetch_decimals=True` | `Decimal('5')` |
| UUID round trip | Bind a `UUID` into a `RAW(16)` column, select it | Equal `UUID` |
| JSON bind | Bind a `dict` into a JSON column, select it | Equal `dict` |
| DDL | `CREATE TABLE ...` | `description` is `None` |
| DML count | `UPDATE` on two rows | `rowcount` 2 |
| PL/SQL block | `BEGIN NULL; END;` | `description` is `None`; no error |
| Named binds | `SELECT :a + :b FROM dual` with `{'a': 1, ':b': 2}` | `[(3,)]` |
| executemany | Three sets | `rowcount` 3 |
| Unique violation | ORA-00001 | `IntegrityError` |
| Bad vector format | `cursor(vector_return_format='x')` | `InterfaceError` |
| Session flag | Execute a `DBMS_SESSION` block | `touches_session_context` is `True` |
| Closed | `close()` then `execute` | `InterfaceError` |
| Async | The same cases through `AsyncOracleCursor` | Same results |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-oracle/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` and `test_async_cursor_*` cover every matrix case with the fixtures the existing adapter tests use.
- [ ] The stub diff adds two classes and two methods.
