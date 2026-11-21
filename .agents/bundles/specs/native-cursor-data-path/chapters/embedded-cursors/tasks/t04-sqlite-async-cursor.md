---
type: Task
id: "native-cursor-data-path:embedded:t04"
chapter_id: embedded-cursors
title: "AsyncSQLiteCursor and AsyncSQLiteClient.cursor"
description: "Add the awaitable native SQLite cursor on top of the shared cursor state, its factory on the async client, and its stub."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t03"
read_set:
  - "src/rs/vertebra-sqlite/src/py_client.rs:776-800"
  - "src/rs/vertebra-sqlite/src/py_client.rs:849-920"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1259-1294"
  - "src/rs/vertebra-sqlite/src/py_client.rs"
  - "src/rs/vertebra-sqlite/src/lib.rs:19-41"
  - "src/py/vertebra/_sqlite.pyi::AsyncSQLiteClient"
  - "src/py/tests/unit/db/test_sqlite_driver.py:40-60"
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/rs/vertebra-sqlite/src/lib.rs
  - src/py/vertebra/_sqlite.pyi
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 240
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlite-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs
    title: "SyncSQLiteClient and AsyncSQLiteClient PyO3 classes"
tags: [sqlite, pyo3, cursor, async]
---

# AsyncSQLiteCursor and AsyncSQLiteClient.cursor

## Objective & Scope Boundary

`AsyncSQLiteClient.cursor()` returns an `AsyncSQLiteCursor`. Its six methods return awaitables. Its four attributes are plain reads.

`CursorState`, the conversion helpers, and the error helpers from t03 are reused without change. The sync cursor is not edited.

## Bounded Read Manifest

- `src/rs/vertebra-sqlite/src/py_client.rs` — search for the `CursorState` struct and the sync cursor class that t03 adds, and read those two blocks only.
- `py_client.rs:776-800` — `AsyncSQLiteClient` and its `pool` field.
- `py_client.rs:849-920` — `execute` and `execute_many` on the async client: the `spawn_rust_db_task_with_attr_and_cancel` call shape, `spawn_blocking`, the join-error mapping, and the cancel closure.
- `py_client.rs:1259-1294` — async `close` and `to_sync`; the new `cursor` method goes after `to_sync`.
- `lib.rs:19-41` — registration.
- `src/py/vertebra/_sqlite.pyi`, class `AsyncSQLiteClient` — stub style for awaitable methods.
- `test_sqlite_driver.py:40-60` — async test style (`pytest.mark.anyio`).

## Contracts & Edge Cases

```rust
#[pyclass(name = "AsyncSQLiteCursor", frozen, immutable_type)]
pub struct AsyncSQLiteCursor { pool: Arc<SqlitePool>, state: Arc<Mutex<CursorState>>, executing: Arc<AtomicBool>, lazy_begin: bool }
```

`AsyncSQLiteClient` gains `cursor`, with the PyO3 signature `(*, lazy_begin = false)`, returning `AsyncSQLiteCursor`. `lib.rs` registers the class and adds `"AsyncSQLiteCursor"` to `__all__`.

| Member | Behavior |
| --- | --- |
| `execute(sql, params=())` | Same PyO3 signature and `text_signature` as the sync cursor. Converts parameters before spawning. Spawns `pool.run_with_op_id` through `spawn_blocking` with `spawn_rust_db_task_with_attr_and_cancel`. The success mapper installs the outcome and resolves to the cursor object. The cancel closure calls `pool.cancel_operation(op_id)`. |
| `executemany(sql, seq_of_params)` | Same shape over `pool.run_many_with_op_id`. |
| `fetchone()`, `fetchmany(size=None)`, `fetchall()` | Take rows from the state before spawning. Spawn a future that is ready at once and carries the Rust rows. The success mapper builds the tuple or list. |
| `close()` | Closes the state, then spawns a ready future that resolves to `None`. |
| `description`, `rowcount`, `lastrowid`, `arraysize` | Same getters and setter as the sync cursor. |

- Every spawn uses `SQLITE_COMPLETION_DRAINER_ATTR`.
- In flight: `execute` and `executemany` set `executing` with a compare-and-swap before spawning. When it is already set, the call raises `InterfaceError` with the message `cursor is already executing` and changes nothing. The flag is cleared when the task finishes, fails, or is cancelled.
- Lazy begin: when `lazy_begin` is true and the pool reports no open transaction, the spawned task first runs the pool function that `AsyncSQLiteClient.begin` runs with its default mode `IMMEDIATE`, then the statement. A failed begin clears the result and the awaitable raises.
- The method takes `slf: &Bound<'py, Self>` so the success mapper can hold a `Py<AsyncSQLiteCursor>` and return it.
- Parameter conversion errors and state errors (`Closed`, `NoResultSet`) are raised by the method call itself, before any awaitable is returned. Engine errors are raised when the awaitable is awaited. Both use `into_py_err` and `cursor_state_err`, as the sync cursor does.
- A failed or cancelled `execute` or `executemany` clears the result with `clear_result` before the error is raised. A parameter conversion error clears it before the method call raises. An engine error or cancellation clears it before the awaitable raises.
- An outcome that arrives after `close` is dropped. The awaitable still resolves to the cursor.
- The ready-future form costs one completion round trip per fetch. `vertebra-core` has no helper for a finished awaitable and is outside this chapter.

Stub: add `"AsyncSQLiteCursor"` to `__all__`, add `def cursor(self, *, lazy_begin: bool = False) -> AsyncSQLiteCursor` to `AsyncSQLiteClient`, and add `AsyncSQLiteCursor` with the same attributes as `SyncSQLiteCursor`. Method return types are `Awaitable[AsyncSQLiteCursor]` for `execute` and `executemany`, `Awaitable[tuple[Any, ...] | None]` for `fetchone`, `Awaitable[list[tuple[Any, ...]]]` for `fetchmany` and `fetchall`, and `Awaitable[None]` for `close`.

## Test Matrix

New `pytest.mark.anyio` tests in `test_sqlite_driver.py` build `vertebra._sqlite.AsyncSQLiteClient(":memory:")` directly. Exception names in this worksheet are the `vertebra._vertebra` classes. `vertebra.db.dbapi` exposes the same class objects under the same names.

| Case | Steps | Expected |
| --- | --- | --- |
| Query | `cur = await cursor.execute("SELECT 1 AS a")` | `cur is cursor`; `await cursor.fetchall() == [(1,)]`; `description` names `a` |
| Insert | `await execute(INSERT ...)` | `rowcount == 1`, `lastrowid == 1`, `description is None` |
| Fetch walk | three rows | `await fetchone()`, `await fetchmany(1)`, `await fetchall()`, then `await fetchone() is None` |
| Empty result | false predicate | `await fetchall() == []`, `description is not None` |
| `executemany` | three sets | `rowcount == 3` |
| Fetch before execute | `cursor.fetchall()` | raises `ProgrammingError` at the call |
| Closed | `await cursor.close()` twice, then `cursor.execute(...)` | raises `InterfaceError` at the call |
| Constraint | duplicate key | `await` raises `IntegrityError` with `sqlstate` set |
| Lazy begin | `client.cursor(lazy_begin=True)`; awaited insert; read `client.in_transaction`; `await client.rollback()`; count rows | `True`; zero rows |
| Lazy begin after commit | same cursor: insert, commit, insert, rollback | one row |
| Execute in flight | call `cursor.execute(...)` twice before awaiting the first | the second call raises `InterfaceError` with `cursor is already executing`; the first awaitable completes |
| Failed execute clears | successful query, then failing `execute` | the awaitable raises; `description is None`, `rowcount == -1`, `lastrowid is None`; `cursor.fetchall()` raises `ProgrammingError` at the call |
| Failed bind clears | successful query, then `cursor.execute("SELECT ?", (object(),))` | `InterfaceError` at the call; `description is None` |
| `arraysize` and `fetchmany` sizes | set `arraysize` to `0`; `cursor.fetchmany(-1)`; `await cursor.fetchmany(0)` | `ProgrammingError`; `ProgrammingError` at the call; `[]` |
| Shared pool | `client.to_sync().cursor()` reads a row the async cursor inserted in a file database | row is visible |

## Verification

- Build: `uv run maturin develop --uv -m src/rs/vertebra-sqlite/Cargo.toml`
- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q -k cursor`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._sqlite`
- Rust: `(cd src/rs && cargo test -p vertebra-sqlite --locked && cargo clippy -p vertebra-sqlite --all-targets -- -D warnings)`

## Acceptance Criteria

- [ ] Every row of the test matrix passes.
- [ ] `mypy.stubtest vertebra._sqlite` passes with no allow-list change.
- [ ] The stub diff adds `AsyncSQLiteCursor`, `AsyncSQLiteClient.cursor`, and one `__all__` entry only.
- [ ] The t03 tests pass unchanged.

## Notes & Discoveries

- The completion drainer skips both mappers when the Python future is cancelled, so neither mapper can clear the result or the in-flight flag on cancellation. An `ExecutingGuard` travels with the task output instead: a mapper settles it, and a guard dropped unsettled clears the result. The flag is released last in every case.
- Deviation: the cancel closure calls `pool.cancel_operation(op_id)` and also clears the result when the statement has not settled. This is what makes "cancellation clears the result before the awaitable raises" true. The helper also runs this closure when an `execute` awaitable is dropped without being awaited.
- The in-flight check runs before parameter conversion, so a call made while a statement is in flight raises `cursor is already executing` even when its parameters are also invalid.
- Lazy begin reuses the statement `op_id` for `begin_with_op_id`, so one cancel covers both steps.
- Added one test outside the matrix: a cancelled `execute` clears the result and the cursor accepts a later statement.
- `make lint` stops at Pyright on `src/py/vertebra/_typing.py:360` (`Import "numpy" could not be resolved`), a file outside this task. Its Ruff, rustfmt, Clippy, and Mypy steps pass. The `check_extras` and 13-extension stubtest steps after Pyright did not run; `mypy.stubtest vertebra._sqlite` was run directly and passes.
