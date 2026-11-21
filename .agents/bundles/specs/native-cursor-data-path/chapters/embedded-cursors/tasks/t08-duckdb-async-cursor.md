---
type: Task
id: "native-cursor-data-path:embedded:t08"
chapter_id: embedded-cursors
title: "AsyncDuckDBCursor and AsyncDuckDBClient.cursor"
description: "Add the awaitable native DuckDB cursor on top of the shared cursor state, its factory on the async client, and its stub."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t07"
read_set:
  - "src/rs/vertebra-duckdb/src/cursor.rs"
  - "src/rs/vertebra-duckdb/src/lib.rs:2426-2454"
  - "src/rs/vertebra-duckdb/src/lib.rs:2862-2870"
  - "src/rs/vertebra-duckdb/src/lib.rs:3043-3073"
  - "src/rs/vertebra-duckdb/src/lib.rs:3438-3457"
  - "src/rs/vertebra-sqlite/src/py_client.rs"
  - "src/py/vertebra/_duckdb.pyi::AsyncDuckDBClient"
  - "src/py/vertebra/_sqlite.pyi"
files:
  - src/rs/vertebra-duckdb/src/cursor.rs
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 230
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "DuckDBInner pool, bind conversion, and client PyO3 classes"
tags: [duckdb, pyo3, cursor, async]
---

# AsyncDuckDBCursor and AsyncDuckDBClient.cursor

## Objective & Scope Boundary

`AsyncDuckDBClient.cursor()` returns an `AsyncDuckDBCursor`. Its six methods return awaitables and its four attributes are plain reads, as with `AsyncSQLiteCursor`.

`CursorState` and the helpers in `cursor.rs` are reused without change. The sync cursor is not edited.

## Bounded Read Manifest

- `cursor.rs` — the whole file from t07.
- `lib.rs:2426-2454` — `CancelOnDropGuard`.
- `lib.rs:2862-2870` — `AsyncDuckDBClient` and its `inner` field.
- `lib.rs:3043-3073` — async `execute_with_description`: the `spawn_rust_db_task_with_attr` call shape, `spawn_blocking`, the drop guard, and the join-error mapping.
- `lib.rs:3438-3457` — async `close` and `to_sync`; `cursor` goes after `to_sync`.
- `src/rs/vertebra-sqlite/src/py_client.rs` — search for the async cursor class from t04 and read that block only. It is the method shape to mirror.
- `src/py/vertebra/_duckdb.pyi`, class `AsyncDuckDBClient`, and the async cursor class in `src/py/vertebra/_sqlite.pyi` — stub style.

## Contracts & Edge Cases

```rust
#[pyclass(name = "AsyncDuckDBCursor", frozen, immutable_type)]
pub struct AsyncDuckDBCursor { inner: Arc<DuckDBInner>, state: Arc<Mutex<CursorState>>, executing: Arc<AtomicBool>, lazy_begin: bool }

impl AsyncDuckDBCursor { pub(crate) fn new(inner: Arc<DuckDBInner>, lazy_begin: bool) -> Self; }
```

`lib.rs` changes: `pub use cursor::AsyncDuckDBCursor;`, `cursor` on `AsyncDuckDBClient` with the PyO3 signature `(*, lazy_begin = false)`, `m.add_class::<AsyncDuckDBCursor>()`, and `"AsyncDuckDBCursor"` in `__all__`.

| Member | Behavior |
| --- | --- |
| `execute(sql, params=())` | Converts parameters before spawning. Registers a cancel flag and a `CancelOnDropGuard`. Spawns `inner.run_statement` through `spawn_blocking` with `spawn_rust_db_task_with_attr`. The success mapper installs the outcome and resolves to the cursor object. |
| `executemany(sql, seq_of_params)` | Same shape over `inner.run_statement_many`. |
| `fetchone()`, `fetchmany(size=None)`, `fetchall()` | Take rows from the state before spawning. Spawn a ready future that carries the Rust rows. The success mapper builds the tuple or list. |
| `close()` | Closes the state, then spawns a ready future that resolves to `None`. |
| `description`, `rowcount`, `lastrowid`, `arraysize` | Same as the sync cursor. |

- Every spawn uses `DUCKDB_COMPLETION_DRAINER_ATTR`.
- In flight: a second `execute` or `executemany` while one is in flight raises `InterfaceError` with the message `cursor is already executing`, using the same compare-and-swap flag as the SQLite async cursor.
- Lazy begin: with `lazy_begin` true and no open transaction, the spawned task first runs the begin path `AsyncDuckDBClient.begin` runs, then the statement.
- The drop guard is marked completed inside the future after the blocking call returns, as at `lib.rs:3043-3073`.
- Parameter conversion errors and state errors are raised by the method call itself. Engine errors are raised when the awaitable is awaited. Both use `into_py_err` and `cursor_state_err`, as the sync cursor does.
- A failed or cancelled `execute` or `executemany` clears the result with `clear_result` before the error is raised, as in the SQLite async cursor.
- An outcome that arrives after `close` is dropped.

Stub: add `"AsyncDuckDBCursor"` to `__all__`, add `def cursor(self, *, lazy_begin: bool = False) -> AsyncDuckDBCursor` to `AsyncDuckDBClient`, and add an `AsyncDuckDBCursor` class with the same members and awaitable return types as `AsyncSQLiteCursor`.

## Test Matrix

New `pytest.mark.anyio` tests in `test_duckdb_driver.py` build `vertebra._duckdb.AsyncDuckDBClient(":memory:")` directly. Test names carry the `native_cursor` infix. Exception names in this worksheet are the `vertebra._vertebra` classes. `vertebra.db.dbapi` exposes the same class objects under the same names.

| Case | Steps | Expected |
| --- | --- | --- |
| Query | `cur = await cursor.execute("SELECT 1 AS a")` | `cur is cursor`; `await cursor.fetchall() == [(1,)]` |
| Insert | two rows | `rowcount == 2`, `description is None`, `lastrowid is None` |
| Fetch walk | three rows | `await fetchone()`, `await fetchmany(1)`, `await fetchall()`, then `await fetchone() is None` |
| Empty result | false predicate | `await fetchall() == []`, `description` names the columns |
| `executemany` | three sets | `rowcount == 3` |
| Fetch before execute | `cursor.fetchall()` | raises `ProgrammingError` at the call |
| Closed | `await cursor.close()` twice, then `cursor.execute(...)` | raises `InterfaceError` at the call |
| Constraint | duplicate primary key | `await` raises `IntegrityError` with `sqlstate == "23000"` |
| Lazy begin | `client.cursor(lazy_begin=True)`; awaited insert; read `client.in_transaction`; `await client.rollback()`; count rows | `True`; zero rows |
| Lazy begin after commit | same cursor: insert, commit, insert, rollback | one row |
| Execute in flight | call `cursor.execute(...)` twice before awaiting the first | the second call raises `InterfaceError` with `cursor is already executing`; the first awaitable completes |
| Failed execute clears | successful query, then failing `execute` | the awaitable raises; `description is None`, `rowcount == -1`; `cursor.fetchall()` raises `ProgrammingError` at the call |
| `arraysize` and `fetchmany` sizes | set `arraysize` to `0`; `cursor.fetchmany(-1)`; `await cursor.fetchmany(0)` | `ProgrammingError`; `ProgrammingError` at the call; `[]` |

## Verification

- Build: `uv run maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml`
- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q -k native_cursor`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._duckdb`
- Rust: `(cd src/rs && cargo test -p vertebra-duckdb --locked && cargo clippy -p vertebra-duckdb --all-targets -- -D warnings)`

## Acceptance Criteria

- [ ] Every row of the test matrix passes.
- [ ] `mypy.stubtest vertebra._duckdb` passes with no allow-list change.
- [ ] The stub diff adds `AsyncDuckDBCursor`, `AsyncDuckDBClient.cursor`, and one `__all__` entry only.
- [ ] The t07 tests pass unchanged.

## Notes & Discoveries

- `ExecutingGuard` and `ready_awaitable` mirror the SQLite async cursor. The guard keeps `settled` as a plain `bool` because `spawn_rust_db_task_with_attr` takes no cancel callback that would need to share it.
- Cancellation relies on the future being dropped: `CancelOnDropGuard` interrupts the statement and the dropped `ExecutingGuard` clears the result. No test cancels an in-flight awaitable.
- The description getter body is repeated from `SyncDuckDBCursor` because the sync cursor is not edited in this task.
- Tests beyond the matrix: empty `executemany`, `INSERT ... RETURNING`, parameter conversion errors raised at the call, and a default cursor that autocommits.
- `make lint` stops at Pyright with one error outside this task: `src/py/vertebra/_typing.py:360` cannot resolve `numpy`, which is not installed in `.venv`. Biome, tsc, Ruff, rustfmt, Clippy, and Mypy passed before it; `check_extras` and the 13-extension stubtest stage did not run. `mypy.stubtest vertebra._duckdb` passes on its own.
- The build ran as `uv run --no-sync maturin develop --uv ...` with `UV_CACHE_DIR` set to a writable directory. Without `--no-sync`, `uv` tries to fetch `maturin` and the request is refused (HTTP 403); the default cache is read-only on this host.
