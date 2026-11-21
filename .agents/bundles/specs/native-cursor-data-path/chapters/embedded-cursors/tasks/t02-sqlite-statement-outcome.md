---
type: Task
id: "native-cursor-data-path:embedded:t02"
chapter_id: embedded-cursors
title: "SQLite statement outcome and statement facts cache"
description: "Add one SqlitePool entry point that returns columns, rows, rowcount, and lastrowid, and cache per-SQL facts in the pool."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/rs/vertebra-sqlite/src/worker.rs:89-160"
  - "src/rs/vertebra-sqlite/src/worker.rs:228-330"
  - "src/rs/vertebra-sqlite/src/worker.rs:404-530"
  - "src/rs/vertebra-sqlite/src/worker.rs:633-660"
  - "src/rs/vertebra-sqlite/src/worker.rs:745-942"
  - "src/rs/vertebra-sqlite/src/value.rs:244-260"
  - "src/rs/vertebra-sqlite/src/errors.rs:110-162"
  - "src/rs/vertebra-sqlite/tests/sqlite_client.rs:1-60"
  - "src/rs/vertebra-core/src/text.rs:1043-1049"
files:
  - src/rs/vertebra-sqlite/src/worker.rs
  - src/rs/vertebra-sqlite/src/value.rs
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
estimated_loc: 200
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs
    title: "SqlitePool, worker tasks, and statement execution"
tags: [sqlite, rust, statement-cache]
---

# SQLite statement outcome and statement facts cache

## Objective & Scope Boundary

The pool can run any statement and report what the engine did: whether it produced a result set, how many rows changed, and the last inserted rowid. The cursor classes in t03 and t04 call this and nothing else.

No PyO3 class changes here. Existing pool methods (`execute`, `fetch_all`, and the rest) keep their signatures and results. `src/rs/vertebra-sqlite/src/errors.rs` is not edited: the cursor raises through the existing `into_py_err`.

## Bounded Read Manifest

- `worker.rs:89-160` — `WorkerTask` variants.
- `worker.rs:228-330` — `run_flagged_op` and `run_worker_task`, including how `Execute` stores `last_rowid`.
- `worker.rs:404-530` — `execute_statement`, `execute_many_statement`, `fetch_all_query`.
- `worker.rs:633-660` — `is_read_only_sql` and the `SqlitePool` fields.
- `worker.rs:745-942` — `dispatch_read`, `dispatch_write`, `execute_with_op_id`, `execute_many_with_op_id`, `fetch_all_with_op_id`.
- `value.rs:244-260` — `QueryResult`.
- `errors.rs:110-162` — the `SqliteClientError` variants the new functions return.
- `tests/sqlite_client.rs:1-60` — how tests open a pool.
- `vertebra-core/src/text.rs:1043-1049` — `classify_sql_statement` returns `(is_row_returning, is_insert, is_read_only)`.

## Contracts & Edge Cases

`value.rs`:

```rust
#[derive(Clone, Debug, Default)]
pub struct StatementOutcome {
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<SqliteValue>>,
    pub rowcount: i64,
    pub lastrowid: Option<i64>,
}
```

`worker.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatementFacts { pub read_only: bool, pub is_insert: bool }

impl SqlitePool {
    pub fn statement_facts(&self, sql: &str) -> StatementFacts;
    pub fn statement_facts_len(&self) -> usize;
    pub fn run(&self, sql: String, params: BoundParams) -> Result<StatementOutcome, SqliteClientError>;
    pub fn run_with_op_id(&self, op_id: u64, sql: String, params: BoundParams) -> Result<StatementOutcome, SqliteClientError>;
    pub fn run_many(&self, sql: String, params_seq: Vec<BoundParams>) -> Result<StatementOutcome, SqliteClientError>;
    pub fn run_many_with_op_id(&self, op_id: u64, sql: String, params_seq: Vec<BoundParams>) -> Result<StatementOutcome, SqliteClientError>;
}
```

- Facts cache: a new `SqlitePool` field `statement_facts: parking_lot::RwLock<HashMap<Box<str>, StatementFacts>>`. A hit takes the read lock. A miss computes `read_only = is_read_only_sql(sql)` and `is_insert = vertebra_core::text::classify_sql_statement(sql).1`, takes the write lock, clears the map when `len() == STATEMENT_CACHE_CAPACITY`, then inserts.
- `fetch_all_with_op_id` reads `self.statement_facts(&sql).read_only` in place of its direct `is_read_only_sql(&sql)` call.
- New `Run { sql, params, is_insert, op_id, resp }` variant on the `WorkerTask` enum, with `resp: mpsc::Sender<Result<StatementOutcome, SqliteClientError>>`. `run_with_op_id` sends it through `dispatch_read` when `read_only`, otherwise `dispatch_write`, with the same closed-connection and dropped-response errors as `fetch_all_with_op_id`.
- New private `fn run_statement(conn: &mut Connection, sql: &str, params: &BoundParams, is_insert: bool) -> Result<StatementOutcome, SqliteClientError>`:
  1. `prepare_cached`, then `params.bind_to_stmt`.
  2. Read `stmt.column_count()` and `stmt.readonly()`. Read `conn.total_changes()` as `before`.
  3. Step every row through `raw_query`. Collect values only when the column count is above zero.
  4. `columns` is `Some(names)` when the column count is above zero, otherwise `None`.
  5. `rowcount` is `-1` when read-only. Otherwise it is `0` when `conn.total_changes() == before`, else `conn.changes() as i64`.
  6. `lastrowid` is `Some(id)` only when `is_insert`, `total_changes` moved, and `conn.last_insert_rowid() > 0`.
- The `Run` arm of `run_worker_task` wraps `run_statement` in `run_flagged_op` and stores `conn.last_insert_rowid()` into `last_rowid` on success, as the `Execute` arm does.
- `run_many_with_op_id` sends the existing `ExecuteMany` task and maps the count into `StatementOutcome { columns: None, rows: Vec::new(), rowcount: count as i64, lastrowid: None }`. An empty `params_seq` returns that outcome with `rowcount: 0` without dispatching.
- A statement with a returning clause sets `columns` to `Some` and `rowcount` to the changed-row count.
- DDL sets `columns` to `None` and `rowcount` to `0`.

## Test Matrix

Add to `tests/sqlite_client.rs`.

| Case | Input | Expected |
| --- | --- | --- |
| Query | `run("SELECT 1 AS a")` | `columns == Some(["a"])`, one row, `rowcount == -1`, `lastrowid == None` |
| Empty result | `SELECT` with a false predicate | `columns` is `Some`, `rows` empty |
| Insert | `INSERT` into a rowid table | `columns == None`, `rowcount == 1`, `lastrowid == Some(1)` |
| Update after insert | `UPDATE` of two rows | `rowcount == 2`, `lastrowid == None` |
| DDL after DML | `CREATE TABLE` following an insert | `rowcount == 0`, `columns == None` |
| Returning | `INSERT ... RETURNING id` | `columns == Some(["id"])`, `rowcount == 1`, `lastrowid` set |
| Many | `run_many` with three parameter sets | `rowcount == 3`, `columns == None` |
| Many, empty | `run_many` with no sets | `rowcount == 0` |
| Constraint | duplicate primary key | an `Err` holding the `Integrity` variant |
| Facts cached | same SQL text run twice | `statement_facts_len()` grows by one |
| Facts cleared | 257 distinct SQL texts | `statement_facts_len() == 1` after the last |

## Verification

- Red then green: `(cd src/rs && cargo test -p vertebra-sqlite --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-sqlite && cargo clippy -p vertebra-sqlite --all-targets -- -D warnings)`

## Acceptance Criteria

- [ ] `run` and `run_many` on `SqlitePool` return the outcome described above for every row of the test matrix.
- [ ] `fetch_all_with_op_id` no longer calls `is_read_only_sql` directly.
- [ ] `src/rs/vertebra-sqlite/src/errors.rs` has no diff.
- [ ] Existing tests in the crate pass unchanged.

## Notes & Discoveries

- `cargo test -p vertebra-sqlite --locked` passes (8 unit, 16 integration; 5 new integration tests cover every matrix row including "Facts cleared"). `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass for the crate.
- `make lint` exits non-zero in the worker sandbox. Biome, `tsc`, Ruff, Rust fmt and Clippy, and Mypy pass. Pyright reports one error outside this task: `src/py/vertebra/_typing.py:360` cannot resolve `numpy`, which is not installed in `.venv`. The `check_extras` and stubtest steps that follow Pyright did not run.
- `make lint` needed `uv` added to `PATH` and `UV_CACHE_DIR` pointed at a scratch directory with `UV_NO_SYNC=1`, because `~/.cache/uv` is read-only in the sandbox.
- `run_many` reuses the `ExecuteMany` task, so it does not update the pool's `last_insert_rowid()`. Recorded in `knowledge/domains/data/sqlite-limitations.md`.
- `select_to_arrow_with_op_id` still calls `is_read_only_sql` directly; the worksheet only moves `fetch_all_with_op_id` to the facts cache.
