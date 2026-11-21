---
type: Task
id: "native-cursor-data-path:embedded:t06"
chapter_id: embedded-cursors
title: "DuckDB statement outcome and statement facts cache"
description: "Add one DuckDBInner entry point that returns columns, rows, and rowcount, and cache per-SQL facts in the pool."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t05"
read_set:
  - "src/rs/vertebra-duckdb/src/lib.rs:76-86"
  - "src/rs/vertebra-duckdb/src/lib.rs:244-258"
  - "src/rs/vertebra-duckdb/src/lib.rs:506-702"
  - "src/rs/vertebra-duckdb/src/lib.rs:1104-1155"
  - "src/rs/vertebra-duckdb/src/lib.rs:1230-1236"
  - "src/rs/vertebra-duckdb/src/lib.rs:1387-1474"
  - "src/rs/vertebra-duckdb/src/lib.rs:3611-3614"
  - "src/rs/vertebra-duckdb/src/lib.rs:3897-4026"
  - "src/rs/vertebra-duckdb/src/errors.rs:130-177"
files:
  - src/rs/vertebra-duckdb/src/lib.rs
tests:
  - src/rs/vertebra-duckdb/src/lib.rs
estimated_loc: 200
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
tags: [duckdb, rust, statement-cache]
---

# DuckDB statement outcome and statement facts cache

## Objective & Scope Boundary

The DuckDB pool can run any statement and report columns, rows, and the changed-row count in one call. SQL text is scanned once per distinct statement and the result is cached in the pool. The cursor classes in t07 and t08 call this entry point.

No PyO3 class changes here. `execute_sql`, `query_sql`, and `execute_many_sql` keep their signatures and results, including transaction-control handling, the read-only rejection, and the transaction journal. `src/rs/vertebra-duckdb/src/errors.rs` is not edited: the cursor raises through the existing `into_py_err`.

## Bounded Read Manifest

- `lib.rs:76-86` — `QueryResultSet`.
- `lib.rs:244-258` — `DuckDBInner` fields.
- `lib.rs:506-702` — `select_worker`, `run_on_pool`, `execute_sql`, `execute_many_sql`, `query_sql`.
- `lib.rs:1104-1155` — `is_read_only_sql`, `is_mutating_sql`, `leading_sql_keyword`.
- `lib.rs:1230-1236` — `normalize_named_sql` signature.
- `lib.rs:1387-1474` — `execute_statement_on_conn`, `query_rows_on_conn`.
- `lib.rs:3611-3614` — `sql_returns_rows`.
- `lib.rs:3897-4026` — test module setup and two existing tests.
- `errors.rs:130-177` — the `DuckDBClientError` variants the new functions return.

## Contracts & Edge Cases

`lib.rs`:

```rust
#[derive(Clone, Debug)]
pub struct StatementFacts {
    pub read_only: bool,
    pub mutating: bool,
    pub returns_rows: bool,
    pub maybe_tx_control: bool,
    pub named_sql: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct StatementOutcome {
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<DuckValue>>,
    pub rowcount: i64,
}

impl DuckDBInner {
    pub fn statement_facts(&self, sql: &str) -> StatementFacts;
    pub fn statement_facts_len(&self) -> usize;
    pub fn run_statement(&self, sql: String, params: BoundParams, cancel_flag: &Arc<AtomicBool>) -> Result<StatementOutcome, DuckDBClientError>;
    pub fn run_statement_many(&self, sql: String, param_sets: Vec<BoundParams>, cancel_flag: &Arc<AtomicBool>) -> Result<StatementOutcome, DuckDBClientError>;
}
```

- Facts cache: a new `DuckDBInner` field `statement_facts: parking_lot::RwLock<HashMap<Box<str>, StatementFacts>>` with a new constant `STATEMENT_FACTS_CAPACITY: usize = 256`. A hit takes the read lock and clones. A miss computes the facts, takes the write lock, clears the map when it is full, and inserts.
- Facts: `read_only = is_read_only_sql(sql)`, `mutating = is_mutating_sql(sql)`, `returns_rows = sql_returns_rows(sql)`, `named_sql = normalize_named_sql(sql)`. `maybe_tx_control` is true when `leading_sql_keyword(sql)` is one of `BEGIN`, `START`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT`, `RELEASE`.
- `execute_sql`, `execute_many_sql`, and `query_sql` read `self.statement_facts(&sql)` in place of their direct `is_mutating_sql` and `is_read_only_sql` calls. `execute_sql` runs its transaction-control parsing only when `maybe_tx_control` is true.
- `execute_statement_on_conn` and `query_rows_on_conn` gain a `named_sql: &str` parameter and use it when the parameters are the `Named` variant of `BoundParams`. All callers pass the cached value. The broker backend callers compute it with `normalize_named_sql` as today.
- `run_statement`: when `returns_rows` is false, call `execute_sql` and return `StatementOutcome { columns: None, rows: Vec::new(), rowcount: changed }`. When true, call `query_sql` and return `columns: Some(result.columns)`, the rows, and `rowcount` equal to `rows.len()` when `mutating`, otherwise `-1`.
- `run_statement_many`: call `execute_many_sql` and return `columns: None`, no rows, and the total as `rowcount`. An empty `param_sets` returns `rowcount: 0`.
- A transaction-control statement through `run_statement` returns `columns: None` and `rowcount: 0`.
- A mutating statement on a read-only database returns the `Programming` variant of `DuckDBClientError`, as `execute_sql` does.

## Test Matrix

Add to the `tests` module in `lib.rs`.

| Case | Input | Expected |
| --- | --- | --- |
| Query | `run_statement("SELECT 1 AS a")` | `columns == Some(["a"])`, one row, `rowcount == -1` |
| Empty result | `SELECT` with false predicate on a table | `columns` is `Some` with the column names, no rows |
| Insert | `INSERT` of two rows | `columns == None`, `rowcount == 2` |
| DDL | `CREATE TABLE` | `columns == None`, `rowcount == 0` |
| Returning | `INSERT ... RETURNING id` | `columns == Some(["id"])`, `rowcount == rows.len()` |
| Named parameters | `SELECT $v` with a named map | one row |
| Transaction control | `BEGIN`, insert, `ROLLBACK` through `run_statement` | `in_transaction` toggles; the row is gone |
| Many | `run_statement_many` with three sets | `rowcount == 3` |
| Read-only | insert on a read-only database | `Err(Programming)` |
| Facts cached | same SQL text twice | `statement_facts_len()` grows by one |
| Facts cleared | 257 distinct SQL texts | `statement_facts_len() == 1` after the last |
| Existing behavior | all five existing tests | pass unchanged |

## Verification

- Red then green: `(cd src/rs && cargo test -p vertebra-duckdb --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-duckdb && cargo clippy -p vertebra-duckdb --all-targets -- -D warnings)`
- Driver suite still green: `uv run maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml && uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q`

## Acceptance Criteria

- [ ] `run_statement` and `run_statement_many` return the outcome described above for every row of the test matrix.
- [ ] `execute_sql`, `execute_many_sql`, and `query_sql` read cached facts and no longer scan SQL text on a cache hit.
- [ ] `src/rs/vertebra-duckdb/src/errors.rs` has no diff.
- [ ] `test_duckdb_driver.py` passes unchanged.

## Notes & Discoveries

- The facts cache field is `std::sync::RwLock<HashMap<Box<str>, StatementFacts>>`, not `parking_lot::RwLock`. `parking_lot` is not a dependency of `vertebra-duckdb`, and adding it means editing `Cargo.toml` and `Cargo.lock`, which this task does not own. Lock poisoning is recovered with `PoisonError::into_inner`, as the other locks in this file do.
- The tests were written together with the implementation, so no failing (red) run was recorded.
- `execute_sql` transaction-control parsing moved into a private `run_tx_control` helper, called only when `maybe_tx_control` is true. The matching rules are unchanged.
- `execute_statement_on_conn` and `query_rows_on_conn` have no broker backend callers in this crate. Their only other caller is the savepoint journal replay in `rollback_to_savepoint_tx`, which now looks up cached facts for each replayed statement before handing the work to the worker.
- `query_rows_on_conn` still calls `is_read_only_sql(sql)` when a statement yields zero columns. That path is not covered by the cache.
- `query_arrow_payload` and `stream_arrow_shm_batches` are outside this task and still scan SQL text on every call.
- A SQL text containing a NUL byte, or one sent to a closed pool, is entered in the facts cache before `execute_sql` or `query_sql` rejects it.
- The driver suite ran as `uv run --no-sync maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml` (added `--no-sync`) with `UV_CACHE_DIR` pointed at a scratch directory, because `~/.cache/uv` is read-only in the sandbox.
