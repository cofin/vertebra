---
type: Task
id: "native-cursor-data-path:single:t03"
chapter_id: single-call-paths
title: "SQLite statement run: one interrupt lock, single facts lookup, flat row buffer"
description: "Trim SqlitePool::run: borrow the SQL text, call the connection slot directly, lock the interrupt state once per statement, and look statement facts up only when they are needed. The row buffer stays nested, with the reason recorded."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on: []
read_set:
  - "src/rs/vertebra-sqlite/src/worker.rs:18-67"
  - "src/rs/vertebra-sqlite/src/worker.rs:88-125"
  - "src/rs/vertebra-sqlite/src/worker.rs:168-213"
  - "src/rs/vertebra-sqlite/src/worker.rs:243-257"
  - "src/rs/vertebra-sqlite/src/worker.rs:259-344"
  - "src/rs/vertebra-sqlite/src/worker.rs:552-607"
  - "src/rs/vertebra-sqlite/src/worker.rs:727-816"
  - "src/rs/vertebra-sqlite/src/worker.rs:842-888"
  - "src/rs/vertebra-sqlite/src/worker.rs:1057-1134"
  - "src/rs/vertebra-sqlite/src/worker.rs:1465-1486"
  - "src/rs/vertebra-sqlite/src/py_client.rs:826-839"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1104-1118"
  - "src/rs/vertebra-sqlite/tests/sqlite_client.rs:350-380"
  - "src/rs/vertebra-sqlite/tests/sqlite_client.rs:509-553"
  - "src/rs/vertebra-sqlite/tests/sqlite_client.rs:556-720"
files:
  - src/rs/vertebra-sqlite/src/worker.rs
  - src/rs/vertebra-sqlite/src/py_client.rs
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
estimated_loc: 240
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: sqlite-pool
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: "SQLite pool, statement facts cache, and interrupt bookkeeping"
tags: [sqlite, rust, performance, cancellation]
---

# SQLite statement run: one interrupt lock, single facts lookup, flat row buffer

## Objective & Scope Boundary

One statement through `run` on `SqlitePool` does less bookkeeping: it borrows the SQL text, takes the interrupt-state lock once, skips the per-statement channel, and hashes the SQL text for statement facts only when the answer is used.

Nothing visible from Python changes. Results, error classes, and cancel behavior are the same.

The edit to `py_client.rs` is two call sites that now pass a reference. The cursor keeps its owned copy of the SQL text in this task; t02 removes it.

The title names a flat row buffer. This task keeps `Vec<Vec<SqliteValue>>`. The reason is under Notes & Discoveries.

## Bounded Read Manifest

- `worker.rs:18-67` — `InterruptState` and its methods.
- `worker.rs:88-125` — `WorkerTask`, with the `Run` variant.
- `worker.rs:168-213` — `WorkerSlot`, `spawn`, `send`.
- `worker.rs:243-257` — `run_flagged_op`.
- `worker.rs:259-344` — `run_worker_task` through the `Run` arm.
- `worker.rs:552-607` — `run_statement`.
- `worker.rs:727-816` — `StatementFacts`, `SqlitePool`, `open`, `statement_facts`.
- `worker.rs:842-888` — `ensure_open_and_not_cancelled`, `dispatch_read`, `dispatch_write`.
- `worker.rs:1057-1134` — `run`, `run_with_op_id`, `run_many`, `run_many_with_op_id`.
- `worker.rs:1465-1486` — `cancel`, `cancel_operation`, `close`.
- `py_client.rs:826-839`, `1104-1118` — the two callers of `run` and `run_with_op_id`.
- `tests/sqlite_client.rs:350-380`, `509-553` — the two cancel tests. `556-720` — the `run` tests and the facts cache test.

## Symbol & Signature Contracts

### Interrupt state

```rust
struct SlotInterrupt {
    handle: Arc<InterruptHandle>,
    running_op: Arc<AtomicU64>,
}

#[derive(Default)]
struct InterruptState {
    slots: Vec<SlotInterrupt>,
    cancelled: HashSet<u64>,
    cancelled_order: VecDeque<u64>,
}

impl InterruptState {
    fn register_slot(&mut self, handle: &Arc<InterruptHandle>) -> Arc<AtomicU64>;
    fn take_if_cancelled(&mut self, op_id: u64) -> bool;
    fn begin_op(&mut self, op_id: u64, running_op: &AtomicU64) -> bool;
    fn cancel_op(&mut self, op_id: u64) -> bool;
    fn cancel_all(&self);
}
```

- The `active` map and `end_op` are deleted.
- `running_op` holds the id of the operation running on that connection, or `0` when it is idle. Operation ids start at `1`.
- `register_slot` pushes a `SlotInterrupt` with a new atomic set to `0` and returns the atomic.
- `take_if_cancelled` is unchanged.
- `begin_op` returns `false` when `cancelled.remove(&op_id)` is true. Otherwise it stores `op_id` in `running_op` with `Release` ordering and returns `true`.
- `cancel_op` scans `slots`. When a slot's `running_op` equals `op_id`, it calls `handle.interrupt()` and returns `true`. When none matches, it records `op_id` in `cancelled` with the existing eviction and returns `true`.
- `cancel_all` calls `handle.interrupt()` on every slot whose `running_op` is not `0`.

`WorkerSlot` gains the field `running_op: Arc<AtomicU64>`. `spawn` on `WorkerSlot` sets it from `interrupts.lock().register_slot(&interrupt_handle)`.

```rust
fn run_flagged_op<T>(
    interrupts: &Mutex<InterruptState>,
    op_id: u64,
    running_op: &AtomicU64,
    f: impl FnOnce() -> Result<T, SqliteClientError>,
) -> Result<T, SqliteClientError>;
```

It locks once for `begin_op`, returns `Cancelled("operation cancelled")` when that is `false`, runs `f`, then stores `0` in `running_op` with `Release` ordering without the lock. `run_worker_task` takes `running_op: &AtomicU64` in place of `interrupt_handle` and passes it to every `run_flagged_op` call. `send` on `WorkerSlot` passes `&self.running_op`.

`ensure_open_and_not_cancelled` is renamed `ensure_open`. When the pool is closed it does what it does today, including `take_if_cancelled`. When the pool is open it returns `Ok(())` without locking. An operation cancelled before it started is caught by `begin_op`, with the same error. `dispatch_read`, `dispatch_write`, and `begin_with_op_id` call the new name.

### Statement run

```rust
pub fn run(&self, sql: &str, params: BoundParams) -> Result<StatementOutcome, SqliteClientError>;

pub fn run_with_op_id(
    &self,
    op_id: u64,
    sql: &str,
    params: BoundParams,
) -> Result<StatementOutcome, SqliteClientError>;
```

`run_with_op_id`, in order:

1. `self.ensure_open(op_id)?`.
2. `reader_eligible` is true when `self.readers` is not empty and no transaction is open.
3. `known` is `Some(self.statement_facts(sql))` when `reader_eligible`, otherwise `None`.
4. The slot is the next reader, picked with `reader_cursor` as `dispatch_read` picks it, when `known` says `read_only`. Otherwise it is the writer.
5. Return `slot.run_statement(op_id, sql, &params, is_insert)`, where `is_insert` is a closure that returns `known`'s `is_insert` when present and `self.statement_facts(sql).is_insert` otherwise.

New method on `WorkerSlot`:

```rust
fn run_statement(
    &self,
    op_id: u64,
    sql: &str,
    params: &BoundParams,
    is_insert: impl FnOnce() -> bool,
) -> Result<StatementOutcome, SqliteClientError>;
```

It locks `self.conn`. With no connection it returns the `Operational` error `SQLite connection is closed` with sqlstate `08000`, the value the `Run` dispatch returns today. Otherwise it calls `run_flagged_op`, and inside it the free function `run_statement`, then stores `conn.last_insert_rowid()` in `last_rowid` on success, as the `Run` arm does today.

The free function becomes:

```rust
fn run_statement(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
    is_insert: impl FnOnce() -> bool,
) -> Result<StatementOutcome, SqliteClientError>;
```

`lastrowid` is computed as `if changed && is_insert() { ... }`. `changed` is tested first, so the closure does not run for a statement that changed no rows. Everything else in the function is unchanged.

Deleted: the `Run` variant of `WorkerTask`, its arm in `run_worker_task`, and the `mpsc` channel in `run_with_op_id`.

`run_many` and `run_many_with_op_id` keep `sql: String`.

### Callers

- `py_client.rs:837` becomes `pool.run(&sql_owned, bound)`.
- `py_client.rs:1116` becomes `pool.run_with_op_id(op_id, &sql_owned, bound)`.
- In `tests/sqlite_client.rs`, every `run(` call drops its `.to_string()` on the SQL argument.

## Edge Cases & Error Semantics

- `cancel()` while a statement runs: the slot's `running_op` is not `0`, so its handle is interrupted. The statement returns the cancelled error, as today.
- `cancel()` with nothing running: no handle is interrupted. A later statement runs normally.
- `cancel_operation(id)` while that operation runs: its slot is interrupted.
- `cancel_operation(id)` before the operation starts: the id goes into `cancelled`. `begin_op` returns `false`, and the statement does not run. The caller now waits for the connection lock before it gets the error; before, it got the error ahead of the lock when the cancel arrived before dispatch.
- `cancel_operation(id)` after the operation finished: the id goes into `cancelled` and is evicted by the existing 4096-entry limit. No later operation reuses the id.
- A cancel that reads `running_op` just before the statement ends interrupts an idle connection. SQLite clears the interrupt flag when the next statement starts with no statement active. The same window exists today between the end of the statement and `end_op`.
- The next statement on that connection cannot start stepping while `cancel_op` or `cancel_all` holds the lock, because `begin_op` needs the lock.
- A closed connection inside an open pool (`send` on `WorkerSlot` returns `Err`, or `run_statement` finds no connection) leaves a pre-cancelled id in `cancelled`. The eviction limit bounds it.
- A panic inside `f` leaves `running_op` set. A later cancel for that id interrupts an idle connection, which is harmless. No guard is added.
- With reader connections and no open transaction, facts are looked up once before dispatch and reused for `is_insert`.
- With no reader connections, or inside a transaction, facts are looked up only for a statement that changed rows. A read never touches the facts cache.
- The prepared-statement cache hashes the SQL text once per statement. That stays: `prepare_cached` takes the text and offers no entry point for a precomputed key.

## Public-Seam Test Blueprint

Existing tests that pin cancel behavior. Both must pass with no edit:

- `test_recursive_cte_cancellation` — `cancel()` interrupts a running query within 200 ms, and the next query succeeds.
- `test_begin_mode_whitelist_and_op_id_cancellation` — a pre-cancelled id fails before execution, a live id is interrupted, and a closed pool rejects queries.

Edited test:

- `test_statement_facts_cache_reuses_and_clears` — the loop that ran `SELECT 1 AS a` twice through `run` on an in-memory pool no longer adds a cache entry. Create `items` first, then change the loop to run `INSERT INTO items (name) VALUES ('a')` twice, and assert `before + 1`. The create statement changes no rows and adds no entry; the repeated insert adds one. Then assert that two `run` calls of `SELECT 1 AS a` leave `statement_facts_len()` unchanged. The direct `statement_facts` assertions and the 257-entry clear stay.

New tests in `tests/sqlite_client.rs`:

| Test | Steps | Expected |
| --- | --- | --- |
| `test_run_pre_cancelled_op_does_not_execute` | create a table; `id = next_op_id()`; `cancel_operation(id)`; `run_with_op_id(id, "INSERT ...")`; count rows | `Cancelled` error; zero rows |
| `test_run_live_op_cancellation_interrupts_statement` | thread runs the recursive CTE through `run_with_op_id(live, ...)`; sleep 20 ms; `cancel_operation(live)` | the thread returns an error; a later `run("SELECT 1")` succeeds |
| `test_run_cancel_all_interrupts_running_statement` | thread runs the recursive CTE through `run`; sleep 20 ms; `cancel()` | error within 200 ms; a later `run` succeeds |
| `test_cancel_with_nothing_running_leaves_next_statement_alone` | `cancel()`; `run("SELECT 1 AS a")` | one row |
| `test_cancel_of_finished_op_leaves_later_ops_alone` | `run_with_op_id(a, "SELECT 1")`; `cancel_operation(a)`; `run("SELECT 2")` | `true`; one row |
| `test_run_routes_reads_to_readers_and_reports_insert_rowid` | file-backed pool with readers: create, insert through `run`, select through `run` | `lastrowid` is `Some(1)` for the insert; the select returns the row; `statement_facts_len()` is `3` |
| `test_run_in_memory_looks_facts_up_only_for_changes` | in-memory pool: select, then insert | `statement_facts_len()` is `0` after the select and `1` after the insert |

## Verification

- Exports for raw `cargo test`: copy the interpreter exports from the `rs-test` recipe in `Makefile`.
- Red then green: `(cd src/rs && cargo test -p vertebra-sqlite --test sqlite_client --locked)`
- Whole crate: `(cd src/rs && cargo test -p vertebra-sqlite --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-sqlite && cargo clippy -p vertebra-sqlite --all-targets --locked -- -D warnings)`
- Rebuild and run the Python suite, which must pass with no edit: `make dev`, then `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q`
- Removed names, each must print nothing:
  - `git grep -n "end_op\|ensure_open_and_not_cancelled\|WorkerTask::Run" -- src/rs/vertebra-sqlite/src`

## Acceptance Criteria

- [ ] The two existing cancel tests pass with no edit.
- [ ] The seven new tests and the edited facts test pass.
- [ ] A statement through `run_with_op_id` locks the interrupt state in `begin_op` and nowhere else on the success path.
- [ ] `run` and `run_with_op_id` take `sql: &str`, and no `mpsc` channel is created for them.
- [ ] `test_sqlite_driver.py` passes with no edit.
- [ ] The removed-names check prints nothing.

## Notes & Discoveries

- Row buffer: `StatementOutcome.rows` stays `Vec<Vec<SqliteValue>>`. Each row vector is moved into its Python tuple, so the nested shape costs one allocation per row and no copy. A flat buffer with a column stride would remove that allocation. It would also change `StatementOutcome`, the state of both cursors, the row converters, and six tests, which does not fit beside the interrupt and facts work in one run. The single-call methods in t04 turn the outcome straight into Python objects, so no second buffer is filled on that path. If the closing capture in t27 still misses the native SQLite row, the flat buffer is the next step.
- `begin_with_op_id` already routes through `dispatch_write` (which calls `ensure_open`), so `dispatch_read`, `dispatch_write`, and `run_with_op_id` are the direct callers of `ensure_open`.
- Rebuilt `vertebra._sqlite` directly via `.venv/bin/maturin develop --uv -m src/rs/vertebra-sqlite/Cargo.toml` with `UV_CACHE_DIR=/tmp/uv-cache` (sandbox `~/.cache/uv` is read-only) rather than full `make dev` across all 13 extensions; `test_sqlite_driver.py` passed (`95 passed`) with no edits.
