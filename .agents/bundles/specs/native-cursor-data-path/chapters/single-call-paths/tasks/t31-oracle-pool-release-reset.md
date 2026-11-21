---
type: Task
id: "native-cursor-data-path:single:t31"
chapter_id: single-call-paths
title: "Oracle pool release rolls back only a connection that may hold a transaction"
description: "An Oracle pool release sends a rollback only when a transaction is or may be open on the connection, and no round trip after a committed statement."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t15"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t15-oracle-bind-and-sync-single-call.md"
  - "src/rs/vertebra-oracle/src/client.rs:1075-1150"
  - "src/rs/vertebra-oracle/src/client.rs:1349-1372"
  - "src/rs/vertebra-oracle/src/client.rs:1505-1518"
  - "src/rs/vertebra-oracle/src/client.rs:1672-1765"
  - "src/rs/vertebra-oracle/src/client.rs:2052-2096"
  - "src/rs/vertebra-oracle/src/client.rs:2231-2265"
  - "src/rs/vertebra-oracle/src/client.rs:3573-3600"
  - "src/rs/vertebra-oracle/src/pool.rs:280-355"
  - "src/rs/vertebra-oracle/src/pool.rs:504-570"
  - "src/rs/vertebra-oracle/src/pool.rs:1250-1272"
  - "tools/benchmarks/adapters/sql.py:675-715"
files:
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/rs/vertebra-oracle/src/pool.rs"
tests:
  - "src/py/tests/integration/db/test_oracle_native.py"
estimated_loc: 70
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-21T16:54:59Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, acceptance table and the wire-parity chapter scope"
  - id: oracle-pool-core
    resource: "src/rs/vertebra-oracle/src/pool.rs::OraclePoolCore"
    title: "Pool core whose release sends a rollback on every return"
  - id: oracle-worker
    resource: "src/rs/vertebra-oracle/src/client.rs::OracleWorkerHandle"
    title: "Worker handle, session state, and command handling"
tags: [oracle, rust, pool, performance]
---

# Oracle pool release rolls back only a connection that may hold a transaction

## Objective & Scope Boundary

Returning an Oracle connection to the pool after a committed statement costs no round trip. A connection that holds, or may hold, a transaction is still rolled back before it is reused.

`estimated_loc` counts about 70 Rust lines in `client.rs` and `pool.rs`. Tests add about 90 lines.

Out of scope:

- The single-call methods and the bind fast path (t15, t16).
- Skipping the rollback after a query. The contract below says why that is not provable.
- The async reset tests. Task t16 owns them.
- The acquire path, tags, purity, and session attributes.

## Bounded Read Manifest

- `release_sync` on `OraclePoolCore` (`pool.rs:504-570`) and the blocking call to it in `AsyncOraclePool.release` (`pool.rs:1250-1272`).
- `handle_command` on the worker session (`client.rs:1075-1150` for the transaction commands) and `run_query_or_execute` (`client.rs:1349-1372`, `:1505-1518`) for the autocommit step.
- The worker spawn and its loop (`client.rs:1672-1765`), `dispatch_sync` (`:2052-2096`), and `rollback_with_timeout` (`:2231-2265`).
- The worker test without a connection at `client.rs:3573-3600`.

## Contracts & Edge Cases

`release_sync` on `OraclePoolCore` (`pool.rs:504-570`) sends a rollback on every release through `rollback_with_timeout`. `AsyncOraclePool.release` runs the same function on a blocking thread, so one change covers both pools and the single-call pool methods of t15 and t16.

What the code knows about an open transaction:

- The worker's `in_transaction` flag is local bookkeeping. `Begin`, `Savepoint`, and `RollbackToSavepoint` set it; `Commit`, `Rollback`, and `Close` clear it (`client.rs:1092-1150`). `Begin` sends nothing to the server.
- With the flag clear, `run_query_or_execute` commits after a successful DML or PL/SQL statement and ignores the commit result (`client.rs:1359`, `:1513-1515`). After a failed statement it sends neither commit nor rollback.
- The query branch (`client.rs:1372`) never commits. A query can open a transaction, for example `SELECT ... FOR UPDATE` or a query on a remote object.
- The server reports a transaction-in-progress bit with every reply. The `oracledb` crate, pinned at `26.0.0-beta.5`, keeps it in a private field of its client and exposes no accessor.

So "no open transaction" is provable in one case only: the last server call on the connection was a commit or a rollback that succeeded, or no statement has run since one did. It is not provable after a query.

Contract, in `client.rs`:

- `WorkerSession` gains `may_have_open_txn: bool`, false at spawn. It is read and written only inside `handle_command`, which runs under the session lock.
- Add the private function `fn command_may_open_txn(cmd: &WorkerCommand) -> bool`. It returns false for `Connect`, `Ping`, `Commit`, `Rollback`, `Close`, and the new `ResetIfDirty`, and true for every other command.
- `handle_command` sets `may_have_open_txn = true` before it runs a command for which that function returns true.
- The `Commit` and `Rollback` arms set it to false when they return `Ok`, including when the worker has no connection.
- In `run_query_or_execute`, the autocommit step records the commit result. Add the private method `fn note_autocommit(&mut self, statement_ok: bool, commit_ok: bool)`, which sets `may_have_open_txn = false` only when both are true. Call it after the block that borrows the connection. The statement's own result is unchanged, as today.
- Add the variant `ResetIfDirty` to `WorkerCommand`. Its arm runs the body of the `Rollback` arm when `in_transaction` is set or `may_have_open_txn` is true, and returns the unit reply without touching the connection otherwise.
- Add `pub fn reset_with_timeout(&self, timeout: Duration) -> Result<(), OracleError>` on `OracleWorkerHandle`. It is `rollback_with_timeout` with `ResetIfDirty` as the command; both share one private function that takes the command. The command goes through the worker channel, so it runs after any command still queued on that worker. That ordering is what makes the check sound for the async classes.

Contract, in `pool.rs`: `release_sync` calls `reset_with_timeout` where it calls `rollback_with_timeout` today. Nothing else in the release changes. A failed or timed-out reset closes the worker, as a failed rollback does today.

Resulting rule:

| State at release | Round trip |
| --- | --- |
| Last server call was a successful autocommit of DML or PL/SQL, or an explicit commit or rollback | none |
| Fresh connection with no statement run | none |
| `begin()` without commit or rollback | rollback |
| Any query (`Query` command), with or without rows | rollback |
| A statement that failed, or an autocommit whose commit failed | rollback |
| `ExecuteMany`, Arrow, pipeline, or notification commands | rollback, unless followed by a successful commit |
| `discard` requested, pool closed, or worker closed | none; the worker is closed, as today |

A pooled autocommit select therefore still pays the rollback. Removing it needs the server's transaction-in-progress bit, which the pinned crate does not expose. Record that in the task result for the `wire-parity` chapter.

Benchmark attribution: `capture_oracle_report` in `tools/benchmarks/adapters/sql.py` builds `SyncOracleDriver(cfg)` and `AsyncOracleDriver(cfg)` with no pool (`:693`, `:710`). The Oracle acceptance rows run on a single client and never reach `release_sync`, so this change does not move them.

The pool's acquire connects a new worker (`pool.rs:331`), so pool-level release cases need a database and are live tests. The worker-level cases run without one, on a worker built as `worker_zero_io_spawn_session_attr_staging_and_break_handle_lifecycle` builds its own.

To count resets in tests, `WorkerSession` gains `#[cfg(test)] reset_rollbacks: usize`, incremented each time the `ResetIfDirty` arm runs the rollback body, with a `#[cfg(test)]` accessor on `OracleWorkerHandle`. This follows the `scan_count` field of the statement facts cache.

## Test Matrix

Rust unit tests go in the existing `tests` module of `client.rs`. Live tests named `test_pool_release_reset_*` go in `src/py/tests/integration/db/test_oracle_native.py` and are skipped without a database.

| Case | Input | Expected |
| --- | --- | --- |
| Reset, clean worker (Rust, `client.rs` tests, worker without a connection) | `reset_with_timeout` on a fresh worker | `Ok`; `reset_rollbacks` is 0 |
| Reset, abandoned transaction (Rust) | `Begin`, then `reset_with_timeout` | `Ok`; `reset_rollbacks` is 1; `in_transaction` is false |
| Reset after commit (Rust) | `Begin`, `Commit`, then `reset_with_timeout` | `reset_rollbacks` is 0 |
| Autocommit bookkeeping (Rust) | `note_autocommit` with each pair of flags after a dirty mark | Clean only for `(true, true)` |
| Command classes (Rust) | `command_may_open_txn` for each command | False for `Connect`, `Ping`, `Commit`, `Rollback`, `Close`, `ResetIfDirty`; true for `Query`, `Execute`, `ExecuteMany`, `Begin` |
| Live autocommit DML | `pool.execute_dml(insert)`, then count the row from a second connection | The row is visible |
| Live abandoned transaction | Acquire, `begin()`, insert, release without commit; count from a second connection | The row is absent |
| Live select for update | `pool.fetch_rows("SELECT ... FOR UPDATE")`, then update the same row from a second connection | The update does not block |
| Existing release behavior | The existing pool and adapter tests | Pass unchanged |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make oracle-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py -p no:xdist -q`
- `git grep -n "rollback_with_timeout" -- src/rs/vertebra-oracle/src/pool.rs`

`make oracle-test` runs the crate's `cargo test` with the interpreter exports a PyO3 crate needs. The live tests in it run only when an Oracle database is reachable. The last command prints nothing.

## Acceptance Criteria

- [ ] `release_sync` sends a rollback only in the states the table lists, and the Rust tests count it.
- [ ] The reset command goes through the worker channel, so it runs after any queued command.
- [ ] No Python-visible signature changes, and `mypy.stubtest` passes with the allow-list unchanged.
- [ ] The task result states that pooled autocommit selects still pay the rollback, names the private crate field as the cause, and states that the Oracle acceptance rows do not use the pool.
