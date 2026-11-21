---
type: Task
id: "native-cursor-data-path:network:t05"
chapter_id: network-engines
title: "AsyncPostgresCursor on the asynchronous client"
description: "Add AsyncPostgresCursor, created by AsyncPostgresClient.cursor(), with awaitable execute and fetch methods."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t42"
  - "native-cursor-data-path:network:t04"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/rs/vertebra-postgres/src/async_client.rs:100-125"
  - "src/rs/vertebra-postgres/src/async_client.rs:1850-1905"
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/py/vertebra/_postgres.pyi:228-262"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/async_client.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_queries.py"
estimated_loc: 230
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
  - id: pg-async-client
    resource: "src/rs/vertebra-postgres/src/async_client.rs::PyAsyncPostgresClient"
    title: "Async client that creates the cursor"
tags: [postgres, rust, pyo3, async]
---

# AsyncPostgresCursor on the asynchronous client

## Objective & Scope Boundary

A caller holding an `AsyncPostgresClient` gets a cursor whose `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, and `close` are awaitable and whose attributes are plain values.

Out of scope:

- Pool-backed cursors (t06).
- Python driver changes (t08).

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-postgres/src/async_client.rs:1850-1905` is the async `query` method. The cursor builds its awaitable the same way.
- `src/rs/vertebra-postgres/src/async_client.rs:100-125` is the struct and `register`.
- `src/rs/vertebra-postgres/src/cursor.rs` after t04 holds the sync class to mirror.

## Contracts & Edge Cases

- `cursor` also takes keyword-only `lazy_begin: bool = False`, in the Rust signature and in the stub. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when the session has none open, then run the statement. A failed begin clears the result and raises. Test rows: a cursor created with `lazy_begin=True` leaves `in_transaction` true after an insert and `rollback()` removes the row; the same cursor after `commit()` opens a new transaction on its next statement; a default cursor opens none.
- Lease methods, on each client class this task edits: `attach_lease(self, release: Callable[[], None]) -> None` and `release_lease(self) -> None`, both plain methods, with stub entries. The client holds a `Mutex<Option<LeaseGuard>>` using the guard from t42. `attach_lease` releases a guard that is already held, then stores the new one. `release_lease` releases now and is idempotent. The client's `close` releases the guard, and dropping the client releases it through the guard's drop. Test rows: `attach_lease` with a counting callable then `release_lease` twice gives one call; `attach_lease` then `del client` and `gc.collect()` gives one call; a second `attach_lease` releases the first callable.
- Rust type `PyAsyncPostgresCursor`, Python name `AsyncPostgresCursor`. Same state type and same `execute` steps as t04.
- `AsyncPostgresClient.cursor(self) -> AsyncPostgresCursor` is a plain method, not awaitable.
- `execute` and `executemany` return the crate's `RustDbTaskAwaitable`, resolving to the cursor itself. Parameter ordering, normalization, and extraction happen on the calling thread before the task starts, so argument errors raise at call time as `InterfaceError`.
- `fetchone`, `fetchmany`, `fetchall`, and `close` return a `RustDbTaskAwaitable` that completes without I/O from the buffered state.
- `description`, `rowcount`, `lastrowid`, and `arraysize` are plain properties with the same rules as t04.
- A second `execute` started while one is in flight on the same cursor raises `InterfaceError` with the message `cursor is already executing`.
- Cancelling the awaiting task cancels the server query through the session's existing cancel path and leaves the cursor with no result.
- Stub: `class AsyncPostgresCursor` with `def execute(...) -> Awaitable[Self]`, `def fetchall(self) -> Awaitable[list[tuple[Any, ...]]]`, and the matching forms for the other members; `def cursor(self) -> AsyncPostgresCursor: ...` on `AsyncPostgresClient`.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `await cur.execute('SELECT 1')` then `await cur.fetchall()` | `[(1,)]` |
| Attributes | After a select | `cur.description` and `cur.rowcount` readable without `await` |
| Command | DDL | `description` is `None` |
| executemany | Three sets | `rowcount` 3 |
| Error class | Duplicate key | `IntegrityError` |
| Concurrent execute | Two `execute` awaits in flight on one cursor | Second raises `InterfaceError` |
| Cancellation | Cancel a task awaiting `pg_sleep(5)` | `CancelledError`; next `execute` works |
| Closed | `await cur.close()` then `execute` | `InterfaceError` |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q -k "cursor"`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] New tests named `test_async_cursor_*` cover every matrix case.
- [ ] The stub diff adds one class and one method.
