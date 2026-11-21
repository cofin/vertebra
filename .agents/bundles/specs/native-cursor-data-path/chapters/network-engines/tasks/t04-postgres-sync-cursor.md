---
type: Task
id: "native-cursor-data-path:network:t04"
chapter_id: network-engines
title: "SyncPostgresCursor on the synchronous client"
description: "Add SyncPostgresCursor, created by SyncPostgresClient.cursor(), with tuple rows, description, rowcount, and the error classes the client raises today."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t42"
  - "native-cursor-data-path:network:t03"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-postgres/src/sync_client.rs:686-710"
  - "src/rs/vertebra-postgres/src/sync_client.rs:1122-1160"
  - "src/rs/vertebra-postgres/src/sync_client.rs:1275-1322"
  - "src/rs/vertebra-postgres/src/python_values.rs:100-110"
  - "src/rs/vertebra-postgres/src/bind_plan.rs"
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/py/vertebra/_postgres.pyi:24-60"
  - "src/py/tests/unit/db/test_postgres_native_queries.py:1-80"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/sync_client.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_queries.py"
estimated_loc: 250
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
  - id: pg-sync-client
    resource: "src/rs/vertebra-postgres/src/sync_client.rs::PySyncPostgresClient"
    title: "Client that creates the cursor"
tags: [postgres, rust, pyo3, dbapi]
---

# SyncPostgresCursor on the synchronous client

## Objective & Scope Boundary

A caller holding a `SyncPostgresClient` gets a PEP 249 cursor from `client.cursor()`. One `execute` call rewrites placeholders from the cached plan, runs the statement on the client's session, and stores tuple rows, the description, and the row count.

Out of scope:

- The async cursor (t05) and pool-backed cursors (t06).
- Python driver changes (t07).
- Removing `SyncPostgresClient.query`. Pipelines and Arrow paths still use it.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-postgres/src/sync_client.rs:1275-1322` is `query`. The cursor runs the same session call with the same budget and generation handling.
- `src/rs/vertebra-postgres/src/sync_client.rs:686-710` is the struct and its `register` function, where the new class is added.
- `src/rs/vertebra-postgres/src/python_values.rs:100` is `extract_parameters`, reused for parameter conversion.

## Contracts & Edge Cases

- `cursor` also takes keyword-only `lazy_begin: bool = False`, in the Rust signature and in the stub. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when the session has none open, then run the statement. A failed begin clears the result and raises. Test rows: a cursor created with `lazy_begin=True` leaves `in_transaction` true after an insert and `rollback()` removes the row; the same cursor after `commit()` opens a new transaction on its next statement; a default cursor opens none.
- Lease methods, on each client class this task edits: `attach_lease(self, release: Callable[[], None]) -> None` and `release_lease(self) -> None`, both plain methods, with stub entries. The client holds a `Mutex<Option<LeaseGuard>>` using the guard from t42. `attach_lease` releases a guard that is already held, then stores the new one. `release_lease` releases now and is idempotent. The client's `close` releases the guard, and dropping the client releases it through the guard's drop. Test rows: `attach_lease` with a counting callable then `release_lease` twice gives one call; `attach_lease` then `del client` and `gc.collect()` gives one call; a second `attach_lease` releases the first callable.
- Rust type `PySyncPostgresCursor`, Python name `SyncPostgresCursor`, module `vertebra._postgres`. It holds an `Arc` of the client session and a `std::sync::Mutex<CursorState>`.
- `SyncPostgresClient.cursor(self) -> SyncPostgresCursor` takes no arguments and does no I/O.
- `execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] = (), *, parameter_oids: Sequence[int] | None = None, timeout: float | None = None) -> Self`.
- `execute` steps: `ensure_open`; `bind_plan::plan_for(sql)`; `order_parameters`; for each ordered value whose exact type is not `NoneType`, `bool`, `int`, `float`, `str`, or `bytes`, call the cached Python callable `vertebra.db.adapters.postgres.binds.normalize_postgres_parameter`; `extract_parameters`; run `query_as_with_generation_and_budget` with the GIL released; build the description and tuple rows; `store`.
- The normalizer callable is imported once per process into a `PyOnceLock`.
- `description` is `None` when the reply carries no row description. `rowcount` and `lastrowid` follow t03.
- `executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> Self` runs the statement once per parameter set on the same session, sums the affected rows into `rowcount`, and leaves `description` as `None`. An empty sequence sets `rowcount` to `0` and sends nothing.
- `fetchone`, `fetchmany(size: int | None = None)`, `fetchall`, `close`, and the properties `description`, `rowcount`, `lastrowid` delegate to `CursorState`. `arraysize` is a read-write property. Setting it below `1` raises `ProgrammingError`.
- Every engine error leaves through `postgres_error_to_py`, unchanged. Argument errors from parameter conversion (`TypeError`, `ValueError`) are raised as `InterfaceError` through `cursor_interface_err`.
- A failed `execute` clears the previous result: `description` is `None`, `rowcount` is `-1`, and a fetch raises `ProgrammingError`.
- Remove the `allow(dead_code)` attribute from `cursor.rs`.
- Stub: add `class SyncPostgresCursor` with the members above and `def cursor(self) -> SyncPostgresCursor: ...` on `SyncPostgresClient`. `__init__` is not exposed; the class cannot be constructed from Python.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `SELECT 1 AS a, 'x' AS b` | `fetchall()` is `[(1, 'x')]`; description names `a`, `b`; `rowcount` 1 |
| Command | `CREATE TEMP TABLE t (id int)` | `description` is `None`; fetch raises `ProgrammingError` |
| DML count | `INSERT` of three rows | `rowcount` 3; `description` is `None` |
| Returning | `INSERT ... RETURNING id` | `description` present and `rowcount` equals rows returned |
| Empty set | `SELECT 1 WHERE false` | `fetchall()` is `[]`; description present |
| Named parameters | `SELECT %(a)s + %(b)s` with a mapping | Correct sum |
| Format parameters | `SELECT %s::int + %s::int` with a tuple | Correct sum |
| executemany | Three parameter sets on an `INSERT` | `rowcount` 3 |
| Unique violation | Duplicate key insert | `IntegrityError` with `sqlstate` `23505`; `vertebra.db.dbapi.IntegrityError` is the same class |
| Syntax error | `SELEC 1` | `ProgrammingError` |
| New execute discards rows | Two `execute` calls, fetch after the second | Only rows of the second |
| Closed cursor | `close()` then `execute` | `InterfaceError` |
| Timeout | `SELECT pg_sleep(1)` with `timeout=0.05` | The class `client.execute` raises for the same timeout today, which is an `OperationalError` |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-postgres/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` cover every matrix case and assert on tuples, description, `rowcount`, and exception classes.
- [ ] The stub diff adds one class and one method.
- [ ] `SyncPostgresClient.query` behavior is unchanged.
