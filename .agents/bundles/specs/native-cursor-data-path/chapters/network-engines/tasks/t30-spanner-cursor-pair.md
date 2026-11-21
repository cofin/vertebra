---
type: Task
id: "native-cursor-data-path:network:t30"
chapter_id: network-engines
title: "SyncSpannerCursor and AsyncSpannerCursor"
description: "Add the Spanner cursor pair, created from a client or a read-write transaction, over the existing query, DML, and DDL calls."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t29"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:150-180"
  - "src/rs/vertebra-spanner/src/lib.rs:169-180"
  - "src/rs/vertebra-spanner/src/lib.rs:279-299"
  - "src/rs/vertebra-spanner/src/lib.rs:300-330"
  - "src/rs/vertebra-spanner/src/lib.rs:425-440"
  - "src/rs/vertebra-spanner/src/lib.rs:503-515"
  - "src/rs/vertebra-spanner/src/lib.rs:613-660"
  - "src/rs/vertebra-spanner/src/lib.rs:889-1075"
  - "src/rs/vertebra-spanner/src/cursor.rs"
  - "src/py/vertebra/_spanner.pyi:69-160"
  - "src/py/vertebra/_spanner.pyi:288-300"
  - "src/py/tests/unit/db/test_spanner.py:1-80"
files:
  - "src/rs/vertebra-spanner/src/cursor.rs"
  - "src/rs/vertebra-spanner/src/lib.rs"
  - "src/py/vertebra/_spanner.pyi"
tests:
  - "src/py/tests/unit/db/test_spanner.py"
estimated_loc: 290
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
  - id: spanner-client
    resource: "src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient"
    title: "Client and transaction classes that create the cursors"
tags: [spanner, rust, pyo3, dbapi]
---

# SyncSpannerCursor and AsyncSpannerCursor

## Objective & Scope Boundary

`client.cursor()` and `transaction.cursor()` return PEP 249 cursors for Spanner. One `execute` picks the query, DML, or DDL call from the cached statement kind and stores tuple rows, the description, and the row count from the reply.

Out of scope:

- Python driver changes (t31).
- Mutations, batch writes, partitioned DML, partitioned queries, and change streams.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `query` on `PySyncSpannerClient`, `::execute_dml`, and `::update_ddl` in `lib.rs` are the calls the client-backed cursor makes. The async client has the same three.
- `src/rs/vertebra-spanner/src/lib.rs:889-1075` holds the two read-write transaction classes with their `query` and `execute_dml` methods.
- `src/rs/vertebra-spanner/src/cursor.rs::register` is called from the module init since t29. This task fills its body.

## Contracts & Edge Cases

- Each client `cursor` also takes keyword-only `lazy_begin: bool = False`. `lazy_begin=True` raises `NotSupportedError`, because the DB-API connection for this engine requires autocommit. Test row: that call raises and sends nothing.
- Classes `SyncSpannerCursor` and `AsyncSpannerCursor`, module `vertebra._spanner`, defined in `cursor.rs` and added in `register` in `cursor.rs`. Each holds `enum CursorSource { Client(...), ReadWriteTx(...) }`, the options below, and a `std::sync::Mutex<CursorState>`.
- On both client classes: `cursor(self, *, staleness_mode: str = 'strong', staleness_value: int = 0, request_tag: str | None = None, transaction_tag: str | None = None, param_encoder: Callable[[Any], str | None] | None = None)`.
- On both read-write transaction classes: `cursor(self, *, request_tag: str | None = None, param_encoder: Callable[[Any], str | None] | None = None)`.
- `execute(self, sql: str, params: str | Sequence[Any] | Mapping[str, Any] = ()) -> Self`. A `str` is taken as encoded parameter JSON. An empty value means no parameters. Any other value is passed to `param_encoder`, and its result is used; with no encoder that case raises `InterfaceError` with the message `Spanner cursor needs encoded parameters or a param_encoder`.
- Client source: `Query` calls the client query with the staleness options; `Dml` calls `execute_dml` with the tags; `Ddl` calls `update_ddl` with the one statement.
- Transaction source: `Query` and `Dml` call the transaction's `query` and `execute_dml`. `Ddl` raises `NotSupportedError` with the message the Python driver uses today for DDL inside a read-write transaction.
- `executemany(self, sql: str, seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]]) -> Self` requires `Dml`, sends one batch-DML call with every parameter set, and sets `rowcount` to the sum. `Query` or `Ddl` raises `NotSupportedError`.
- Async methods return `RustDbTaskAwaitable`. Parameter encoding runs on the calling thread.
- Fetch methods, properties, `arraysize`, `close`, and the failed-execute and concurrent-execute rules follow chapter spec section 2. Engine errors leave through `spanner_error_to_py`, unchanged; an exception raised by `param_encoder` propagates unchanged.
- Remove the `allow(dead_code)` attribute from `cursor.rs`.
- Stub: two cursor classes and one `cursor` method on each of the four classes.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Select | `SELECT 1 AS a, 'x' AS b` | `[(1, 'x')]`; names `a`, `b` |
| Stale read | Cursor with `staleness_mode='max_staleness'` | Request carries the staleness bound |
| DML | `UPDATE t SET v = 1 WHERE true` on two rows | `rowcount` 2; `description` is `None` |
| DML returning | `INSERT ... THEN RETURN id` | `description` present; rows returned |
| DDL | `CREATE TABLE ...` on a client cursor | `description` is `None`; `rowcount` -1 |
| DDL in transaction | Same on a transaction cursor | `NotSupportedError` |
| Encoded parameters | `execute(sql, '{...}')` | Parameters applied |
| Encoder | `execute(sql, {'a': 1})` with an encoder | Encoder called once; parameters applied |
| No encoder | `execute(sql, {'a': 1})` without one | `InterfaceError` |
| executemany | Three DML sets | `rowcount` 3 |
| executemany on a query | `SELECT` with sets | `NotSupportedError` |
| Already exists | Duplicate key insert | `SpannerError` with `code == 6`, as `spanner_error_to_py` raises today |
| Closed | `close()` then `execute` | `InterfaceError` |
| Async | The same cases through `AsyncSpannerCursor` | Same results |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._spanner`
- `uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py -q -k "cursor"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner cursor`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-spanner --all-targets -- -D warnings`
- `test -z "$(git grep -n 'allow(dead_code)' -- src/rs/vertebra-spanner/src/cursor.rs)"`

## Acceptance Criteria

- [ ] New tests named `test_sync_cursor_*` and `test_async_cursor_*` use the client doubles or emulator fixture the existing Spanner tests use and cover every matrix case.
- [ ] The stub diff adds two classes and four methods.
