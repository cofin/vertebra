---
type: Task
id: "native-cursor-data-path:single:t10"
chapter_id: single-call-paths
title: "PostgreSQL bind fast path and cached-statement frames"
description: "A cached PostgreSQL statement sends Bind, Execute, and Sync only, and parameter extraction checks exact container types first."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/research/native-cursor-hot-paths_20261121.md"
  - "src/rs/vertebra-postgres/src/session.rs:1754-2061"
  - "src/rs/vertebra-postgres/src/session.rs:2063-2250"
  - "src/rs/vertebra-postgres/src/extended.rs:54-61"
  - "src/rs/vertebra-postgres/src/extended.rs:171-233"
  - "src/rs/vertebra-postgres/src/extended.rs:263-417"
  - "src/rs/vertebra-postgres/src/pipeline.rs:159-215"
  - "src/rs/vertebra-postgres/src/pipeline.rs:497-592"
  - "src/rs/vertebra-postgres/src/pipeline.rs:1054-1164"
  - "src/rs/vertebra-postgres/src/pool.rs:296-336"
  - "src/rs/vertebra-postgres/src/statements.rs:186-197"
  - "src/rs/vertebra-postgres/src/statements.rs:452-494"
  - "src/rs/vertebra-postgres/src/python_values.rs:99-189"
  - "src/rs/vertebra-postgres/src/bind_plan.rs:626-788"
  - "src/rs/vertebra-postgres/tests/statement_cache.rs:168-262"
  - "src/rs/vertebra-postgres/tests/cancellation_protocol.rs:66-100"
  - "src/rs/vertebra-postgres/tests/pipeline_wire.rs:206-340"
  - "src/rs/vertebra-postgres/tests/pipeline_outcomes.rs:114-176"
  - "src/py/tests/unit/db/test_postgres_native_cancel.py:51-82"
files:
  - "src/rs/vertebra-postgres/src/session.rs"
  - "src/rs/vertebra-postgres/src/extended.rs"
  - "src/rs/vertebra-postgres/src/pipeline.rs"
  - "src/rs/vertebra-postgres/src/python_values.rs"
  - "src/rs/vertebra-postgres/src/bind_plan.rs"
tests:
  - "src/rs/vertebra-postgres/tests/statement_cache.rs"
  - "src/rs/vertebra-postgres/tests/cancellation_protocol.rs"
  - "src/rs/vertebra-postgres/tests/pipeline_wire.rs"
  - "src/rs/vertebra-postgres/tests/pipeline_outcomes.rs"
  - "src/rs/vertebra-postgres/tests/python_value_contract.rs"
  - "src/py/tests/unit/db/test_postgres_native_cancel.py"
estimated_loc: 110
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-21T16:23:02Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, requirements R19 and R21"
  - id: pg-session
    resource: "src/rs/vertebra-postgres/src/session.rs::PostgresConnection"
    title: "Extended-protocol framing for cached statements"
  - id: pg-binds
    resource: "src/rs/vertebra-postgres/src/python_values.rs::extract_parameters"
    title: "Python-to-wire parameter extraction"
tags: [postgres, rust, wire-protocol, performance]
---

# PostgreSQL bind fast path and cached-statement frames

## Objective & Scope Boundary

A statement that is already in the statement cache costs the server three frames instead of five. A query with a `tuple`, `list`, or `dict` of parameters skips the protocol checks, and an empty container is treated as no parameters.

`estimated_loc` counts source lines. Tests add or change about 150 lines.

Out of scope:

- Statements that are not yet in the cache. They keep `Parse`, `Describe`, `Bind`, `Describe`, `Execute`, `Close`, `Sync`.
- `query_with_description` on `PostgresConnection`, `query_with_row_sink`, the pipeline branch for a known description without a named statement, and the named-portal code in `portal.rs`. Each sends `Parse` or uses a named portal, so none is a cached run.
- `cursor.rs`, the client classes, and the stub. Tasks t11 and t12 own them.

## Bounded Read Manifest

- `query` on `PostgresConnection` in `session.rs` (`:1755-2061`) is the only caller of `execute_cached_named_statement` (`:2063-2250`). It takes the cached branch at `:1787-1791`.
- The session path (`query_as_with_generation_and_budget` on `SessionHandle`) and the one-shot pool path (`query_oneshot` on `PostgresPool` in `pool.rs`, `:299-336`) both call `query` on `PostgresConnection`. A pool cursor's `executemany` calls it through the checked-out connection. One edit therefore covers the session path and the one-shot pool path. `pool.rs` is read only.
- `submit_pipeline` in `pipeline.rs` builds the frames for a cached statement at `:515-592`. `collect_submitted_pipeline` reads the replies at `:1054-1164`.
- `ExtendedQueryCollector` in `extended.rs` (`:171-468`) and `PreparedDescription` (`:54-61`).
- `extract_parameters` in `python_values.rs` (`:100-189`) and `order_parameters` in `bind_plan.rs` (`:651-788`) are the parameter extraction.

## Contracts & Edge Cases

### Where the row description comes from

- The cache entry is `CachedStatement` in `statements.rs`, which owns `description: PreparedDescription`. `insert_named` on `StatementCache` takes the description by value, so no entry exists without one.
- The description is captured when the statement is prepared. In `query` on `PostgresConnection` the first run sends `Describe` for the statement. `handle_row_description` on `ExtendedQueryCollector` stores the columns, `prepared_description()` returns them at `ReadyForQuery`, and `insert_named` stores them (`session.rs:1988-2011`). The pipeline does the same at `pipeline.rs:1173-1182` and `:953-961`.
- A statement described with `NoData` has an empty `result_columns`. That is the stored row description for a statement without a result set.

### Result-format rule (unchanged)

For a cached statement, `Bind` asks for one result format per column: `format_for_oid_with_catalog(column.type_oid, type_catalog)`. Each parameter uses the same function on its effective OID, and text when the OID is `0`. This task does not edit either computation. The `Bind` bytes for a cached statement are the same before and after.

The columns from the statement description carry format code `0`. The decoder needs the formats that `Bind` asked for, so the description is combined with them:

- Add `PreparedDescription::portal_columns(&self, formats: &[Format]) -> Vec<Column>` in `extended.rs`. It returns `result_columns` cloned, with `format` of column `i` set to `formats[i]`. The caller passes one format per column.
- Add `ExtendedQueryCollector::with_known_columns(expected_param_count: usize, requested_result_formats: Vec<Format>, columns: Vec<Column>) -> Self`. It builds the collector with `statement_describe_done` and `portal_describe_done` true, `expect_portal_describe` false, and `portal_columns` set to `columns`. On such a collector `handle_row_description` and `handle_no_data` return their existing "unexpected extra" protocol errors.

### `execute_cached_named_statement` (session path and one-shot pool path)

Frames written, in order: one `Close` for each evicted statement name (unchanged), `Bind`, `Execute`, `Sync`. Delete the `Describe` for the unnamed portal and the `Close` for it.

Reply handling:

- Build the collector with `with_known_columns(resolved.parameters.len(), result_formats.clone(), description.portal_columns(&result_formats))`.
- Delete the arms for the `RowDescription` and `NoData` variants of `Message`. Either frame now reaches the final arm, which closes the connection and returns the existing protocol error `unexpected backend message frame during cached extended query`.
- `CloseComplete` is accepted only for evicted statement names. Keep a counter that starts at `evicted.len()`. The arm becomes `Message::CloseComplete if pending_closes > 0`, and it decrements the counter. Any further `CloseComplete` reaches the final arm.
- The codec context used for rows is the one built before `Bind`. The binding is no longer mutable.
- A cached statement with no result columns: no columns are seeded, a `DataRow` raises the existing protocol error, and the result has no columns.
- A stored description that no longer matches the table: the server answers `Bind` with SQLSTATE `0A000`. The existing `ErrorResponse` arm clears the statement cache and the error is raised as today. The next run of that SQL prepares again.
- Unnamed-portal lifetime: the next `Bind` replaces the unnamed portal, and the end of the transaction drops it. No frame is needed to close it.

### Pipeline (`submit_pipeline`, cached named statement branch)

- Frames per operation: `Bind`, `Execute`. Delete the `Describe` at `pipeline.rs:577-579`. The single `Sync` at the end is unchanged. The pipeline never sent `Close` for the unnamed portal, so its `CloseComplete` handling is unchanged.
- `PlannedPipelineOp` gains `known_columns: Vec<Column>`. The cached branch sets it to `cached.description.portal_columns(&result_formats)` and sets `expect_portal_describe: false`. The other two branches set it to an empty `Vec` and keep `expect_portal_describe: true`.
- In `collect_submitted_pipeline`, the arm `(OpDemuxPhase::BindComplete, Message::BindComplete)` sets `portal_columns = plan.known_columns.clone()` when `plan.expect_portal_describe` is false, then moves to `RowsOrComplete`. A `RowDescription` or `NoData` in that phase reaches the existing unexpected-frame arm.

### Parameter extraction (R19)

`order_parameters` in `bind_plan.rs`, new order of the container checks:

1. `None`.
2. Exact `tuple` (`is_exact_instance_of` for `PyTuple`): a sequence.
3. Exact `list`: a sequence.
4. Exact `dict`: a mapping.
5. `str`, `bytes`, `bytearray`, `memoryview`: the existing `ProgrammingError`.
6. Subclasses of `dict`, then of `tuple` or `list`.
7. `collections.abc.Mapping`, then `collections.abc.Sequence`, then the existing unsupported-type error.

An empty exact `tuple`, `list`, or `dict` with the `None` variant of `BindStyle` returns the shared empty tuple (`empty` on `PyTuple`) before any other work. With another bind style an empty container follows the existing branch for its kind, so the error texts do not change.

`extract_parameters` in `python_values.rs`, new order:

1. `None`: the existing no-parameter branch. Move it into a private function `no_parameters(parameter_oids)` that all empty cases share.
2. Exact `tuple`: empty calls `no_parameters`. Otherwise the items are read from the tuple directly.
3. Exact `list`: the same, read from the list directly.
4. Exact `dict`: the existing `InterfaceError` text `parameters must be an ordered sequence (tuple or list), not str/bytes/dict`.
5. Any other type: the existing `str`, `bytes`, `bytearray`, and `dict` subclass rejection, then `try_iter`.

`resolve_python_types(py)` is called after the empty returns. The per-item loop moves into one private generic function that takes an iterator of items and the item count, so the three non-empty paths share it. An empty container with non-empty `parameter_oids` keeps the error `parameter_oids length N does not match parameters length 0`.

### Tests that assert the old frames

A scripted server answers only the frames it receives: `2` for `Bind`, `T` or `n` for `Describe`, `3` for `Close`.

| Test | Change |
| --- | --- |
| `src/rs/vertebra-postgres/tests/statement_cache.rs::named_statement_cache_hit_skips_parse_and_evicts_lru_with_close_frame` | Second group is `[B, E, S]`; its reply is `2`, the binary `DataRow`, `C`, `Z`. Fourth group is `[C, B, E, S]` with the first body still `Svtb_s_1\0`; its reply is `3`, `2`, `DataRow`, `C`, `Z`. The first and third groups and their replies are unchanged. |
| `src/rs/vertebra-postgres/tests/cancellation_protocol.rs::select_int_response` | With `warm` true the reply is `2`, `D`, `C`, `Z`. With `warm` false it is unchanged. |
| `src/rs/vertebra-postgres/tests/pipeline_wire.rs::cold_ddl_and_warm_batches_emit_single_sync_and_single_flush_without_catalog_queries` | Third group and `stats.parsed_frontend_tags()` are `[B, E, B, E, S]`. The reply per operation is `2`, `DataRow`, `C`. The `Bind` format assertions stay. |
| `src/rs/vertebra-postgres/tests/pipeline_outcomes.rs::demultiplexes_heterogeneous_4op_result_batch_in_open_transaction` | The reply for the cached `SELECT` operation drops its `RowDescription`. The expected columns (`id` binary, `lbl` text) are unchanged. |
| `src/py/tests/unit/db/test_postgres_native_cancel.py::_select_pid_response` | With `warm=True` the reply is `2`, `D`, `C`, `Z`. |

Already correct, because they answer per received frame: `tests/broker_pinning_tests.rs`, `tests/postgres_pgoutput_tests.rs` (`read_ext_batch`), the server loop at `tests/pipeline_wire.rs:400-440`, `src/py/tests/unit/db/postgres_data_peers.py`, and `src/py/tests/unit/db/postgres_notification_harness.py`. `src/py/tests/unit/db/postgres_ports.py` holds class doubles and no wire frames.

Any other scripted reply to a group without `Parse` follows the same rule. The full crate run in Verification finds it.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Cached select | Same SQL twice on one connection | Second group `[B, E, S]`; row decoded from binary |
| Cached statement without result set | `INSERT` twice, reply `2`, `C`, `Z` | Second group `[B, E, S]`; `affected_rows` 1; no columns |
| Evicted name | Cache capacity 1, two statements, then the second again | Group `[C, B, E, S]`; one `CloseComplete` accepted |
| Unrequested description | Cached run answered with `2`, `T`, rows | Protocol error; connection closed |
| Stale description | Cached run answered with `ErrorResponse` `0A000`, `Z` | Error raised; next run of that SQL starts with `P` |
| Bind bytes | Cached `SELECT $1::int4` | Parameter format code 1 and result format code 1, as before |
| Pipeline cached operations | Two cached operations in one pipeline | `[B, E, B, E, S]`; both results decoded |
| First run | New SQL | `[P, D, B, D, E, C, S]` |
| Exact containers | `(1,)`, `[1]` to `extract_parameters` | Equal parameter lists |
| Empty containers | `()`, `[]`, `None` | `(vec![], None)` |
| Empty with OIDs | `()` with `parameter_oids=(23,)` | Data error naming length 0 |
| Dict to `extract_parameters` | `{}` and `{"a": 1}` | Existing `InterfaceError` |
| Subclasses | A named tuple; an `OrderedDict` in `order_parameters` | Sequence; mapping |
| Other iterables | A generator to `extract_parameters` | Items extracted |
| Empty with no placeholders | `order_parameters` with `()`, `[]`, `{}` and the `None` variant of `BindStyle` | The shared empty tuple |
| Text container | `"abc"` | Existing `ProgrammingError` |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make postgres-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_cancel.py src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_postgres_native_pools.py src/py/tests/unit/db/test_postgres_native_pipeline.py -p no:xdist -q`
- `git grep -n "describe(b'P', \"\"" -- src/rs/vertebra-postgres/src/session.rs src/rs/vertebra-postgres/src/pipeline.rs`

`make postgres-test` sets the interpreter exports that a PyO3 crate needs for `cargo test`. The last command lists the `Describe` calls for the unnamed portal that remain. Each remaining line sits in a function that also sends `Parse`.

## Acceptance Criteria

- [ ] `execute_cached_named_statement` and the cached branch of `submit_pipeline` write no `Describe` and no `Close` for the unnamed portal.
- [ ] The frames of a statement that is not in the cache are unchanged, and a test asserts them.
- [ ] The cached reply loop rejects `RowDescription`, `NoData`, and an uncounted `CloseComplete`.
- [ ] The five tests in the table assert the new frames, and the new matrix cases are covered in the same files.
- [ ] `extract_parameters` and `order_parameters` test exact `tuple`, `list`, and `dict` before any other check, and the empty cases return without building a parameter list.
