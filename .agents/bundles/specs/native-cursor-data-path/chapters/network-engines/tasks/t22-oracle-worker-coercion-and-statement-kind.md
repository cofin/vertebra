---
type: Task
id: "native-cursor-data-path:network:t22"
chapter_id: network-engines
title: "Oracle value coercion and statement kind in the worker"
description: "Port bind coercion, row cell coercion, and DML or PL/SQL detection from OracleCore into the Rust worker with a per-SQL-text cache."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t20"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/py/vertebra/db/adapters/oracle/core.py:657-715"
  - "src/py/vertebra/db/adapters/oracle/core.py:741-750"
  - "src/py/vertebra/db/adapters/oracle/core.py:797-812"
  - "src/rs/vertebra-oracle/src/types.rs:408-560"
  - "src/rs/vertebra-oracle/src/types.rs:860-1036"
  - "src/rs/vertebra-oracle/src/client.rs:730-866"
  - "src/rs/vertebra-oracle/src/client.rs:273-336"
files:
  - "src/rs/vertebra-oracle/src/types.rs"
  - "src/rs/vertebra-oracle/src/client.rs"
tests:
  - "src/rs/vertebra-oracle/src/types.rs"
  - "src/rs/vertebra-oracle/src/client.rs"
estimated_loc: 260
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
  - id: oracle-core-py
    resource: "src/py/vertebra/db/adapters/oracle/core.py::coerce_row_cell"
    title: "Python coercion that moves into the worker"
tags: [oracle, rust, performance]
---

# Oracle value coercion and statement kind in the worker

## Objective & Scope Boundary

The Rust worker can produce the same Python values the driver produces today after its Python post-processing, and it knows whether a statement is a query without Python reading the SQL text. Nothing calls the new functions yet; the cursor in t24 does.

Out of scope:

- The cursor classes (t24) and out-binds (t25).
- Deleting the Python helpers (t27).
- Porting the OSON and vector codecs. They are called through cached Python callables.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/py/vertebra/db/adapters/oracle/core.py:657-672` is `coerce_bind_parameter`; `:675-715` is `coerce_row_cell`; `:741-750` is `is_dml_or_plsql`.
- `src/rs/vertebra-oracle/src/types.rs:408-560` is `OracleCellValue` and `extract_bind_value`; `:860-1036` is `decode_oracledb_row`.
- `src/rs/vertebra-oracle/src/client.rs:834-866` is `strip_leading_sql_comments`.

## Contracts & Edge Cases

- `types.rs` adds `pub enum VectorReturnFormat { List, VectorView, Bytes }` with `parse(&str)` accepting `list`, `vector_view`, and `bytes` and rejecting other text with an interface error.
- `types.rs` adds `#[derive(Clone, Copy)] pub struct OracleValueOptions { pub enable_uuid_binary: bool, pub fetch_decimals: bool, pub vector_return_format: VectorReturnFormat }` with `Default` values `true`, `false`, `List`.
- `pub fn coerce_cell_to_py(py, cell: OracleCellValue, column_type: Option<&str>, opts: OracleValueOptions) -> PyResult<Py<PyAny>>` ports `coerce_row_cell` branch for branch. Byte values starting with the OSON magic are decoded by the cached callable `OracleCore.decode_oson`. Byte values starting with the vector magic are decoded by the cached callable `OracleCore.decode_vector` with the return format name. A 16-byte value becomes `uuid.UUID` when `enable_uuid_binary` is true and the column type contains `RAW` or is unknown. Numbers follow the `fetch_decimals` rules. Every other value is returned as `OracleCellValue` converts it today.
- `pub fn coerce_bind_from_py(py, value: &Bound<PyAny>, opts: OracleValueOptions) -> Result<OracleCellValue, OracleError>` ports `coerce_bind_parameter` and then calls `extract_bind_value`: `uuid.UUID` becomes its 16 bytes or its string per `enable_uuid_binary`; `VectorView` goes through the cached callable `OracleCore.encode_vector`; `dict` and `msgspec.Struct` go through the cached callable `OracleCore.encode_oson`. Exact-type checks for `NoneType`, `bool`, `int`, `float`, `str`, and `bytes` run first.
- The four Python callables are imported once per process into `PyOnceLock` values from `vertebra.db.adapters.oracle.core`.
- `client.rs` adds `#[derive(Clone, Copy, PartialEq, Eq, Debug)] pub(crate) enum StatementKind { Query, DmlOrPlsql }` and `pub(crate) struct StatementFacts { pub kind: StatementKind, pub touches_session_context: bool }`.
- `pub(crate) fn statement_facts(sql: &str) -> StatementFacts` reads a standard-library `Mutex` LRU keyed by SQL text with capacity `512`. `kind` ports `is_dml_or_plsql`: after leading whitespace and block comments, text starting with `SELECT` or `WITH` in any case is `Query`; all else is `DmlOrPlsql`. `touches_session_context` is true when the upper-cased text contains `DBMS_SESSION`, `SET_IDENTIFIER`, or `CLEAR_IDENTIFIER`.
- Both new function groups carry `allow(dead_code)` on the items until t24 calls them. Task t24 removes those attributes.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| UUID out | 16-byte cell, type `RAW`, default options | `uuid.UUID` |
| UUID disabled | Same cell, `enable_uuid_binary` false | `bytes` |
| 16 bytes in a BLOB | 16-byte cell, type `BLOB` | `bytes` |
| Integer decimal | `Decimal('5')`, `fetch_decimals` false | `int` 5 |
| Fractional decimal | `Decimal('5.5')`, `fetch_decimals` false | `Decimal('5.5')` |
| Float to decimal | `2.5`, `fetch_decimals` true | `Decimal('2.5')` |
| Bool untouched | `True`, `fetch_decimals` true | `True` |
| Null | Null cell | `None` |
| Bind UUID | `uuid.UUID` with default options | 16-byte bind value |
| Bind dict | `{'a': 1}` | Bytes starting with the OSON magic |
| Kind select | `  /* c */ select 1 from dual` | `Query` |
| Kind with | `WITH x AS (...) SELECT ...` | `Query` |
| Kind PL/SQL | `BEGIN NULL; END;` | `DmlOrPlsql` |
| Kind DML | `insert into t values (1)` | `DmlOrPlsql` |
| Session flag | `BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;` | `touches_session_context` true |
| Facts cached | Same text twice | Second call does not rescan; cache length 1 |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle types`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle client`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing Oracle Rust tests pass unchanged.
- [ ] No Python-visible name is added.
