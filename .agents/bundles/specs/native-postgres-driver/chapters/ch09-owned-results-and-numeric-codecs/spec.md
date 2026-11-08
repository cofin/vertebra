---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch09-owned-results-and-numeric-codecs
title: "Chapter 09: Owned Query Results, Value Records, Simple Query, and Scalar/Numeric Codecs"
description: Define lossless Rust and Python PostgreSQL result and value models, simple-query protocol framing with SQLSTATE classification, and checked scalar and arbitrary-precision numeric codecs.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch08-constructor-parity-and-wheel-gil
replan_trigger: null
current_task: "native-postgres-driver:ch09:t01"
plan_revision: 9
plan_commit: b0de4dcb
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen PostgreSQL value records, column metadata, and SQLSTATE contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 framing, base-10000 NUMERIC wire layout, and read-only PG18.3 probes
  - id: pg-core-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame
    title: Existing PostgreSQL wire framing baseline in vertebra-db
  - id: py-db-errors
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical shared DatabaseError exception hierarchy in vertebra._vertebra
tags:
  - postgres
  - rust
  - codecs
  - numeric
  - results
---

# Chapter 09: Owned Query Results, Value Records, Simple Query, and Scalar/Numeric Codecs

<!-- truth: start -->
- **Owned Value & Result Model**: `vertebra_postgres::result::{Format, TransactionStatus, Column, QueryResult, PgValue, PgNumeric}` and frozen `gc=False` `msgspec.Struct` records in `vertebra.db.adapters.postgres.types` own every row, column descriptor, transaction flag (`I`/`T`/`E`), and lossless PostgreSQL value without holding `PyObject` handles or borrowed wire buffers [^pg-contracts].
- **Simple Query Drain & SQLSTATE Discipline**: `PostgresConnection::simple_query` collects ordered multi-statement result sets, parses checked `u64` command-tag row counts, drains recoverable `ErrorResponse` exchanges through `ReadyForQuery`, invalidates unnamed prepared state on every `Query`, and maps SQLSTATE classes (`22`, `23`, `42`, `08`, `40001`/`40P01`, `57014`) onto canonical `vertebra._vertebra` exceptions [^pg-contracts] [^py-db-errors].
- **Per-Column Scalar & Base-10000 Numeric Codecs**: Built-in scalar OIDs and `NUMERIC` (`OID 1700`) encode and decode checked binary and text representations—preserving arbitrary decimal precision, declared `dscale`, `NaN`, and `±Infinity` without `f64` or fixed-width `Decimal` narrowing, while unknown OIDs negotiate `Format::Text` and reject unexpected binary frames [^pg-research] [^pg-core-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Query execution, `COPY`, `VTBARROW` batch builders, single-`Sync` pipelines, and Python result hydration require one shared owned Rust and Python data model that survives socket-buffer reuse and preserves every PostgreSQL wire invariant: duplicate or empty column labels, exact `u64` affected row counts, `I`/`T`/`E` transaction state, arbitrary-precision `NUMERIC` scale and special values, epoch-exact temporals, independent interval fields, dimensioned arrays, ordered composites, bitstrings, and `pgvector` payloads [^pg-contracts] [^pg-research]. Chapter 09 establishes that owned result/value contract, implements multi-result simple-query execution and structured SQLSTATE classification, and installs checked binary and text codecs for built-in scalars and arbitrary-precision `NUMERIC`.

### 1.2 Non-Goals

- Binary and session-style text codecs for `date`/`time`/`timestamp`/`timestamptz`/`interval` and `json`/`jsonb` (owned by Chapter 10).
- Session `pg_catalog` type discovery, array codecs, and composite codecs (owned by Chapter 11).
- `pgvector`/`halfvec`/`bit` codecs, cold-query metadata normalization, and extended `Parse`/`Bind`/`Execute` queries (owned by Chapter 12).
- PyO3 value hydration (`python_values.rs`) and `SyncPostgresClient`/`AsyncPostgresClient` query facades (owned by Chapter 14).

### 1.3 Upstream Execution Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface authority: [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Depends on `ch08-constructor-parity-and-wheel-gil` (`SessionHandle`, `PostgresConnection`, `protocol::read_message`, and `PostgresError` foundation).

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Public Value & Result Contracts | `.agents/bundles/specs/native-postgres-driver/contracts.md` [^pg-contracts] | Freezes `msgspec.Struct(frozen=True, gc=False)` shapes in `vertebra.db.adapters.postgres.types` and Rust `QueryResult`/`Column`/`PgValue` invariants (in-process). |
| Wire Framing & `NUMERIC` Probes | `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` [^pg-research] | Confirms `postgres-protocol =0.6.12` `frontend::query`, `Message::parse`, and `numeric_send` base-10000 header (`ndigits`, `weight`, `sign`, `dscale`) bytes (`1.2300` -> `0002000000000004000108fc`). |
| Legacy Wire Helpers | `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` [^pg-core-rs] | Existing stateless frame helpers in `vertebra-db`; live socket state and multi-result simple query live exclusively in `vertebra-postgres`. |
| Shared Exception Hierarchy | `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` [^py-db-errors] | Canonical `DatabaseError`, `DataError`, `IntegrityError`, `ProgrammingError`, `ConnectionFailure`, `SerializationFailure`, and `QueryCancelledError` classes resolved via `PyOnceLock`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [broker-and-shm-leases.md](../../../../knowledge/domains/data/broker-and-shm-leases.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).
- Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), [python-framework](../../../../../skills/python-framework/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R10 (`ch09:t01`, `ch09:t02`)**: Provide owned `Column` and `QueryResult` structures, adapter-owned frozen `msgspec` records, exact `u64` affected command counts, `I`/`T`/`E` transaction status tracking, and structured SQLSTATE classification without duplicate Python exception hierarchies.
- **R11 (`ch09:t01`, `ch09:t03`, `ch09:t04`)**: Preserve lossless scalar and arbitrary-precision `NUMERIC` values across binary and text wire formats, retain unknown-OID columns as owned `PgValue::Text` with column `type_oid`, and reject unexpected binary frames or signaling `NaN` inputs before corruption.
- **R12 (`ch09:t02`)**: Execute multi-statement simple queries (`PostgresConnection::simple_query`), retaining every ordered result set and draining recoverable server `ErrorResponse` frames through `ReadyForQuery`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- **Rust Owned Result & Value Model (`src/rs/vertebra-postgres/src/result.rs`)**:
  - `#[repr(i16)] pub enum Format { Text = 0, Binary = 1 }`
  - `pub use crate::session::TransactionStatus;` (re-exporting upfront `TransactionStatus::{Idle, InTransaction, Failed}` from `session.rs`, wire tags `b'I'`, `b'T'`, `b'E'`)
  - `pub struct Column { pub name: String, pub type_oid: u32, pub format: Format, pub type_modifier: i32, pub table_oid: u32, pub attribute_number: i16 }`
  - `pub struct QueryResult { pub columns: Vec<Column>, pub rows: Vec<Vec<PgValue>>, pub affected_rows: u64, pub command_tag: String, pub transaction_status: TransactionStatus }` with centralized `QueryResult::try_new(...) -> Result<Self, PostgresError>` row-width validation (`row.len() == columns.len()`).
  - `pub enum PgNumeric { Finite { negative: bool, weight: i16, dscale: u16, digits: Vec<i16> }, NaN, PosInf, NegInf }` (base-10000 digits `0..=9999` with exact `to_canonical_string(&self) -> String` and decimal coefficient/scale conversion).
  - `pub enum TemporalKind { Date, Time, Timestamp, TimestampTz }`, `pub enum VectorKind { Vector, HalfVec }`
  - `pub enum PgValue { Null, Bool(bool), Int2(i16), Int4(i32), Int8(i64), Float4(f32), Float8(f64), Text(String), Bytes(Vec<u8>), Uuid([u8; 16]), Numeric(PgNumeric), Date(i32), Time(i64), Timestamp(i64), TimestampTz(i64), TemporalText { kind: TemporalKind, wire_text: String, date_style: String, time_zone: String }, Interval { months: i32, days: i32, microseconds: i64 }, Json(Vec<u8>), Array { values: Vec<PgValue>, dimensions: Vec<i32>, lower_bounds: Vec<i32>, element_oid: u32 }, Composite { type_oid: u32, field_names: Vec<String>, field_oids: Vec<u32>, values: Vec<PgValue> }, BitString { data: Vec<u8>, bit_length: i32 }, Vector { values: Vec<f32>, kind: VectorKind } }`.
- **Python Adapter Value Contract (`src/py/vertebra/db/adapters/postgres/types.py`)**:
  - Frozen `gc=False` `msgspec.Struct` classes matching `contracts.md` verbatim: `PostgresColumn`, `PostgresQueryResult`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`, `PgParameter`, `PostgresErrorInfo`, `PostgresPipelineOperation`, `PostgresPipelineOutcome`, recursive `PostgresJsonValue` and `PostgresValue` type aliases, and `_project_public_row(row: tuple[PostgresValue, ...]) -> tuple[PostgresValue, ...]` (mapping only top-level `PgJson(None)` to `None` while preserving inner containers and rich native results).
- **Simple Query & SQLSTATE (`src/rs/vertebra-postgres/src/{lib,query,session}.rs` over upfront `errors::ServerError`)**:
  - Mounts `pub mod query;` directly in `src/rs/vertebra-postgres/src/lib.rs`, consuming upfront `errors::ServerError { severity, sqlstate, message, diagnostics }`, `PostgresError::Server(ServerError)`, `PostgresError::sqlstate(&self) -> Option<&str>` (`ch01:t04`), and `session::TransactionStatus` (`ch02:t02`).
  - `pub struct SimpleQueryCollector` and `impl PostgresConnection { pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PostgresError> }`
- **Scalar & Numeric Codecs (`src/rs/vertebra-postgres/src/codecs/{mod,numeric}.rs`)**:
  - `pub struct CodecContext` carrying session `DateStyle`, `IntervalStyle`, `TimeZone`, `max_field_bytes`, and `max_nesting_depth`.
  - `pub fn format_for_oid(oid: u32) -> Format`
  - `pub fn decode_value(oid: u32, format: Format, raw: Option<&[u8]>, ctx: &CodecContext) -> Result<PgValue, PostgresError>`
  - `pub fn encode_value(value: &PgValue, target_oid: u32, format: Format, out: &mut BytesMut, ctx: &CodecContext) -> Result<bool, PostgresError>` (`Ok(false)` for SQL `NULL`)
  - `pub fn decode_numeric_binary(raw: &[u8]) -> Result<PgNumeric, PostgresError>`, `pub fn encode_numeric_binary(num: &PgNumeric, out: &mut BytesMut) -> Result<(), PostgresError>`, `pub fn decode_numeric_text(text: &str) -> Result<PgNumeric, PostgresError>`, `pub fn encode_numeric_text(num: &PgNumeric) -> String`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `PostgresConnection::simple_query` (`session.rs`) -> invalidates unnamed prepared statement state -> encodes `postgres_protocol::message::frontend::query` -> drives `SimpleQueryCollector` (`query.rs`) over `protocol::read_message` -> decodes cells via `codecs::decode_value` (`codecs/mod.rs` -> scalar & `codecs/numeric.rs`) -> routes `NotificationResponse` to central session hook -> returns `Vec<QueryResult>` or `PostgresError::Server(ServerError)` (`errors.rs`).
- Python callers / future `python_values.rs` -> construct and validate `PostgresQueryResult`, `PgArray`, `PgComposite`, `PgTemporal`, `PgBitString`, `PgVector`, `PgJson`, `PgParameter` (`src/py/vertebra/db/adapters/postgres/types.py`) without importing `vertebra._postgres`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Source buffer reuse | Drop/overwrite `BytesMut` wire frame after `QueryResult` construction | `QueryResult` retains independent owned `String`/`Vec<u8>` data | Zero borrowed wire lifetimes across FFI/tasks |
| Mid-script `ErrorResponse` | `"SELECT 1; SELECT 1/0; SELECT 2"` in `simple_query` | Retains first `ServerError("22012")`, drains through `ReadyForQuery`, records final `I`/`T`/`E`, subsequent idle query succeeds | Socket synchronization without leaked unread frames |
| Unknown column OID | Unregistered OID `99999` in `Format::Text` vs `Format::Binary` | `Text` returns `PgValue::Text` with `Column.type_oid = 99999`; `Binary` returns `Err(PostgresError::Data)` | Never decodes opaque binary bytes as UTF-8 |
| `NUMERIC` trailing scale & specials | `1.2300`, `-0.000`, `1e1000`, `NaN`, `±Infinity`, signaling `sNaN` | Preserves exact `dscale` (`4`, `3`), arbitrary digits, and specials; rejects `sNaN` and `digit >= 10000` with `DataError` | Zero `f64` or `Decimal128` precision/scale loss |
| Wrapper structural bounds | `PgBitString(b"\xff", 3)` (nonzero low padding) or `PgVector((1.0,) * 16001, "vector")` | Raises `ValueError` in Python constructor without native wheel | Fail-fast local structural validation |

### 2.5 Acceptance Scenarios (Given / When / Then)

- **Given** a scripted loopback peer emitting two `RowDescription` + `DataRow` + `CommandComplete` sequences followed by `ReadyForQuery(b'I')`, **When** `conn.simple_query("SELECT 42 AS a; UPDATE items SET x = 1")` runs, **Then** two `QueryResult` values return with typed `PgValue::Int4(42)` in result `0` and `affected_rows == 3`, `command_tag == "UPDATE 3"` in result `1`.
- **Given** PostgreSQL `numeric_send` bytes `0002000000000004000108fc` (`1.2300`), **When** `decode_numeric_binary` and `encode_numeric_binary` execute, **Then** `dscale == 4` and exact wire bytes round-trip identically without consulting thread decimal precision.

### 2.6 Alternatives Considered

| Alternative | Why Rejected | Chosen Seam |
| --- | --- | --- |
| Narrowing `NUMERIC` through `rust_decimal` (96-bit) or `f64` | Truncates PostgreSQL's 131,072-digit integer / 16,383-digit scale range and loses trailing zeros or `±Infinity`. | Owned base-10000 `PgNumeric` preserving exact `weight`, `dscale`, digits, `NaN`, and `±Infinity`. |
| Returning `dict[str, Any]` rows directly from Rust | Drops duplicate/empty column names, column OIDs, `command_tag`, and top-level `PgJson(None)` vs SQL `NULL` distinction. | Ordered `QueryResult` / `PostgresQueryResult` tuples with explicit `_project_public_row` helper for legacy high-level APIs. |
| Defining standalone `vertebra._postgres` exception classes | Breaks `except vertebra.db.DataError:` across adapters when optional wheels load independently. | Resolve canonical `vertebra._vertebra` exception classes via `PyOnceLock` in `errors.rs`. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch09]` (public docstrings in `src/py/vertebra/db/adapters/postgres/types.py` ship inline in `ch09:t01`; user documentation and OKF synthesis consolidate in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R10, R11 (owned models & Python records) | [t01](tasks/t01-owned-results-and-value-records.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/result_contract.rs`, `src/py/tests/unit/db/test_postgres_types.py` |
| R10, R12 (simple query & SQLSTATE) | [t02](tasks/t02-simple-query-and-sqlstate.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/query_protocol.rs` |
| R11 (scalar binary & text codecs) | [t03](tasks/t03-scalar-codecs.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/scalar_codecs.rs`, `src/rs/vertebra-postgres/tests/query_protocol.rs` |
| R11 (arbitrary-precision `NUMERIC`) | [t04](tasks/t04-numeric-codecs.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/numeric_codecs.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch09:t01 — Define owned query results and lossless PostgreSQL value records (tasks/t01-owned-results-and-value-records.md)
- [ ] native-postgres-driver:ch09:t02 — Implement multi-result simple query execution and canonical SQLSTATE error draining (tasks/t02-simple-query-and-sqlstate.md)
- [ ] native-postgres-driver:ch09:t03 — Implement checked built-in scalar binary and text codecs with per-column fallback (tasks/t03-scalar-codecs.md)
- [ ] native-postgres-driver:ch09:t04 — Implement arbitrary-precision base-10000 numeric binary and text codecs (tasks/t04-numeric-codecs.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch10-temporal-interval-and-json-codecs`, `ch11-type-catalog-arrays-and-composites`, `ch12-pgvector-and-extended-query`, and `ch14-physical-pools-and-query-facades`.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen PostgreSQL value records, column metadata, and SQLSTATE contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 framing, base-10000 NUMERIC wire layout, and read-only PG18.3 probes.
[^pg-core-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_query_frame` — Existing PostgreSQL wire framing baseline in `vertebra-db`.
[^py-db-errors]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical shared `DatabaseError` exception hierarchy in `vertebra._vertebra`.
