---
type: Task
id: "native-postgres-driver:ch09:t01"
chapter_id: ch09-owned-results-and-numeric-codecs
title: Define owned query results and lossless PostgreSQL value records
description: Create owned Rust result/value models in result.rs and frozen gc=False msgspec value records in vertebra.db.adapters.postgres.types without eager native wheel imports.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch08:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch09-owned-results-and-numeric-codecs/spec.md
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/skills/pyo3-bridge/SKILL.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/db/adapters/postgres/types.py
tests:
  - src/rs/vertebra-postgres/tests/result_contract.rs
  - src/py/tests/unit/db/test_postgres_types.py
estimated_loc: 245
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: null
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
    title: Exact PostgreSQL value records and column/result invariants
  - id: pg-adapter-init
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: PostgreSQL Python adapter package layout
  - id: db-drivers-guide
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Per-adapter database package boundaries and FFI ownership invariants
tags:
  - postgres
  - rust
  - msgspec
  - results
---

# Task t01: Define Owned Query Results and Lossless PostgreSQL Value Records

## Objective & Scope Boundary

Define the single owned Rust (`src/rs/vertebra-postgres/src/result.rs`) and Python (`src/py/vertebra/db/adapters/postgres/types.py`) result and value contract shared across simple/extended queries, `COPY`, `VTBARROW`, pipelines, and Python projection [^pg-contracts] [^pg-adapter-init]. Wire protocol execution (`query.rs`), binary/text wire codecs (`codecs/`), and PyO3 conversion (`python_values.rs`) are out of scope and belong to `ch09:t02`–`ch14:t03`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Exact `msgspec.Struct` definitions, field order, defaults, and OID/temporal/array/composite/bitstring/vector invariants [^pg-contracts].
- [spec.md](../spec.md) — Chapter 09 Rust `result.rs` and Python `types.py` contracts.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — Existing PostgreSQL adapter package layout (must remain importable without `vertebra._postgres`) [^pg-adapter-init].
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Adapter ownership and cross-`.so` FFI rules [^db-drivers-guide].
- `.agents/skills/pyo3-bridge/SKILL.md` — Owned Rust data invariants outside the GIL.
- `.agents/skills/python-framework/SKILL.md` — `msgspec.Struct(frozen=True, gc=False)`, PEP 604/695 typing, and zero inline `#` comments in Python.

## Symbol & Signature Contracts

- **Rust (`src/rs/vertebra-postgres/src/result.rs`, re-exported explicitly from `src/rs/vertebra-postgres/src/lib.rs`)**:
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] #[repr(i16)] pub enum Format { Text = 0, Binary = 1 }` (`Format::from_code(i16) -> Result<Self, PostgresError>`).
  - `pub use crate::session::TransactionStatus;` (re-exporting the upfront `TransactionStatus::{Idle, InTransaction, Failed}` enum defined in `src/rs/vertebra-postgres/src/session.rs` in `ch02:t02` for wire tags `b'I'`, `b'T'`, `b'E'`).
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct Column { pub name: String, pub type_oid: u32, pub format: Format, pub type_modifier: i32, pub table_oid: u32, pub attribute_number: i16 }` (`type_oid >= 1` validated).
  - `#[derive(Clone, Debug, PartialEq)] pub struct QueryResult { pub columns: Vec<Column>, pub rows: Vec<Vec<PgValue>>, pub affected_rows: u64, pub command_tag: String, pub transaction_status: TransactionStatus }` with `QueryResult::try_new(...) -> Result<Self, PostgresError>` enforcing `row.len() == columns.len()` for every row.
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub enum PgNumeric { Finite { negative: bool, weight: i16, dscale: u16, digits: Vec<i16> }, NaN, PosInf, NegInf }` (arbitrary base-10000 digits `0..=9999`, preserving sign, coefficient digits, `dscale <= 16383`, and specials without `f64` or fixed-width `Decimal` narrowing).
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum TemporalKind { Date, Time, Timestamp, TimestampTz }` and `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum VectorKind { Vector, HalfVec }`.
  - `#[derive(Clone, Debug, PartialEq)] pub enum PgValue` with variants: `Null`, `Bool(bool)`, `Int2(i16)`, `Int4(i32)`, `Int8(i64)`, `Float4(f32)`, `Float8(f64)`, `Text(String)`, `Bytes(Vec<u8>)`, `Uuid([u8; 16])`, `Numeric(PgNumeric)`, `Date(i32)`, `Time(i64)`, `Timestamp(i64)`, `TimestampTz(i64)`, `TemporalText { kind: TemporalKind, wire_text: String, date_style: String, time_zone: String }`, `Interval { months: i32, days: i32, microseconds: i64 }`, `Json(Vec<u8>)`, `Array { values: Vec<PgValue>, dimensions: Vec<i32>, lower_bounds: Vec<i32>, element_oid: u32 }`, `Composite { type_oid: u32, field_names: Vec<String>, field_oids: Vec<u32>, values: Vec<PgValue> }`, `BitString { data: Vec<u8>, bit_length: i32 }`, `Vector { values: Vec<f32>, kind: VectorKind }`. Zero `PyObject` or borrowed slice lifetimes enter `result.rs`.
- **Python (`src/py/vertebra/db/adapters/postgres/types.py`)**:
  - Implement all frozen `gc=False` `msgspec.Struct` classes and PEP 695 `type` aliases from `contracts.md` (`TemporalKind`, `TemporalSpecial`, `PostgresJsonValue`, `PostgresValue`, `PostgresColumn`, `PostgresQueryResult`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`, `PgParameter`, `PostgresErrorInfo`, `PostgresPipelineOperation`, `PostgresPipelineOutcome`) with `__post_init__` structural validation (no I/O, no eager `vertebra._postgres` import).
  - Implement `_project_public_row(row: tuple[PostgresValue, ...]) -> tuple[PostgresValue, ...]` mapping only top-level `PgJson(None)` cells to `None` while keeping every other value (including inner JSON nulls, `PgArray`, `PgComposite`, `PgBitString`, `PgVector`, `PgTemporal`) unchanged.

## Edge Cases & Error Semantics

- **Integer vs `bool` strictness**: Python `bool` is a subclass of `int`; every integer field validator in `types.py` (`type_oid`, `format`, `type_modifier`, `table_oid`, `attribute_number`, `affected_rows`, `months`, `days`, `microseconds`, `dimensions`, `lower_bounds`, `element_oid`, `field_oids`, `value`, `bit_length`, `index`, `failed_index`) explicitly rejects `isinstance(v, bool)` with `TypeError`.
- **OID & Modifier Bounds**: Result `type_oid`, `PgParameter.type_oid`, `PgArray.element_oid`, `PgComposite.type_oid`, and `PgComposite.field_oids` require `1 <= oid <= 2**32 - 1`; `table_oid` allows `0..=2**32 - 1`; `PostgresPipelineOperation.parameter_oids` allows `0..=2**32 - 1` (`0` = server inference) and must equal `len(parameters)`. `format` must be exact integer `0` or `1`.
- **Container & Wrapper Invariants**:
  - `PgArray`: rank `0..=6`, `len(dimensions) == len(lower_bounds)`, rank `0` requires `values == ()`, non-empty requires `dim >= 1` in `i32`, `checked_product(dimensions) == len(values)`, and `lbound + dim - 1 <= 2**31 - 1`.
  - `PgComposite`: `len(field_names) == len(field_oids) == len(values)`.
  - `PgBitString`: `0 <= bit_length <= 2**31 - 1`, `isinstance(data, bytes)` (reject mutable `bytearray`/`memoryview`), `len(data) == (bit_length + 7) // 8`, `(b"", 0)` valid, and unused low bits in `data[-1]` must be `0` (`(b"\xa0", 3)` valid for `101`; `(b"\xff", 3)` raises `ValueError`).
  - `PgVector`: `kind in ("vector", "halfvec")`, `1 <= len(values) <= 16000`, every element finite `int | float` (not `bool`, not `NaN`/`±inf`), normalized or validated as finite floats.
  - `PgTemporal`: `wire_text` mode (`kind == "timestamptz"`, `special == "wire_text"`, `value is None`, non-empty `wire_text`, `date_style`, `time_zone`) and raw-integer mode (`value is not None`, `wire_text`/`date_style`/`time_zone` all `None`) are mutually exclusive; `end_of_day` requires `kind == "time"` and `value == 86_400_000_000`; infinities reject `kind == "time"` and enforce exact `±2**31`/`2**31-1` (`date`) or `±2**63`/`2**63-1` (`timestamp`/`timestamptz`) sentinels.
  - `PgParameter`: rejected if nested inside `PgParameter`, `PgArray`, `PgComposite`, or `PgJson`.

## Public-Seam Test Blueprint

- **Canonical test files**:
  - `src/rs/vertebra-postgres/tests/result_contract.rs` (in-process Rust crate public model tests)
  - `src/py/tests/unit/db/test_postgres_types.py` (pure-Python `msgspec` model tests without requiring `vertebra._postgres`)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Buffer destruction survival & duplicate/empty columns | Construct `QueryResult` from temporary `Vec<u8>` buffer, zeroize/drop source buffer; columns `["id", "", "id"]`, row `[Int4(7), Null, Bytes(vec![])]` | `QueryResult` retains `["id", "", "id"]`, `PgValue::Null != PgValue::Bytes(vec![])`, and all 3 `TransactionStatus` states (`Idle`, `InTransaction`, `Failed`). |
| Row-width mismatch rejected | `QueryResult::try_new` with 2 columns and a 3-cell row | Returns `Err(PostgresError::Protocol(_))` (or `Data`). |
| `PgBitString` padding & empty invariants | `PgBitString(b"", 0)`, `PgBitString(b"\xa0", 3)`, `PgBitString(b"\xa8", 3)`, `PgBitString(b"\xa0", 9)` | First two succeed; `(b"\xa8", 3)` (nonzero bit 3) and `(b"\xa0", 9)` (wrong byte len) raise `ValueError`. |
| `PgVector` dimension & finiteness bounds | `PgVector((0.25, -0.5), "halfvec")`, `PgVector((), "vector")`, `PgVector((0.0,) * 16001, "vector")`, `PgVector((float("nan"),), "vector")`, `PgVector((True,), "vector")` | First succeeds; empty, `16001`-dim, `NaN`/`inf`, and `bool` raise `ValueError` / `TypeError`. |
| `PgArray` & `PgComposite` shape invariants | `PgArray((1, None, 2, 3), (2, 2), (0, 3), 23)` vs `PgArray((1, 2, 3), (2, 2), (1, 1), 23)` | `2x2` with `lower_bounds=(0, 3)` succeeds; mismatched product `3 != 4` raises `ValueError`. |
| `PgTemporal` sentinel & `wire_text` exclusivity | `PgTemporal("time", 86_400_000_000, "end_of_day")`, `PgTemporal("timestamptz", None, "wire_text", "2026-07-01 12:00:00 CST", "Postgres, MDY", "Asia/Shanghai")`, `PgTemporal("time", 2**63 - 1, "positive_infinity")` | First two succeed; `time` with `positive_infinity` or mixed `value` + `wire_text` raises `ValueError`. |
| `_project_public_row` top-level JSON null | `_project_public_row((PgJson(None), {"a": None}, None, PgBitString(b"\x80", 1)))` | Returns `(None, {"a": None}, None, PgBitString(b"\x80", 1))`; original `PostgresQueryResult.rows` preserves `PgJson(None)`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test result_contract`
  - `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q`
- Final Green + Lint/Typecheck gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test result_contract && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
  - `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/types.py src/py/tests/unit/db/test_postgres_types.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/types.py`

## Acceptance Criteria

- [ ] Owned Rust (`result.rs`) and Python (`types.py`) model fields, discriminants, and exact units match `contracts.md` verbatim (R10, R11).
- [ ] Column order, duplicate/empty column names, `NUMERIC` `dscale`/specials, `PgInterval` months, `PgArray` lower bounds, `PgComposite` field OIDs, `PgBitString` zero padding, and `PgVector` `1..=16000` finiteness invariants survive without loss (R10, R11).
- [ ] Importing and constructing `vertebra.db.adapters.postgres.types` works in pure Python without `vertebra._postgres` installed, with zero inline `#` comments in Python (R10, R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exact PostgreSQL value records and column/result invariants.
[^pg-adapter-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — PostgreSQL Python adapter package layout.
[^db-drivers-guide]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Per-adapter database package boundaries and FFI ownership invariants.
