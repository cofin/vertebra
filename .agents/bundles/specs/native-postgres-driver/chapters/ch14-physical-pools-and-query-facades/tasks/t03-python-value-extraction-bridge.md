---
type: Task
id: "native-postgres-driver:ch14:t03"
chapter_id: ch14-physical-pools-and-query-facades
title: "Bridge lossless native PostgreSQL values and SQLSTATE errors to Python"
description: "Convert owned Rust PgValue and QueryResult records to/from frozen Python msgspec records and map PostgresError (including AfterCompletion.completed_result) to canonical vertebra._vertebra exceptions."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch14:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch14-physical-pools-and-query-facades/spec.md
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-py/src/errors.rs
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/rs/vertebra-postgres/src/python_values.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/python_value_contract.rs
estimated_loc: 250
plan_revision: 5
plan_commit: 0147d80c
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
commit: e7c0a42f
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen Python msgspec PostgreSQL value records, JSON/temporal discriminants, and AfterCompletion contracts
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical vertebra._vertebra DatabaseError subclasses and SQLSTATE attribute contract
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Python PostgreSQL adapter namespace owning vertebra.db.adapters.postgres.types
tags:
  - postgres
  - pyo3
  - values
  - errors
  - msgspec
---

# Task t03: Bridge Lossless Native PostgreSQL Values and SQLSTATE Errors to Python

## Objective & Scope Boundary

Implement the single shared Rust/PyO3 conversion bridge (`python_values.rs`) between owned native `PgValue` / `QueryResult` types and the adapter-owned frozen `msgspec.Struct` records in `vertebra.db.adapters.postgres.types`, plus the canonical `vertebra._vertebra` SQLSTATE / `AfterCompletion` exception mapper in `errors.rs` [^pg-contracts] [^db-errors-rs]. Public `SyncPostgresClient` / `AsyncPostgresClient` query methods belong to `ch14:t04`; no Python-visible test-only helpers are added.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 14 value extraction/projection and `AfterCompletion` diagnostic rules.
- [contracts.md](../../../contracts.md) — Complete `Exact PostgreSQL value records` specification (`PostgresColumn`, `PostgresQueryResult`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`, `PgParameter`) and JSON/temporal/vector/bit invariants [^pg-contracts].
- `src/rs/vertebra-postgres/src/result.rs` — Native `PgValue`, `Column`, `QueryResult`, `TemporalValue`, and container invariants from Chapters 09–12.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError`, `ServerError`, and `PostgresError::AfterCompletion { source, completed_result }` from `ch12:t02`.
- `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `vertebra._vertebra` exception hierarchy (`DatabaseError`, `InterfaceError`, `DataError`, `OperationalError`, `IntegrityError`, `ProgrammingError`, `NotSupportedError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`) [^db-errors-rs].
- [pyo3-bridge/SKILL.md](../../../../../../skills/pyo3-bridge/SKILL.md) — `PyOnceLock` class caching, owned extraction before `py.detach`, and zero `Py<PyAny>` on Tokio worker threads [^pg-driver-py].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/python_values.rs` (exported in `lib.rs` for Rust/FFI consumers):
  - `pub fn extract_parameters(py: Python<'_>, parameters: &Bound<'_, PyAny>, parameter_oids: Option<&Bound<'_, PyAny>>) -> Result<(Vec<Parameter>, Option<Vec<u32>>), PostgresError>`
  - `pub fn extract_pg_value(py: Python<'_>, obj: &Bound<'_, PyAny>, target_oid: Option<u32>) -> Result<PgValue, PostgresError>`
  - `pub fn pg_value_to_python(py: Python<'_>, value: &PgValue) -> PyResult<Py<PyAny>>`
  - `pub fn query_result_to_python(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>>`
  - Resolve and cache constructors from `vertebra.db.adapters.postgres.types` (`PostgresColumn`, `PostgresQueryResult`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`, `PgParameter`), `decimal.Decimal`, `uuid.UUID`, `datetime.{date, time, datetime, timezone}`, and `msgspec.json` via `PyOnceLock`.
  - Exact extraction and projection rules [^pg-contracts]:
    - Check `PyBool` BEFORE `PyInt` so `bool` never silently coerces to integer fields/parameters; `PgParameter` is valid only as an outer parameter wrapper (nested `PgParameter` inside arrays/composites/JSON/`PgParameter` raises `DataError` / `InterfaceError`).
    - `Decimal`: extract via `as_tuple()` (sign, exact digits, exponent/scale, `NaN`, `+-Infinity`) and project from exact decimal spelling / tuple so narrow `decimal.getcontext().prec` (such as `prec = 2`) never rounds precision or drops display scale.
    - Temporal: representable finite values project to `date`, `time`, naive `datetime`, or UTC-aware `datetime`; `+-infinity`, Python-range overflow (`out_of_range`), `24:00:00` (`end_of_day`, `86_400_000_000`), and `PgValue::TemporalText` (`special="wire_text"`, `value=None`, exact `wire_text`/`date_style`/`time_zone`) project to `PgTemporal`. Every `PgTemporal(special="wire_text")` / `TemporalText` typed bind fails with `PostgresError::Data` (`DataError`) before submission across text, binary, pipeline, and COPY paths, even under matching context.
    - JSON: SQL `NULL` -> `None`; top-level JSON/JSONB `null` -> `PgJson(None)`; inner JSON `null` stays `None` inside its `list`/`dict` (`[None]`, `{"k": None}`); arbitrary-precision JSON integers survive via `msgspec.json` without `f64` rounding; cycle-containing or non-JSON Python objects fail before detach.
    - `PgInterval`, `PgArray` (rank `0..=6`, lower bounds, element OID), `PgComposite` (`type_oid`, `field_names`, `field_oids`, `values`), `PgBitString` (`data: bytes`, `bit_length: int`, zero unused low bits), and `PgVector` (`kind` `"vector"` | `"halfvec"`, dim `1..=16000`, finite floats widened from exact `f32`/`f16`): preserve full round-trip fidelity; copy mutable `bytearray`/`memoryview` inputs into owned `Vec<u8>` before `py.detach`.
- `src/rs/vertebra-postgres/src/errors.rs`:
  - `pub fn postgres_error_to_py(py: Python<'_>, err: PostgresError) -> PyErr`:
    - Resolve canonical exception classes from `vertebra._vertebra` via `PyOnceLock` (never defining a duplicate runtime `DatabaseError` hierarchy) [^db-errors-rs].
    - Map SQLSTATEs: `08*` -> `ConnectionFailure`, `22*` -> `DataError`, `23*` -> `IntegrityError`, `40001`/`40P01` -> `SerializationFailure`, `42*`/`26000`/`0A000` -> `ProgrammingError` (or `NotSupportedError` for `0A*` where specified), `57014` -> `QueryCancelledError`, `PoolExhausted` -> `PoolExhaustedError`, `Operational` -> `OperationalError`, `Interface`/`InvalidConfig` -> `InterfaceError`, `UnsupportedFeature` -> `NotSupportedError`; attach `sqlstate`, `vendor_code`, `is_transient`, and structured server diagnostic attributes (`severity`, `detail`, `hint`, `schema_name`, `table_name`, `column_name`, `constraint_name`, `datatype_name`, `where_context`, `position`).
    - Unwrap `PostgresError::AfterCompletion { source, completed_result }`: map `*source` to its exact canonical exception class/SQLSTATE and set `.completed_result` on the exception instance using `query_result_to_python(py, &completed_result)`.

## Edge Cases & Error Semantics

- **Conflicting `parameter_oids` vs `PgParameter.type_oid`**: Length mismatch between `parameters` and `parameter_oids`, negative/overflow OIDs, or a positive `parameter_oids[i]` disagreeing with `PgParameter.type_oid` fails deterministically (`DataError` / `InterfaceError`) before network I/O.
- **Zero `Py<PyAny>` across Tokio**: Extraction produces pure Rust `Vec<Parameter>` before releasing the GIL; projection runs only after session locks release on the caller/drainer thread.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/python_value_contract.rs` initializing the embedded Python interpreter and exercising `python_values` + `postgres_error_to_py` directly against `vertebra.db.adapters.postgres.types` and `vertebra._vertebra`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Hostile `Decimal` context (`prec = 2`) | Extract & project `Decimal("1234567890.004500")`, `Decimal("NaN")`, `Decimal("-Infinity")` under `localcontext(prec=2)` | Exact digits, trailing scale `-6`, `NaN`, and `-Infinity` survive unchanged without rounding. |
| SQL `NULL` vs `PgJson(None)` vs `[None]` | Project `[PgValue::Null, PgValue::Json(JsonNull), PgValue::Json("[null]")]`, then re-extract | Projects to `(None, PgJson(None), [None])`; re-extraction produces SQL `NULL` (`oid=0` / typed `NULL`), JSONB `null`, and JSONB `[null]`. |
| `PgTemporal` specials & `wire_text` refusal | Project `positive_infinity`, `end_of_day` (`86_400_000_000`), `out_of_range` (`year 20000`), and `TemporalText("Wed Jul 09 07:30:00 2026 CST")`; attempt `extract_pg_value` on `PgTemporal(special="wire_text")` | Projects exact `PgTemporal` frozen structs; re-extracting `special="wire_text"` returns `Err(PostgresError::Data)` -> `DataError` before submission. |
| Nested `PgArray`, `PgComposite`, `PgBitString`, `PgVector` | Non-default lower bounds `lower_bounds=(0, 3)`, `PgBitString(b"\xa0", 3)`, `PgVector((1.5, -0.0), "halfvec")`, nested inside `PgComposite` | Round-trips exact dimensions/bounds, MSB bits, quantized `f16` floats (including `-0.0`), and ordered composite field names/OIDs; rejects `bool` inside integer/vector slots. |
| `AfterCompletion` & SQLSTATE mapping | `PostgresError::AfterCompletion { source: ConnectionClosed, completed_result }` and `ServerError("23505", constraint="users_pkey")` | Maps to `vertebra._vertebra.ConnectionFailure` with `.completed_result == PostgresQueryResult(...)` and `vertebra._vertebra.IntegrityError` with `.sqlstate == "23505"`. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test python_value_contract
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  make dev
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test python_value_contract
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `python_values.rs` converts every scalar, container, and `Pg*` wrapper losslessly between Rust and `vertebra.db.adapters.postgres.types`, keeping all Python objects off Tokio worker threads and outside session locks (R6, R7, R11).
- [ ] `Decimal` scale/specials, `PgInterval` months/days/microseconds, `PgArray` lower bounds, `PgJson(None)` vs `None`, `PgBitString`, `PgVector`, and `PgTemporal(special="wire_text")` `DataError` bind refusal hold directly and when nested (R11).
- [ ] `postgres_error_to_py` maps SQLSTATEs and `AfterCompletion.completed_result` onto canonical `vertebra._vertebra` exceptions via `PyOnceLock` (R6, R7).
- [ ] `python_value_contract` tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen `msgspec.Struct` PostgreSQL value records and `AfterCompletion` diagnostics.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Shared `vertebra._vertebra` database exception hierarchy.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Adapter package namespace for `vertebra.db.adapters.postgres.types`.
