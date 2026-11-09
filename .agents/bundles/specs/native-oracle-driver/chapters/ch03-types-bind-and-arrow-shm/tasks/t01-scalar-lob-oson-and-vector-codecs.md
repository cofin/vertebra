---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch03-types-bind-and-arrow-shm
id: "native-oracle-driver:ch03:t01"
title: "Implement scalar, temporal, LOB, OSON, and VECTOR codecs and export OracleColumn and OracleQueryResult"
description: "Define OracleColumn and OracleQueryResult once in src/rs/vertebra-oracle/src/types.rs and implement bidirectional NUMBER, VARCHAR2/NVARCHAR2/CHAR, RAW, DATE/TIMESTAMP/TSTZ, BOOLEAN, inline CLOB/BLOB, OSON, and VECTOR (0xDB 0x01 FLOAT32, FLOAT64, INT8, BINARY) codecs across Rust and Python."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch02:t03"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md
  - src/rs/vertebra-db/src/oracle/mod.rs
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/rs/vertebra-py/src/vector.rs
files:
  - src/rs/vertebra-oracle/src/types.rs
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/db/adapters/oracle/core.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 225
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
commit_sha: null
completed_at: "2026-11-09T14:51:30Z"
created_at: "2026-11-09T00:56:00Z"
updated_at: "2026-11-09T14:51:30Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public interface, FFI, type, pooling, tenancy, and ownership contracts
  - id: ora-rs-codecs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Shared Rust Oracle OSON and VECTOR wire codecs (ORACLE_VECTOR_MAGIC = 0xDB 0x01)
  - id: ora-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Python OracleCore OSON, VECTOR, and UUID codec helpers
  - id: vector-view
    resource: src/rs/vertebra-py/src/vector.rs::VectorView
    title: Zero-copy PyO3 VectorView class exported by vertebra._vertebra
tags:
  - oracle
  - codecs
  - oson
  - vector
  - pyo3
---

# Task `native-oracle-driver:ch03:t01`: Implement scalar, temporal, LOB, OSON, and VECTOR codecs and export OracleColumn and OracleQueryResult

<!-- truth: start -->
- **Single Canonical `OracleColumn` & `OracleQueryResult` Definition**: `src/rs/vertebra-oracle/src/types.rs` defines `OracleColumn` (`name: String`, `type_code: u16`, `type_name: String` + `oracle_type` getter, `db_size: Option<usize>`, `precision: Option<i64>`, `scale: Option<i64>`, `nullable: bool`) and `OracleQueryResult` (`columns: Vec<Py<OracleColumn>>`, `rows: Vec<Py<PyTuple>>`, `rowcount: usize`, `lastrowid: Option<String>`, `out_binds: Option<Py<PyAny>>` with `to_dicts(py)`, `__len__`, `is_empty()`) once, registered on `vertebra._oracle` in `src/rs/vertebra-oracle/src/lib.rs` [^ora-contracts].
- **Full 26ai Scalar, Temporal, LOB, OSON & `0xDB 0x01` VECTOR Coverage**: Rust and Python codecs decode and encode `NUMBER` (`int`, `float`, `Decimal`), `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW` (`RAW(16)` UUID conversion), `DATE`/`TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`, `BOOLEAN`, inline `CLOB`/`BLOB`, `OSON` (`0xFF 'J' 'Z' 0x01`), and `VECTOR` (`ORACLE_VECTOR_MAGIC = [0xDB, 0x01]` `FLOAT32`, `FLOAT64`, `INT8`, `BINARY` via `vertebra_db::oracle::decode_oracle_vector_{f32,f64,i8,binary}` and `py.import("vertebra._vertebra").getattr("VectorView")` / `OracleCore`) [^ora-contracts] [^ora-rs-codecs] [^ora-core] [^vector-view].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Define `OracleColumn` and `OracleQueryResult` once in `src/rs/vertebra-oracle/src/types.rs`, register them in `src/rs/vertebra-oracle/src/lib.rs`, and implement bidirectional Oracle 26ai scalar, temporal, inline LOB, `OSON`, and `VECTOR` (`0xDB 0x01`) codecs across `src/rs/vertebra-oracle/src/types.rs` and `src/py/vertebra/db/adapters/oracle/core.py` [^ora-contracts] [^ora-rs-codecs] [^ora-core].

- **In Scope**:
  - Canonical `OracleColumn` and `OracleQueryResult` PyO3 classes in `src/rs/vertebra-oracle/src/types.rs` and registration in `src/rs/vertebra-oracle/src/lib.rs` (`ch03:t02` wires them into `client.rs` and `_oracle.pyi`).
  - Rust intermediate owned cell and bind enums (`OracleCellValue`, `OracleVectorData`) using `crate::errors::OracleError` so worker threads decode `oracledb` rows without holding the Python GIL and materialize Python objects during `Python::attach`.
  - Bidirectional `NUMBER` (`int`, `float`, `decimal.Decimal`), `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW`, `DATE`/`TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`, `BOOLEAN`, inline `CLOB`/`BLOB`, `OSON` (`vertebra_db::oracle::{decode_oson_payload, encode_oson_payload}`), and `VECTOR` (`0xDB 0x01`, `ORACLE_VECTOR_MAGIC` `FLOAT32`, `FLOAT64`, `INT8`, `BINARY` via `vertebra_db::oracle::{decode_oracle_vector_f32, encode_oracle_vector_f32, decode_oracle_vector_f64, encode_oracle_vector_f64, decode_oracle_vector_i8, encode_oracle_vector_i8, decode_oracle_vector_binary, encode_oracle_vector_binary}`) [^ora-rs-codecs] [^ora-core].
  - Constructing `vertebra._vertebra.VectorView` via `py.import("vertebra._vertebra")` (or returning `list[float]` / `list[int]` / `bytes`) without linking `vertebra-py` as a Rust dependency of `vertebra-oracle` [^vector-view].
- **Out of Scope**:
  - Client query/execute methods (`execute`, `execute_many`, `query`, `query_one`, `query_arrow`), positional/named bind lowering, `DML RETURNING INTO`, and `_oracle.pyi` stub declarations (`ch03:t02`).
  - Arrow 59 `RecordBatch` IPC serialization (`batches_to_arrow_ipc`) and `VTBARROW` SHM cursors (`ch03:t03`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | Canonical `OracleColumn`, `OracleQueryResult`, and 26ai codec contracts [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md` | Chapter 03 architecture and codec requirements |
| `src/rs/vertebra-db/src/oracle/mod.rs` | `ORACLE_VECTOR_MAGIC` (`[0xDB, 0x01]`), `decode_oson_payload`, `encode_oson_payload`, and `decode_oracle_vector_*` / `encode_oracle_vector_*` [^ora-rs-codecs] |
| `src/py/vertebra/db/adapters/oracle/core.py` | Existing `OracleCore` OSON, VECTOR (`ORACLE_VECTOR_MAGIC = b"\xdb\x01"`), and UUID helpers [^ora-core] |
| `src/rs/vertebra-py/src/vector.rs` | Python-facing `vertebra._vertebra.VectorView` constructor and methods (`to_list`, `to_bytes`, `dtype`, `dimensions`) [^vector-view] |

## 3. Symbol & Signature Contracts

### 3.1 `src/rs/vertebra-oracle/src/types.rs` & `src/rs/vertebra-oracle/src/lib.rs`

```rust
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyTuple};
use crate::errors::OracleError;

#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
#[cfg_attr(not(Py_GIL_DISABLED), pyclass(freelist = 128))]
#[derive(Clone, Debug)]
pub struct OracleColumn {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub type_code: u16,
    #[pyo3(get)]
    pub type_name: String,
    #[pyo3(get)]
    pub db_size: Option<usize>,
    #[pyo3(get)]
    pub precision: Option<i64>,
    #[pyo3(get)]
    pub scale: Option<i64>,
    #[pyo3(get)]
    pub nullable: bool,
}

#[pymethods]
impl OracleColumn {
    #[new]
    #[pyo3(signature = (name, type_name, type_code=0, db_size=None, precision=None, scale=None, nullable=true))]
    pub fn new(
        name: String,
        type_name: String,
        type_code: u16,
        db_size: Option<usize>,
        precision: Option<i64>,
        scale: Option<i64>,
        nullable: bool,
    ) -> Self;

    #[getter]
    pub fn oracle_type(&self) -> &str {
        &self.type_name
    }
}

#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
#[derive(Debug)]
pub struct OracleQueryResult {
    pub columns: Vec<Py<OracleColumn>>,
    pub rows: Vec<Py<PyTuple>>,
    #[pyo3(get)]
    pub rowcount: usize,
    #[pyo3(get)]
    pub lastrowid: Option<String>,
    pub out_binds: Option<Py<PyAny>>,
}

#[pymethods]
impl OracleQueryResult {
    #[new]
    #[pyo3(signature = (columns, rows, rowcount=0, out_binds=None, lastrowid=None))]
    pub fn new(
        py: Python<'_>,
        columns: Vec<Py<OracleColumn>>,
        rows: Vec<Py<PyTuple>>,
        rowcount: usize,
        out_binds: Option<Py<PyAny>>,
        lastrowid: Option<String>,
    ) -> Self;

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>>;

    #[getter]
    pub fn rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>>;

    #[getter]
    pub fn out_binds<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyAny>>;

    pub fn to_dicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>>;

    pub fn __len__(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum OracleVectorData {
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8(Vec<i8>),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum OracleCellValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    DecimalStr(String),
    Str(String),
    Bytes(Vec<u8>),
    Date { year: i32, month: u8, day: u8 },
    Timestamp {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
        tz_hour_offset: Option<i8>,
        tz_minute_offset: Option<i8>,
    },
    Oson(Vec<u8>),
    Vector(OracleVectorData),
}

impl OracleCellValue {
    pub fn into_py_object<'py>(
        self,
        py: Python<'py>,
        fetch_decimals: bool,
    ) -> Result<Bound<'py, PyAny>, OracleError>;
}

pub fn extract_bind_value(obj: &Bound<'_, PyAny>) -> Result<OracleCellValue, OracleError>;
```

### 3.2 `src/py/vertebra/db/adapters/oracle/core.py`

Ensure `OracleCore` defines `ORACLE_VECTOR_MAGIC = b"\xdb\x01"` and supports `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`), `OSON` (`0xFF 'J' 'Z' 0x01`), `UUID` (`RAW(16)`), and scalar/temporal normalization across all vector return formats (`"vector_view"`, `"list"`, `"bytes"`) [^ora-core]:

```python
class OracleCore:
    ORACLE_VECTOR_MAGIC: Final[bytes] = b"\xdb\x01"

    @staticmethod
    def encode_vector(
        values: Sequence[float | int] | bytes | VectorView,
        *,
        dtype: Literal["FLOAT32", "FLOAT64", "INT8", "BINARY"] = "FLOAT32",
    ) -> bytes: ...

    @staticmethod
    def decode_vector(
        payload: bytes | VectorView | Sequence[float | int],
        *,
        return_format: Literal["vector_view", "list", "bytes"] = "vector_view",
    ) -> VectorView | list[float] | list[int] | bytes: ...

    @staticmethod
    def coerce_bind_parameter(
        value: Any,
        *,
        features: OracleDriverFeatures | None = None,
    ) -> Any: ...

    @staticmethod
    def coerce_row_cell(
        value: Any,
        column_type: str | None = None,
        *,
        features: OracleDriverFeatures | None = None,
    ) -> Any: ...
```

## 4. Edge Cases & Error Semantics

- **`NUMBER` Precision vs `int` Fast Path**: When `scale == 0` (or `scale <= 0` without fractional digits) and `fetch_decimals=False`, `NUMBER` values that fit in `i64` or integer strings convert to Python `int`. Fractional numbers convert to `decimal.Decimal` when `fetch_decimals=True` (or `DecimalStr`) and `float` for `BINARY_FLOAT` / `BINARY_DOUBLE`.
- **High-Precision Timestamps & Timezones**: Oracle `TIMESTAMP(9)` nanoseconds (`0..999_999_999`) convert to Python `datetime.datetime` microseconds (`nanosecond / 1000`). `TIMESTAMP WITH TIME ZONE` with `tz_hour_offset` and `tz_minute_offset` constructs `datetime.timezone(datetime.timedelta(hours=..., minutes=...))`.
- **Canonical `0xDB 0x01` `VECTOR` & `0xFF 'J' 'Z' 0x01` `OSON` Wire Headers**: `encode_oracle_vector_*` and `decode_oracle_vector_*` in `vertebra_db::oracle` and `OracleCore` validate the `[0xDB, 0x01]` magic prefix (`ORACLE_VECTOR_MAGIC`). Corrupted `OSON` or `VECTOR` frames map to `OracleError` -> `DataError` (`sqlstate="22000"`) rather than panicking [^ora-contracts] [^ora-rs-codecs].
- **Cross-Wheel `VectorView` Construction**: In `types.rs`, decoded vector bytes or `list[float]` construct `VectorView` via `py.import("vertebra._vertebra")?.getattr("VectorView")` when requested, avoiding any Rust crate dependency from `vertebra-oracle` onto `vertebra-py` [^vector-view].
- **`RAW(16)` UUID Toggle**: When `features.enable_uuid_binary=True`, `OracleCore.coerce_bind_parameter` converts `uuid.UUID` into `16`-byte `value.bytes`, and `OracleCore.coerce_row_cell` converts 16-byte `RAW` payloads into `uuid.UUID(bytes=value)`.

## 5. Public-Seam Test Blueprint

Add behavioral codec unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_oracle_column_and_query_result_pyclasses` | Construct `OracleColumn("id", "NUMBER", 2, 22, 10, 0, False)` and `OracleQueryResult([col], [(1,)], rowcount=1, out_binds=(42,), lastrowid="AAAR3sAAEAAAACXAAA")` | Inspect `name`, `type_code`, `type_name`, `oracle_type`, `to_dicts()`, `len(res)`, `res.is_empty()`, and attempt mutation (`col.name = "x"`) | Attributes match exact values, `res.to_dicts() == [{"id": 1}]`, `len(res) == 1`, `res.is_empty() is False`, and mutation raises `AttributeError` |
| `test_oracle_scalar_and_temporal_codecs_roundtrip` | Parametrized inputs: `int`, `Decimal("12345.67")`, `float`, `str` (UTF-8), `bytes`, `bool`, `datetime.date(2026, 11, 9)`, naive `datetime.datetime`, timezone-aware `datetime.datetime` (`+05:30`), inline `CLOB`/`BLOB` | Pass through `vertebra._oracle` codec bridge and `OracleCore.coerce_bind_parameter` / `coerce_row_cell` | Decoded Python objects equal inputs with exact types (`bool` stays `bool`, `TSTZ` retains `tzinfo` offset `+05:30`, `Decimal` preserves exact scale) |
| `test_oracle_oson_codec_roundtrip_and_invalid_magic` | Nested dict `{"tenant": "acme", "scores": [1, 2, 3]}` and corrupted header `b"\xffJZ\x99..."` | Call `OracleCore.encode_oson` -> `OracleCore.decode_oson`; call `decode_oson` on corrupted bytes | Valid payload round-trips to identical dict via `0xFF 'J' 'Z' 0x01` frame; corrupted header raises `DataError` / `ValueError` |
| `test_oracle_vector_codec_all_dtypes_and_return_formats` | Parametrized dtypes: `FLOAT32` `[1.5, -2.25, 3.0]`, `FLOAT64` `[1.125, -0.5]`, `INT8` `[-128, 0, 127]`, `BINARY` `b"\xaa\x55"` across `return_format in ("vector_view", "list", "bytes")` | Encode via `OracleCore.encode_vector(..., dtype=...)` (checking `payload.startswith(b"\xdb\x01")`) and decode via `OracleCore.decode_vector(..., return_format=...)` | Encoded frame starts with `b"\xdb\x01"`; `"vector_view"` returns `VectorView` with matching `dtype`, `dimensions`, and `to_list()`; `"list"` returns Python `list`; `"bytes"` returns raw payload bytes |
| `test_oracle_uuid_raw16_feature_toggle` | `u = uuid.uuid4()` with `OracleDriverFeatures(enable_uuid_binary=True)` vs `OracleDriverFeatures(enable_uuid_binary=False)` | Call `OracleCore.coerce_bind_parameter` and `OracleCore.coerce_row_cell(u.bytes, "RAW", features=...)` | When `True`, binds `16`-byte `bytes` and hydrates `uuid.UUID`; when `False`, leaves `RAW(16)` as `bytes` |

## 6. Verification

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "column or query_result or scalar or oson or vector or uuid" -q
```

## 7. Acceptance Criteria

- [x] `OracleColumn` and `OracleQueryResult` are defined once in `src/rs/vertebra-oracle/src/types.rs` and registered on `vertebra._oracle` as `frozen, immutable_type` PyO3 classes with `to_dicts(py)`, `__len__`, and `is_empty()`.
- [x] `NUMBER` (`int`, `float`, `Decimal`), `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW`, `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`, `BOOLEAN`, inline `CLOB`/`BLOB`, `OSON` (`0xFF 'J' 'Z' 0x01`), and `VECTOR` (`0xDB 0x01` `FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) round-trip cleanly across Rust and Python.
- [x] Corrupted `OSON` or `VECTOR` binary frames raise deterministic `DataError` / `ValueError` exceptions without panicking.
- [x] All unit tests in `src/py/tests/unit/db/test_oracle_adapter.py` pass.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-rs-codecs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Shared Rust Oracle OSON and VECTOR wire codecs (`ORACLE_VECTOR_MAGIC = 0xDB 0x01`).
[^ora-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Python `OracleCore` OSON, VECTOR, and UUID codec helpers.
[^vector-view]: `src/rs/vertebra-py/src/vector.rs::VectorView` — Zero-copy PyO3 `VectorView` class exported by `vertebra._vertebra`.
