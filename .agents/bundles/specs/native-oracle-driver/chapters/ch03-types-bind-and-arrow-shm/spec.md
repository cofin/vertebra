---
type: Spec
parent_flow: native-oracle-driver
chapter_id: ch03-types-bind-and-arrow-shm
title: "Chapter 03: Type Codecs, Positional/Named Binds, RETURNING, and Arrow 59 SHM Bridge"
description: "Implement scalar, temporal, LOB, OSON, and VECTOR codecs, positional and named bind lowering, single-round-trip DML RETURNING out-binds, lowercase column normalization, and the Arrow 59 to VTBARROW SHM data plane."
status: stable
state: in_progress
planning_status: complete
depends_on_chapters:
  - ch02-connect-auth-and-error-hierarchy
replan_trigger: null
current_task: "native-oracle-driver:ch03:t02"
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
last_verified_checkpoint: null
last_reviewed_commit: null
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
  - id: ora-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle OSON, VECTOR, TNS, and session context helpers
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver and AsyncOracleDriver adapter surface
  - id: ora-rs-codecs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Shared Rust Oracle OSON, VECTOR, and TTC piggyback codecs
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Unregistered VTBARROW shared-memory Arrow IPC stream writer
  - id: py-async-arrow
    resource: src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor
    title: PyO3 constructor helper for ArrowCursor over inline IPC and ShmCapsule
  - id: arrow-cursor
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper over inline IPC and VTBARROW ShmCapsule
tags:
  - oracle
  - codecs
  - oson
  - vector
  - arrow
  - shm
---

# Chapter 03: Type Codecs, Positional/Named Binds, RETURNING, and Arrow 59 SHM Bridge

<!-- truth: start -->
- **Canonical `OracleColumn` & `OracleQueryResult` + 26ai Codecs (`R5`)**: `src/rs/vertebra-oracle/src/types.rs` defines `OracleColumn` (`name`, `type_code`, `type_name` / `oracle_type`, `db_size`, `precision`, `scale`, `nullable`) and `OracleQueryResult` (`columns`, `rows`, `rowcount`, `lastrowid`, `out_binds`, `to_dicts`, `__len__`, `is_empty`) once and bridges `NUMBER`, `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW`, `DATE`/`TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`, `BOOLEAN`, inline `CLOB`/`BLOB`, `OSON` (`0xFF 'J' 'Z' 0x01`, `msgspec`), and `VECTOR` (`0xDB 0x01` `ORACLE_VECTOR_MAGIC` `FLOAT32`, `FLOAT64`, `INT8`, `BINARY` via `vertebra_db::oracle` and `vertebra._vertebra.VectorView`) [^ora-contracts] [^ora-core] [^ora-rs-codecs].
- **Bind Lowering, `DML RETURNING INTO` & Lowercase Column Normalization (`R6`)**: `SyncOracleClient`, `AsyncOracleClient`, `SyncOracleDriver`, and `AsyncOracleDriver` lower positional (`:1..:n`) and named (`:name`) binds, capture single-round-trip `DML ... RETURNING ... INTO` out-binds into `OracleQueryResult.out_binds` and row projections, batch `execute_many` array DML, and normalize unquoted uppercase column identifiers to lowercase when `enable_lowercase_column_names=True` (`True` default) [^ora-contracts] [^ora-driver].
- **Crate-Local Arrow 59 to `VTBARROW` SHM Data Plane (`R7`)**: `batches_to_arrow_ipc` in `src/rs/vertebra-oracle/src/arrow.rs` consumes `oracledb` `RecordBatch` streams (`arrow-array = "59"`, `arrow-schema = "59"`), serializes them via crate-local `arrow-ipc = "59"` (`StreamWriter`) into `(Vec<u8>, usize)`, and passes them into `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` (`ShmArrowFrameHeader::SIZE == 16` bytes, `ArrowCursor.used_shm`, `ArrowCursor.buffer_view()`) without leaking `arrow 59` outside `vertebra-oracle` [^ora-contracts] [^arrow-shm] [^py-async-arrow] [^arrow-cursor].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Once `SyncOracleClient` and `AsyncOracleClient` manage worker-thread TNS connections and SQLSTATE error mapping ([ch02-connect-auth-and-error-hierarchy](../ch02-connect-auth-and-error-hierarchy/spec.md)), the execution data plane must convert Oracle 26ai scalar, temporal, LOB, JSON (`OSON`), and AI `VECTOR` columns and bind parameters cleanly across the PyO3 FFI boundary, normalize Oracle's default uppercase column metadata, and stream tabular results into `ArrowCursor` (`VTBARROW` shared memory) [^ora-contracts] [^ora-core] [^ora-driver].

This chapter implements Requirements `R5`, `R6`, and `R7` from the [parent specification](../../spec.md) and [public contracts](../../contracts.md):
- `src/rs/vertebra-oracle/src/types.rs` and `src/py/vertebra/db/adapters/oracle/core.py` define `OracleColumn` and `OracleQueryResult` once in `types.rs` and implement bidirectional scalar, LOB, `OSON` (`0xFF 'J' 'Z' 0x01`), and `VECTOR` (`0xDB 0x01`, `ORACLE_VECTOR_MAGIC`) codecs reusing `vertebra_db::oracle::{decode_oson_payload, encode_oson_payload, decode_oracle_vector_f32, encode_oracle_vector_f32, decode_oracle_vector_f64, encode_oracle_vector_f64, decode_oracle_vector_i8, encode_oracle_vector_i8, decode_oracle_vector_binary, encode_oracle_vector_binary}` (`ch03:t01`) [^ora-core] [^ora-rs-codecs].
- `src/rs/vertebra-oracle/src/client.rs`, `src/py/vertebra/db/adapters/oracle/driver.py`, and `src/py/vertebra/_oracle.pyi` wire `OracleColumn` and `OracleQueryResult` into `execute`, `execute_many`, `query`, `query_one`, and `query_arrow`, implement positional (`:1..:n`) and named (`:name`) bind lowering, single-round-trip `DML ... RETURNING ... INTO` out-bind capture (`OracleQueryResult.out_binds`), and `enable_lowercase_column_names` (`True` default) normalization (`ch03:t02`) [^ora-contracts] [^ora-driver].
- `src/rs/vertebra-oracle/src/arrow.rs`, `src/rs/vertebra-oracle/src/client.rs`, and `src/py/vertebra/db/adapters/oracle/driver.py` implement `batches_to_arrow_ipc` and `query_arrow` using `oracledb`'s `features = ["arrow"]` (`arrow 59`) and `construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)`, plus `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` (`ch03:t03`) [^ora-contracts] [^arrow-shm] [^py-async-arrow] [^arrow-cursor].

### 1.2 Non-Goals

- Physical connection pooling, DRCP `cclass`/`purity`/`connection_tag` state tracking, and TTC session attribute piggyback (owned by [ch04-pools-drcp-and-session-piggyback](../ch04-pools-drcp-and-session-piggyback/spec.md)).
- Delayed-commit sequential pipelines (`SyncOraclePipeline` / `AsyncOraclePipeline`) and VPD reset-or-evict tenancy (owned by [ch05-pipelines-tenancy-and-apex](../ch05-pipelines-tenancy-and-apex/spec.md)).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Concrete Type, Codec, Bind, & Arrow 59 Contracts (`R5`, `R6`, `R7`)

#### `OracleColumn`, `OracleQueryResult` & 26ai Codecs (`R5` — [t01](tasks/t01-scalar-lob-oson-and-vector-codecs.md))

`src/rs/vertebra-oracle/src/types.rs` defines the single canonical `OracleColumn` and `OracleQueryResult` PyO3 classes registered on `vertebra._oracle` (`src/rs/vertebra-oracle/src/lib.rs`) [^ora-contracts]:

```rust
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
```

`OracleColumn` also exposes `#[getter] pub fn oracle_type(&self) -> &str` returning `&self.type_name`, and `OracleQueryResult` exposes `columns` (as a Python `tuple[OracleColumn, ...]`), `rows` (as `list[tuple[Any, ...]]`), `out_binds`, `to_dicts(py) -> PyResult<Bound<'_, PyList>>`, `__len__() -> usize`, and `is_empty() -> bool` [^ora-contracts].

Codec conversion rules across `src/rs/vertebra-oracle/src/types.rs` and `src/py/vertebra/db/adapters/oracle/core.py` [^ora-contracts] [^ora-core] [^ora-rs-codecs]:
1. **`NUMBER`**: Decodes to Python `int` when `scale == 0` (or integer-valued string representation) and `fetch_decimals=False`; decodes to `decimal.Decimal` when `fetch_decimals=True` or fractional with exact scale; decodes to `float` for binary floating-point (`BINARY_FLOAT`, `BINARY_DOUBLE`).
2. **`VARCHAR2` / `NVARCHAR2` / `CHAR` and Inline `CLOB`**: Decodes directly to Python `str` without streaming locator round-trips when `fetch_lobs=False`.
3. **`RAW` and Inline `BLOB`**: Decodes directly to Python `bytes`. When `enable_uuid_binary=True`, `OracleCore` converts 16-byte `RAW(16)` payloads to `uuid.UUID` and encodes `uuid.UUID` binds to 16-byte big-endian `bytes`.
4. **`DATE` / `TIMESTAMP` / `TIMESTAMP WITH TIME ZONE`**: Decodes `DATE` to `datetime.date` (or `datetime.datetime` when time fields are non-zero), `TIMESTAMP` to naive `datetime.datetime`, and `TIMESTAMP WITH TIME ZONE` to timezone-aware `datetime.datetime`.
5. **`BOOLEAN`**: Decodes Oracle 23ai/26ai native SQL `BOOLEAN` to Python `bool` and binds Python `bool` as native TNS boolean.
6. **`JSON` (`OSON` `0xFF 'J' 'Z' 0x01`)**: Uses `vertebra_db::oracle::{decode_oson_payload, encode_oson_payload}` and `msgspec` (`decode_json` / `encode_json` in `vertebra.utils.serializers`) when `enable_json_deserialization=True` [^ora-core] [^ora-rs-codecs].
7. **`VECTOR` (`0xDB 0x01` `ORACLE_VECTOR_MAGIC`, `FLOAT32`, `FLOAT64`, `INT8`, `BINARY`)**: Because `vertebra-oracle` does not depend on `vertebra-py` at the Rust crate level, `types.rs` decodes `VECTOR` frames (`vertebra_db::oracle::ORACLE_VECTOR_MAGIC = [0xDB, 0x01]`) via `vertebra_db::oracle::decode_oracle_vector_{f32,f64,i8,binary}` into Python `list[float]` / `list[int]` / `bytes` or constructs `vertebra._vertebra.VectorView` dynamically via `py.import("vertebra._vertebra")`, while `OracleCore.ORACLE_VECTOR_MAGIC = b"\xdb\x01"` provides matching Python-level `encode_vector` and `decode_vector` helpers [^ora-core] [^ora-rs-codecs].

#### Bind Lowering, `DML RETURNING INTO`, Batching & Column Casing (`R6` — [t02](tasks/t02-binds-returning-and-lowercase-columns.md))

- **Positional & Named Binds**: `execute`, `execute_many`, `query`, `query_one`, `query_arrow`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, and `select_to_arrow` accept positional sequences (`:1..:n`) or named mappings (`:name`, stripping any leading `:` on dictionary keys) using `from collections.abc import Mapping, Sequence` [^ora-contracts] [^ora-driver].
- **Single-Round-Trip `DML ... RETURNING ... INTO`**: Statements with `RETURNING ... INTO :out_1, ...` register output binds on the worker thread, populate `OracleQueryResult.out_binds` (`tuple[Any, ...]` or `dict[str, Any]`), and project rows into `execute_tuples`, `select`, and `exec_returning_async` in a single round-trip [^ora-contracts].
- **`execute_many` Array DML Batching**: `SyncOracleClient.execute_many(sql, params_seq)` and `AsyncOracleClient.execute_many(sql, params_seq)` batch parameter rows on the worker thread and return the aggregate affected `rowcount: int` (`0` for an empty sequence without socket I/O).
- **Lowercase Column Normalization**: When `enable_lowercase_column_names=True` (`True` default), unquoted uppercase Oracle column identifiers are normalized to lowercase across `OracleColumn.name`, `OracleQueryResult.to_dicts()`, `execute_with_description()`, and `select()` / `_hydrate_rows_with_schema()`.

#### Arrow 59 Zero-Copy SHM Bridge & `load_from_arrow` (`R7` — [t03](tasks/t03-arrow59-shm-and-load-from-arrow.md))

`src/rs/vertebra-oracle/src/arrow.rs` isolates `oracledb`'s Arrow 59 dependency (`arrow-array = "59"`, `arrow-buffer = "59"`, `arrow-ipc = "59"`, `arrow-schema = "59"`) inside `vertebra-oracle` and returns `(Vec<u8>, usize)` to `vertebra_db::py_async::construct_py_arrow_cursor` [^ora-contracts] [^arrow-shm] [^py-async-arrow]:

```rust
pub fn batches_to_arrow_ipc(
    batches: &[arrow_array::RecordBatch],
    lowercase_column_names: bool,
) -> Result<(Vec<u8>, usize), crate::errors::OracleError>
```

1. **Schema Normalization & Arrow 59 Stream Serialization**: `batches_to_arrow_ipc` rewrites uppercase field names in `arrow_schema::Schema` to lowercase when `lowercase_column_names` is `true` and writes batches into `arrow_ipc::writer::StreamWriter<Vec<u8>>` during `py.detach(...)` (or on the async worker thread), returning `(ipc_bytes, row_count)`.
2. **`VTBARROW` Shared-Memory Escalation**: `SyncOracleClient.query_arrow` and `AsyncOracleClient.query_arrow` pass `(ipc_bytes, row_count, force_shm)` into `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)`. When `row_count >= ARROW_SHM_THRESHOLD_ROWS` (or `force_shm == Some(true)`), `ShmArrowStreamWriter` writes the 16-byte `VTBARROW` frame header (`ShmArrowFrameHeader::SIZE == 16`: `b"VTBARROW"` magic + `u64` little-endian `byte_len`) into an unregistered POSIX SHM segment and returns `ArrowCursor` with `cursor.used_shm == True`, `cursor.buffer_view() -> memoryview`, `len(cursor) == row_count`, and `cursor.byte_len` [^arrow-shm] [^py-async-arrow] [^arrow-cursor].
3. **`SyncOracleDriver` & `AsyncOracleDriver` Arrow Facades**: `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` (with `validate_identifier(table_name, "table_name", allow_dots=True)`, column `validate_identifier`, and `/*+ APPEND_VALUES */` direct-path hint support) run natively on `SyncOracleDriver` and `AsyncOracleDriver` (`AsyncDatabaseDriver[OracleConfig]`) [^ora-driver].

### 2.2 Caller -> Seam -> Collaborator Integration Map

```text
SyncOracleDriver / AsyncOracleDriver (src/py/vertebra/db/adapters/oracle/driver.py)
  -> OracleCore.coerce_bind_parameter / decode_oson / decode_vector (src/py/vertebra/db/adapters/oracle/core.py)
  -> SyncOracleClient / AsyncOracleClient.{query,query_one,execute,execute_many,query_arrow} (src/rs/vertebra-oracle/src/client.rs)
    -> types::{OracleColumn,OracleQueryResult,OracleValue} (src/rs/vertebra-oracle/src/types.rs)
      -> vertebra_db::oracle::{decode_oson_payload,decode_oracle_vector_*} (src/rs/vertebra-db/src/oracle/mod.rs)
    -> arrow::batches_to_arrow_ipc (src/rs/vertebra-oracle/src/arrow.rs)
      -> vertebra_db::py_async::construct_py_arrow_cursor -> ShmArrowStreamWriter (16-byte VTBARROW header)
```

### 2.3 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| `NUMBER(10, 0)` vs `NUMBER(10, 2)` vs `BINARY_DOUBLE` | `scale == 0` with `fetch_decimals=False` decodes to Python `int`; fractional decimal or `fetch_decimals=True` decodes to `decimal.Decimal`; binary float decodes to `float`. |
| Corrupted `OSON` (`!= 0xFF 'J' 'Z' 0x01`) or `VECTOR` (`!= 0xDB 0x01`) wire payload | Returns `OracleError` mapped to `DataError` (`sqlstate="22000"`) without panicking or returning silent `None`. |
| Named bind dict with leading colon `{":id": 1, "name": "Ada"}` | Strips leading `:` so both `":id"` and `"id"` bind cleanly to `:id`. |
| `DML ... RETURNING id INTO :out_id` via `select_one()` or `execute_tuples()` | Captures `:out_id` in `OracleQueryResult.out_binds` and projects the returned row in a single network round-trip. |
| `query_arrow` below vs above `threshold_rows` (`force_shm`) | Below threshold returns inline `ArrowCursor` (`cursor.used_shm is False`); at/above threshold or `force_shm=True` writes a 16-byte `VTBARROW` SHM frame (`cursor.used_shm is True`, `cursor.buffer_view()`) that unlinks on `close()`. |
| `load_from_arrow` with invalid identifier `"users; DROP TABLE x"` | `validate_identifier` rejects the table or column name with `ValueError` / `ProgrammingError` before constructing SQL. |

### 2.4 Alternatives Considered

| Seam | Chosen Design | Rejected Alternative | Rationale |
| :--- | :--- | :--- | :--- |
| `OracleColumn` & `OracleQueryResult` definition site | Define once in `src/rs/vertebra-oracle/src/types.rs` (`ch03:t01`) and wire into `client.rs` & `_oracle.pyi` (`ch03:t02`) | Duplicate struct definitions across `client.rs` and `types.rs` | Single canonical PyO3 type definition prevents duplicate `#[pyclass]` symbol collisions and keeps `mypy.stubtest vertebra._oracle` green at every task commit. |
| `VectorView` construction from `vertebra-oracle` | Decode `0xDB 0x01` frames via `vertebra_db::oracle::decode_oracle_vector_*` and construct `vertebra._vertebra.VectorView` via `py.import("vertebra._vertebra")` / `OracleCore.decode_vector` | Linking `vertebra-py` as a Rust Cargo dependency of `vertebra-oracle` | `vertebra-py` is a `cdylib` extension crate; importing `vertebra._vertebra.VectorView` through Python/PyO3 avoids duplicate symbol linkage across native wheels. |
| Arrow 59 to `ArrowCursor` handoff | `batches_to_arrow_ipc` -> `(Vec<u8>, usize)` -> `construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` | Returning `arrow 59` `RecordBatch` across crate boundaries into `vertebra-db` | Keeps `arrow 59` strictly private to `vertebra-oracle` while reusing `vertebra-db`'s 16-byte `VTBARROW` `ShmArrowStreamWriter` and `ArrowCursor` lifecycle. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (all user documentation and OKF v0.2 knowledge updates in `docs/usage/databases.md`, `README.md`, `AGENTS.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`, and `.agents/bundles/product/tech-stack.md` are consolidated in [ch07-conformance-and-okf-synthesis](../ch07-conformance-and-okf-synthesis/spec.md) tasks `ch07:t03` and `ch07:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement ID | Requirement Summary | Owning Task(s) | Verification Artifact |
| :--- | :--- | :--- | :--- |
| `R5` | Scalar (`NUMBER`, `VARCHAR2`, `RAW`, `DATE`/`TIMESTAMP`/`TSTZ`, `BOOLEAN`), inline LOB, `OSON`, and `VECTOR` (`0xDB 0x01` `FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) codecs plus `OracleColumn` / `OracleQueryResult` | [t01](tasks/t01-scalar-lob-oson-and-vector-codecs.md) (`native-oracle-driver:ch03:t01`) | `src/py/tests/unit/db/test_oracle_adapter.py` |
| `R6` | Positional (`:1..:n`) and named (`:name`) bind lowering, single-round-trip `DML RETURNING INTO` out-binds, `execute_many` batching, and `enable_lowercase_column_names` (`True` default) normalization | [t02](tasks/t02-binds-returning-and-lowercase-columns.md) (`native-oracle-driver:ch03:t02`) | `src/py/tests/unit/db/test_oracle_adapter.py`, `mypy.stubtest vertebra._oracle` |
| `R7` | Crate-local `arrow 59` (`batches_to_arrow_ipc`) to `construct_py_arrow_cursor` (`VTBARROW` 16-byte SHM header), `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` with identifier validation and `APPEND_VALUES` direct-path hints | [t03](tasks/t03-arrow59-shm-and-load-from-arrow.md) (`native-oracle-driver:ch03:t03`) | `src/py/tests/unit/db/test_oracle_adapter.py`, `mypy.stubtest vertebra._oracle` |

## 5. Implementation Plan

- [x] native-oracle-driver:ch03:t01 — Implement OracleColumn, OracleQueryResult, and scalar, temporal, inline LOB, OSON, and VECTOR codecs across Rust and Python (tasks/t01-scalar-lob-oson-and-vector-codecs.md)
- [~] native-oracle-driver:ch03:t02 — Implement positional and named bind lowering, single-round-trip DML RETURNING out-bind capture, execute_many batching, and enable_lowercase_column_names normalization (tasks/t02-binds-returning-and-lowercase-columns.md)
- [ ] native-oracle-driver:ch03:t03 — Implement crate-local arrow 59 batches_to_arrow_ipc to construct_py_arrow_cursor SHM bridge and wire select_to_arrow, stream_arrow_batches, and load_from_arrow (tasks/t03-arrow59-shm-and-load-from-arrow.md)

**Chapter Recovery Checkpoint:**
```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q
```

## 6. Current State & Remaining Plan Snapshot

- **Completed Tasks**: None (`0/3`).
- **Active Frontier**: [t01](tasks/t01-scalar-lob-oson-and-vector-codecs.md) (`native-oracle-driver:ch03:t01`, waiting on `ch02-connect-auth-and-error-hierarchy`).
- **Remaining Tasks**: [t01](tasks/t01-scalar-lob-oson-and-vector-codecs.md), [t02](tasks/t02-binds-returning-and-lowercase-columns.md), and [t03](tasks/t03-arrow59-shm-and-load-from-arrow.md).
- **Documentation & OKF Status**: `[0/6 files documented]`, scheduled in [ch07-conformance-and-okf-synthesis](../ch07-conformance-and-okf-synthesis/spec.md) (`ch07:t03`–`t04`).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle OSON, VECTOR, TNS, and session context helpers.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
[^ora-rs-codecs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Shared Rust Oracle OSON, VECTOR, and TTC piggyback codecs.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Unregistered `VTBARROW` shared-memory Arrow IPC stream writer.
[^py-async-arrow]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — PyO3 constructor helper for `ArrowCursor` over inline IPC and `ShmCapsule`.
[^arrow-cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper over inline IPC and `VTBARROW` `ShmCapsule`.
