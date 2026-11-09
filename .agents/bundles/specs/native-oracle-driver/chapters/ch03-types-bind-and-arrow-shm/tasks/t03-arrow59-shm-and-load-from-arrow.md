---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch03-types-bind-and-arrow-shm
id: "native-oracle-driver:ch03:t03"
title: "Implement Arrow 59 to VTBARROW SHM bridge and wire select_to_arrow, stream_arrow_batches, and load_from_arrow"
description: "Implement batches_to_arrow_ipc in src/rs/vertebra-oracle/src/arrow.rs returning (Vec<u8>, usize) to construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm), and wire select_to_arrow, stream_arrow_batches, and load_from_arrow on SyncOracleDriver and AsyncOracleDriver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch03:t02"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/db/arrow.py
  - src/py/vertebra/db/adapters/oracle/driver.py
files:
  - src/rs/vertebra-oracle/src/arrow.rs
  - src/rs/vertebra-oracle/src/client.rs
  - src/py/vertebra/db/adapters/oracle/driver.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 235
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
commit_sha: null
completed_at: null
created_at: "2026-11-09T00:56:00Z"
updated_at: "2026-11-09T01:28:30Z"
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
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Unregistered VTBARROW 16-byte framed shared-memory Arrow IPC stream writer
  - id: py-async-arrow
    resource: src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor
    title: PyO3 constructor helper for ArrowCursor over inline IPC and ShmCapsule
  - id: arrow-cursor
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper exposing used_shm, buffer_view(), byte_len, and __len__
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver and AsyncOracleDriver adapter surface
tags:
  - oracle
  - arrow
  - shm
  - vtbarrow
  - direct-path
---

# Task `native-oracle-driver:ch03:t03`: Implement Arrow 59 to VTBARROW SHM bridge and wire select_to_arrow, stream_arrow_batches, and load_from_arrow

<!-- truth: start -->
- **Crate-Local Arrow 59 IPC Serialization (`batches_to_arrow_ipc`)**: `src/rs/vertebra-oracle/src/arrow.rs` consumes `oracledb`'s `features = ["arrow"]` (`arrow-array = "59"`, `arrow-schema = "59"`), normalizes lowercase field names when `enable_lowercase_column_names=True`, and serializes `RecordBatch` slices via crate-local `arrow-ipc = "59"` (`StreamWriter`) into `(Vec<u8>, usize)` without leaking `arrow 59` outside `vertebra-oracle` [^ora-contracts] [^arrow-shm].
- **16-Byte `VTBARROW` SHM Frame & `construct_py_arrow_cursor` Handoff**: `query_arrow` passes `(ipc_bytes, row_count, force_shm)` into `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` (`ShmArrowFrameHeader::SIZE == 16` bytes: `b"VTBARROW"` + `u64` LE `byte_len`), returning `ArrowCursor` (`cursor.used_shm`, `cursor.buffer_view()`, `len(cursor)`, `cursor.byte_len`), while `SyncOracleDriver` and `AsyncOracleDriver` expose `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` with identifier validation and `/*+ APPEND_VALUES */` direct-path hints [^ora-contracts] [^arrow-shm] [^py-async-arrow] [^arrow-cursor] [^ora-driver].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Implement `batches_to_arrow_ipc` in `src/rs/vertebra-oracle/src/arrow.rs`, wire `query_arrow` in `src/rs/vertebra-oracle/src/client.rs` to `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)`, and implement `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `SyncOracleDriver` and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-contracts] [^arrow-shm] [^py-async-arrow] [^ora-driver].

- **In Scope**:
  - `src/rs/vertebra-oracle/src/arrow.rs`: `batches_to_arrow_ipc(batches: &[RecordBatch], lowercase_column_names: bool) -> Result<(Vec<u8>, usize), OracleError>` using crate-local `arrow-array = "59"`, `arrow-schema = "59"`, and `arrow-ipc = "59"` (`StreamWriter<Vec<u8>>`), plus `rows_to_arrow_ipc` fallback for synthetic/row-based test batches.
  - `SyncOracleClient.query_arrow` and `AsyncOracleClient.query_arrow` in `src/rs/vertebra-oracle/src/client.rs` calling `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` to return `vertebra.db.arrow.ArrowCursor` [^py-async-arrow] [^arrow-cursor].
  - `SyncOracleDriver` and `AsyncOracleDriver` methods `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` (with `validate_identifier` for `table_name` and `columns`, and `/*+ APPEND_VALUES */` hint emission when `direct_path=True` or `driver_features.enable_direct_path_load=True`) in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-driver].
- **Out of Scope**:
  - Physical connection pooling, DRCP purity/tagging, and TTC piggyback (`ch04:t01`–`t03`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | Arrow 59 zero-copy SHM bridge contract [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md` | Chapter 03 Arrow 59 and `load_from_arrow` specifications |
| `src/rs/vertebra-db/src/arrow_shm.rs` | `ShmArrowStreamWriter`, `ShmArrowFrameHeader::SIZE == 16` (`b"VTBARROW"` + `u64` LE `byte_len`), and `ARROW_SHM_THRESHOLD_ROWS` [^arrow-shm] |
| `src/rs/vertebra-db/src/py_async.rs` | `construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` PyO3 helper [^py-async-arrow] |
| `src/py/vertebra/db/arrow.py` | `ArrowCursor` public interface (`used_shm`, `buffer_view()`, `byte_len`, `__len__`, `close`) [^arrow-cursor] |
| `src/py/vertebra/db/adapters/oracle/driver.py` | `SyncOracleDriver` and `AsyncOracleDriver` Arrow and bulk load methods [^ora-driver] |

## 3. Symbol & Signature Contracts

### 3.1 `src/rs/vertebra-oracle/src/arrow.rs` & `src/rs/vertebra-oracle/src/client.rs`

```rust
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use pyo3::prelude::*;
use crate::errors::OracleError;
use crate::types::{OracleCellValue, OracleColumn};

pub fn normalize_arrow59_schema(
    schema: &Arc<Schema>,
    lowercase_column_names: bool,
) -> Arc<Schema>;

pub fn batches_to_arrow_ipc(
    batches: &[RecordBatch],
    lowercase_column_names: bool,
) -> Result<(Vec<u8>, usize), OracleError>;

pub fn rows_to_arrow_ipc(
    columns: &[OracleColumn],
    rows: &[Vec<OracleCellValue>],
    lowercase_column_names: bool,
) -> Result<(Vec<u8>, usize), OracleError>;
```

Inside `src/rs/vertebra-oracle/src/client.rs`, `SyncOracleClient::query_arrow` and `AsyncOracleClient::query_arrow` (whose signatures were declared in `_oracle.pyi` in `ch03:t02`) invoke `batches_to_arrow_ipc` on the worker thread and construct the Python `ArrowCursor` via:

```rust
vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)
```

### 3.2 `src/py/vertebra/db/adapters/oracle/driver.py`

```python
class SyncOracleDriver(SyncDatabaseDriver[OracleConfig]):
    def select_to_arrow(
        self,
        query: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
    ) -> ArrowCursor: ...

    def stream_arrow_batches(
        self,
        query: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
    ) -> Iterator[ArrowCursor]: ...

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
    ) -> int: ...

class AsyncOracleDriver(AsyncDatabaseDriver[OracleConfig]):
    async def select_to_arrow(
        self,
        query: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
    ) -> ArrowCursor: ...

    async def stream_arrow_batches(
        self,
        query: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
    ) -> AsyncIterator[ArrowCursor]: ...

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
    ) -> int: ...
```

## 4. Edge Cases & Error Semantics

- **Inline vs 16-Byte `VTBARROW` SHM Escalation**: `construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` keeps small payloads (`row_count < ARROW_SHM_THRESHOLD_ROWS` and `force_shm != Some(true)`) inline (`cursor.used_shm is False`), and writes payloads with `row_count >= ARROW_SHM_THRESHOLD_ROWS` or `force_shm == Some(true)` into an unregistered POSIX SHM segment framed by the 16-byte `ShmArrowFrameHeader` (`ShmArrowFrameHeader::SIZE == 16`: `b"VTBARROW"` + `u64` LE `byte_len`) via `ShmArrowStreamWriter::finish_unregistered` (`cursor.used_shm is True`, `cursor.buffer_view() -> memoryview`, `len(cursor) == row_count`, `cursor.byte_len == len(ipc_bytes)`) [^arrow-shm] [^py-async-arrow] [^arrow-cursor].
- **Identifier Validation on `load_from_arrow`**: Malicious or malformed `table_name` (e.g., `"users; DROP TABLE x"`) or column identifiers raise `ProgrammingError` / `ValueError` via `validate_identifier(table_name, "table_name", allow_dots=True)` and `validate_identifier(col, "column_name")` before building SQL.
- **Direct-Path `APPEND_VALUES` Hint Precedence**: Explicit `direct_path=True` emits `INSERT /*+ APPEND_VALUES */ INTO <table> (<cols>) VALUES (:1, ...)`; explicit `direct_path=False` omits the hint even if `config.driver_features.enable_direct_path_load` is `True`; `direct_path=None` falls back to `config.driver_features.enable_direct_path_load`.
- **Empty Arrow Payload in `load_from_arrow`**: Passing an empty `ArrowCursor`, empty list, or 0-row Arrow table returns `0` without executing SQL.

## 5. Public-Seam Test Blueprint

Add behavioral Arrow and bulk-load tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_oracle_arrow59_inline_and_shm_escalation` | Query returning 5 rows with `force_shm=False` (inline) and `force_shm=True` (SHM) and uppercase columns `("ID", "NAME")` | Call `SyncOracleDriver.select_to_arrow` and `AsyncOracleDriver.select_to_arrow` inside `with cursor:` | Inline cursor has `cursor.used_shm is False`, `len(cursor) == 5`; SHM cursor has `cursor.used_shm is True`, `bytes(cursor.buffer_view())` decodes as valid Arrow IPC stream with lowercase field names `["id", "name"]`, and unmaps cleanly on `close()` |
| `test_oracle_stream_arrow_batches_sync_and_async` | 5 rows streamed with `batch_size=2` | Iterate `SyncOracleDriver.stream_arrow_batches` and `AsyncOracleDriver.stream_arrow_batches` | Yields 3 `ArrowCursor` batches with `len(cursor)` equal to `2`, `2`, `1` and matching IPC stream payloads via `cursor.buffer_view()` |
| `test_oracle_load_from_arrow_direct_path_and_identifier_validation` | `ArrowCursor` / columnar dict `{"id": [1, 2], "name": ["a", "b"]}` with `direct_path=True`, `direct_path=False`, and invalid `table_name="bad;table"` | Call `SyncOracleDriver.load_from_arrow` and `AsyncOracleDriver.load_from_arrow` | `direct_path=True` executes `INSERT /*+ APPEND_VALUES */ INTO ... (id, name) VALUES (:1, :2)` with `[(1, "a"), (2, "b")]`; `direct_path=False` omits hint; invalid table or column name raises `ValueError` / `ProgrammingError` |

## 6. Verification

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "arrow or load_from_arrow" -q
```

## 7. Acceptance Criteria

- [ ] `src/rs/vertebra-oracle/src/arrow.rs` implements `batches_to_arrow_ipc(batches: &[RecordBatch], lowercase_column_names: bool) -> Result<(Vec<u8>, usize), OracleError>` without leaking `arrow 59` outside `vertebra-oracle`.
- [ ] `SyncOracleClient.query_arrow` and `AsyncOracleClient.query_arrow` pass `(ipc_bytes, row_count, force_shm)` into `construct_py_arrow_cursor` and return `ArrowCursor` (`cursor.used_shm`, `cursor.buffer_view()`, `len(cursor)`).
- [ ] `SyncOracleDriver` and `AsyncOracleDriver` (`AsyncDatabaseDriver[OracleConfig]`) implement `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` with identifier validation and `APPEND_VALUES` direct-path support.
- [ ] `uv run --no-sync python -m mypy.stubtest vertebra._oracle` and all unit tests in `src/py/tests/unit/db/test_oracle_adapter.py` pass.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Unregistered `VTBARROW` 16-byte framed shared-memory Arrow IPC stream writer.
[^py-async-arrow]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — PyO3 constructor helper for `ArrowCursor` over inline IPC and `ShmCapsule`.
[^arrow-cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper exposing `used_shm`, `buffer_view()`, `byte_len`, and `__len__`.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
