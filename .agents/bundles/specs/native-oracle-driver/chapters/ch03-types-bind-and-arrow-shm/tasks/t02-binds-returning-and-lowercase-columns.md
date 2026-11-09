---
type: Task
parent_flow: native-oracle-driver
chapter_id: ch03-types-bind-and-arrow-shm
id: "native-oracle-driver:ch03:t02"
title: "Implement positional and named bind lowering, DML RETURNING out-binds, execute_many, and lowercase column normalization"
description: "Wire OracleColumn and OracleQueryResult into SyncOracleClient and AsyncOracleClient (execute, execute_many, query, query_one, query_arrow) and _oracle.pyi, implement positional (:1..:n) and named (:name) bind lowering, single-round-trip DML RETURNING INTO out-bind capture, and enable_lowercase_column_names (True default) normalization."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch03:t01"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/driver/_common.py
files:
  - src/rs/vertebra-oracle/src/client.rs
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 235
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
commit_sha: 95be1327
completed_at: "2026-11-09T15:36:30Z"
created_at: "2026-11-09T00:56:00Z"
updated_at: "2026-11-09T15:36:30Z"
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
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver and AsyncOracleDriver adapter surface
  - id: ora-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle OSON, VECTOR, and column/bind coercion helpers
  - id: driver-common
    resource: src/py/vertebra/db/driver/_common.py::_hydrate_rows_with_schema
    title: Row hydration and schema projection helpers
tags:
  - oracle
  - binds
  - returning
  - casing
  - driver
---

# Task `native-oracle-driver:ch03:t02`: Implement positional and named bind lowering, DML RETURNING out-binds, execute_many, and lowercase column normalization

<!-- truth: start -->
- **Positional & Named Bind Lowering + Client Result Wiring**: `SyncOracleClient`, `AsyncOracleClient`, `SyncOracleDriver`, and `AsyncOracleDriver` wire `crate::types::{OracleColumn, OracleQueryResult}` into `execute`, `execute_many`, `query`, `query_one`, and `query_arrow`, accepting positional sequences (`:1..:n`) and named mappings (`:name`) [^ora-contracts] [^ora-driver].
- **Single-Round-Trip `DML RETURNING INTO` & Lowercase Column Normalization**: `DML ... RETURNING ... INTO` captures out-binds into `OracleQueryResult.out_binds` and row projections (`execute_tuples`, `select`, `exec_returning_async`) in one network round-trip, and `enable_lowercase_column_names=True` (`True` default) normalizes unquoted uppercase column identifiers to lowercase across `OracleColumn.name`, `OracleQueryResult.to_dicts()`, `execute_with_description()`, and `select()` row dicts/hydrators [^ora-contracts] [^ora-driver] [^driver-common].
<!-- truth: end -->

## 1. Objective & Scope Boundary

Wire `OracleColumn` and `OracleQueryResult` from `crate::types` into `src/rs/vertebra-oracle/src/client.rs` and `src/py/vertebra/_oracle.pyi`, and implement positional (`:1..:n`) and named (`:name`) bind lowering, single-round-trip `DML ... RETURNING ... INTO` out-bind capture (`OracleQueryResult.out_binds`), `execute_many` batching, and `enable_lowercase_column_names` (`True` default) normalization across `src/rs/vertebra-oracle/src/client.rs`, `src/py/vertebra/db/adapters/oracle/driver.py`, and `src/py/vertebra/_oracle.pyi` [^ora-contracts] [^ora-driver].

- **In Scope**:
  - `SyncOracleClient` and `AsyncOracleClient` methods `query`, `query_one`, `execute`, `execute_many`, and `query_arrow` (with `query_arrow` signature declared on `client.rs` and `_oracle.pyi` in `ch03:t02` and its `batches_to_arrow_ipc` body completed in `ch03:t03` so `mypy.stubtest vertebra._oracle` stays green at both `ch03:t02` and `ch03:t03` while keeping `len(files) == 3`).
  - Positional (`Sequence[Any]`) and named (`Mapping[str, Any]`) bind parameter extraction (`from collections.abc import Mapping, Sequence`), including `OracleCore.coerce_bind_parameter` (`OSON`, `VECTOR`, `UUID`) and `DML ... RETURNING <cols> INTO :out_1, ...` out-bind registration and capture into `OracleQueryResult.out_binds` and `OracleQueryResult.rows` (`exec_returning_async` / sync returning) [^ora-contracts] [^ora-core].
  - `enable_lowercase_column_names` (`True` default) normalization on `OracleColumn.name`, `execute_with_description()`, and `select()` / `select_one()` / `select_one_or_none()` / `select_value()` row dictionaries and `schema_type` hydrators (`_hydrate_rows_with_schema`) in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-driver] [^driver-common].
  - Type stub declarations for `OracleColumn`, `OracleQueryResult`, `SyncOracleClient`, and `AsyncOracleClient` (`query`, `query_one`, `execute`, `execute_many`, `query_arrow`) in `src/py/vertebra/_oracle.pyi`.
- **Out of Scope**:
  - Arrow 59 `batches_to_arrow_ipc` implementation in `src/rs/vertebra-oracle/src/arrow.rs` and `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow` driver wiring (`ch03:t03`).
  - Physical connection pooling, DRCP purity/tagging, and TTC piggyback (`ch04:t01`–`t03`).

## 2. Bounded Read Manifest

| File Path | Purpose |
| :--- | :--- |
| `.agents/bundles/specs/native-oracle-driver/contracts.md` | Bind lowering, `DML RETURNING INTO`, and `enable_lowercase_column_names` contracts [^ora-contracts] |
| `.agents/bundles/specs/native-oracle-driver/chapters/ch03-types-bind-and-arrow-shm/spec.md` | Chapter 03 architecture and task contracts |
| `src/py/vertebra/db/adapters/oracle/driver.py` | Existing `SyncOracleDriver` and `AsyncOracleDriver` methods [^ora-driver] |
| `src/py/vertebra/db/adapters/oracle/core.py` | `OracleCore` bind and cell coercion helpers [^ora-core] |
| `src/py/vertebra/db/driver/_common.py` | `_hydrate_rows_with_schema` and cardinality helpers [^driver-common] |

## 3. Symbol & Signature Contracts

### 3.1 `src/rs/vertebra-oracle/src/client.rs` & `src/py/vertebra/_oracle.pyi`

```python
from collections.abc import Mapping, Sequence
from typing import Any, final
from vertebra.db.arrow import ArrowCursor

@final
class OracleColumn:
    @property
    def name(self) -> str: ...
    @property
    def type_code(self) -> int: ...
    @property
    def type_name(self) -> str: ...
    @property
    def oracle_type(self) -> str: ...
    @property
    def db_size(self) -> int | None: ...
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...
    @property
    def nullable(self) -> bool: ...
    def __init__(
        self,
        name: str,
        type_name: str,
        type_code: int = 0,
        db_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        nullable: bool = True,
    ) -> None: ...

@final
class OracleQueryResult:
    @property
    def columns(self) -> tuple[OracleColumn, ...]: ...
    @property
    def rows(self) -> list[tuple[Any, ...]]: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def out_binds(self) -> tuple[Any, ...] | dict[str, Any] | None: ...
    @property
    def lastrowid(self) -> str | None: ...
    def __init__(
        self,
        columns: Sequence[OracleColumn],
        rows: Sequence[tuple[Any, ...]],
        rowcount: int = 0,
        out_binds: tuple[Any, ...] | dict[str, Any] | None = None,
        lastrowid: str | None = None,
    ) -> None: ...
    def to_dicts(self) -> list[dict[str, Any]]: ...
    def __len__(self) -> int: ...
    def is_empty(self) -> bool: ...

class SyncOracleClient:
    def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> OracleQueryResult: ...
    def query_one(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> tuple[Any, ...] | None: ...
    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> OracleQueryResult: ...
    def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int: ...
    def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> ArrowCursor: ...

class AsyncOracleClient:
    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> OracleQueryResult: ...
    async def query_one(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> tuple[Any, ...] | None: ...
    async def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        out_binds: Sequence[str] | Mapping[str, str] | None = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> OracleQueryResult: ...
    async def execute_many(
        self,
        sql: str,
        params_seq: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int: ...
    async def query_arrow(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        force_shm: bool | None = None,
        enable_lowercase_column_names: bool | None = None,
    ) -> ArrowCursor: ...
```

### 3.2 `src/py/vertebra/db/adapters/oracle/driver.py`

Update `SyncOracleDriver` and `AsyncOracleDriver` (`AsyncDatabaseDriver[OracleConfig]` backed by `AsyncOracleClient` / `_AsyncClientProtocol`) using top-level `from collections.abc import Mapping, Sequence` so:
- `execute(sql, params=None) -> int` returns `result.rowcount` (or integer status from raw connection).
- `execute_many(sql, params_seq) -> int` delegates to `client.execute_many(sql, coerced_seq)`.
- `execute_with_description(sql, params=None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` normalizes column names to lowercase when `config.driver_features.enable_lowercase_column_names` is `True` and post-processes cells via `OracleCore.coerce_row_cell`.
- `execute_tuples(sql, params=None) -> list[tuple[Any, ...]]`, `select(sql, params=None, *, schema_type=None)`, and `exec_returning_async` return projected rows from both `SELECT` queries and `DML ... RETURNING ... INTO` statements (hydrating from `OracleQueryResult.rows` or `OracleQueryResult.out_binds` via `_hydrate_rows_with_schema` when `rows` is empty and `RETURNING` out-binds are present) [^ora-driver] [^driver-common].

## 4. Edge Cases & Error Semantics

- **Leading Colon Normalization on Mapping Keys**: Named parameter dicts passed as `{"id": 10}` or `{":id": 10}` both bind cleanly to `:id` without requiring the caller to strip or prepend `:`.
- **Single-Round-Trip `DML ... RETURNING ... INTO`**: When a statement contains `RETURNING col1, col2 INTO :out_1, :out_2` (or named `:ret_id`), the worker registers output binds in the same execution call, populates `OracleQueryResult.out_binds` (`(val1, val2)` or `{"ret_id": val1}`), and synthesizes a result row/column projection when `rows` is empty so `driver.select_one("INSERT INTO users (name) VALUES (:name) RETURNING id, created_at INTO :out_id, :out_ts", {"name": "Ada"})` returns `{"id": ..., "created_at": ...}` (with lowercase column keys when `enable_lowercase_column_names=True`) in one round-trip.
- **Empty `execute_many` Sequence**: Calling `execute_many(sql, [])` returns `0` immediately without dispatching a worker command or network round-trip.
- **Quoted vs Unquoted Column Casing**: When `enable_lowercase_column_names=True`, `OracleColumn.name` and driver column descriptors normalize uppercase column names to lowercase (`"USER_ID" -> "user_id"`). When `enable_lowercase_column_names=False`, `"USER_ID"` is preserved verbatim.

## 5. Public-Seam Test Blueprint

Add behavioral unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:

| Test Function | Input / Setup | Action | Expected Observable Assertion |
| :--- | :--- | :--- | :--- |
| `test_oracle_positional_and_named_bind_lowering` | Parametrized positional `(42, "Ada")` (`:1, :2`) and named `{"id": 42, ":name": "Ada"}` (`:id, :name`) with `UUID`, `OSON` dict, and `VectorView` values | Call `SyncOracleDriver.select` and `AsyncOracleDriver.select` | Binds are coerced (`UUID` -> `bytes`, `dict` -> OSON bytes when configured) and leading `:` in dict keys is normalized |
| `test_oracle_dml_returning_into_single_round_trip` | `INSERT INTO items (name) VALUES (:name) RETURNING id, name INTO :out_id, :out_name` | Execute via `SyncOracleDriver.select_one` / `execute_tuples` and `AsyncOracleDriver.select_one` (`exec_returning_async`) | Single client execution call captures `out_binds`, returns `{"id": 101, "name": "widget"}` (and `[(101, "widget")]` on `execute_tuples`) |
| `test_oracle_execute_many_batches_and_empty_short_circuit` | `INSERT INTO items (id, name) VALUES (:1, :2)` with `[(1, "a"), (2, "b"), (3, "c")]` and `[]` | Call `SyncOracleDriver.execute_many` and `AsyncOracleDriver.execute_many` | Non-empty batch dispatches once and returns `3`; empty `[]` returns `0` with zero client calls |
| `test_oracle_lowercase_column_names_toggle` | Query returning columns `("EMPLOYEE_ID", "FIRST_NAME")` with `enable_lowercase_column_names=True` vs `False` | Call `execute_with_description()` and `select(..., schema_type=EmployeeStruct)` | When `True`, description is `("employee_id", "first_name")` and `select()` returns `{"employee_id": 1, "first_name": "Ada"}` / hydrated `EmployeeStruct`; when `False`, preserves `("EMPLOYEE_ID", "FIRST_NAME")` |

## 6. Verification

```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "bind or returning or execute_many or lowercase" -q
```

## 7. Acceptance Criteria

- [x] `SyncOracleClient` and `AsyncOracleClient` expose `query`, `query_one`, `execute`, `execute_many`, and `query_arrow` accepting both positional sequences and named mappings (`from collections.abc import Mapping, Sequence`).
- [x] `DML ... RETURNING ... INTO` populates `OracleQueryResult.out_binds` and projects rows into `execute_tuples`, `select*`, and `exec_returning_async` in a single round-trip.
- [x] `enable_lowercase_column_names=True` (default) normalizes column names to lowercase across `OracleColumn.name`, `OracleQueryResult.to_dicts()`, `execute_with_description()`, and `select()` dict/struct hydration.
- [x] `uv run --no-sync python -m mypy.stubtest vertebra._oracle` and `ruff` / `mypy` checks pass with zero allowlist overrides.

## 8. Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public interface, FFI, type, pooling, tenancy, and ownership contracts.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
[^ora-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle OSON, VECTOR, and column/bind coercion helpers.
[^driver-common]: `src/py/vertebra/db/driver/_common.py::_hydrate_rows_with_schema` — Row hydration and schema projection helpers.
