---
type: Spec
flow_id: db-layout-and-contract
title: Adapter Package Layout, GCP Wheel Extraction, Driver Contract, and Conformance Suite
description: Relocate engine packages under vertebra/db/adapters/<engine> and vertebra-db/src/<engine>, extract standalone vertebra-bigquery and vertebra-spanner PyO3 wheels, unify the Sync/Async Python driver contract and typed DatabaseError hierarchy, define Sync/Async engine capability protocols, make DatabaseRegistry polymorphic, enforce the vertebra/db upward-import boundary, and establish the cross-engine conformance suite.
state: active
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/gap-analysis_20261103.md
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 5
plan_commit: "2c0d365"
state_revision: 10
current_task: null
last_verified_checkpoint: "a5dbea6"
created_at: 2026-11-03T04:05:00Z
updated_at: "2026-11-04T21:17:00Z"
tags: [db, layout, wheels, contract, registry, conformance]
---

# Flow: Adapter Package Layout, GCP Wheel Extraction, Driver Contract, and Conformance Suite

## 1. Specification & Context

### 1.1 Problem Statement & Objectives

`vertebra.db` mixes engine-agnostic abstractions (`driver/`, `_registry.py`, `_arrow.py`, `cdc.py`, `rls.py`, `dialer/`) with eleven engine implementations (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion.py`) at the same package level and eagerly imports every engine in `vertebra/db/__init__.py`. On the Rust side, `vertebra-db` and `vertebra-py` (`vertebra._vertebra`) unconditionally compile `google-cloud-bigquery`, `google-cloud-spanner`, `tonic`, and gRPC transport dependencies directly into `vertebra-core` instead of isolating each database engine in its own optional PyO3 extension wheel (`src/rs/vertebra-<engine>` -> `vertebra._<engine>` + `vertebra-core[<engine>]`). Across the eight SQL and analytical drivers, method signatures, parameter names, and return types diverge behind `# type: ignore[override]` annotations:

- `postgres`, `mysql`, and `sqlite` accept variadic `*params: Any` instead of `parameters: Sequence[Any] | Mapping[str, Any] | None = None`, return `list[tuple[Any, ...]]` from `execute()`, and return 7-element DB-API column descriptors from `execute_with_description()`; `postgres` and `mysql` also return `pyarrow.Table` from `select_to_arrow()` instead of `ArrowCursor`, and `postgres` diverges on `stream_arrow_batches`.
- `oracle`, `mssql`, and `duckdb` name their second parameter `params` instead of `parameters`, name `select_to_arrow`'s threshold keyword `shm_threshold_rows` instead of `threshold_rows` (along with `sqlite`), and return a 3-tuple `(rows, description, rowcount)` from `execute_with_description()`.
- `bigquery` and `spanner` (including `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `SyncSpannerSnapshot`, and `AsyncSpannerSnapshot`) return `None` from `select_one()` on empty results instead of raising an exception, and `_sync.py`, `bigquery`, and `spanner` conflate 0-row results with 1-row `SQL NULL` scalars in `select_value()`.
- `DataFusionEngine.execute_dicts()` silently falls back to `sqlite3.connect(":memory:")` when `datafusion` is absent or raises an error.
- `DatabaseRegistry` dispatches URL schemes, pool fingerprints, and driver construction through monolithic `isinstance` chains and exposes GCP-only `provide_bigquery_driver` / `provide_spanner_driver` helpers.
- `vertebra-db` places engine wire codecs in flat files with glob `pub use *` re-exports in `lib.rs` and lacks a shared Rust transport/TLS/dialer connection core (`connection.rs`), shared per-extension PyO3 async completion helper (`py_async.rs`), cross-`cdylib` zero-copy `ShmCapsule` handoff for `ArrowCursor`, and unified Python `DatabaseError` hierarchy.

This flow establishes the structural, packaging, and behavioral foundation that every native Rust driver (`native-postgres-driver`, `native-oracle-driver`, `native-mysql-mssql-drivers`, `native-embedded-and-cache-drivers`), `broker-pool-sharing`, and `db-engine-primitives` builds on.

**Explicit Non-Goals:**
- Implementing native Rust network I/O wheels for PostgreSQL, Oracle, MySQL, MSSQL, SQLite, DuckDB, or Redis/Valkey (owned by `native-data-platform` Chapters 2, 4, 5, and 6).
- Relocating `SyncSpannerChangeStreamSource` / `AsyncSpannerChangeStreamSource`, PostgreSQL `NOTIFY` spill helpers, `DatabaseChannelBackend`, `SyncQueryPipeline` / `AsyncQueryPipeline`, `db/mcp_tools.py`, or SQL embedding pushdown classes (owned by `integration-remediation/db-engine-primitives`, immediately following this flow).
- Cross-worker SHM broker RPC pooling (owned by `native-data-platform/broker-pool-sharing`).

### 1.2 Upstream Execution Baseline

- **Branch baseline**: `main @ f741701` (incorporating completed flows `critical-correctness-fixes`, `gcp-wire-protocol-correctness`, `gcp-driver-resilience`, and the codebase-wide `Sync*` / `Async*` class split).
- **Parent PRD**: [`native-data-platform`](../native-data-platform/spec.md) (Chapter 1), co-consumed by [`integration-remediation`](../integration-remediation/spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Python DB package & adapters | `src/py/vertebra/db/__init__.py`, `src/py/vertebra/db/{_arrow,_registry,datafusion}.py`, `src/py/vertebra/db/{postgres,mysql,sqlite,oracle,mssql,duckdb,bigquery,spanner,redis,valkey}/` | **In-process / Local-substitutable**: Moves engines under `vertebra.db.adapters.<engine>`, renames `arrow.py` and `registry.py`, switches `vertebra.db` to `lazy_exports` over engine-agnostic symbols (`SyncCloudSQLDialer`, `AsyncCloudSQLDialer`, `SyncAlloyDBDialer`, `AsyncAlloyDBDialer`, etc.), and updates OKF `sources:` and doc import fences in lockstep. |
| Rust `vertebra-db` shared core & FFI errors | `src/rs/vertebra-core/src/shm.rs::ShmRegion`, `src/rs/vertebra-db/Cargo.toml`, `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-db/src/{gcp,dialer,tns,arrow_shm}.rs`, `src/rs/vertebra-py/src/{completion,db,errors}.rs` | **Remote-owned port / In-process**: Organizes pure-CPU codecs under `vertebra-db/src/<engine>/mod.rs` (`oracle/tns.rs`), adds `connection.rs` (`Connection`, `TransportStream`, `CancelHandle`, `EndpointTarget`, `install_rustls_ring_provider`), optional `py_async.rs` (`#[cfg(feature = "pyo3")]` per-extension `spawn_rust_db_task_with_attr`), `ShmCapsule` zero-copy `ArrowCursor` handoff across `.so` boundaries, and `DatabaseError` + `GoogleCloudError` multi-base registration on `vertebra._vertebra`. |
| Per-driver GCP PyO3 wheels | `src/rs/vertebra-bigquery/{Cargo,pyproject}.toml`, `src/rs/vertebra-spanner/{Cargo,pyproject}.toml`, `src/py/vertebra/_{bigquery,spanner}.pyi`, `pyproject.toml`, `Makefile` | **Extension packaging seam**: Extracts `SyncBigQueryClient` / `AsyncBigQueryClient` into `vertebra._bigquery` (`[bigquery]` extra) and `SyncSpannerClient` / `AsyncSpannerClient` into `vertebra._spanner` (`[spanner]` extra, plus `[storage]` extra for `vertebra-storage`), removing `google-cloud-*` and `tonic` from `vertebra-py` and default `vertebra-db`. |
| Driver base contract | `src/py/vertebra/db/driver/{_common,_sync,_async}.py`, `src/py/vertebra/extensions/sqlalchemy/dialects.py::{SyncDBAPIConnection,AsyncDBAPICursor}`, `src/py/vertebra/extensions/sqlspec/{bigquery,spanner}.py` | **Local-substitutable**: Aligns sync/async signatures (`parameters`, `execute -> int`, `execute_with_description -> (rows, col_names)`, `select_one -> raises NotFoundError`, `select_value -> NotFoundError on 0 rows / None on SQL NULL`, `select_to_arrow(..., threshold_rows=...) -> ArrowCursor`, `stream_arrow_batches`, `transaction()`, `cancel()`, `close()`) with zero `# type: ignore[override]`. |
| Engine capability protocols | `src/py/vertebra/db/driver/capabilities.py` | **Ports & Adapters**: Runtime-checkable `SyncPipelineCapable` (`SyncPipeline`), `AsyncPipelineCapable` (`AsyncPipeline`), `NotificationSource` (`NotificationEvent`), `SyncChangeSource`, `AsyncChangeSource`, `QueueStore` (`ClaimedJob`), and `EmbeddingPushdown` protocols plus frozen `msgspec.Struct` records. |
| Polymorphic registry & DI | `src/py/vertebra/db/registry.py::DatabaseRegistry`, `src/py/vertebra/db/adapters/*/__init__.py`, `src/py/vertebra/extensions/{litestar,fastapi,starlette,flask}/__init__.py` | **In-process**: Converts `src/py/vertebra/db/adapters/<engine>/__init__.py` barrels to `lazy_exports` so `config.py` imports never load `driver.py`, and provides table-driven `parse_url`, `pool_fingerprint`, `@overload`ed `get_dialer(config, *, sync=False)`, lazy driver builders via `BaseDatabaseConfig.{url_schemes,driver_module,sync_driver_name,async_driver_name,from_url,pool_key,build_sync_driver,build_async_driver}`, and `provide_driver`. |
| Cross-engine conformance | `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/conftest.py` | **Local-substitutable & External DB boundary**: Parameterized `pytest-databases` conformance suite running `sqlite` and `datafusion`/`redis`/`valkey` config contracts natively now and marking pending native SQL engines `xfail(strict=True)`. |

### 1.4 Relevant Patterns, Domain Terms & Knowledge Chapters

- [Product & Engineering Guidelines](../../product/product-guidelines.md) — Clean-break naming, leading `Sync*` / `Async*` class splits, zero backward-compatibility shims, `msgspec.Struct(..., frozen=True, gc=False)`.
- [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md) — Per-engine `config.py`/`core.py`/`driver.py` package pattern, `Spangres` naming, and `SyncCloudSQLDialer` / `AsyncCloudSQLDialer` / `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`.
- [Broker & SHM Leases](../../knowledge/domains/data/broker-and-shm-leases.md) — Registry pool fingerprinting (`BaseDatabaseConfig.pool_key()`) and SHM lease coordination.
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md) — PEP 249 `SyncDBAPIConnection` / `AsyncDBAPIConnection` and shared `DatabaseRegistry` pooling.
- [PyO3 0.30.1 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md) — Exception registration on `vertebra._vertebra`, `#[pymodule(gil_used = false)]`, and `mypy.stubtest` parity.
- [Python Module Structure, Class Layout & PEP 695 Typing](../../knowledge/patterns/python/module-and-class-layout.md) — Module layout, PEP 695 generics, `lazy_exports`, and zero inline `#` comments.
- Domain skills: [`runtime-ipc`](../../../skills/runtime-ipc/SKILL.md), [`pyo3-bridge`](../../../skills/pyo3-bridge/SKILL.md), [`python-framework`](../../../skills/python-framework/SKILL.md).

## 2. Requirements & Architecture

### 2.1 Functional Requirements

- **R1 (Python Adapter Package Layout & Clean Break)**: Move `src/py/vertebra/db/{postgres,mysql,sqlite,oracle,mssql,duckdb,bigquery,spanner,redis,valkey}/` to `src/py/vertebra/db/adapters/<engine>/`, split `src/py/vertebra/db/datafusion.py` into `src/py/vertebra/db/adapters/datafusion/{__init__,config,driver}.py`, rename `_registry.py` to `registry.py` and `_arrow.py` to `arrow.py`, switch `vertebra/db/__init__.py` to `lazy_exports` over engine-agnostic names only, update relocated paths in OKF `sources:` and `docs/` Python code fences in the same commit, and leave zero compatibility shims at the old `vertebra.db.<engine>` paths.
- **R2 (DataFusion Fail-Closed Execution)**: Remove `sqlite3` and `_FallbackArrowTable` from `DataFusionEngine` (`src/py/vertebra/db/adapters/datafusion/driver.py`, adding optional `ctx: Any = None` constructor injection and `_coerce_arrow_table` PyArrow normalization) so `execute_dicts` and `execute_arrow` require `datafusion` via `ensure_module("datafusion", package="datafusion")` when `self._ctx is None` and propagate DataFusion registration and execution errors unchanged.
- **R3 (Rust `vertebra-db` Shared Core, `py_async.rs` & `ShmCapsule`)**: Reorganize pure-CPU codecs under `src/rs/vertebra-db/src/<engine>/mod.rs` (`postgres`, `mysql`, `sqlite`, `oracle` with `oracle/tns.rs`, `mssql`, `duckdb`, `resp3`), replace glob `pub use *` re-exports in `lib.rs` with explicit exports, register `src/rs/vertebra-db/**` in `.agents/skills/review-core/references/areas.toml`, implement `src/rs/vertebra-db/src/connection.rs` (`Connection`, `CancelHandle`, `TransportStream`, `ConnectOptions`, `EndpointTarget`, `TlsMode`, `TransportError`, `install_rustls_ring_provider`), add optional `pyo3` feature (`src/rs/vertebra-db/src/py_async.rs`) providing per-extension `spawn_rust_db_task_with_attr(..., attr_name: &'static str)` so separate `.so` modules never collide on event-loop drainer attributes, and add `#[repr(C)] ShmCapsule` (`vertebra_core::shm`) + `ArrowCursor._from_shm_capsule` (`vertebra._vertebra`) so per-driver `.so` crates return zero-copy `vertebra._vertebra.ArrowCursor` instances without cross-linking `cdylib` crates or leaking SHM segments.
- **R4 (Typed `DatabaseError` Exception Hierarchy)**: Register `DatabaseError(VertebraError)` and PEP 249 / transient subclasses (`InterfaceError`, `DataError`, `NotFoundError(DataError, ValueError)`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) on `vertebra._vertebra` (`src/rs/vertebra-py/src/errors.rs` + `_vertebra.pyi`) and re-export them from `vertebra.db.driver` and `vertebra.db`, with `sqlstate: str | None`, `vendor_code: int | None`, and `is_transient: bool` attributes, and multi-base inheritance so `SpannerError`, `SpannerAbortedError`, `BigQueryError`, and `BigQueryRowError` satisfy both `GoogleCloudError` and `DatabaseError`.
- **R5 (Standalone `vertebra-bigquery` & `vertebra-spanner` PyO3 Wheels & Optional Extras)**: Extract BigQuery (`SyncBigQueryClient`, `AsyncBigQueryClient`) and Spanner (`SyncSpannerClient`, `AsyncSpannerClient`) Rust clients and PyO3 bindings out of `vertebra-db` and `vertebra-py` into workspace crates `src/rs/vertebra-bigquery` (`vertebra._bigquery`, `src/py/vertebra/_bigquery.pyi`) and `src/rs/vertebra-spanner` (`vertebra._spanner`, `src/py/vertebra/_spanner.pyi`) using `#[pymodule(gil_used = false)]` and `vertebra-db` (`features = ["gcp", "pyo3"]`); gate `vertebra_db::gcp` behind optional feature `gcp` and remove `google-cloud-*` and `tonic` from `vertebra-py/Cargo.toml` and default `vertebra-db/Cargo.toml`; declare `[project.optional-dependencies]` `bigquery`, `spanner`, and `storage` + `[tool.uv.sources]` + `[[tool.bumpversion.files]]` in `pyproject.toml`; update `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`), `areas.toml`, and `docs/reference/native-stubs.md`; and load `vertebra._bigquery` / `vertebra._spanner` via `ensure_module(..., package="vertebra-core[<engine>]", extra="<engine>")` in `db/adapters/{bigquery,spanner}/driver.py`.
- **R6 (Unified Sync & Async Driver Contract)**: Standardize `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`, all eight SQL/analytical adapter drivers, and `SyncSpannerTransaction`/`AsyncSpannerTransaction`/`SyncSpannerSnapshot`/`AsyncSpannerSnapshot` on one method, parameter-name, and return-type contract with zero `# type: ignore[override]` annotations, updating `SyncDBAPIConnection` / `AsyncDBAPICursor` (`extensions/sqlalchemy/dialects.py`), `DataFusionEngine.register_{bigquery,spanner}_{table,query}` (`db/adapters/datafusion/driver.py`), and `extensions/sqlspec/{bigquery,spanner}.py` accordingly.
- **R7 (Engine Capability Protocols)**: Define `@runtime_checkable` protocols `SyncPipelineCapable` (`SyncPipeline`), `AsyncPipelineCapable` (`AsyncPipeline`), `NotificationSource` (`NotificationEvent`), `SyncChangeSource`, `AsyncChangeSource`, `QueueStore` (`ClaimedJob`), and `EmbeddingPushdown` in `src/py/vertebra/db/driver/capabilities.py` with full typed `def` / `async def` signatures and `msgspec.Struct(..., frozen=True, gc=False)` value objects.
- **R8 (Polymorphic `DatabaseRegistry` & `provide_driver`)**: Convert all eleven `src/py/vertebra/db/adapters/<engine>/__init__.py` barrels to `lazy_exports` so importing `vertebra.db.adapters.<engine>.config` never eagerly loads `driver.py`; add `url_schemes`, lazy `driver_module`/`sync_driver_name`/`async_driver_name`, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver` to `BaseDatabaseConfig` and every adapter config (preserving `"memory"` on `RedisConfig` and `"bq"` on `BigQueryConfig`, plus `url_schemes`/`from_url`/`pool_key` on `CloudSQLConfig` and `AlloyDBConfig`); replace `isinstance` / scheme `if` chains in `DatabaseRegistry` with table lookups (including `@overload`ed `get_dialer(config, *, sync=False)` returning `SyncCloudSQLDialer | SyncAlloyDBDialer` or `AsyncCloudSQLDialer | AsyncAlloyDBDialer`, and skipping shared caching when `pool_key()` is `None`); and replace `provide_bigquery_driver` / `provide_spanner_driver` with `provide_driver(name_or_config, *, sync=False, registry=None)` across `vertebra.db` and `vertebra.extensions.{litestar,fastapi,starlette,flask}`.
- **R9 (Upward-Import Gate & Cross-Engine Conformance Suite)**: Add an AST import-boundary test in `src/py/tests/unit/db/test_native_db_drivers.py` blocking `vertebra/db/**` imports from `vertebra.{agent,framework,extensions,server,embeddings}` (with sole temporary allowlist `{"vertebra.db.mcp_tools"}` removed in `db-engine-primitives:t05`), add the parameterized conformance suite `src/py/tests/integration/db/test_driver_conformance.py`, and update `docs/` and OKF data chapters.

### 2.2 Public Interfaces, Deep Modules & Seam Contracts

#### Exception Hierarchy (`vertebra._vertebra`, `vertebra.db.driver`, `vertebra.db`)

```python
class DatabaseError(VertebraError):
    sqlstate: str | None = None
    vendor_code: int | None = None
    is_transient: bool = False

class InterfaceError(DatabaseError): ...
class DataError(DatabaseError): ...
class NotFoundError(DataError, ValueError): ...
class OperationalError(DatabaseError): ...
class ConnectionFailure(OperationalError):
    is_transient: bool = True

class PoolExhaustedError(OperationalError):
    is_transient: bool = True

class QueryCancelledError(OperationalError):
    sqlstate: str | None = "57014"
    is_transient: bool = False

class SerializationFailure(OperationalError):
    sqlstate: str | None = "40001"
    is_transient: bool = True

class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...
```

`SpannerError` inherits `(GoogleCloudError, DatabaseError)`, `SpannerAbortedError` inherits `(SpannerError, SerializationFailure)`, `BigQueryError` inherits `(GoogleCloudError, DatabaseError)`, and `BigQueryRowError` inherits `(BigQueryError, DataError)`. All exception classes remain registered once on `vertebra._vertebra` so callers can catch `DatabaseError` or `GoogleCloudError` without installing `vertebra-bigquery` or `vertebra-spanner`, while `vertebra._bigquery` and `vertebra._spanner` instantiate those exact exception types when raising errors.

#### Unified Driver & Config Contract (`vertebra.db.driver`)

```python
class BaseDatabaseConfig(FrozenBaseStruct, frozen=True, gc=False):
    url_schemes: ClassVar[tuple[str, ...]] = ()
    driver_module: ClassVar[str] = ""
    sync_driver_name: ClassVar[str] = ""
    async_driver_name: ClassVar[str] = ""

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self: ...
    def pool_key(self) -> tuple[Any, ...] | None: ...
    def resolve_driver_classes(self) -> tuple[type[Any], type[Any]]: ...
    def build_sync_driver(self, registry: Any) -> Any: ...
    def build_async_driver(self, registry: Any) -> Any: ...

class DatabaseRegistry:
    @overload
    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: Literal[False] = False,
    ) -> AsyncCloudSQLDialer | AsyncAlloyDBDialer: ...
    @overload
    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: Literal[True],
    ) -> SyncCloudSQLDialer | SyncAlloyDBDialer: ...
    def get_dialer(
        self,
        config: CloudSQLConfig | AlloyDBConfig,
        *,
        sync: bool = False,
    ) -> SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer: ...

@overload
def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: Literal[False] = False,
    registry: DatabaseRegistry | None = None,
) -> Any: ...
@overload
def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: Literal[True],
    registry: DatabaseRegistry | None = None,
) -> Any: ...
def provide_driver(
    name_or_config: str | BaseDatabaseConfig = "default",
    *,
    sync: bool = False,
    registry: DatabaseRegistry | None = None,
) -> Any: ...

class SyncDatabaseDriver(CommonDriverMixin):
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> int: ...
    def execute_many(self, sql: str, parameter_sets: Sequence[Mapping[str, Any] | Sequence[Any]], **kwargs: Any) -> int: ...
    def execute_tuples(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> list[tuple[Any, ...]]: ...
    def execute_with_description(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]: ...
    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[T] | None = None, **kwargs: Any) -> list[Any]: ...
    def select_one(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[T] | None = None, **kwargs: Any) -> Any: ...
    def select_one_or_none(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[T] | None = None, **kwargs: Any) -> Any | None: ...
    def select_value(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, value_type: type[T] | None = None, **kwargs: Any) -> Any: ...
    def select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, **kwargs: Any) -> ArrowCursor: ...
    def stream_arrow_batches(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, batch_size: int = 65_536, **kwargs: Any) -> Iterator[Any]: ...
    def load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, **kwargs: Any) -> int: ...
    def begin(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def transaction(self) -> AbstractContextManager[Self]: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...
```

`AsyncDatabaseDriver` mirrors every method asynchronously (`async def`, `AbstractAsyncContextManager[Self]`, and `def stream_arrow_batches(...) -> AsyncIterator[Any]`). Legacy driver aliases (`executemany`, `fetch`, `fetchrow`, `fetchval`, `fetch_arrow`, `insert_arrow`) are deleted cleanly across base classes and adapters. `provide_driver` raises `TypeError` when given a `CloudSQLConfig` / `AlloyDBConfig` or `cloudsql://` / `alloydb://` URL directly (callers use `DatabaseRegistry.get_dialer` or attach dialer fields on a database config).

`select_one` and `select_value` raise `NotFoundError` strictly when `len(rows) == 0`; when 1 row is returned whose first column is `SQL NULL` (`rows[0][0] is None`), `select_value` returns `None`. `SyncSQLiteDriver` translates `sqlite3.Error` subclasses into `DatabaseError` subclasses (`_translate_sqlite_error`) and normalizes `Decimal`, `datetime`/`date`/`time`, `UUID`, and `dict`/`list` JSON bind parameters.

`transaction()` calls `begin()`, yields `self`, calls `commit()` on clean exit, and calls `rollback()` before re-raising on exception: `sqlite`, `oracle`, `mssql`, and `duckdb` gate per-statement auto-commit via `self._in_transaction`; `postgres` and `mysql` pin `self._tx_conn` across the transaction (and snapshot/restore `_in_memory_rows` in unit-test harnesses); `bigquery` executes `"BEGIN TRANSACTION"`, `"COMMIT TRANSACTION"`, and `"ROLLBACK TRANSACTION"` over `self._session_id`; and `spanner` buffers DML and mutations in `self._tx_dml_buffer` and `self._tx_mutation_buffer`, flushing atomically via `execute_batch_dml` and `apply_mutations` on `commit()` and clearing both buffers on `rollback()` (`run_in_transaction` remains Spanner's retryable read-write callback API).

#### Capability Protocols (`vertebra.db.driver.capabilities`)

```python
@runtime_checkable
class SyncPipeline(Protocol):
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def results(self) -> list[Any]: ...

@runtime_checkable
class AsyncPipeline(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    async def results(self) -> list[Any]: ...

@runtime_checkable
class SyncPipelineCapable(Protocol):
    def pipeline(self) -> SyncPipeline: ...

@runtime_checkable
class AsyncPipelineCapable(Protocol):
    def pipeline(self) -> AsyncPipeline: ...

@runtime_checkable
class NotificationSource(Protocol):
    async def publish(self, channel: str, payload: bytes | str) -> None: ...
    async def subscribe(self, channels: Sequence[str]) -> None: ...
    async def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> AsyncIterator[NotificationEvent]: ...

@runtime_checkable
class SyncChangeSource(Protocol):
    def stream(self) -> Iterator[ChangeEvent]: ...

@runtime_checkable
class AsyncChangeSource(Protocol):
    def stream(self) -> AsyncIterator[ChangeEvent]: ...

@runtime_checkable
class QueueStore(Protocol):
    async def ensure_schema(self) -> None: ...
    async def enqueue(
        self,
        queue: str,
        task_name: str,
        payload: bytes,
        *,
        job_id: str | None = None,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
        headers: Mapping[str, str] | None = None,
    ) -> str: ...
    async def enqueue_many(
        self,
        queue: str,
        items: Sequence[tuple[str, bytes]],
        *,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
    ) -> list[str]: ...
    async def claim(
        self,
        queue: str,
        *,
        limit: int = 1,
        lease_ms: int = 30_000,
        worker_id: str = "",
    ) -> list[ClaimedJob]: ...
    async def heartbeat(self, job_id: str, attempt: int, *, extend_ms: int = 30_000) -> bool: ...
    async def complete(self, job_id: str, attempt: int) -> bool: ...
    async def fail(self, job_id: str, attempt: int, *, error: str, retry_delay_ms: int | None = None) -> None: ...
    async def promote_scheduled(self, queue: str | None = None, *, now_ms: int | None = None, limit: int = 100) -> int: ...

@runtime_checkable
class EmbeddingPushdown(Protocol):
    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView: ...
    async def embed_query(
        self,
        text: str,
        *,
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorView: ...
    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]: ...
```

#### Rust Shared Core (`vertebra_db`) & Per-Driver PyO3 Wheels (`vertebra-bigquery`, `vertebra-spanner`)

- `vertebra_db::connection`:
  - `install_rustls_ring_provider()`: unconditional `rustls::crypto::ring::default_provider().install_default()` helper (available without enabling `gcp`).
  - `EndpointTarget`: `Tcp`, `Unix`, `CloudSql`, and `AlloyDb`.
  - `TransportStream`: unified `AsyncRead + AsyncWrite + Unpin` enum over plain TCP, `tokio_rustls::client::TlsStream<TcpStream>` (`webpki_root_certs::TLS_SERVER_ROOT_CERTS` + `rustls-pemfile`), and Unix domain sockets (`#[cfg(unix)]`).
  - `CancelHandle` & `Connection` traits: shared lifecycle contract (`ping`, `is_broken`, `cancel_handle`, `close`) implemented by every native engine client.
- `vertebra_db::py_async` (`#[cfg(feature = "pyo3")]`):
  - Shared `db_runtime_handle`, `block_on_db`, `RustDbTaskAwaitable`, and `spawn_rust_db_task_with_attr(..., attr_name: &'static str)` over `AsyncCompletionQueue` + `Notifier`, keying the event-loop drainer attribute uniquely per `.so` extension (`"_vertebra_core_completion_drainer"`, `"_vertebra_bigquery_completion_drainer"`, `"_vertebra_spanner_completion_drainer"`) and dynamically resolving `TaskPanicError` from `vertebra._vertebra`.
- `vertebra_core::shm::ShmCapsule` + `ArrowCursor._from_shm_capsule`:
  - `#[repr(C)]` capsule payload carrying `(ptr, len, ctx, drop_fn)` so `vertebra._bigquery` (and future `vertebra._<engine>` wheels) hand off an initialized `Arc<ShmRegion>` directly to `vertebra._vertebra.ArrowCursor` with zero memory copies, zero extra `shm_open`/`mmap` syscalls, and zero `DashMap` leaks across `cdylib` crates.
- `src/rs/vertebra-bigquery` (`vertebra._bigquery`) & `src/rs/vertebra-spanner` (`vertebra._spanner`):
  - Standalone `cdylib` + `rlib` crates exposing `SyncBigQueryClient` / `AsyncBigQueryClient` (`vertebra._bigquery`) and `SyncSpannerClient` / `AsyncSpannerClient` (`vertebra._spanner`) with `#[pymodule(gil_used = false)]`, complete `.pyi` stubs verified by `mypy.stubtest`, and `[bigquery]` / `[spanner]` / `[storage]` install extras in `pyproject.toml`.

### 2.3 Non-Functional & Safety Constraints

- **Clean break**: Zero compatibility aliases or re-export shims at `vertebra.db.<engine>`, `vertebra.db._registry`, `vertebra.db._arrow`, `vertebra._vertebra.{SyncBigQueryClient,AsyncBigQueryClient,SyncSpannerClient,AsyncSpannerClient}`, `provide_bigquery_driver`, `provide_spanner_driver`, or `vertebra_db::tns`.
- **Lightweight core**: `vertebra-py/Cargo.toml` (`vertebra._vertebra`) and default `vertebra-db/Cargo.toml` (`default = []`) depend on zero `google-cloud-*` or `tonic` crates.
- **Startup cost**: `import vertebra.db` and `BaseDatabaseConfig` URL parsing execute zero adapter `driver.py` or `vertebra._<engine>` imports until a driver is constructed and used via `ensure_module`.
- **Type & stub parity**: `mypy` passes with zero `# type: ignore[override]` in `src/py/vertebra/db/adapters/*/driver.py`, and `mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner` passes with zero errors.
- **GIL & TLS safety**: `connection.rs` uses `rustls` with `ring` (`install_rustls_ring_provider()`), all PyO3 driver modules declare `#[pymodule(gil_used = false)]`, and async tasks never call `Python::attach` on Tokio worker threads.

### 2.4 Acceptance Scenarios (Given / When / Then)

1. **Clean-Break Adapter Imports & Per-Driver GCP Wheels**
   - **Given** the relocated `vertebra.db.adapters.<engine>` layout and extracted `vertebra._bigquery` / `vertebra._spanner` wheels
   - **When** user code imports `from vertebra.db.adapters.bigquery import SyncBigQueryDriver` or inspects `vertebra._vertebra`
   - **Then** `SyncBigQueryClient` / `AsyncBigQueryClient` live on `vertebra._bigquery` (raising `MissingDependencyError` naming `vertebra-core[bigquery]` when `vertebra._bigquery` is absent), `hasattr(vertebra._vertebra, "SyncBigQueryClient")` and `hasattr(vertebra._vertebra, "SyncSpannerClient")` are `False`, and `import vertebra.db.postgres` raises `ModuleNotFoundError`.
2. **Unified `select_one`, `select_value`, & `execute` Contract**
   - **Given** any SQL or analytical driver (`sqlite`, `postgres`, `mysql`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`)
   - **When** `select_one()` or `select_value()` executes a query returning 0 rows vs. a 1-row `SELECT NULL`
   - **Then** 0 rows raises `NotFoundError` (`isinstance(err, (DatabaseError, ValueError))`), while 1-row `SELECT NULL` returns `None` from `select_value()` without raising.
3. **Polymorphic Registry, Sync/Async Dialers & Ephemeral `:memory:` Isolation**
   - **Given** `DatabaseRegistry.default()` and calls to `provide_driver("sqlite:///:memory:", sync=True)` and `registry.get_dialer(cloudsql_cfg, sync=True)` / `registry.get_dialer(cloudsql_cfg, sync=False)`
   - **When** `SQLiteConfig.from_url("sqlite:///:memory:").pool_key()` returns `None`
   - **Then** `provide_driver` lazily imports `vertebra.db.adapters.sqlite.driver` and returns distinct `SyncSQLiteDriver` instances, while `get_dialer` caches `SyncCloudSQLDialer` and `AsyncCloudSQLDialer` separately by `(*config.pool_key(), sync)`.
4. **DataFusion Fail-Closed Execution**
   - **Given** `DataFusionEngine` without `datafusion` installed or given invalid SQL
   - **When** `execute_dicts("SELECT * FROM missing_table")` is called
   - **Then** it raises `MissingDependencyError` or DataFusion's execution error directly and never opens `sqlite3`.
5. **Upward-Import Boundary Gate**
   - **Given** all Python modules under `src/py/vertebra/db/`
   - **When** the AST boundary test inspects every `import` and `from ... import` statement
   - **Then** zero modules outside the explicit temporary `{"vertebra.db.mcp_tools"}` allowlist import from `vertebra.agent`, `vertebra.framework`, `vertebra.extensions`, `vertebra.server`, or `vertebra.embeddings`.

### 2.5 Alternatives Considered

1. **Compile all native Rust drivers directly into `vertebra._vertebra`**: Rejected. Splitting every database driver into its own PyO3 crate/wheel (`src/rs/vertebra-<engine>` -> `vertebra._<engine>` + `vertebra-core[<engine>]`) keeps `vertebra-core` fast to build and free of unused database wire/gRPC/C++ dependencies, and eliminates `arrow` crate version coupling across wheels.
2. **Keep flat `vertebra/db/<engine>/` packages and move engine-agnostic modules into `vertebra/db/common/`**: Rejected. Nesting engines under `vertebra/db/adapters/<engine>/` mirrors `vertebra.extensions.sqlspec.adapters.<engine>` and keeps top-level `vertebra.db.{driver,registry,arrow,cdc,rls,dialer}` short and discoverable.
3. **Single union protocol instead of `Sync*` / `Async*` protocol pairs**: Rejected. Explicit `SyncPipelineCapable` / `AsyncPipelineCapable` and `SyncChangeSource` / `AsyncChangeSource` pairs let `isinstance(driver, SyncPipelineCapable)` narrow `driver.pipeline()` directly to `SyncPipeline` and `isinstance(driver, AsyncPipelineCapable)` narrow directly to `AsyncPipeline` without union casts.

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task(s) | Verification Strategy | Confirmed Public Seam & Canonical Test Surface |
| --- | --- | --- | --- |
| R1 | `t01` | `behavior_tdd` | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/docs/test_examples.py`, `make review-check` |
| R2 | `t01` | `behavior_tdd` | `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_pg_gateway.py` |
| R3 | `t02` | `behavior_tdd` | `src/rs/vertebra-db/src/connection.rs` (`cargo test -p vertebra-db`), `src/rs/vertebra-py/src/db.rs` (`cargo test -p vertebra-py`), `make review-check` |
| R4 | `t02` | `behavior_tdd` | `src/rs/vertebra-py/src/errors.rs` (`cargo test -p vertebra-py`), `src/py/tests/unit/db/test_native_db_drivers.py` |
| R5 | `t03` | `behavior_tdd` | `cargo test -p vertebra-bigquery -p vertebra-spanner`, `mypy.stubtest vertebra._bigquery vertebra._spanner`, `src/py/tests/unit/db/test_{bigquery,spanner}.py` |
| R6 | `t04` | `behavior_tdd` | `src/py/tests/unit/db/test_{native_db_drivers,postgres_driver,mysql_driver,enterprise_olap_drivers,bigquery,spanner}.py`, `src/py/tests/unit/extensions/test_{sqlalchemy_and_sqlspec_vtb,sqlspec_bigquery,sqlspec_spanner}.py` |
| R7 | `t05` | `behavior_tdd` | `src/py/tests/unit/db/test_native_db_drivers.py` |
| R8 | `t06` | `behavior_tdd` | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| R9 | `t06`, `t07` | `behavior_tdd`, `integration_acceptance` | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/integration/db/test_driver_conformance.py`, `make docs` |

## 4. Implementation Plan

### Phase 1: Package Layout, Shared Rust Core, Error Hierarchy & GCP Wheel Extraction

- [x] [8675187] t01 — Relocate Python engine packages to `vertebra/db/adapters/<engine>` and remove DataFusion SQLite fallback (tasks/t01-python-adapters-package-move.md)
- [x] [2fa781f] t02 — Reorganize `vertebra-db` shared core, add `connection.rs` + `py_async.rs` + `ShmCapsule`, and register `DatabaseError` hierarchy (tasks/t02-rust-db-layout-and-connector.md)
- [x] [95afe7a] t03 — Extract `vertebra-bigquery` and `vertebra-spanner` standalone PyO3 wheels and optional install extras (tasks/t03-extract-gcp-driver-wheels.md)
- **Phase 1 Recovery Checkpoint**: `make dev && make lint-all && cargo test -p vertebra-db -p vertebra-py -p vertebra-bigquery -p vertebra-spanner && uv run pytest src/py/tests/unit`

### Phase 2: Unified Driver Contract, Capability Protocols & Polymorphic Registry

- [x] [c031fc9] t04 — Unify `SyncDatabaseDriver` / `AsyncDatabaseDriver` method signatures and eliminate `# type: ignore[override]` (tasks/t04-driver-contract-and-signatures.md)
- [x] [2c0d365] t05 — Define runtime-checkable `Sync*` / `Async*` engine capability protocols and `msgspec` records (tasks/t05-capability-protocols.md)
- [x] [144ef8b] t06 — Make `DatabaseRegistry` polymorphic, add `provide_driver`, and enforce the `vertebra/db` upward-import gate (tasks/t06-polymorphic-registry-and-import-gate.md)
- **Phase 2 Recovery Checkpoint**: `make lint-all && uv run pytest src/py/tests/unit`

### Phase 3: Cross-Engine Conformance Suite & Documentation

- [x] [a5dbea6] t07 — Add parameterized `pytest-databases` driver conformance suite and update docs and OKF chapters (tasks/t07-conformance-suite-and-docs.md)
- **Phase 3 Recovery Checkpoint**: `make lint-all && make rs-test && make py-test && uv run pytest src/py/tests/integration/db/test_driver_conformance.py && make docs && make review-check`

## 5. Continuity Snapshot

- **Flow ID**: `db-layout-and-contract` (`state: active`, `plan_revision: 5`, `plan_commit: "2c0d365"`)
- **Current Task**: `null`
- **Last Verified Checkpoint**: `a5dbea6`
- **Key Decisions**:
  - Engine adapters live under `vertebra.db.adapters.<engine>` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion`); engine-agnostic modules stay directly under `vertebra.db.{driver,registry,arrow,cdc,rls,dialer}`.
  - Every database engine compiles as its own optional PyO3 extension crate `src/rs/vertebra-<engine>` (`vertebra._<engine>` + `[project.optional-dependencies].<engine>`); `t03` extracts `vertebra-bigquery` and `vertebra-spanner` immediately so `vertebra._vertebra` and default `vertebra-db` are free of `google-cloud-*` / `tonic`.
  - `NotFoundError(DataError, ValueError)` unifies empty `select_one()` / `select_value()` errors across all drivers while returning `None` for 1-row `SQL NULL` scalars in `select_value()`, and `SpannerError` / `BigQueryError` inherit from both `GoogleCloudError` and `DatabaseError` on `vertebra._vertebra`.
  - Capability protocols split dual-mode capabilities into `SyncPipelineCapable` / `AsyncPipelineCapable` and `SyncChangeSource` / `AsyncChangeSource`, and `DatabaseRegistry.get_dialer(config, *, sync=False)` caches `Sync*` and `Async*` dialers separately.
- **Recent Discoveries**:
  - Separate `cdylib` `.so` extensions each have distinct PyO3 `PyTypeObject` static instances and private `rlib` globals: `vertebra-db/src/py_async.rs` keys event-loop completion drainers uniquely per extension (`attr_name: &'static str`), and `vertebra_core::shm::ShmCapsule` transfers `Arc<ShmRegion>` ownership into `vertebra._vertebra.ArrowCursor` with zero copies and zero `DashMap` leaks.
  - `.github/workflows/ci.yml` runs `uv sync --frozen` in `build-docs` and runs `make lint-all` without building optional `.so` extensions first, so `t03` updates `uv.lock` via `uv lock --offline` and guards optional extension `stubtest` imports in `Makefile`.
  - Importing `vertebra.db.adapters.<engine>.config` in Python always executes parent package `vertebra/db/adapters/<engine>/__init__.py` first, so `t06` converts all eleven adapter `__init__.py` barrels to `lazy_exports` alongside `DatabaseRegistry` table dispatch.
