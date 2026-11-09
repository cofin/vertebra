---
type: Guide
flow_id: native-oracle-driver
title: Native Oracle public contracts
description: Public interface, FFI, type, pooling, tenancy, and ownership contracts for the standalone native Oracle driver plan.
status: stable
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T01:28:30Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:28:30Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Oracle DSN, TNS, wallet, DRCP, and driver feature configuration
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Public SyncOracleDriver and AsyncOracleDriver adapter surface
  - id: ora-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle codecs, TNS helpers, and session context SQL builders
  - id: ora-rs-codecs
    resource: src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs
    title: Shared Rust Oracle TTC piggyback, OSON, and VECTOR codecs
  - id: ora-rs-tns
    resource: src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora
    title: Shared Rust TNS Connect Descriptor parser and alias resolver
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical Vertebra database exception hierarchy exported via vertebra._vertebra and vertebra.db.driver
  - id: arrow-cursor
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper over inline IPC and VTBARROW ShmCapsule
  - id: db-rls
    resource: src/py/vertebra/db/rls.py::TenantPredicate
    title: Row-level security and session-scoped tenant predicate primitives
tags:
  - oracle
  - interfaces
  - ownership
  - contracts
---

# Native Oracle public contracts

<!-- truth: start -->
- **Standalone `gil_used = false` Wheel Boundary**: `src/rs/vertebra-oracle` (`vertebra._oracle`, `vertebra-core[oracle]`) wraps `oracledb = "=26.0.0-beta.5"` (`features = ["arrow"]`) and crate-local `arrow-* = "59"` behind primitive PyO3 kwargs, exporting `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, `OracleColumn`, `OracleQueryResult`, `OraclePipelineOutcome`, `OracleNotification`, `AsyncOracleListener`, and `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED` without leaking `arrow 59` into `vertebra-core` [^ora-config] [^ora-driver] [^arrow-cursor].
- **Worker-Thread Completion & Out-of-Band Cancellation**: Blocking `oracledb::Connection` sessions run on dedicated Rust worker threads bridged via `SyncCompletionQueue` (`py.detach`) and `AsyncCompletionQueue` (`spawn_rust_db_task_with_attr`), with out-of-band `connection.break_execution()` mapping `ORA-01013` to `QueryCancelledError` (`sqlstate="57014"`, `vendor_code=1013`) [^ora-driver] [^db-errors-rs].
- **Delayed-Commit Pipelines & Reset-or-Evict Tenancy**: `SyncOraclePipeline` and `AsyncOraclePipeline` hold their transaction or `SAVEPOINT` across `results()` and commit only on clean context exit; VPD tenant scopes in `vertebra.db.rls` and `OracleCore` support trusted context package procedures alongside `DBMS_SESSION.SET_IDENTIFIER` / TTC piggyback and evict any pooled session whose lease-exit reset fails [^ora-core] [^ora-rs-codecs] [^db-rls].
<!-- truth: end -->

All features ship in one complete-driver PR across `ch01`–`ch07`. These contracts govern every chapter specification and task worksheet.

## 1. Native PyO3 Extension Exports (`vertebra._oracle`)

`src/rs/vertebra-oracle` compiles to `vertebra._oracle` (`#[pymodule(gil_used = false)] fn _oracle`) and exports:

- `__version__: str` (`env!("CARGO_PKG_VERSION")`)
- `FREE_THREADED: bool` (`true` under `#[cfg(Py_GIL_DISABLED)]`, `false` otherwise)
- `GIL_USED: bool` (`False`)
- `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool` (`False` on `oracledb = "=26.0.0-beta.5"`)
- `OracleColumn` — immutable column descriptor (`name: str`, `oracle_type: str`, `db_size: int | None`, `precision: int | None`, `scale: int | None`, `nullable: bool`)
- `OracleQueryResult` — immutable execution result (`columns: tuple[OracleColumn, ...]`, `rows: list[tuple[Any, ...]]`, `rowcount: int`, `out_binds: tuple[Any, ...] | dict[str, Any] | None`, `lastrowid: str | None`)
- `OraclePipelineOutcome` — per-operation pipeline outcome (`status: Literal["completed", "error", "skipped"]`, `result: OracleQueryResult | None`, `error: DatabaseError | None`)
- `OracleNotification` — immutable AQ / TxEventQ message envelope (`queue_name: str`, `consumer_name: str | None`, `msg_id: bytes`, `payload: bytes`, `enqueued_at_epoch_ms: int | None`)
- `AsyncOracleListener` — dedicated asynchronous AQ / TxEventQ dequeue pump handle
- `SyncOracleClient` — synchronous physical Oracle session client (exposing `connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`, `execute_many`, `query`, `query_arrow`, `run_pipeline`, `set_session_attrs`, and properties `closed`, `in_transaction`, `cclass`, `purity`, `connection_tag`, `tag_matched`, `pending_session_attrs`, `active_session_attrs`, `last_piggyback_frame`)
- `AsyncOracleClient` — asynchronous physical Oracle session client backed by `AsyncCompletionQueue` (exposing the async counterparts and identical properties)
- `SyncOraclePool` — synchronous Rust-backed physical Oracle connection pool (`acquire`, `release`, `evict`, `close`, `stats`, `closed`)
- `AsyncOraclePool` — asynchronous Rust-backed physical Oracle connection pool (`acquire`, `release`, `evict`, `close`, `stats`, `closed`)

## 2. Connection and Pool Constructors & Descriptor Precedence

`SyncOracleClient` and `AsyncOracleClient` share this keyword-only constructor:

```python
def __init__(
    self,
    *,
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
    host: str = "localhost",
    port: int = 1521,
    service_name: str = "FREEPDB1",
    sid: str | None = None,
    tns_admin: str | None = None,
    connect_descriptor: str | None = None,
    protocol: str = "tcp",
    ssl_server_dn_match: bool = True,
    ssl_server_cert_dn: str | None = None,
    wallet_location: str | None = None,
    wallet_password: str | None = None,
    pem_cert_data: bytes | None = None,
    cwallet_sso_data: bytes | None = None,
    auth_mode: str = "password",
    access_token: str | None = None,
    token_auth_options: Mapping[str, str] | None = None,
    cclass: str | None = None,
    purity: str = "SELF",
    connection_tag: str | None = None,
    stmtcachesize: int = 64,
    prefetchrows: int = 256,
    arraysize: int = 256,
    call_timeout_ms: int | None = None,
    connect_timeout: float = 10.0,
    enable_lowercase_column_names: bool = True,
    client_identifier: str | None = None,
    module: str | None = None,
    action: str | None = None,
    ecid: str | None = None,
) -> None: ...
```

`SyncOraclePool` and `AsyncOraclePool` accept the full connection keyword signature above plus:

```python
min_size: int = 1
max_size: int = 10
acquire_timeout: float = 30.0
reset_timeout: float = 10.0
close_timeout: float = 30.0
max_lifetime_seconds: float = 3600.0
idle_timeout_seconds: float = 600.0
health_check_interval_seconds: float = 30.0
```

Constructor and descriptor resolution invariants:
- Constructors validate and clone primitive options in memory without performing DNS, filesystem, or socket I/O (`NativeOracleConfig` in `src/rs/vertebra-oracle/src/config.rs`).
- `OracleConfig.connect_descriptor()` resolves endpoints in strict priority order:
  1. Explicit `connect_descriptor` (or `tns_admin` / `wallet` alias resolution via `parse_tnsnames_ora` / `resolve_tns_alias` [^ora-rs-tns]).
  2. Explicit `topology` (`OracleTopologyConfig.to_tns_descriptor()`).
  3. Explicit non-default `dsn` (`dsn != "oracle://localhost:1521/FREEPDB1"` or when `host == "localhost"`, `port == 1521`, and `service_name == "FREEPDB1"`).
  4. Explicit `host`, `port`, and `service_name` / `sid` when any of those fields differ from the default and `dsn` was left at its class default.
- Wallet archives (`.zip`) or directories (`ewallet.pem`, `cwallet.sso`, `tnsnames.ora`) are extracted in memory via `OracleWallet` and validated by `OracleWalletMaterial` in `src/rs/vertebra-oracle/src/dialer.rs` without writing temporary files to disk [^ora-config].
- `auth_mode` accepts `"password"` (O5LOGON verifier) or `"token"` (IAM / OAuth2 bearer token via `access_token` or `vertebra_core::gcp_auth::GcpTokenCache` / `GcpAuthSource::from_ADC()` derived from `token_auth_options`).
- `purity` accepts `"SELF"` or `"NEW"` (`OracleDrcpConfig`); any other value raises `ImproperConfigurationError` / `InterfaceError`.

## 3. Worker-Thread Execution, Cancellation & SQLSTATE Error Hierarchy

Each open `oracledb::Connection` is owned by a dedicated Rust worker thread (never wrapped in a contended cross-thread `Mutex<Connection>` during socket I/O) and exposes a thread-safe cancellation handle calling `connection.break_execution()` out-of-band:

- `SyncOracleClient` dispatches requests over a bounded command channel and waits via `SyncCompletionQueue` inside `py.detach(...)`.
- `AsyncOracleClient` dispatches requests and registers an `asyncio.Future` via `vertebra_db::py_async::spawn_rust_db_task_with_attr` using `ORACLE_COMPLETION_DRAINER_ATTR = "_vertebra_oracle_completion_drainer"`.
- Calling `client.cancel()`, `driver.cancel()`, or cancelling an in-flight `asyncio.Future` invokes the out-of-band break handle immediately.

All `ORA-xxxxx` database and transport failures map deterministically in `crate::errors::OracleError` to the canonical `vertebra_py::errors::DbExceptionTypes` classes exported by `vertebra.db.driver` and `vertebra._vertebra` [^db-errors-rs], setting `.sqlstate`, `.vendor_code`, and `.is_transient` on the raised exception instance:

| Oracle Error Code(s) | `DbExceptionTypes` Class (`vertebra.db.driver`) | `sqlstate` | `is_transient` |
| :--- | :--- | :--- | :--- |
| `ORA-00001` | `IntegrityError` | `"23505"` | `False` |
| `ORA-02291`, `ORA-02292` | `IntegrityError` | `"23503"` | `False` |
| `ORA-01400`, `ORA-01407` | `IntegrityError` | `"23502"` | `False` |
| `ORA-02290` | `IntegrityError` | `"23514"` | `False` |
| `ORA-08177` | `SerializationFailure` | `"40001"` | `True` |
| `ORA-00060` | `SerializationFailure` | `"40P01"` | `True` |
| `ORA-00054`, `ORA-30006` | `OperationalError` | `"55P03"` | `True` |
| `ORA-01013` | `QueryCancelledError` | `"57014"` | `False` |
| `ORA-01017`, `ORA-28000` | `ConnectionFailure` | `"28P01"` | `False` |
| `ORA-01403` | `NotFoundError` | `"02000"` | `False` |
| `ORA-01438`, `ORA-01476`, `ORA-01722`, `ORA-01830`–`ORA-01899`, `ORA-12899` | `DataError` | `"22000"` | `False` |
| `ORA-00900`–`ORA-00999`, `ORA-06550` | `ProgrammingError` | `"42000"` | `False` |
| `ORA-03113`, `ORA-03114`, `ORA-03135`, `ORA-04061`, `ORA-04068`, `ORA-12154`, `ORA-12170`, `ORA-12514`, `ORA-12537`, `ORA-12541`, `ORA-12545` | `OperationalError` (`ConnectionFailure` for `12154`/`12170`/`12514`/`12541`/`12545`) | `"08006"` | `True` (`False` for `12154`) |

## 4. Type Codecs, Binds, DML RETURNING, and Arrow 59 SHM Data Plane

- **Column Casing**: When `enable_lowercase_column_names=True` (the default on `OracleDriverFeatures` and native clients), unquoted uppercase Oracle column identifiers are normalized to lowercase across `OracleColumn.name`, `execute_with_description()` column tuples, `select()` dict/struct keys, and `select_to_arrow()` Arrow schema field names.
- **Bind Parameters**: `execute`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, and `select_to_arrow` accept positional sequences (`:1..:n`) or named mappings (`:name`). `DML ... RETURNING ... INTO :out_1, ...` populates `OracleQueryResult.out_binds` and returns projections through `execute_tuples` / `select` without extra round-trips.
- **26ai Native Codecs**:
  - `NUMBER` -> `int` (when `scale == 0` and integer) or `Decimal` / `float`
  - `VARCHAR2` / `NVARCHAR2` / `CHAR` / `CLOB` -> `str`
  - `RAW` / `BLOB` -> `bytes`
  - `DATE` / `TIMESTAMP` / `TIMESTAMP WITH TIME ZONE` -> `datetime.date` / `datetime.datetime` (timezone-aware for `TSTZ`)
  - `BOOLEAN` (Oracle 23ai/26ai SQL boolean) -> `bool`
  - `JSON` (`OSON` binary format `0xFF 'J' 'Z' 0x01`) -> decoded Python `dict | list | Any` via `vertebra_db::oracle::decode_oson_payload` and `msgspec` (`enable_json_deserialization=True`) [^ora-rs-codecs]
  - `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY` magic `0xDB 0x01`) -> `list[float] | bytes` / `vertebra._vertebra.VectorView` via `vertebra_db::oracle` (`enable_vector_support=True`) [^ora-rs-codecs]
- **Arrow 59 Zero-Copy SHM Bridge**:
  - `src/rs/vertebra-oracle/Cargo.toml` pins `arrow-array = "59"`, `arrow-buffer = "59"`, `arrow-ipc = "59"`, and `arrow-schema = "59"` crate-locally to match `oracledb = "=26.0.0-beta.5"`.
  - `batches_to_arrow_ipc` in `src/rs/vertebra-oracle/src/arrow.rs` serializes `oracledb`'s `RecordBatch` stream into `(Vec<u8>, usize)` (Arrow IPC stream bytes + total row count) and passes them into `vertebra_db::py_async::construct_py_arrow_cursor(py, ipc_bytes, row_count, force_shm)` (which writes payloads `>= ARROW_SHM_THRESHOLD_ROWS` or `force_shm == Some(true)` into a `VTBARROW` 16-byte framed shared-memory region via `ShmArrowStreamWriter` and returns an `ArrowCursor` with `cursor.used_shm` and `cursor.buffer_view()`) [^arrow-cursor].

## 5. Sequential Pipelines, VPD Trusted Tenancy, Pool Reset, and Extensions

- **Delayed-Commit Pipelines (`SyncOraclePipeline` & `AsyncOraclePipeline`)**:
  - Entering `with driver.pipeline() as pipe:` or `async with driver.pipeline() as pipe:` pins a pooled session lease and opens a transaction (or issues `SAVEPOINT _vtb_pipe_<n>` if the driver is already inside an outer transaction).
  - `pipe.results()` executes queued operations sequentially on the pinned worker connection, populates each `PipelineHandle`, and leaves the transaction/savepoint open so caller cardinality checks (`one`, `one_or_none`) and DTO hydration inside the `with` block execute inside the transactional boundary.
  - Clean `__exit__` / `__aexit__` (`exc_type is None` after `results()` succeeded) commits the transaction (or leaves the outer transaction intact for a savepoint). If any statement fails, if caller code inside the `with` block raises, or if the context exits with unexecuted operations, the pipeline rolls back (`ROLLBACK` or `ROLLBACK TO SAVEPOINT _vtb_pipe_<n>`) before releasing the lease.
- **VPD Trusted-Package + `SET_IDENTIFIER` Tenancy & Mandatory Pool Reset**:
  - `OracleConfig` (adding `context_namespace: str = "USERENV"`, `context_package_proc: str | None = None`, `context_clear_proc: str | None = None` in `ch05:t02`) and `OracleCore` support two VPD / session context mechanisms:
    1. **Trusted PL/SQL Context Package (`context_package_proc` / `context_clear_proc`)**: When configured (e.g., `context_package_proc="APP_CTX_PKG.SET_TENANT"`, `context_clear_proc="APP_CTX_PKG.CLEAR_TENANT"`), `OracleCore.build_session_context_sql` and `OracleCore.build_clear_context_sql` emit `BEGIN <pkg>.<proc>(:namespace, :attribute, :value); END;` and `BEGIN <pkg>.<clear_proc>(:namespace, :attribute); END;` so Oracle 26ai `CREATE CONTEXT <namespace> USING <pkg>` succeeds without `ORA-01031`.
    2. **Built-in `USERENV` `CLIENT_IDENTIFIER` (`DBMS_SESSION.SET_IDENTIFIER` / `CLEAR_IDENTIFIER` and TTC Piggyback)**: When targeting `USERENV` (`attribute="CLIENT_IDENTIFIER"`) or when no custom trusted package is configured for identifier propagation, `OracleCore` emits `BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;` and `BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;` (or stages `set_session_attrs(client_identifier=...)` on TTC piggyback `[0x11, 0x6e, ...]`), which Oracle 26ai permits without custom DBA grants.
  - Because Oracle session context and `CLIENT_IDENTIFIER` survive `ROLLBACK`, releasing a tenant-scoped connection back to `SyncOraclePool` / `AsyncOraclePool` or exiting a tenant scope in `src/py/vertebra/db/rls.py` always executes the configured reset procedure; if the reset fails or the session is broken, the pool permanently evicts/retires the physical connection (`evict_connection()`) so tenant state can never bleed into a subsequent borrower [^db-rls].
- **Extensions & Capabilities**:
  - `OracleSyncDriver` and `OracleAsyncDriver` in `src/py/vertebra/extensions/sqlspec/adapters.py` set `_execution_parameter_style = ParameterStyle.NAMED_COLON`.
  - `OracleSessionManager` in `src/py/vertebra/extensions/apex/session.py` routes `APEX_SESSION.ATTACH` and VPD context setup/teardown through the trusted-package / `SET_IDENTIFIER` contract and `set_session_attrs`.
  - `OracleNotificationSource` in `src/py/vertebra/db/adapters/oracle/notify.py` implements the `@runtime_checkable NotificationSource` protocol (`publish(self, channel: str, payload: bytes | str) -> None`, `subscribe(self, channels: Sequence[str]) -> None`, `unsubscribe(self, channels: Sequence[str]) -> None`, `listen(self) -> AsyncIterator[NotificationEvent]`) with optional `change_feed: ChangeFeed | None = None` from `vertebra.db.cdc` and `decode_json`/`encode_json` from `vertebra.utils.serializers`.
  - `OracleEmbeddingPushdown` in `src/py/vertebra/db/adapters/oracle/embeddings.py` preserves its `EmbeddingPushdown` method signatures (`embed_documents -> VectorBatchView`, `embed_query -> VectorView`, `similarity_search(self, query_vector, *, table, embedding_column="embedding", top_k=10, metric="cosine") -> list[dict[str, Any]]`) while extending `_extract_vector(row, dimensions)` and `_extract_batch_vectors(cls, rows, expected_count, dimensions)` to unwrap `VectorView`, `ORACLE_VECTOR_MAGIC` `bytes`/`memoryview`, and dict/tuple row payloads returned by native `_oracle` execution.

[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Oracle DSN, TNS, wallet, DRCP, and driver feature configuration.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Public `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
[^ora-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle codecs, TNS helpers, and session context SQL builders.
[^ora-rs-codecs]: `src/rs/vertebra-db/src/oracle/mod.rs::OracleSessionAttrs` — Shared Rust Oracle TTC piggyback, `OSON`, and `VECTOR` codecs.
[^ora-rs-tns]: `src/rs/vertebra-db/src/oracle/tns.rs::parse_tnsnames_ora` — Shared Rust TNS Connect Descriptor parser and alias resolver.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical Vertebra database exception hierarchy exported via `vertebra._vertebra` and `vertebra.db.driver`.
[^arrow-cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper over inline IPC and `VTBARROW` `ShmCapsule`.
[^db-rls]: `src/py/vertebra/db/rls.py::TenantPredicate` — Row-level security and session-scoped tenant predicate primitives.
