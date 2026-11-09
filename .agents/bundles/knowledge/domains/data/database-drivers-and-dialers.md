---
type: Guide
title: Database Drivers & Cloud Dialers
description: Per-adapter database and cache package contracts, shared Rust transport and Arrow SHM bridges, non-GCP wire codecs, and Cloud SQL / AlloyDB mTLS dialers across vertebra-db and vertebra.db.
status: stable
tags:
  - domains
  - database
  - drivers
  - cloud-sql
  - alloydb
  - arrow
  - shm
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-archive
    at: "2026-11-09T20:56:00Z"
    commit: 62a29db7
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
  - by: agent:flow-review
    at: "2026-11-05T00:35:59Z"
    commit: 21c99f9571ab044741a10f7bbd7be05cb0eb2117
sources:
  - id: src-vertebra-core-src-shm-rs
    resource: src/rs/vertebra-core/src/shm.rs
    title: ShmRegion and ShmCapsule shared-memory primitives
  - id: src-vertebra-core-src-proto-rs
    resource: src/rs/vertebra-core/src/proto.rs
    title: Zero-dependency Protobuf v3 wire helpers
  - id: src-vertebra-db-src-lib-rs
    resource: src/rs/vertebra-db/src/lib.rs
    title: Explicit vertebra-db Rust crate exports
  - id: src-vertebra-db-src-arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: Shared-memory Arrow IPC writers and unregistered regions
  - id: src-vertebra-db-src-connection-rs
    resource: src/rs/vertebra-db/src/connection.rs
    title: TransportStream, TlsMode, EndpointTarget, and Connection traits
  - id: src-vertebra-db-src-py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Per-extension event-loop completion drainer and ShmCapsule bridge
  - id: src-vertebra-db-src-dialer-rs
    resource: src/rs/vertebra-db/src/dialer.rs
    title: Zero-sidecar Cloud SQL and AlloyDB mTLS dialers
  - id: src-vertebra-db-src-postgres-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs
    title: PostgreSQL v3 wire framing and binary pgvector codecs
  - id: src-vertebra-db-src-oracle-rs
    resource: src/rs/vertebra-db/src/oracle/mod.rs
    title: Oracle tnsnames.ora parser, session attributes, and 26ai VECTOR and OSON codecs
  - id: src-vertebra-db-src-duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs
    title: DuckDB 2.0-safe SET bootstrap statements and Arrow SHM re-exports
  - id: src-vertebra-db-src-resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: Redis and Valkey RESP2/RESP3 protocol parser
  - id: src-vertebra-py-src-db-rs
    resource: src/rs/vertebra-py/src/db.rs
    title: PyO3 ArrowCursor, dialers, and binary vector bindings
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: Canonical DatabaseError and multi-base cloud exception hierarchy
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: Lazy engine-agnostic vertebra.db public exports
  - id: src-db-arrow-py
    resource: src/py/vertebra/db/arrow.py
    title: ArrowCursor Python wrapper and zero-copy views
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: Declarative DatabaseRegistry URL parsing and pool deduplication
  - id: src-db-driver-init-py
    resource: src/py/vertebra/db/driver/__init__.py
    title: SyncDatabaseDriver and AsyncDatabaseDriver base hierarchy
  - id: src-db-driver-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: Runtime-checkable driver capability protocols
  - id: src-db-dialer-cloudsql-py
    resource: src/py/vertebra/db/dialer/cloudsql.py
    title: Cloud SQL instance URI parser and Python mTLS dialer
  - id: src-db-dialer-alloydb-py
    resource: src/py/vertebra/db/dialer/alloydb.py
    title: AlloyDB cluster URI parser and metadata exchange dialer
  - id: src-sqlalchemy-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: SQLAlchemy +vtb DBAPI dialects and transaction policy
  - id: src-tests-integration-db-conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-driver acceptance and conformance suite
  - id: src-oracle-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleWallet
    title: Python Autonomous Database wallet parser
  - id: src-missing-dependency
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and default install hint
  - id: src-module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Dynamic optional dependency loader
  - id: src-packaging-extras-gate
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras gate verifying ensure_module extra literals
  - id: src-oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
  - id: src-mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: SQL Server driver native-module guard
  - id: src-duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver duckdb package guard
  - id: src-redis-config
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis configuration and driver memory:// rejection
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Structured Per-Adapter Package Layout (`src/py/vertebra/db/adapters/<engine>/{config,core,driver,__init__}.py` & `src/py/vertebra/db/driver/`)**: Every built-in database, cache, and federated query adapter under `vertebra.db.adapters` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`, `datafusion`) lives in its own package, while `vertebra.db` lazily exports only engine-agnostic base contracts, `DatabaseError` subclasses, dialers, `ArrowCursor`, `DatabaseRegistry`, CDC, and RLS primitives.
- **Shared Connector & Lifecycle Traits (`vertebra-db/src/connection.rs`, `vertebra-db/src/lib.rs`)**: `connection.rs` defines `install_rustls_ring_provider()`, `TlsMode` (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`), `ConnectOptions`, `TransportError` (`is_transient`, `sqlstate`), `EndpointTarget` (`Tcp`, `Unix`, `CloudSql`, `AlloyDb`), `TransportStream` (`AsyncRead + AsyncWrite + Unpin`), `CancelHandle`, and `Connection` (`ping`, `is_broken`, `cancel_handle`, `close`), re-exported via explicit symbol lists without wildcard globs.
- **Shared PyO3 Async Drainer & Cross-`.so` `ShmCapsule` (`vertebra-db/src/py_async.rs`, `vertebra-core/src/shm.rs`, `vertebra-db/src/arrow_shm.rs`, `vertebra-py/src/db.rs`)**: `spawn_rust_db_task_with_attr(py, fut, map_ok, map_err, attr_name)` keys each extension's event-loop completion drainer by `attr_name` and binds Python future cancellation to Tokio `AbortHandle`, while `#[repr(C)] ShmCapsule` transfers `Arc<ShmRegion>` ownership across `.so` boundaries via `PyCapsule` (`c"vertebra.ShmCapsule"`) into `ArrowCursor._from_shm_capsule`.
- **Canonical `DatabaseError` Hierarchy (`vertebra-py/src/errors.rs`)**: `vertebra_py::errors` registers `DatabaseError(VertebraError)` and 12 typed subclasses (`InterfaceError`, `DataError`, `NotFoundError(DataError, ValueError)`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) plus multi-base `BigQueryError` and `SpannerError` subclasses with zero `google-cloud-*` dependencies in `vertebra-py`.
- **Binary Vector Wire Codecs & DuckDB Secret Rotation (`postgres/mod.rs`, `oracle/mod.rs`, `mssql/mod.rs`, `duckdb/mod.rs`)**: `vertebra-db` holds codecs, parsers, and dialers only, with no `*Pool` lease types: PostgreSQL encodes and decodes binary `pgvector` (`vector` `f32`, `halfvec` `f16`, `bit`) frames, Oracle 26ai encodes and decodes binary `0xDB` `VECTOR` and `OSON` frames over PEP 3118 `PyBuffer` views and parses `tnsnames.ora` descriptors, SQL Server rewrites `?` and `$1` placeholders to `@P1` via `rewrite_mssql_placeholders`, and pooled DuckDB sessions rotate `vtb_gcs_adc` `httpfs` secrets whenever `applied_token_generation` lags the shared `GcpTokenCache`.
- **Fail-Loud Engine Availability (`oracle/driver.py`, `mssql/driver.py`, `duckdb/driver.py`)**: Oracle and MSSQL drivers resolve `vertebra._oracle` / `vertebra._mssql` through `ensure_module` and raise `MissingDependencyError`; DuckDB requires `duckdb`, and DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there; no non-SQLite adapter opens `sqlite3`; PostgreSQL, MySQL, Redis, and Valkey drivers raise `ImproperConfigurationError` for `memory://` while their configs construct.
- **Install Hints (`_typing.py`, `module_loader.py`, `test_packaging_and_pgo.py`)**: `MissingDependencyError` defaults `install_package` to `vertebra-core[<extra>]`, the packaging gate rejects undeclared extras, and the Oracle and MSSQL guards name the planned, unpublished `vertebra-core[oracle]` / `vertebra-core[mssql]` extras.
- **Zero-Sidecar Cloud SQL & AlloyDB Dialers (`vertebra-db/src/dialer.rs`, `vertebra-core/src/proto.rs`)**: `CloudSQLDialer` (`:3307`, custom `project:instance` `ServerCertVerifier`) and `AlloyDBDialer` (`:5433`, mTLS + 4-byte BE length-prefixed `MetadataExchangeRequest` encoded via `vertebra_core::proto` varint/bytes wire helpers) establish IAM or password connections without external proxy processes.
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] define Vertebra's database and cache adapter contracts, shared-memory `ArrowCursor` transport, binary wire codecs, and zero-sidecar Cloud SQL and AlloyDB dialers. Standalone Google Cloud analytical and distributed SQL wheels (`vertebra._bigquery` and `vertebra._spanner`), multicloud `ObjectStoreStaging`, and adapter `ToolProvider` policies are documented in [GCP BigQuery & Spanner Native Drivers](gcp-bigquery-and-spanner-drivers.md) and [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md); query composition, CDC, and RLS live in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

## Polymorphic Registry & Driver Contracts

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] builds `SCHEME_REGISTRY` declaratively from `REGISTERED_CONFIG_TYPES` (`BaseDatabaseConfig.url_schemes`) without engine-specific branching:

- **URL Dispatch & Pool Deduplication**: `parse_url(url, **overrides)` resolves schemes (`postgresql`/`postgres`, `mysql`/`mariadb`, `sqlite`, `oracle`, `mssql`/`sqlserver`, `duckdb`, `bigquery`/`bq`, `spanner`/`spangres`/`spanner+googlesql`/`spanner+spangres`, `redis`/`rediss`, `valkey`/`valkeys`, `datafusion`, `cloudsql`, `alloydb`) to `config_cls.from_url(...)`. `register(name, config_or_url)` retains cached drivers when `existing == resolved` and invalidates on change; `get_shared_driver` and `get_shared_sync_driver` deduplicate by `config.pool_key()` (`None` isolates `:memory:` SQLite/DuckDB). `provide_driver(name_or_config, *, sync=False, registry=None)` resolves database, cache, and DataFusion drivers for DI while rejecting `CloudSQLConfig` and `AlloyDBConfig` with `TypeError` (use `get_dialer`).
- **Uniform Driver Surface & Capabilities**: [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] (`SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`) standardizes `execute -> int`, `execute_many -> int`, `execute_tuples`, `execute_with_description`, `select`, `select_one` (`NotFoundError` on `0` rows), `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow -> int`, `transaction()`, `cancel()`, and `close()`. Optional capabilities live in [`capabilities.py`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py]: `SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, and `EmbeddingPushdown`.

## Relational, Analytical & Cache Adapters

| Adapter & Rust Module | Config & Core | Wire & Runtime Behavior |
|---|---|---|
| [`adapters/postgres/`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) / [`postgres/mod.rs`](../../../../../src/rs/vertebra-db/src/postgres/mod.rs)[^src-vertebra-db-src-postgres-rs] | `PostgreSQLConfig`, `PostgreSQLCore` | v3 frame encoders (`encode_startup_message`, `encode_parse_frame`, `encode_bind_frame`, `encode_execute_frame`, `encode_sync_frame`, `encode_query_frame`), `PGCOPY` binary codec, binary `pgvector` (`f32`/`f16`/`bit`) codecs, zero-copy `strip_pg_jsonb_header` (`0x01`), `:5432` IAM auth, `PoolerMode` statement-cache gating (`0` in `transaction` mode unless `protocol_level_pooler_cache`), `effective_direct_dsn`, `compute_worker_pool_budget`, and `alloydb.enable_columnar_engine=on` session GUCs; rejects `memory://`. |
| [`adapters/mysql/`](../../../../../src/py/vertebra/db/adapters/mysql/__init__.py) | `MySQLConfig`, `MySQLCore` | Python `pymysql` (sync) and `asyncmy` (async) client transports, summed `int` `execute_many` row counts, `:3306` IAM token auth, and `to_cloud_dialer_config()` (`:3307`); rejects `memory://`. |
| [`adapters/sqlite/`](../../../../../src/py/vertebra/db/adapters/sqlite/__init__.py) / [`sqlite/mod.rs`](../../../../../src/rs/vertebra-db/src/sqlite/mod.rs) | `SQLiteConfig`, `SQLiteCore` | `SqliteConfig` pragmas (`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`, `mmap_size`), `LeaseBroker`-serialized writes when `broker` is attached, and `ArrowCursor` batch streaming. |
| [`adapters/oracle/`](../../../../../src/py/vertebra/db/adapters/oracle/__init__.py) / [`oracle/mod.rs`](../../../../../src/rs/vertebra-db/src/oracle/mod.rs)[^src-vertebra-db-src-oracle-rs] | `OracleConfig`, `OracleCore` | Native driver pending; drivers raise `MissingDependencyError`[^src-oracle-driver]. Python [`OracleWallet`](../../../../../src/py/vertebra/db/adapters/oracle/config.py)[^src-oracle-config] parses Autonomous Database wallet zip/directory archives (`from_zip_bytes`, `from_path`, `resolve_alias`), `oracle/tns.rs` handles `tnsnames.ora` and connect-descriptor parsing (`parse_tnsnames_ora`, `resolve_tns_alias`, `OracleConnectDescriptor`), and `oracle/mod.rs` provides `OracleSessionAttrs` plus Oracle 26ai binary `0xDB` `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`) and `OSON` codecs. |
| [`adapters/mssql/`](../../../../../src/py/vertebra/db/adapters/mssql/__init__.py) / [`mssql/mod.rs`](../../../../../src/rs/vertebra-db/src/mssql/mod.rs) | `MSSQLConfig`, `MSSQLCore` | Native driver pending; drivers raise `MissingDependencyError`[^src-mssql-driver]. `?` and `$1` -> `@P1..@Pn` placeholder rewriting through Rust `vertebra_db::rewrite_mssql_placeholders`, its Python binding `rewrite_mssql_placeholders`, and `MSSQLConfig.rewrite_placeholders`, plus Cloud SQL `:3307` TLS routing. |
| [`adapters/duckdb/`](../../../../../src/py/vertebra/db/adapters/duckdb/__init__.py) / [`duckdb/mod.rs`](../../../../../src/rs/vertebra-db/src/duckdb/mod.rs)[^src-vertebra-db-src-duckdb-rs] | `DuckDBConfig`, `DuckDBCore` | Requires `duckdb`; DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there[^src-duckdb-driver]. `DuckdbConfig` 2.0-safe bootstrap pragmas (`threads`, `memory_limit`, `parquet_metadata_cache`), zero-copy `ShmRegion` `ArrowCursor` streaming, and per-session `CREATE OR REPLACE SECRET vtb_gcs_adc` rotation via `applied_token_generation`. |
| [`adapters/redis/`](../../../../../src/py/vertebra/db/adapters/redis/__init__.py) & [`valkey/`](../../../../../src/py/vertebra/db/adapters/valkey/__init__.py) / [`resp3/mod.rs`](../../../../../src/rs/vertebra-db/src/resp3/mod.rs)[^src-vertebra-db-src-resp3-rs] | `RedisConfig`, `ValkeyConfig` | `RESP2`/`RESP3` framing (`HELLO 3`), `CLIENT TRACKING ON ... BCAST` invalidation LRU cache, Sentinel discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), URL `ssl`/`tls`/`resp3` parsing, and Valkey GCP Memorystore IAM auth; reject `memory://`[^src-redis-config]. |
| [`adapters/datafusion/`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) | `DataFusionConfig`, `DataFusionEngine` | Embedded Apache DataFusion SQL federation joining Parquet/CSV object stores with live relational and [GCP BigQuery & Spanner](gcp-bigquery-and-spanner-drivers.md) `ArrowCursor` tables. |

## Shared Connector, Async Drainer & Arrow Cursor

- **Transport & Lifecycle (`connection.rs`)**: [`TransportStream`](../../../../../src/rs/vertebra-db/src/connection.rs)[^src-vertebra-db-src-connection-rs] unifies TCP (`TCP_NODELAY`), `tokio-rustls` (`webpki-root-certs` + `rustls-pemfile`), Unix sockets, Cloud SQL `:3307` mTLS, and AlloyDB `:5433` mTLS + metadata exchange, mapping transport errors to `is_transient()` and SQLSTATEs (`08001`, `08006`, `57014`, `28000`).
- **5-Argument PyO3 Async Drainer (`py_async.rs`, `errors.rs`)**: [`spawn_rust_db_task_with_attr(py, fut, map_ok, map_err, attr_name)`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] attaches one `AsyncCompletionQueue` drainer per extension on the running `asyncio` loop (`_vertebra_core_completion_drainer`, `_vertebra_bigquery_completion_drainer`, `_vertebra_spanner_completion_drainer`), converts panics into `TaskPanicError`, and aborts Tokio tasks on `Future.cancel()`. [`vertebra-py/src/errors.rs`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs] defines the shared `DatabaseError` hierarchy with `sqlstate`, `vendor_code`, and `is_transient` class defaults.
- **Shared-Memory `ArrowCursor` & `ShmCapsule` (`arrow.py`, `shm.rs`, `arrow_shm.rs`, `db.rs`)**: [`ArrowCursor`](../../../../../src/py/vertebra/db/arrow.py)[^src-db-arrow-py] wraps a [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] written by [`arrow_shm.rs`](../../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-vertebra-db-src-arrow-shm-rs] (`write_shm_arrow_region_unregistered`, `ShmArrowStreamWriter::finish_unregistered`) and imported via `c"vertebra.ShmCapsule"` into [`ArrowCursor._from_shm_capsule`](../../../../../src/rs/vertebra-py/src/db.rs)[^src-vertebra-py-src-db-rs], exposing `buffer_view()`, `to_arrow_table()`, `to_polars()`, and immediate `/dev/shm` unlinking (`ShmRegion::unlink_now`) on `release()` or context-manager exit.

## Missing Dependencies & Install Hints

[`ensure_module(module_name, *, package=None, extra=None, install_package=None)`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-module-loader] raises [`MissingDependencyError(package or module_name, install_package=..., extra=...)`](../../../../../src/py/vertebra/_typing.py)[^src-missing-dependency] when an optional runtime module is absent. When `extra` is supplied without an explicit `install_package`, the install hint defaults to `vertebra-core[<extra>]` (`uv add vertebra-core[<extra>]`). The packaging gate in [`src/py/tests/unit/build/test_packaging_and_pgo.py`](../../../../../src/py/tests/unit/build/test_packaging_and_pgo.py)[^src-packaging-extras-gate] AST-scans `src/py/vertebra` and fails whenever an `ensure_module` or `MissingDependencyError` call carries an `extra=` literal absent from `[project.optional-dependencies]`, a whitespace-containing `package=` / `install_package=` / positional literal, a bracketed Vertebra distribution other than `vertebra-core[...]`, or an unhinted private `vertebra._*` companion extension; the pending Oracle and MSSQL native driver guards pass `install_package="vertebra-core[oracle]"` and `"vertebra-core[mssql]"` without `extra=`.

## Cloud SQL & AlloyDB Dialers

| Dialer | Protocol & Wire Framing |
|---|---|
| [`CloudSQLInstanceURI` / `CloudSQLDialer`](../../../../../src/py/vertebra/db/dialer/cloudsql.py)[^src-db-dialer-cloudsql-py] & [`dialer.rs`](../../../../../src/rs/vertebra-db/src/dialer.rs)[^src-vertebra-db-src-dialer-rs] | Parses `project:region:instance` (including `domain:project:region:instance`), fetches SQL Admin `v1beta4 ...:generateEphemeralCert` client certs (`refresh_lead_seconds = 240`), and connects on `:3307` TLS 1.3 with custom `project:instance` server CN verification (`IpType::{Public, Private, Psc}`). |
| [`AlloyDBClusterURI` / `AlloyDBDialer`](../../../../../src/py/vertebra/db/dialer/alloydb.py)[^src-db-dialer-alloydb-py] & [`proto.rs`](../../../../../src/rs/vertebra-core/src/proto.rs)[^src-vertebra-core-src-proto-rs] | Parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`, fetches `v1beta ...:generateClientCertificate` certs, connects on `:5433` mTLS, and frames 4-byte BE length-prefixed `MetadataExchangeRequest` / `MetadataExchangeResponse` messages (`user_agent` `0x0A`, `auth_type` `0x10` `Password=1`/`Iam=2`, `oauth2_token` `0x1A`) using `vertebra_core::proto` (`put_varint_field`, `put_bytes_field`, `read_tag`, `read_varint`, `read_length_delimited`, `skip_field`). |

## Database Driver Conformance

Driver transaction scopes roll back on `BaseException` (with async rollback shielded from AnyIO cancellation scopes); SQLite finalizes `RETURNING` cursors before reading row counts and committing, and DuckDB sums affected counts across batched statements. [`vertebra.extensions.sqlalchemy.dialects`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py)[^src-sqlalchemy-dialects] registers `+vtb` / `+vtb_async` dialects, using native transactions for relational engines and statement autocommit (`autocommit=False` rejected) for BigQuery, Spanner, and Spangres. Cross-engine parity is enforced by [`test_driver_conformance.py`](../../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-tests-integration-db-conformance-py] and tracked in the [polyglot test matrix](../../testing/polyglot-test-matrix.md#database-driver-conformance): the native module resolves before credential skips, a missing native module skips by default, `VERTEBRA_CONFORMANCE_DOCKER=1` fails the case only for engines without `pending_flow_id` (`bigquery`, `spanner`), and pending engines skip in both modes. While `vertebra._bigquery` and `vertebra._spanner` execute directly over native Rust wheels, the Python adapters for `postgres`, `mysql`, `sqlite`, `duckdb`, `redis`, and `valkey` pair `vertebra-db` Rust codecs, dialers, and `ArrowCursor` writers with Python client transports (`psycopg`/`psycopg_pool`, `pymysql`/`asyncmy`, `sqlite3`, `duckdb`, and `RESP3` streams); `oracle` and `mssql` have no transport until their native modules ship.

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — ShmRegion and ShmCapsule shared-memory primitives.
[^src-vertebra-core-src-proto-rs]: `src/rs/vertebra-core/src/proto.rs` — Zero-dependency Protobuf v3 wire helpers.
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Explicit vertebra-db Rust crate exports.
[^src-vertebra-db-src-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared-memory Arrow IPC writers and unregistered regions.
[^src-vertebra-db-src-connection-rs]: `src/rs/vertebra-db/src/connection.rs` — TransportStream, TlsMode, EndpointTarget, and Connection traits.
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension event-loop completion drainer and ShmCapsule bridge.
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Zero-sidecar Cloud SQL and AlloyDB mTLS dialers.
[^src-vertebra-db-src-postgres-rs]: `src/rs/vertebra-db/src/postgres/mod.rs` — PostgreSQL v3 wire framing and binary pgvector codecs.
[^src-vertebra-db-src-oracle-rs]: `src/rs/vertebra-db/src/oracle/mod.rs` — Oracle tnsnames.ora parser, session attributes, and 26ai VECTOR and OSON codecs.
[^src-vertebra-db-src-duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs` — DuckDB 2.0-safe SET bootstrap statements and Arrow SHM re-exports.
[^src-vertebra-db-src-resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Redis and Valkey RESP2/RESP3 protocol parser.
[^src-vertebra-py-src-db-rs]: `src/rs/vertebra-py/src/db.rs` — PyO3 ArrowCursor, dialers, and binary vector bindings.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Canonical DatabaseError and multi-base cloud exception hierarchy.
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Lazy engine-agnostic vertebra.db public exports.
[^src-db-arrow-py]: `src/py/vertebra/db/arrow.py` — ArrowCursor Python wrapper and zero-copy views.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Declarative DatabaseRegistry URL parsing and pool deduplication.
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — SyncDatabaseDriver and AsyncDatabaseDriver base hierarchy.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Runtime-checkable driver capability protocols.
[^src-db-dialer-cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py` — Cloud SQL instance URI parser and Python mTLS dialer.
[^src-db-dialer-alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py` — AlloyDB cluster URI parser and metadata exchange dialer.
[^src-sqlalchemy-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy +vtb DBAPI dialects and transaction policy.
[^src-tests-integration-db-conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-driver acceptance and conformance suite.
[^src-oracle-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet` — Python Autonomous Database wallet parser.
[^src-missing-dependency]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and default install hint.
[^src-module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Dynamic optional dependency loader.
[^src-packaging-extras-gate]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras gate verifying ensure_module extra literals.
[^src-oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
[^src-mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — SQL Server driver native-module guard.
[^src-duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver duckdb package guard.
[^src-redis-config]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis configuration and driver memory:// rejection.
