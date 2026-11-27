---
type: Guide
title: Database Drivers & Cloud Dialers
description: Per-adapter database and cache contracts, standalone driver wheels, shared Rust transport and Arrow SHM bridges, cloud IAM token providers, and Cloud SQL, AlloyDB, AWS, and Azure dialers.
status: stable
tags:
  - domains
  - database
  - drivers
  - postgres
  - oracle
  - cloud-sql
  - alloydb
  - aws-rds
  - azure-entra
  - shm
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-21T21:06:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-21T01:18:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-19T03:10:00Z"
    commit: 6f76f8a7
  - by: agent:flow-executor
    at: "2026-11-17T01:22:00Z"
    commit: 5a833832
  - by: agent:flow-completion
    at: "2026-11-14T21:38:00Z"
    commit: c526639f
  - by: agent:flow-executor
    at: "2026-11-14T20:49:30Z"
    commit: c526639f
  - by: agent:flow-setup
    at: "2026-11-14T18:43:00Z"
    commit: 1b191388
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
    title: Zero-sidecar Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra dialers plus AWS RDS Root CAs
  - id: src-dialer-base-py
    resource: src/py/vertebra/db/dialer/_base.py
    title: Shared _BaseGCPDialer, EphemeralCertBundle, and VALID_GCP_IP_TYPES primitives
  - id: src-dialer-aws-py
    resource: src/py/vertebra/db/dialer/aws.py
    title: Python SyncAwsRdsDialer, AsyncAwsRdsDialer, SyncAwsAuroraDsqlDialer, and AsyncAwsAuroraDsqlDialer
  - id: src-dialer-azure-py
    resource: src/py/vertebra/db/dialer/azure.py
    title: Python SyncAzureEntraDialer and AsyncAzureEntraDialer TLS and token dialers
  - id: src-vertebra-postgres-src-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone vertebra._postgres free-threaded extension exports
  - id: src-postgres-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: PostgreSQL SyncPostgreSQLDriver and AsyncPostgreSQLDriver re-exports
  - id: src-vertebra-mysql-src-lib-rs
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: Native MySQL and MariaDB binary wire extension crate
  - id: src-vertebra-oracle-src-lib-rs
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: Standalone vertebra._oracle gil_used=false PyO3 extension exports
  - id: src-oracle-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: Python SyncOracleDriver and AsyncOracleDriver barrel over _sync.py and _async.py
  - id: src-oracle-pipeline-py
    resource: src/py/vertebra/db/adapters/oracle/pipeline.py
    title: Delayed-commit SyncOraclePipeline and AsyncOraclePipeline
  - id: src-bigquery-object-tables-py
    resource: src/py/vertebra/db/adapters/bigquery/object_tables.py
    title: BigQuery ObjectRef and ObjectTableConfig domain helpers
  - id: src-spanner-mutations-py
    resource: src/py/vertebra/db/adapters/spanner/mutations.py
    title: Spanner StalenessBound, Mutation, and MutationGroup structs
  - id: src-mssql-driver-py
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: Python SyncMSSQLDriver and AsyncMSSQLDriver over vertebra._mssql
  - id: src-vertebra-duckdb-src-lib-rs
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal
  - id: src-duckdb-driver-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: SyncDuckDBDriver and AsyncDuckDBDriver native DuckDB 2.0 adapters
  - id: src-vertebra-sqlite-src-lib-rs
    resource: src/rs/vertebra-sqlite/src/lib.rs
    title: Native SQLite PyO3 client, WAL worker pool, and Arrow SHM bridge
  - id: src-sqlite-driver-py
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: SyncSQLiteDriver and AsyncSQLiteDriver barrel over _sync.py and _async.py
  - id: src-redis-driver-py
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: SyncRedisDriver and AsyncRedisDriver barrel over _sync.py and _async.py
  - id: src-valkey-driver-py
    resource: src/py/vertebra/db/adapters/valkey/driver.py
    title: SyncValkeyDriver and AsyncValkeyDriver barrel over _sync.py and _async.py
  - id: src-datafusion-core-py
    resource: src/py/vertebra/db/adapters/datafusion/core.py
    title: DataFusionCore session builder, source registration, and explain helpers
  - id: src-datafusion-driver-py
    resource: src/py/vertebra/db/adapters/datafusion/driver.py
    title: DataFusionEngine federated Arrow session context
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: Canonical DatabaseError and multi-base cloud exception hierarchy
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: Lazy engine-agnostic vertebra.db public exports
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: Declarative DatabaseRegistry URL parsing and pool deduplication
  - id: src-db-driver-init-py
    resource: src/py/vertebra/db/driver/__init__.py
    title: SyncDatabaseDriver and AsyncDatabaseDriver base hierarchy
  - id: src-db-driver-common-py
    resource: src/py/vertebra/db/driver/_common.py
    title: CommonDriverMixin shared checkout broker lease, trace annotation, and row hydration helpers
  - id: src-db-driver-resp-py
    resource: src/py/vertebra/db/driver/_resp.py
    title: Shared RESP3 ClusterRedirect, PubSubMessage, CRC16-XMODEM, and Resp3Error helpers
  - id: src-db-driver-cloud-auth-py
    resource: src/py/vertebra/db/driver/_cloud_auth.py
    title: AwsRdsIamTokenProvider, AwsAuroraDsqlIamTokenProvider, and AzureEntraTokenProvider
  - id: src-db-driver-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: Runtime-checkable driver capability protocols
  - id: src-tests-integration-db-conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-driver acceptance and conformance suite
  - id: src-module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Dynamic optional dependency loader and MissingDependencyError
  - id: src-packaging-extras-gate
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras gate verifying ensure_module extra literals
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Per-Adapter Layout (`src/py/vertebra/db/adapters/<engine>/`)**: All ten client/embedded adapters (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`) split sync and async drivers into `_sync.py` and `_async.py` behind a `driver.py` barrel, alongside `config.py`, `core.py`, and engine-specific modules (`pipeline.py`, `object_tables.py`, `mutations.py`, `notify.py`). Embedded `datafusion` uses `DataFusionCore` (`mode="in_process"`), and `vertebra.db` lazily exports only engine-agnostic bases, dialers, `ArrowCursor`, `DatabaseRegistry`, CDC, and RLS primitives [^src-db-init-py] [^src-datafusion-core-py].
- **Cursor Surface, Single-Call Methods & Shared Driver Mixins (`db/driver/`)**: Every SQL driver exposes `cursor()` returning the extension's PEP 249-shaped cursor. SQLite, DuckDB, PostgreSQL, and Oracle call `fetch_rows`, `fetch_dicts`, `fetch_value`, or `execute_dml` directly for query methods; MySQL, MSSQL, Spanner, and BigQuery run queries on the cursor. `CommonDriverMixin` (`_common.py`) attaches per-connection checkout broker leases for network pools (embedded SQLite and DuckDB take none). `_resp.py` centralizes RESP3 cluster/PubSub helpers, and `_cloud_auth.py` implements AWS RDS, Aurora DSQL, and Azure Entra token providers [^src-db-driver-common-py] [^src-db-driver-resp-py] [^src-db-driver-cloud-auth-py].
- **Companion Wheels & Shared Async Bridge**: Optional driver wheels (`vertebra._postgres`, `_mysql`, `_oracle`, `_mssql`, `_duckdb`, `_sqlite`, `_bigquery`, `_spanner`, `_redis`, `_valkey`) load through `ensure_module` (raising `MissingDependencyError` with `vertebra-core[<extra>]`). `vertebra-db` (`connection.rs`, `py_async.rs`) provides stream transport, per-extension completion drainers, and `ShmCapsule` `ArrowCursor` handoff; `postgres`, `mysql`, `redis`, and `valkey` reject `memory://` [^src-module-loader] [^src-vertebra-db-src-connection-rs] [^src-vertebra-db-src-py-async-rs].
- **Cloud Wire Extensions & Dialers**: `vertebra-postgres` handles CockroachDB (`--cluster=<id>`, `RESET ALL`, `SAVEPOINT cockroach_restart`) and AlloyDB (`v1beta`/`v1alpha` fallback, PSC DNS precedence, `MetadataExchangeResponse`). [`dialer.rs`](../../../../../src/rs/vertebra-db/src/dialer.rs)[^src-vertebra-db-src-dialer-rs] and `vertebra.db.dialer` bundle 114 AWS RDS Root CAs alongside Cloud SQL, AlloyDB, AWS RDS/Aurora DSQL, and Azure Entra dialers.
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] define Vertebra's engine-neutral database contracts, companion PyO3 wheel seams, `VTBARROW` `ArrowCursor` transport, wire codecs, cloud IAM token providers, and direct cloud dialers.

Related chapters: [PostgreSQL Native Extension](postgres-native-driver.md), [Google Cloud BigQuery & Spanner Native Drivers](gcp-bigquery-and-spanner-drivers.md), [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Native Pipelines, Change Feeds, Tenant Predicates & DataFusion](query-pipeline-cdc-and-tenancy.md), [PostgreSQL Limitations](postgres-limitations.md), and [Database Driver Limitations](database-driver-limitations.md).

## Registry, Shared Driver Surface & Cloud Auth

- **URL Dispatch & Pool Deduplication**: [`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] builds `SCHEME_REGISTRY` from `REGISTERED_CONFIG_TYPES` (`BaseDatabaseConfig.url_schemes`). `parse_url` resolves all ten engine schemes plus `datafusion`, `cloudsql`, and `alloydb`. `get_shared_driver` and `get_shared_sync_driver` deduplicate by `config.pool_key()` (`None` isolates `:memory:` SQLite/DuckDB), while `provide_driver` rejects `CloudSQLConfig` / `AlloyDBConfig` with `TypeError` (use `get_dialer`).
- **Standard Driver Methods**: [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] standardizes `cursor()`, `execute`, `execute_dml`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one` (`NotFoundError`), `select_one_or_none`, `select_value`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `cancel()`, and `close()`. [`capabilities.py`](../../../../../src/py/vertebra/db/driver/capabilities.py)[^src-db-driver-capabilities-py] defines runtime-checkable protocols for pipelines, notifications, change streams, queue stores, and embedding pushdown.
- **Checkout Broker Leases**: [`CommonDriverMixin`](../../../../../src/py/vertebra/db/driver/_common.py)[^src-db-driver-common-py] manages per-connection checkout leases (`_attach_checkout_lease`, `_acquire_broker_lease`, `_lease_plan`) for network adapters (`postgres`, `mysql`, `mssql`, `oracle`). Embedded engines (`sqlite`, `duckdb`) take no broker lease.
- **AWS & Azure IAM Token Providers**: [`_cloud_auth.py`](../../../../../src/py/vertebra/db/driver/_cloud_auth.py)[^src-db-driver-cloud-auth-py] implements `DatabaseTokenProvider` with pre-expiry refresh margins and temporary credential expiry clamping:
  - `AwsRdsIamTokenProvider` (`ttl <= 900s`) and `AwsAuroraDsqlIamTokenProvider` (`ttl <= 604800s`) resolve `AWS_*` env vars, `~/.aws/{credentials,config}`, Web Identity STS XML, allowlisted container credential endpoints, and EC2 IMDSv2 off-loop via `anyio.to_thread.run_sync`.
  - `AzureEntraTokenProvider` (`AZURE_{POSTGRES,MYSQL,MSSQL,REDIS}_SCOPE`) supports client secrets, ambient `AZURE_*` env vars, `AZURE_FEDERATED_TOKEN_FILE`, App Service `IDENTITY_ENDPOINT`, and VM IMDS.

### Single-Call Native Methods

SQLite, DuckDB, PostgreSQL, and Oracle clients (and PostgreSQL/Oracle pool classes) expose four single-call methods that run through the same statement runner as `cursor()` without keeping cursor state:

| Single-call method | Returns | Driver methods that call it |
| --- | --- | --- |
| `fetch_rows(sql, params=())` | `(rows, names)`; `([], ())` with no result set | `execute_with_description`, `execute_tuples`, and `select*` with `schema_type` |
| `fetch_dicts(sql, params=())` | One `dict` per row keyed by column name | `select`, `select_one`, `select_one_or_none` without `schema_type` (or `dict`) |
| `fetch_value(sql, params=())` | First column of first row; `NotFoundError` if empty | `select_value` |
| `execute_dml(sql, params=())` | `(rowcount, lastrowid)` | `execute` and `execute_dml` |

Pool methods check a connection out, execute the statement, and return the connection inside the extension. MySQL, SQL Server, Spanner, and BigQuery run their query methods on the cursor. On Oracle, out-bind values and session-context tracking come back only from `execute_dml` (called by `execute`), so statements needing both rows and out-binds must use `cursor()`.

## Relational, Analytical & Cache Adapters

| Adapter & Crate | Layout | Wire & Runtime Behavior |
|---|---|---|
| [`adapters/postgres/`](../../../../../src/py/vertebra/db/adapters/postgres/__init__.py)[^src-postgres-driver-py] / [`vertebra-postgres`](../../../../../src/rs/vertebra-postgres/src/lib.rs)[^src-vertebra-postgres-src-lib-rs] | `config.py`, `core.py`, `_sync.py`/`_async.py`, `binds.py`, `options.py`, `change_stream.py` | Protocol 3.0 (PG 14–18), SCRAM-SHA-256-PLUS, Cloud SQL (`:3307`), AlloyDB (`:5433`), CockroachDB, AWS/Azure IAM tokens, `PoolerMode` LRU `StatementCache`, `PGCOPY`, `VTBARROW`, `LISTEN/NOTIFY`, and logical replication; rejects `memory://`. |
| [`adapters/mysql/`](../../../../../src/py/vertebra/db/adapters/mysql/__init__.py) / [`vertebra-mysql`](../../../../../src/rs/vertebra-mysql/src/lib.rs)[^src-vertebra-mysql-src-lib-rs] | `config.py`, `core.py`, `_sync.py`/`_async.py`, `pipeline.py` | Handshake v10, binary prepared statements, MySQL 9 `VECTOR(N)`, pipelines, `VTBARROW`, AWS/Azure IAM, Cloud SQL `:3307` mTLS, broker leasing, and `KILL QUERY`. |
| [`adapters/sqlite/`](../../../../../src/py/vertebra/db/adapters/sqlite/__init__.py)[^src-sqlite-driver-py] / [`vertebra-sqlite`](../../../../../src/rs/vertebra-sqlite/src/lib.rs)[^src-vertebra-sqlite-src-lib-rs] | `config.py`, `core.py`, `_sync.py`/`_async.py`, `pipeline.py` | Bundled SQLite (`rusqlite`), 1-writer/multi-reader WAL pool, autocommit `execute_tuples` outside transactions, pipelines, `VTBARROW`, online backup/restore, and `InterruptHandle`. |
| [`adapters/oracle/`](../../../../../src/py/vertebra/db/adapters/oracle/driver.py)[^src-oracle-driver-py] / [`vertebra-oracle`](../../../../../src/rs/vertebra-oracle/src/lib.rs)[^src-vertebra-oracle-src-lib-rs] | `config.py`, `core.py`, `_sync.py`/`_async.py`, [`pipeline.py`](../../../../../src/py/vertebra/db/adapters/oracle/pipeline.py)[^src-oracle-pipeline-py] | Thin TNS/TTC, `.zip`/`ewallet.pem` mTLS + IAM, optional `enable_reset_relay` (default `True`), DRCP, 26ai `VECTOR`/`OSON`, `VTBARROW`, delayed-commit pipelines, and TxEventQ notifications. |
| [`adapters/mssql/`](../../../../../src/py/vertebra/db/adapters/mssql/driver.py)[^src-mssql-driver-py] / [`vertebra-mssql`](../../../../../src/rs/vertebra-mssql/src/lib.rs) | `config.py`, `core.py`, `_sync.py`/`_async.py`, `pipeline.py` | TDS 7.4 (`tiberius` + `rustls`), Azure Entra `FEDAUTH` (`set_aad_token`), `sp_executesql` RPC with `@P1` rewriter, `BulkLoad`, `ATTENTION` cancel, `VTBARROW`, and Cloud SQL `:3307` mTLS. |
| [`adapters/duckdb/`](../../../../../src/py/vertebra/db/adapters/duckdb/driver.py)[^src-duckdb-driver-py] / [`vertebra-duckdb`](../../../../../src/rs/vertebra-duckdb/src/lib.rs)[^src-vertebra-duckdb-src-lib-rs] | `config.py`, `core.py`, `_sync.py`/`_async.py`, `pipeline.py` | Bundled DuckDB 2.0, single-writer/multi-reader pool, `TxJournal` savepoints, pipelines, `VTBARROW`, `Appender` bulk load, `InterruptHandle`, and GCS `CREATE SECRET` rotation with 401/403 refresh. |
| [`adapters/redis/`](../../../../../src/py/vertebra/db/adapters/redis/driver.py)[^src-redis-driver-py] & [`valkey/`](../../../../../src/py/vertebra/db/adapters/valkey/driver.py)[^src-valkey-driver-py] | `config.py`, `core.py`, `_sync.py`/`_async.py`, `notify.py`, [`_resp.py`](../../../../../src/py/vertebra/db/driver/_resp.py)[^src-db-driver-resp-py] | Standalone `_redis` and `_valkey` wheels over `vertebra-resp`: RESP2/RESP3, Azure Entra & Valkey IAM, `slot_map()` Cluster routing, Sentinel failover preserving TLS/tracking/IAM; reject `memory://`. |
| [`adapters/datafusion/`](../../../../../src/py/vertebra/db/adapters/datafusion/__init__.py) | `config.py`, [`core.py`](../../../../../src/py/vertebra/db/adapters/datafusion/core.py)[^src-datafusion-core-py], [`driver.py`](../../../../../src/py/vertebra/db/adapters/datafusion/driver.py)[^src-datafusion-driver-py] | Embedded `SessionContext` with `target_partitions` and `memory_limit_mb`, registering Parquet, CSV, NDJSON, object-store Parquet, and `ArrowCursor`/PyArrow tables, with planner-backed `explain_plan`. |

BigQuery ([`object_tables.py`](../../../../../src/py/vertebra/db/adapters/bigquery/object_tables.py)[^src-bigquery-object-tables-py]) and Spanner ([`mutations.py`](../../../../../src/py/vertebra/db/adapters/spanner/mutations.py)[^src-spanner-mutations-py]) are covered in [Google Cloud BigQuery & Spanner Native Drivers](gcp-bigquery-and-spanner-drivers.md).

## Shared Transport, Drainers & Cloud Dialers

- **Transport & Error Normalization**: [`connection.rs`](../../../../../src/rs/vertebra-db/src/connection.rs)[^src-vertebra-db-src-connection-rs] normalizes TCP, `rustls`, Unix socket, Cloud SQL `:3307`, and AlloyDB `:5433` streams with `is_transient()` and SQLSTATE mapping. Canonical `DatabaseError` and cloud exception types live in [`errors.rs`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs], and Protobuf v3 wire helpers live in [`proto.rs`](../../../../../src/rs/vertebra-core/src/proto.rs)[^src-vertebra-core-src-proto-rs].
- **Completion Drainers & Arrow SHM**: [`py_async.rs`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] attaches per-extension `AsyncCompletionQueue` drainers, maps worker panics to `TaskPanicError`, ties Python future cancellation to Tokio `AbortHandle` or cancel hooks, and wraps [`ShmRegion`](../../../../../src/rs/vertebra-core/src/shm.rs)[^src-vertebra-core-src-shm-rs] ownership in `vertebra.ShmCapsule`. [`arrow_shm.rs`](../../../../../src/rs/vertebra-db/src/arrow_shm.rs)[^src-vertebra-db-src-arrow-shm-rs] writes `VTBARROW` IPC frames; `ArrowCursor.release()`, `close()`, `__exit__()`, and `Drop` unlink the segment.
- **Missing Dependency Guards**: [`ensure_module`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-module-loader] raises `MissingDependencyError` with `install_package="vertebra-core[<extra>]"`, verified by AST scanning in [`test_packaging_and_pgo.py`](../../../../../src/py/tests/unit/build/test_packaging_and_pgo.py)[^src-packaging-extras-gate].
- **Cloud Dialers**: [`dialer/_base.py`](../../../../../src/py/vertebra/db/dialer/_base.py)[^src-dialer-base-py] (`_BaseGCPDialer`, `EphemeralCertBundle`) shares ephemeral certificate refresh, RSA keygen, verified TLS 1.3 mTLS, IAM propagation, and jittered exponential retries (`connector_retry_count=3`, `0.5s`–`5.0s`) across Cloud SQL, AlloyDB, and SQL configs. [`aws.py`](../../../../../src/py/vertebra/db/dialer/aws.py)[^src-dialer-aws-py] and [`azure.py`](../../../../../src/py/vertebra/db/dialer/azure.py)[^src-dialer-azure-py] provide `verify-full` TLS contexts (defaulting AWS RDS to the bundled 114-cert `aws_rds_root_ca_pem()`) and `connection_kwargs()`.

## Conformance Testing

Operational paths catch `Exception` (preserving `asyncio.CancelledError`, `KeyboardInterrupt`, and `SystemExit`), while transactions roll back on `BaseException` under `anyio.CancelScope(shield=True)`.

Cross-engine parity is enforced by [`test_driver_conformance.py`](../../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-tests-integration-db-conformance-py] and `test_{duckdb,mysql,mssql,oracle}_native.py` under zero-skip `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`) and `ServiceVersionMatrix`, covering DuckDB, MySQL (`8.0`–`9.7`), MariaDB (`11.3`–`13.0`), MSSQL (`2022`, `2025`), Oracle (`23ai`/`26ai Free`), SQLite, Redis, Valkey, PostgreSQL (`14`–`18`), BigQuery, and Spanner.

[^src-vertebra-core-src-shm-rs]: `src/rs/vertebra-core/src/shm.rs` — ShmRegion and ShmCapsule shared-memory primitives.
[^src-vertebra-core-src-proto-rs]: `src/rs/vertebra-core/src/proto.rs` — Zero-dependency Protobuf v3 wire helpers.
[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Explicit vertebra-db Rust crate exports.
[^src-vertebra-db-src-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared-memory Arrow IPC writers and unregistered regions.
[^src-vertebra-db-src-connection-rs]: `src/rs/vertebra-db/src/connection.rs` — TransportStream, TlsMode, EndpointTarget, and Connection traits.
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Per-extension event-loop completion drainer and ShmCapsule bridge.
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Cloud SQL, AlloyDB, AWS RDS, Aurora DSQL, and Azure Entra dialers plus AWS RDS Root CAs.
[^src-dialer-base-py]: `src/py/vertebra/db/dialer/_base.py` — Shared `_BaseGCPDialer`, `EphemeralCertBundle`, and `VALID_GCP_IP_TYPES` primitives.
[^src-dialer-aws-py]: `src/py/vertebra/db/dialer/aws.py` — Python `SyncAwsRdsDialer`, `AsyncAwsRdsDialer`, `SyncAwsAuroraDsqlDialer`, and `AsyncAwsAuroraDsqlDialer`.
[^src-dialer-azure-py]: `src/py/vertebra/db/dialer/azure.py` — Python `SyncAzureEntraDialer` and `AsyncAzureEntraDialer` TLS and token dialers.
[^src-vertebra-postgres-src-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` extension exports.
[^src-postgres-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py` — PostgreSQL `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` re-exports.
[^src-vertebra-mysql-src-lib-rs]: `src/rs/vertebra-mysql/src/lib.rs` — Native MySQL and MariaDB binary wire extension crate.
[^src-vertebra-oracle-src-lib-rs]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` `gil_used=false` PyO3 extension exports.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py` — Python `SyncOracleDriver` and `AsyncOracleDriver` barrel over `_sync.py` and `_async.py`.
[^src-oracle-pipeline-py]: `src/py/vertebra/db/adapters/oracle/pipeline.py` — Delayed-commit `SyncOraclePipeline` and `AsyncOraclePipeline`.
[^src-bigquery-object-tables-py]: `src/py/vertebra/db/adapters/bigquery/object_tables.py` — BigQuery `ObjectRef` and `ObjectTableConfig` domain helpers.
[^src-spanner-mutations-py]: `src/py/vertebra/db/adapters/spanner/mutations.py` — Spanner `StalenessBound`, `Mutation`, and `MutationGroup` structs.
[^src-mssql-driver-py]: `src/py/vertebra/db/adapters/mssql/driver.py` — Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` over `vertebra._mssql`.
[^src-vertebra-duckdb-src-lib-rs]: `src/rs/vertebra-duckdb/src/lib.rs` — Native DuckDB 2.0 PyO3 client, Arrow SHM writer, Appender, and savepoint journal.
[^src-duckdb-driver-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver` and `AsyncDuckDBDriver` native DuckDB 2.0 adapters.
[^src-vertebra-sqlite-src-lib-rs]: `src/rs/vertebra-sqlite/src/lib.rs` — Native SQLite PyO3 client, WAL worker pool, and Arrow SHM bridge.
[^src-sqlite-driver-py]: `src/py/vertebra/db/adapters/sqlite/driver.py` — `SyncSQLiteDriver` and `AsyncSQLiteDriver` barrel over `_sync.py` and `_async.py`.
[^src-redis-driver-py]: `src/py/vertebra/db/adapters/redis/driver.py` — `SyncRedisDriver` and `AsyncRedisDriver` barrel over `_sync.py` and `_async.py`.
[^src-valkey-driver-py]: `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver` barrel over `_sync.py` and `_async.py`.
[^src-datafusion-core-py]: `src/py/vertebra/db/adapters/datafusion/core.py` — `DataFusionCore` session builder, source registration, and explain helpers.
[^src-datafusion-driver-py]: `src/py/vertebra/db/adapters/datafusion/driver.py` — `DataFusionEngine` federated Arrow session context.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Canonical `DatabaseError` and multi-base cloud exception hierarchy.
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Lazy engine-agnostic `vertebra.db` public exports.
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Declarative `DatabaseRegistry` URL parsing and pool deduplication.
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — `SyncDatabaseDriver` and `AsyncDatabaseDriver` base hierarchy.
[^src-db-driver-common-py]: `src/py/vertebra/db/driver/_common.py` — `CommonDriverMixin` shared checkout broker lease, trace annotation, and row hydration helpers.
[^src-db-driver-resp-py]: `src/py/vertebra/db/driver/_resp.py` — Shared RESP3 `ClusterRedirect`, `PubSubMessage`, CRC16-XMODEM, and `Resp3Error` helpers.
[^src-db-driver-cloud-auth-py]: `src/py/vertebra/db/driver/_cloud_auth.py` — `AwsRdsIamTokenProvider`, `AwsAuroraDsqlIamTokenProvider`, and `AzureEntraTokenProvider`.
[^src-db-driver-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — Runtime-checkable driver capability protocols.
[^src-tests-integration-db-conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-driver acceptance and conformance suite.
[^src-module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Dynamic optional dependency loader.
[^src-packaging-extras-gate]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras gate verifying `ensure_module` extra literals.
