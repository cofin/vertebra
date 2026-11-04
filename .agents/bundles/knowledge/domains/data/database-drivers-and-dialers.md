---
type: Guide
title: "Database Drivers & Cloud Dialers"
description: "The `vertebra-db` and `vertebra.db` Rust-native shared-memory drivers across PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey, the `vertebra.db.driver` base hierarchy, `ArrowCursor`, `DatabaseRegistry`, and Cloud SQL / AlloyDB dialers."
status: stable
tags: [domains, database, drivers, bigquery, spanner, cloud-sql, alloydb, arrow, shm]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-db-src-lib-rs
    resource: src/rs/vertebra-db/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/lib.rs)"
  - id: src-vertebra-db-src-postgres-rs
    resource: src/rs/vertebra-db/src/postgres.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/postgres.rs)"
  - id: src-vertebra-db-src-mysql-rs
    resource: src/rs/vertebra-db/src/mysql.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/mysql.rs)"
  - id: src-vertebra-db-src-sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/sqlite.rs)"
  - id: src-vertebra-db-src-oracle-rs
    resource: src/rs/vertebra-db/src/oracle.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/oracle.rs)"
  - id: src-vertebra-db-src-tns-rs
    resource: src/rs/vertebra-db/src/tns.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/tns.rs)"
  - id: src-vertebra-db-src-mssql-rs
    resource: src/rs/vertebra-db/src/mssql.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/mssql.rs)"
  - id: src-vertebra-db-src-duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/duckdb.rs)"
  - id: src-vertebra-db-src-gcp-rs
    resource: src/rs/vertebra-db/src/gcp.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/gcp.rs)"
  - id: src-vertebra-db-src-bigquery-rs
    resource: src/rs/vertebra-db/src/bigquery.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/bigquery.rs)"
  - id: src-vertebra-db-src-spanner-rs
    resource: src/rs/vertebra-db/src/spanner.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/spanner.rs)"
  - id: src-vertebra-db-src-resp3-rs
    resource: src/rs/vertebra-db/src/resp3.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/resp3.rs)"
  - id: src-vertebra-db-src-dialer-rs
    resource: src/rs/vertebra-db/src/dialer.rs
    title: "Rust implementation module (src/rs/vertebra-db/src/dialer.rs)"
  - id: src-vertebra-py-src-completion-rs
    resource: src/rs/vertebra-py/src/completion.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/completion.rs)"
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/errors.rs)"
  - id: src-vertebra-db-tests-gcp-credentials-tests-rs
    resource: src/rs/vertebra-db/tests/gcp_credentials_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/gcp_credentials_tests.rs)"
  - id: src-vertebra-db-tests-spanner-wire-tests-rs
    resource: src/rs/vertebra-db/tests/spanner_wire_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/spanner_wire_tests.rs)"
  - id: src-vertebra-db-tests-bigquery-wire-tests-rs
    resource: src/rs/vertebra-db/tests/bigquery_wire_tests.rs
    title: "Rust implementation module (src/rs/vertebra-db/tests/bigquery_wire_tests.rs)"
  - id: src-db-init-py
    resource: src/py/vertebra/db/__init__.py
    title: "Python implementation module (src/py/vertebra/db/__init__.py)"
  - id: src-db-registry-py
    resource: src/py/vertebra/db/_registry.py
    title: "Python implementation module (src/py/vertebra/db/_registry.py)"
  - id: src-db-driver-init-py
    resource: src/py/vertebra/db/driver/__init__.py
    title: "Python implementation module (src/py/vertebra/db/driver/__init__.py)"
  - id: src-db-driver-common-py
    resource: src/py/vertebra/db/driver/_common.py
    title: "Python implementation module (src/py/vertebra/db/driver/_common.py)"
  - id: src-db-driver-sync-py
    resource: src/py/vertebra/db/driver/_sync.py
    title: "Python implementation module (src/py/vertebra/db/driver/_sync.py)"
  - id: src-db-driver-async-py
    resource: src/py/vertebra/db/driver/_async.py
    title: "Python implementation module (src/py/vertebra/db/driver/_async.py)"
  - id: src-db-postgres-init-py
    resource: src/py/vertebra/db/postgres/__init__.py
    title: "Python implementation module (src/py/vertebra/db/postgres/__init__.py)"
  - id: src-db-mysql-init-py
    resource: src/py/vertebra/db/mysql/__init__.py
    title: "Python implementation module (src/py/vertebra/db/mysql/__init__.py)"
  - id: src-db-sqlite-init-py
    resource: src/py/vertebra/db/sqlite/__init__.py
    title: "Python implementation module (src/py/vertebra/db/sqlite/__init__.py)"
  - id: src-db-oracle-init-py
    resource: src/py/vertebra/db/oracle/__init__.py
    title: "Python implementation module (src/py/vertebra/db/oracle/__init__.py)"
  - id: src-db-mssql-init-py
    resource: src/py/vertebra/db/mssql/__init__.py
    title: "Python implementation module (src/py/vertebra/db/mssql/__init__.py)"
  - id: src-db-duckdb-init-py
    resource: src/py/vertebra/db/duckdb/__init__.py
    title: "Python implementation module (src/py/vertebra/db/duckdb/__init__.py)"
  - id: src-db-bigquery-init-py
    resource: src/py/vertebra/db/bigquery/__init__.py
    title: "Python implementation module (src/py/vertebra/db/bigquery/__init__.py)"
  - id: src-db-spanner-init-py
    resource: src/py/vertebra/db/spanner/__init__.py
    title: "Python implementation module (src/py/vertebra/db/spanner/__init__.py)"
  - id: src-db-redis-init-py
    resource: src/py/vertebra/db/redis/__init__.py
    title: "Python implementation module (src/py/vertebra/db/redis/__init__.py)"
  - id: src-db-valkey-init-py
    resource: src/py/vertebra/db/valkey/__init__.py
    title: "Python implementation module (src/py/vertebra/db/valkey/__init__.py)"
  - id: src-db-dialer-cloudsql-py
    resource: src/py/vertebra/db/dialer/cloudsql.py
    title: "Python implementation module (src/py/vertebra/db/dialer/cloudsql.py)"
  - id: src-db-dialer-alloydb-py
    resource: src/py/vertebra/db/dialer/alloydb.py
    title: "Python implementation module (src/py/vertebra/db/dialer/alloydb.py)"
  - id: src-tests-unit-db-test-native-db-drivers-py
    resource: src/py/tests/unit/db/test_native_db_drivers.py
    title: "Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py)"
  - id: src-tests-unit-db-test-enterprise-olap-drivers-py
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: "Python implementation module (src/py/tests/unit/db/test_enterprise_olap_drivers.py)"
  - id: src-tests-unit-db-test-redis-valkey-resp3-py
    resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
    title: "Python implementation module (src/py/tests/unit/db/test_redis_valkey_resp3.py)"
  - id: src-tests-unit-db-test-bigquery-py
    resource: src/py/tests/unit/db/test_bigquery.py
    title: "Python implementation module (src/py/tests/unit/db/test_bigquery.py)"
  - id: src-tests-unit-db-test-spanner-py
    resource: src/py/tests/unit/db/test_spanner.py
    title: "Python implementation module (src/py/tests/unit/db/test_spanner.py)"
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Structured Per-Driver Package Layout (`src/py/vertebra/db/<driver>/{config,core,driver,__init__}.py` & `src/py/vertebra/db/driver/`)**: Every built-in database and cache driver in `vertebra.db` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`) is organized as a package with `config.py` (`msgspec.Struct(..., frozen=True, gc=False, kw_only=True)` inheriting `BaseDatabaseConfig`), `core.py` (compiled domain core class encapsulating wire codecs, parameter normalization, and SQL rewrites), `driver.py` (`Sync*Driver(SyncDatabaseDriver)` and `Async*Driver(AsyncDatabaseDriver)`), and `__init__.py`.
- **Binary Vector Wire Codecs (`postgres.rs`, `oracle.rs`, `vertebra-py/src/db.rs`)**: PostgreSQL (`postgres.rs`) encodes and decodes binary `pgvector` (`vector` `f32`, `halfvec` `f16`, and `bit` bit-packed) frames without text formatting, while Oracle 26ai (`oracle.rs`, `vertebra-py/src/db.rs`) encodes and decodes binary `0xDB` `VECTOR` frames directly over PEP 3118 `PyBuffer` views (`encode_oracle_vector_binary`, `decode_oracle_vector_binary`).
- **Per-Session DuckDB `httpfs` Secret Rotation (`duckdb/`, `duckdb.rs`)**: Each pooled DuckDB connection tracks `applied_token_generation: u64` against the shared [`SyncGcpTokenCache` / `AsyncGcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs) and executes `CREATE OR REPLACE SECRET vtb_gcs_adc (TYPE GCS, BEARER_TOKEN '...')` whenever the cached ADC token generation advances.
- **Official Google Cloud Rust Clients for Spanner & BigQuery Storage (`gcp.rs`, `spanner.rs`, `bigquery.rs`)**: `SpannerClient` and `BigQueryClient` delegate gRPC Data, Database Admin, Storage Read, and Storage Write APIs to `google-cloud-spanner` (`=0.34.8-preview`) and `google-cloud-bigquery` (`=0.19.0`) bridged from `SyncGcpTokenCache` / `AsyncGcpTokenCache` via `vertebra_db::gcp::{GcpTokenCredentials, official_credentials}`. Spanner DDL executes over REST (`google_cloud_spanner::client::DatabaseAdmin`) with `admin_endpoint` resolution (`*.googleapis.com` → `https://spanner.googleapis.com`; emulator `:9010` → `:9020`; explicit `admin_endpoint` required otherwise) and LRO polling via `DatabaseAdmin::get_operation` (`google_cloud_longrunning::model::operation::Result`). `SpannerConfig.ca_pem` configures Spanner Omni TLS for non-`*.googleapis.com` HTTPS endpoints and is rejected for `*.googleapis.com` or `http://` endpoints; `BigQueryConfig.ca_pem` applies to REST v2 calls only. `ArrowAppendPlan::build` splits Arrow IPC streams into `ArrowAppendPlan { schema, requests }` with one record batch per `ArrowAppendRequest` (passing `schema` once to `open_default_stream(...).build_arrow(schema)`, rejecting dictionary batches and trailing bytes, and verifying response counts and offsets via `ArrowAppendPlan::verify_responses`). Unit tests exercise `Spanner::from_stub`, `DatabaseAdmin::from_stub`, `Read::from_stub`, and pure `ArrowAppendPlan` verification without TCP sockets.
- **Typed Google Cloud Exception Hierarchy, Completion Bridge & Auth Resilience (`vertebra-py/src/{errors,completion}.rs`, `vertebra-core/src/gcp_auth.rs`)**: `vertebra_py::errors` creates `GoogleCloudError(VertebraError)` and subclasses `GoogleCloudAuthError` (`code = 16`), `SpannerError`, `SpannerAbortedError` (`code = 10`), `BigQueryError`, and `BigQueryRowError` (`code = 3`, `row_errors: list[tuple[int, int, str]]`) via `PyErr::new_type` at module init, populating `code` (`0..=16`), `code_name`, `message`, `http_status`, `retry_delay_ms`, and `reason` on both sync and async raises. Python fallbacks check canonical codes (`SpannerCore.is_partitioned_query_fallback_error` for `{3, 7, 9, 12, 14}`; `BigQueryCore.is_storage_api_fallback_error` for `{7, 9, 12, 14}`) and `run_in_transaction` retries only `SpannerAbortedError`. `spawn_rust_db_task` pairs a worker task (`AbortHandle` wired to `_DbTaskCanceller` on the Python future) with a completion task that translates panics in unwinding builds (`make dev` / test profiles; release builds set `panic = "abort"`) into `TaskPanicError`. `GcpTokenCache::from_options(raw_opts, default_scopes)` requires explicit default OAuth scopes from each caller (`SPANNER_OAUTH_SCOPES`, `BIGQUERY_OAUTH_SCOPES`, `GCS_OAUTH_SCOPES`, `DEFAULT_GCP_SCOPES`), honors `ProxyPolicy` (`Environment`, `Direct`, `Explicit(reqwest::Proxy)`) on token/IAM client calls while keeping metadata-server calls direct (`no_proxy()`), and runs `GcpAuthSource::GcloudCli { binary, timeout, .. }` via `tokio::process::Command` with `kill_on_drop(true)` and `GCLOUD_CLI_TIMEOUT` (`30s`).
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs)[^src-vertebra-db-src-lib-rs] and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py)[^src-db-init-py] implement Vertebra's Tier-1 shared-memory database and cache drivers, the [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py)[^src-db-driver-init-py] base hierarchy (`BaseDatabaseConfig`, `CommonDriverMixin`, `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`), zero-copy `ShmRegion` Arrow IPC cursors, and Cloud SQL / AlloyDB mTLS and IAM token dialers. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

## 1. Registry (`_registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/_registry.py)[^src-db-registry-py] maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgreSQLConfig`, `mysql`/`mariadb` → `MySQLConfig`, `sqlite` → `SQLiteConfig` (`:memory:` for an empty path), `oracle` → `OracleConfig`, `mssql`/`sqlserver` → `MSSQLConfig`, `duckdb` → `DuckDBConfig`, `bigquery` → `BigQueryConfig`, `spanner`/`spangres` → `SpannerConfig` (via `SpannerConfig.from_dsn`; `spanner+postgresql` raises `ImproperConfigurationError`), `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `cloudsql` → `CloudSQLConfig`, `alloydb` → `AlloyDBConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_sync_driver(name)` builds the `Sync*Driver` or `Sync*Dialer` and `get_async_driver(name)` builds the `Async*Driver` or `Async*Dialer` that matches the config type.
- [`vertebra.extensions.sqlalchemy.dialects`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) integrates the relational and analytical drivers into SQLAlchemy 2.1 via the `+vtb` dialect suffix (`{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb,vertebra_async,vtb_async}`).

## 2. Driver Implementations (`vertebra-db` & `vertebra.db`)

| Package / Module | Config & Core | Driver behavior |
|---|---|---|
| [`postgres/`](../../../../../src/py/vertebra/db/postgres/__init__.py)[^src-db-postgres-init-py] / [`postgres.rs`](../../../../../src/rs/vertebra-db/src/postgres.rs)[^src-vertebra-db-src-postgres-rs] | `PostgreSQLConfig`, `PostgreSQLCore`, `PoolerMode = Literal["direct", "transaction", "session"]` | Native v3 wire framing (`StartupMessage`, `SASLInitialResponse` / `SASLResponse` SCRAM-SHA-256, `Parse`/`Bind`/`Describe`/`Execute`/`Sync`, binary `DataRow`, `CopyOutResponse`). `PostgreSQLCore.strip_jsonb_header` validates the `0x01` JSONB version byte and returns a zero-copy `memoryview` slice. Supports `execute_arrow_shm()` into `ArrowCursor` and direct `:5432` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`mysql/`](../../../../../src/py/vertebra/db/mysql/__init__.py)[^src-db-mysql-init-py] / [`mysql.rs`](../../../../../src/rs/vertebra-db/src/mysql.rs)[^src-vertebra-db-src-mysql-rs] | `MySQLConfig`, `MySQLCore` | Handshake v10 packet parsing, `caching_sha2_password` / `mysql_native_password` / `mysql_clear_password` auth, `COM_QUERY` and `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` dispatch, length-encoded integer and binary result row decoding, `execute_arrow_shm()`, and direct `:3306` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`sqlite/`](../../../../../src/py/vertebra/db/sqlite/__init__.py)[^src-db-sqlite-init-py] / [`sqlite.rs`](../../../../../src/rs/vertebra-db/src/sqlite.rs)[^src-vertebra-db-src-sqlite-rs] | `SQLiteConfig`, `SQLiteCore` | Opens WAL-mode connections (`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`, `mmap_size`, `temp_store=MEMORY`), serializes writes through `LeaseBroker` slots when `broker` is attached, and streams `execute_arrow_shm()` via `ArrowCursor`. |
| [`oracle/`](../../../../../src/py/vertebra/db/oracle/__init__.py)[^src-db-oracle-init-py] / [`oracle.rs`](../../../../../src/rs/vertebra-db/src/oracle.rs)[^src-vertebra-db-src-oracle-rs] | `OracleConfig`, `OracleCore` | Pure-Rust Thin TNS/TTC framing (zero ODPI-C / Instant Client dependency), Autonomous Database mTLS `ewallet.pem` + `tnsnames.ora` zip extraction, `O5LOGON` verifier parsing, and native Oracle 26ai `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`) and `OSON` binary JSON codecs (`OracleCore.encode_vector_binary`, `OracleCore.decode_vector_binary`, `OracleCore.encode_oson_frame`, `OracleCore.decode_oson_frame`). |
| [`mssql/`](../../../../../src/py/vertebra/db/mssql/__init__.py)[^src-db-mssql-init-py] / [`mssql.rs`](../../../../../src/rs/vertebra-db/src/mssql.rs)[^src-vertebra-db-src-mssql-rs] | `MSSQLConfig`, `MSSQLCore` | Pure-Rust TDS 7.4/8.0 wire protocol (`PRELOGIN` `0x12`, `LOGIN7` `0x10`, `SQL_BATCH` `0x01`, `RPC_REQUEST` `0x03` `sp_executesql`), automatic positional (`?`/`$1`) and named (`:name`) parameter rewriting to `@P1..@Pn` via `MSSQLCore.rewrite_sql_to_tds_params`, and Cloud SQL for SQL Server `:3307` TLS routing. |
| [`duckdb/`](../../../../../src/py/vertebra/db/duckdb/__init__.py)[^src-db-duckdb-init-py] / [`duckdb.rs`](../../../../../src/rs/vertebra-db/src/duckdb.rs)[^src-vertebra-db-src-duckdb-rs] | `DuckDBConfig`, `DuckDBCore` | Single-writer / multi-reader SHM analytical architecture preventing `IO Error: Could not set lock on file` across multi-worker forks, DuckDB 2.0-safe startup pragmas (`threads`, `memory_limit`, `temp_directory`, `preserve_insertion_order`), and zero-copy Arrow `RecordBatch` streaming over `ShmRegion` (`execute_arrow_shm()`). |
| [`bigquery/`](../../../../../src/py/vertebra/db/bigquery/__init__.py)[^src-db-bigquery-init-py] / [`bigquery.rs`](../../../../../src/rs/vertebra-db/src/bigquery.rs)[^src-vertebra-db-src-bigquery-rs] | `BigQueryConfig`, `BigQueryCore` | BigQuery REST v2 (`jobs.query`, `jobs.getQueryResults`, `jobs.insert` dry-run; optional `ca_pem` custom CA for REST; `check_rest_error` fails only on `error` or `status.errorResult` so `errors[]` warnings succeed; `build_get_query_results_url` percent-encodes path segments and query pairs with `url::Url`) + official `google-cloud-bigquery` (`client::Read`, `client::Write`) Storage Read/Write gRPC APIs (`CreateReadSession`, `ReadRows`, `AppendRows`) streaming into `VTBARROW` shared memory (`ArrowCursor`) and sending one record batch per `AppendRowsRequest` (`ArrowAppendPlan::build`, rejecting dictionary batches and verifying response count and offsets), with automatic fallback to `jobs.query` via `BigQueryCore.is_storage_api_fallback_error` (`BigQueryError.code in {7, 9, 12, 14}`). |
| [`spanner/`](../../../../../src/py/vertebra/db/spanner/__init__.py)[^src-db-spanner-init-py] / [`spanner.rs`](../../../../../src/rs/vertebra-db/src/spanner.rs)[^src-vertebra-db-src-spanner-rs] | `SpannerConfig`, `SpannerCore` | Official `google-cloud-spanner` (`client::Spanner`, `client::DatabaseAdmin`) across GoogleSQL and Spangres dialects (`CreateSession` multiplexed session, `ExecuteStreamingSql`, `ExecuteBatchDml`, `Commit` with inline `BeginTransaction`, `MultiplexedSessionPrecommitToken` tracking, `BatchWrite`, `PartitionQuery` + `partitioned_query` Data Boost rows hydrated into `VTBARROW` `ArrowCursor` by `fetch_arrow` with `SpannerCore.is_partitioned_query_fallback_error` checking `SpannerError.code in {3, 7, 9, 12, 14}`, `run_in_transaction` retrying `SpannerAbortedError`, char-aware GoogleSQL placeholder rewriting in `rewrite_spangres_placeholders`, and REST `update_database_ddl` with `admin_endpoint` resolution and `ca_pem` Omni TLS for non-`*.googleapis.com` HTTPS endpoints). |
| [`redis/`](../../../../../src/py/vertebra/db/redis/__init__.py)[^src-db-redis-init-py] / [`resp3.rs`](../../../../../src/rs/vertebra-db/src/resp3.rs)[^src-vertebra-db-src-resp3-rs] | `RedisConfig`, `RedisCore` | Rust `RESP2`/`RESP3` parser (`+`, `-`, `:`, `$`, `*`, `_`, `,`, `#`, `!`, `=`, `%`, `~`, `>`, `\|`), `HELLO 3` negotiation, `CLIENT TRACKING ON ... BCAST` invalidation push handling with bounded LRU cache, Sentinel master discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), and multiplexed Pub/Sub fan-out. |
| [`valkey/`](../../../../../src/py/vertebra/db/valkey/__init__.py)[^src-db-valkey-init-py] | `ValkeyConfig`, `ValkeyCore` | Full Redis RESP3 compatibility plus Valkey 8.0+/9.0+ client tracking, Cluster slot routing, and GCP Memorystore IAM token authentication (`needs_iam_refresh` / `authenticate_iam`). |

`PostgreSQLConfig` rules:

- `__post_init__` rejects invalid `min_size`/`max_size`.
- `effective_statement_cache_size` is `0` in `transaction` pooler mode unless `protocol_level_pooler_cache` is set. Otherwise it is `max_prepared_statements` or 256.
- `effective_direct_dsn` falls back to `dsn`. Use it for `LISTEN`/`NOTIFY`, replication, and migrations, which cannot run through a transaction pooler.
- `compute_worker_pool_budget` (static) divides `max_connections` minus reserved connections across replicas × workers, with a minimum of 1.
- `build_session_gucs` sets `application_name`, the timeouts, and `alloydb.enable_columnar_engine=on` when an AlloyDB URI or the columnar flag is set.
- `to_cloud_dialer_config` returns an `AlloyDBConfig` or `CloudSQLConfig` when an instance is configured. `MySQLConfig.to_cloud_dialer_config` and `MSSQLConfig.to_cloud_dialer_config` do the same for Cloud SQL (`:3307`).

## 3. Shared-Memory Arrow Cursor (`_registry.py`)

[`ArrowCursor`](../../../../../src/py/vertebra/db/_registry.py) wraps a `ShmRegion` containing an Apache Arrow IPC stream written by the Rust driver host (`vertebra-db`) or Tier-2 broker proxy:

- Exposes `memoryview` over the shared-memory buffer (`buffer_view()`), zero-copy `pyarrow.Table` / `pyarrow.RecordBatchReader` / `polars.DataFrame` materialization (`to_arrow()`, `to_polars()`), and fallback row-dict/tuple iteration (`fetchall()`, `fetchmany()`).
- Reclaims or unlinks the backing `ShmRegion` deterministically on `close()` or context-manager exit (`with driver.execute_arrow_shm(...) as cursor:`).

## 4. Cloud Dialers (`dialer/` & `vertebra-db/src/dialer.rs`)

| Symbol | Responsibility |
|---|---|
| [`CloudSQLInstanceURI`](../../../../../src/py/vertebra/db/dialer/cloudsql.py)[^src-db-dialer-cloudsql-py] | Parses `project:region:instance`, including domain-scoped `domain:project:region:instance`. `expected_server_cn` is `project:instance`. `generate_ephemeral_cert_url` builds the SQL Admin `v1beta4 ...:generateEphemeralCert` URL. |
| `CloudSQLConfig` / `SyncCloudSQLDialer` & `AsyncCloudSQLDialer` | Port 3307. `needs_refresh` / `cache_iam_token` track IAM token expiry against `refresh_lead_seconds` (default 240). `build_tls_context` returns a TLS 1.3 context with `check_hostname=False`, because the server CN is not a hostname. Rust `vertebra_db::dialer::CloudSQLDialer` provides native `rustls` custom `ServerCertVerifier` matching `project:instance`. |
| [`AlloyDBClusterURI`](../../../../../src/py/vertebra/db/dialer/alloydb.py)[^src-db-dialer-alloydb-py] | Parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`. `generate_client_cert_url` builds the `v1beta ...:generateClientCertificate` URL. |
| `AlloyDBConfig` / `SyncAlloyDBDialer` & `AsyncAlloyDBDialer` / `AlloyDBAuthType` | Port 5433. `encode_metadata_exchange` (and Rust `vertebra_db::dialer::AlloyDBDialer::encode_metadata_exchange`) writes a 4-byte big-endian length followed by protobuf-style fields: user agent (tag `0x0A`), auth type (`0x10`, `BUILT_IN=1` / `IAM=2`), token (`0x1A`). `build_handshake_frame` picks IAM or built-in auth from `enable_iam_auth`. |

Provenance sources: [^src-vertebra-db-src-tns-rs], [^src-vertebra-db-src-gcp-rs], [^src-vertebra-db-src-dialer-rs], [^src-vertebra-py-src-completion-rs], [^src-vertebra-py-src-errors-rs], [^src-vertebra-db-tests-gcp-credentials-tests-rs], [^src-vertebra-db-tests-spanner-wire-tests-rs], [^src-vertebra-db-tests-bigquery-wire-tests-rs], [^src-db-driver-common-py], [^src-db-driver-sync-py], [^src-db-driver-async-py], [^src-tests-unit-db-test-native-db-drivers-py], [^src-tests-unit-db-test-enterprise-olap-drivers-py], [^src-tests-unit-db-test-redis-valkey-resp3-py], [^src-tests-unit-db-test-bigquery-py], [^src-tests-unit-db-test-spanner-py].

[^src-vertebra-db-src-lib-rs]: `src/rs/vertebra-db/src/lib.rs` — Rust implementation module (src/rs/vertebra-db/src/lib.rs).
[^src-vertebra-db-src-postgres-rs]: `src/rs/vertebra-db/src/postgres.rs` — Rust implementation module (src/rs/vertebra-db/src/postgres.rs).
[^src-vertebra-db-src-mysql-rs]: `src/rs/vertebra-db/src/mysql.rs` — Rust implementation module (src/rs/vertebra-db/src/mysql.rs).
[^src-vertebra-db-src-sqlite-rs]: `src/rs/vertebra-db/src/sqlite.rs` — Rust implementation module (src/rs/vertebra-db/src/sqlite.rs).
[^src-vertebra-db-src-oracle-rs]: `src/rs/vertebra-db/src/oracle.rs` — Rust implementation module (src/rs/vertebra-db/src/oracle.rs).
[^src-vertebra-db-src-tns-rs]: `src/rs/vertebra-db/src/tns.rs` — Rust implementation module (src/rs/vertebra-db/src/tns.rs).
[^src-vertebra-db-src-mssql-rs]: `src/rs/vertebra-db/src/mssql.rs` — Rust implementation module (src/rs/vertebra-db/src/mssql.rs).
[^src-vertebra-db-src-duckdb-rs]: `src/rs/vertebra-db/src/duckdb.rs` — Rust implementation module (src/rs/vertebra-db/src/duckdb.rs).
[^src-vertebra-db-src-gcp-rs]: `src/rs/vertebra-db/src/gcp.rs` — Rust implementation module (src/rs/vertebra-db/src/gcp.rs).
[^src-vertebra-db-src-bigquery-rs]: `src/rs/vertebra-db/src/bigquery.rs` — Rust implementation module (src/rs/vertebra-db/src/bigquery.rs).
[^src-vertebra-db-src-spanner-rs]: `src/rs/vertebra-db/src/spanner.rs` — Rust implementation module (src/rs/vertebra-db/src/spanner.rs).
[^src-vertebra-db-src-resp3-rs]: `src/rs/vertebra-db/src/resp3.rs` — Rust implementation module (src/rs/vertebra-db/src/resp3.rs).
[^src-vertebra-db-src-dialer-rs]: `src/rs/vertebra-db/src/dialer.rs` — Rust implementation module (src/rs/vertebra-db/src/dialer.rs).
[^src-vertebra-py-src-completion-rs]: `src/rs/vertebra-py/src/completion.rs` — Rust implementation module (src/rs/vertebra-py/src/completion.rs).
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Rust implementation module (src/rs/vertebra-py/src/errors.rs).
[^src-vertebra-db-tests-gcp-credentials-tests-rs]: `src/rs/vertebra-db/tests/gcp_credentials_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/gcp_credentials_tests.rs).
[^src-vertebra-db-tests-spanner-wire-tests-rs]: `src/rs/vertebra-db/tests/spanner_wire_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/spanner_wire_tests.rs).
[^src-vertebra-db-tests-bigquery-wire-tests-rs]: `src/rs/vertebra-db/tests/bigquery_wire_tests.rs` — Rust implementation module (src/rs/vertebra-db/tests/bigquery_wire_tests.rs).
[^src-db-init-py]: `src/py/vertebra/db/__init__.py` — Python implementation module (src/py/vertebra/db/__init__.py).
[^src-db-registry-py]: `src/py/vertebra/db/_registry.py` — Python implementation module (src/py/vertebra/db/_registry.py).
[^src-db-driver-init-py]: `src/py/vertebra/db/driver/__init__.py` — Python implementation module (src/py/vertebra/db/driver/__init__.py).
[^src-db-driver-common-py]: `src/py/vertebra/db/driver/_common.py` — Python implementation module (src/py/vertebra/db/driver/_common.py).
[^src-db-driver-sync-py]: `src/py/vertebra/db/driver/_sync.py` — Python implementation module (src/py/vertebra/db/driver/_sync.py).
[^src-db-driver-async-py]: `src/py/vertebra/db/driver/_async.py` — Python implementation module (src/py/vertebra/db/driver/_async.py).
[^src-db-postgres-init-py]: `src/py/vertebra/db/postgres/__init__.py` — Python implementation module (src/py/vertebra/db/postgres/__init__.py).
[^src-db-mysql-init-py]: `src/py/vertebra/db/mysql/__init__.py` — Python implementation module (src/py/vertebra/db/mysql/__init__.py).
[^src-db-sqlite-init-py]: `src/py/vertebra/db/sqlite/__init__.py` — Python implementation module (src/py/vertebra/db/sqlite/__init__.py).
[^src-db-oracle-init-py]: `src/py/vertebra/db/oracle/__init__.py` — Python implementation module (src/py/vertebra/db/oracle/__init__.py).
[^src-db-mssql-init-py]: `src/py/vertebra/db/mssql/__init__.py` — Python implementation module (src/py/vertebra/db/mssql/__init__.py).
[^src-db-duckdb-init-py]: `src/py/vertebra/db/duckdb/__init__.py` — Python implementation module (src/py/vertebra/db/duckdb/__init__.py).
[^src-db-bigquery-init-py]: `src/py/vertebra/db/bigquery/__init__.py` — Python implementation module (src/py/vertebra/db/bigquery/__init__.py).
[^src-db-spanner-init-py]: `src/py/vertebra/db/spanner/__init__.py` — Python implementation module (src/py/vertebra/db/spanner/__init__.py).
[^src-db-redis-init-py]: `src/py/vertebra/db/redis/__init__.py` — Python implementation module (src/py/vertebra/db/redis/__init__.py).
[^src-db-valkey-init-py]: `src/py/vertebra/db/valkey/__init__.py` — Python implementation module (src/py/vertebra/db/valkey/__init__.py).
[^src-db-dialer-cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py` — Python implementation module (src/py/vertebra/db/dialer/cloudsql.py).
[^src-db-dialer-alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py` — Python implementation module (src/py/vertebra/db/dialer/alloydb.py).
[^src-tests-unit-db-test-native-db-drivers-py]: `src/py/tests/unit/db/test_native_db_drivers.py` — Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py).
[^src-tests-unit-db-test-enterprise-olap-drivers-py]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Python implementation module (src/py/tests/unit/db/test_enterprise_olap_drivers.py).
[^src-tests-unit-db-test-redis-valkey-resp3-py]: `src/py/tests/unit/db/test_redis_valkey_resp3.py` — Python implementation module (src/py/tests/unit/db/test_redis_valkey_resp3.py).
[^src-tests-unit-db-test-bigquery-py]: `src/py/tests/unit/db/test_bigquery.py` — Python implementation module (src/py/tests/unit/db/test_bigquery.py).
[^src-tests-unit-db-test-spanner-py]: `src/py/tests/unit/db/test_spanner.py` — Python implementation module (src/py/tests/unit/db/test_spanner.py).
