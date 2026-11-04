---
type: Guide
title: "Database Drivers & Cloud Dialers"
description: "The `vertebra-db` and `vertebra.db` Rust-native shared-memory drivers across PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey, the `vertebra.db.driver` base hierarchy, `ArrowCursor`, `DatabaseRegistry`, and Cloud SQL / AlloyDB dialers."
status: stable
tags: [domains, database, drivers, bigquery, spanner, cloud-sql, alloydb, arrow, shm]
sources:
  - resource: src/rs/vertebra-db/src/lib.rs
  - resource: src/rs/vertebra-db/src/postgres.rs
  - resource: src/rs/vertebra-db/src/mysql.rs
  - resource: src/rs/vertebra-db/src/sqlite.rs
  - resource: src/rs/vertebra-db/src/oracle.rs
  - resource: src/rs/vertebra-db/src/tns.rs
  - resource: src/rs/vertebra-db/src/mssql.rs
  - resource: src/rs/vertebra-db/src/duckdb.rs
  - resource: src/rs/vertebra-db/src/gcp.rs
  - resource: src/rs/vertebra-db/src/bigquery.rs
  - resource: src/rs/vertebra-db/src/spanner.rs
  - resource: src/rs/vertebra-db/src/resp3.rs
  - resource: src/rs/vertebra-db/src/dialer.rs
  - resource: src/rs/vertebra-db/tests/gcp_credentials_tests.rs
  - resource: src/rs/vertebra-db/tests/spanner_wire_tests.rs
  - resource: src/rs/vertebra-db/tests/bigquery_wire_tests.rs
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/vertebra/db/driver/__init__.py
  - resource: src/py/vertebra/db/driver/_common.py
  - resource: src/py/vertebra/db/driver/_sync.py
  - resource: src/py/vertebra/db/driver/_async.py
  - resource: src/py/vertebra/db/postgres/__init__.py
  - resource: src/py/vertebra/db/mysql/__init__.py
  - resource: src/py/vertebra/db/sqlite/__init__.py
  - resource: src/py/vertebra/db/oracle/__init__.py
  - resource: src/py/vertebra/db/mssql/__init__.py
  - resource: src/py/vertebra/db/duckdb/__init__.py
  - resource: src/py/vertebra/db/bigquery/__init__.py
  - resource: src/py/vertebra/db/spanner/__init__.py
  - resource: src/py/vertebra/db/redis/__init__.py
  - resource: src/py/vertebra/db/valkey/__init__.py
  - resource: src/py/vertebra/db/dialer/cloudsql.py
  - resource: src/py/vertebra/db/dialer/alloydb.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
  - resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
  - resource: src/py/tests/unit/db/test_bigquery.py
  - resource: src/py/tests/unit/db/test_spanner.py
---

# Database Drivers & Cloud Dialers

<!-- truth: start -->
- **Structured Per-Driver Package Layout (`src/py/vertebra/db/<driver>/{config,core,driver,__init__}.py` & `src/py/vertebra/db/driver/`)**: Every built-in database and cache driver in `vertebra.db` (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`) is organized as a package with `config.py` (`msgspec.Struct(..., frozen=True, gc=False, kw_only=True)` inheriting `BaseDatabaseConfig`), `core.py` (compiled domain core class encapsulating wire codecs, parameter normalization, and SQL rewrites), `driver.py` (`Sync*Driver(SyncDatabaseDriver)` and `Async*Driver(AsyncDatabaseDriver)`), and `__init__.py`.
- **Binary Vector Wire Codecs (`postgres.rs`, `oracle.rs`, `vertebra-py/src/db.rs`)**: PostgreSQL (`postgres.rs`) encodes and decodes binary `pgvector` (`vector` `f32`, `halfvec` `f16`, and `bit` bit-packed) frames without text formatting, while Oracle 26ai (`oracle.rs`, `vertebra-py/src/db.rs`) encodes and decodes binary `0xDB` `VECTOR` frames directly over PEP 3118 `PyBuffer` views (`encode_oracle_vector_binary`, `decode_oracle_vector_binary`).
- **Per-Session DuckDB `httpfs` Secret Rotation (`duckdb/`, `duckdb.rs`)**: Each pooled DuckDB connection tracks `applied_token_generation: u64` against the shared [`GcpTokenCache`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs) and executes `CREATE OR REPLACE SECRET vtb_gcs_adc (TYPE GCS, BEARER_TOKEN '...')` whenever the cached ADC token generation advances.
- **Official Google Cloud Rust Clients for Spanner & BigQuery Storage (`gcp.rs`, `spanner.rs`, `bigquery.rs`)**: `SpannerClient` and `BigQueryClient` delegate gRPC Data, Database Admin, Storage Read, and Storage Write APIs to `google-cloud-spanner` (`=0.34.8-preview`) and `google-cloud-bigquery` (`=0.19.0`) bridged from `GcpTokenCache` via `vertebra_db::gcp::{GcpTokenCredentials, official_credentials}`. Spanner DDL executes over REST (`google_cloud_spanner::client::DatabaseAdmin`) with `admin_endpoint` resolution (`*.googleapis.com` → `https://spanner.googleapis.com`; emulator `:9010` → `:9020`; explicit `admin_endpoint` required otherwise) and LRO polling via `DatabaseAdmin::get_operation` (`google_cloud_longrunning::model::operation::Result`). `SpannerConfig.ca_pem` configures Spanner Omni TLS for non-`*.googleapis.com` HTTPS endpoints and is rejected for `*.googleapis.com` or `http://` endpoints; `BigQueryConfig.ca_pem` applies to REST v2 calls only. `ArrowAppendPlan::build` splits Arrow IPC streams into `ArrowAppendPlan { schema, requests }` with one record batch per `ArrowAppendRequest` (passing `schema` once to `open_default_stream(...).build_arrow(schema)`, rejecting dictionary batches and trailing bytes, and verifying response counts and offsets via `ArrowAppendPlan::verify_responses`). Unit tests exercise `Spanner::from_stub`, `DatabaseAdmin::from_stub`, `Read::from_stub`, and pure `ArrowAppendPlan` verification without TCP sockets.
<!-- truth: end -->

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs) and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) implement Vertebra's Tier-1 shared-memory database and cache drivers, the [`vertebra.db.driver`](../../../../../src/py/vertebra/db/driver/__init__.py) base hierarchy (`BaseDatabaseConfig`, `CommonDriverMixin`, `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`), zero-copy `ShmRegion` Arrow IPC cursors, and Cloud SQL / AlloyDB mTLS and IAM token dialers. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

## 1. Registry (`_registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/_registry.py) maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgresConfig`, `mysql`/`mariadb` → `MySQLConfig`, `sqlite` → `SQLiteConfig` (`:memory:` for an empty path), `oracle` → `OracleConfig`, `mssql`/`sqlserver` → `MSSQLConfig`, `duckdb` → `DuckDBConfig`, `bigquery` → `BigQueryConfig`, `spanner`/`spangres` → `SpannerConfig` (via `SpannerConfig.from_dsn`; `spanner+postgresql` raises `ImproperConfigurationError`), `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `cloudsql` → `CloudSQLConfig`, `alloydb` → `AlloyDBConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_driver(name)` builds the `Async*Driver` or dialer that matches the config type.
- [`vertebra.extensions.sqlalchemy.dialects`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) integrates the relational and analytical drivers into SQLAlchemy 2.1 via the `+vtb` dialect suffix (`{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb}`).

## 2. Driver Implementations (`vertebra-db` & `vertebra.db`)

| Package / Module | Config & Core | Driver behavior |
|---|---|---|
| [`postgres/`](../../../../../src/py/vertebra/db/postgres/__init__.py) / [`postgres.rs`](../../../../../src/rs/vertebra-db/src/postgres.rs) | `PostgresConfig`, `PostgreSQLCore`, `PoolerMode = Literal["direct", "transaction", "session"]` | Native v3 wire framing (`StartupMessage`, `SASLInitialResponse` / `SASLResponse` SCRAM-SHA-256, `Parse`/`Bind`/`Describe`/`Execute`/`Sync`, binary `DataRow`, `CopyOutResponse`). `PostgreSQLCore.strip_jsonb_header` validates the `0x01` JSONB version byte and returns a zero-copy `memoryview` slice. Supports `execute_arrow_shm()` into `ArrowCursor` and direct `:5432` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`mysql/`](../../../../../src/py/vertebra/db/mysql/__init__.py) / [`mysql.rs`](../../../../../src/rs/vertebra-db/src/mysql.rs) | `MySQLConfig`, `MySQLCore` | Handshake v10 packet parsing, `caching_sha2_password` / `mysql_native_password` / `mysql_clear_password` auth, `COM_QUERY` and `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` dispatch, length-encoded integer and binary result row decoding, `execute_arrow_shm()`, and direct `:3306` IAM OAuth2 token password authentication (`enable_iam_auth=True`). |
| [`sqlite/`](../../../../../src/py/vertebra/db/sqlite/__init__.py) / [`sqlite.rs`](../../../../../src/rs/vertebra-db/src/sqlite.rs) | `SQLiteConfig`, `SQLiteCore` | Opens WAL-mode connections (`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`, `mmap_size`, `temp_store=MEMORY`), serializes writes through `LeaseBroker` slots when `broker` is attached, and streams `execute_arrow_shm()` via `ArrowCursor`. |
| [`oracle/`](../../../../../src/py/vertebra/db/oracle/__init__.py) / [`oracle.rs`](../../../../../src/rs/vertebra-db/src/oracle.rs) | `OracleConfig`, `OracleCore` | Pure-Rust Thin TNS/TTC framing (zero ODPI-C / Instant Client dependency), Autonomous Database mTLS `ewallet.pem` + `tnsnames.ora` zip extraction, `O5LOGON` verifier parsing, and native Oracle 26ai `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`) and `OSON` binary JSON codecs (`OracleCore.encode_vector_binary`, `OracleCore.decode_vector_binary`, `OracleCore.encode_oson_frame`, `OracleCore.decode_oson_frame`). |
| [`mssql/`](../../../../../src/py/vertebra/db/mssql/__init__.py) / [`mssql.rs`](../../../../../src/rs/vertebra-db/src/mssql.rs) | `MSSQLConfig`, `MSSQLCore` | Pure-Rust TDS 7.4/8.0 wire protocol (`PRELOGIN` `0x12`, `LOGIN7` `0x10`, `SQL_BATCH` `0x01`, `RPC_REQUEST` `0x03` `sp_executesql`), automatic positional (`?`/`$1`) and named (`:name`) parameter rewriting to `@P1..@Pn` via `MSSQLCore.rewrite_sql_to_tds_params`, and Cloud SQL for SQL Server `:3307` TLS routing. |
| [`duckdb/`](../../../../../src/py/vertebra/db/duckdb/__init__.py) / [`duckdb.rs`](../../../../../src/rs/vertebra-db/src/duckdb.rs) | `DuckDBConfig`, `DuckDBCore` | Single-writer / multi-reader SHM analytical architecture preventing `IO Error: Could not set lock on file` across multi-worker forks, DuckDB 2.0-safe startup pragmas (`threads`, `memory_limit`, `temp_directory`, `preserve_insertion_order`), and zero-copy Arrow `RecordBatch` streaming over `ShmRegion` (`execute_arrow_shm()`). |
| [`bigquery/`](../../../../../src/py/vertebra/db/bigquery/__init__.py) / [`bigquery.rs`](../../../../../src/rs/vertebra-db/src/bigquery.rs) | `BigQueryConfig`, `BigQueryCore` | BigQuery REST v2 (`jobs.query`, `jobs.getQueryResults`, `jobs.insert` dry-run; optional `ca_pem` custom CA for REST) + official `google-cloud-bigquery` (`client::Read`, `client::Write`) Storage Read/Write gRPC APIs (`CreateReadSession`, `ReadRows`, `AppendRows`) streaming into `VTBARROW` shared memory (`ArrowCursor`) and sending one record batch per `AppendRowsRequest` (`ArrowAppendPlan::build`, rejecting dictionary batches and verifying response count and offsets), with automatic fallback to `jobs.query` when `bigquerystorage.googleapis.com` is disabled (`SERVICE_DISABLED` / `PERMISSION_DENIED`). |
| [`spanner/`](../../../../../src/py/vertebra/db/spanner/__init__.py) / [`spanner.rs`](../../../../../src/rs/vertebra-db/src/spanner.rs) | `SpannerConfig`, `SpannerCore` | Official `google-cloud-spanner` (`client::Spanner`, `client::DatabaseAdmin`) across GoogleSQL and Spangres dialects (`CreateSession` multiplexed session, `ExecuteStreamingSql`, `ExecuteBatchDml`, `Commit` with inline `BeginTransaction`, `MultiplexedSessionPrecommitToken` tracking, `BatchWrite`, `PartitionQuery` + `partitioned_query` Data Boost rows hydrated into `VTBARROW` `ArrowCursor` by `fetch_arrow`, and REST `update_database_ddl` with `admin_endpoint` resolution and `ca_pem` Omni TLS for non-`*.googleapis.com` HTTPS endpoints). |
| [`redis/`](../../../../../src/py/vertebra/db/redis/__init__.py) / [`resp3.rs`](../../../../../src/rs/vertebra-db/src/resp3.rs) | `RedisConfig`, `RedisCore` | Rust `RESP2`/`RESP3` parser (`+`, `-`, `:`, `$`, `*`, `_`, `,`, `#`, `!`, `=`, `%`, `~`, `>`, `\|`), `HELLO 3` negotiation, `CLIENT TRACKING ON ... BCAST` invalidation push handling with bounded LRU cache, Sentinel master discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), and multiplexed Pub/Sub fan-out. |
| [`valkey/`](../../../../../src/py/vertebra/db/valkey/__init__.py) | `ValkeyConfig`, `ValkeyCore` | Full Redis RESP3 compatibility plus Valkey 8.0+/9.0+ client tracking, Cluster slot routing, and GCP Memorystore IAM token authentication (`needs_iam_refresh` / `authenticate_iam`). |

`PostgresConfig` rules:

- `__post_init__` rejects invalid `min_size`/`max_size`.
- `effective_statement_cache_size` is `0` in `transaction` pooler mode unless `protocol_level_pooler_cache` is set. Otherwise it is `max_prepared_statements` or 256.
- `effective_direct_dsn` falls back to `dsn`. Use it for `LISTEN`/`NOTIFY`, replication, and migrations, which cannot run through a transaction pooler.
- `compute_worker_pool_budget` (static) divides `max_connections` minus reserved connections across replicas × workers, with a minimum of 1.
- `build_session_gucs` sets `application_name`, the timeouts, and `alloydb.enable_columnar_engine=on` when an AlloyDB URI or the columnar flag is set.
- `to_cloud_dialer_config` returns an `AlloyDbConfig` or `CloudSqlConfig` when an instance is configured. `MysqlConfig.to_cloud_dialer_config` and `MssqlConfig.to_cloud_dialer_config` do the same for Cloud SQL (`:3307`).

## 3. Shared-Memory Arrow Cursor (`_registry.py`)

[`ArrowCursor`](../../../../../src/py/vertebra/db/_registry.py) wraps a `ShmRegion` containing an Apache Arrow IPC stream written by the Rust driver host (`vertebra-db`) or Tier-2 broker proxy:

- Exposes `memoryview` over the shared-memory buffer (`buffer_view()`), zero-copy `pyarrow.Table` / `pyarrow.RecordBatchReader` / `polars.DataFrame` materialization (`to_arrow()`, `to_polars()`), and fallback row-dict/tuple iteration (`fetchall()`, `fetchmany()`).
- Reclaims or unlinks the backing `ShmRegion` deterministically on `close()` or context-manager exit (`with driver.execute_arrow_shm(...) as cursor:`).

## 4. Cloud Dialers (`dialer/` & `vertebra-db/src/dialer.rs`)

| Symbol | Responsibility |
|---|---|
| [`CloudSqlInstanceUri`](../../../../../src/py/vertebra/db/dialer/cloudsql.py) | Parses `project:region:instance`, including domain-scoped `domain:project:region:instance`. `expected_server_cn` is `project:instance`. `generate_ephemeral_cert_url` builds the SQL Admin `v1beta4 ...:generateEphemeralCert` URL. |
| `CloudSqlConfig` / `CloudSqlDialer` | Port 3307. `needs_refresh` / `cache_iam_token` track IAM token expiry against `refresh_Lead_seconds` (default 240; the field name has a capital `L`). `build_tls_context` returns a TLS 1.3 context with `check_hostname=False`, because the server CN is not a hostname. Rust `vertebra_db::dialer::CloudSqlDialer` provides native `rustls` custom `ServerCertVerifier` matching `project:instance`. |
| [`AlloyDbClusterUri`](../../../../../src/py/vertebra/db/dialer/alloydb.py) | Parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`. `generate_client_cert_url` builds the `v1beta ...:generateClientCertificate` URL. |
| `AlloyDbConfig` / `AlloyDbDialer` / `AlloyDbAuthType` | Port 5433. `encode_metadata_exchange` (and Rust `vertebra_db::dialer::AlloyDbDialer::encode_metadata_exchange`) writes a 4-byte big-endian length followed by protobuf-style fields: user agent (tag `0x0A`), auth type (`0x10`, `BUILT_IN=1` / `IAM=2`), token (`0x1A`). `build_handshake_frame` picks IAM or built-in auth from `enable_iam_auth`. |
