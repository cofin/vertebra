---
type: Guide
title: "Database Drivers & Cloud Dialers"
description: "The `vertebra-db` and `vertebra.db` Rust-native shared-memory drivers across PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, Redis, and Valkey, `ArrowCursor`, `DatabaseRegistry`, and Cloud SQL and AlloyDB mTLS dialers."
status: stable
tags: [domains, database, drivers, cloud-sql, alloydb, arrow, shm]
sources:
  - resource: src/rs/vertebra-db/src/lib.rs
  - resource: src/rs/vertebra-db/src/postgres.rs
  - resource: src/rs/vertebra-db/src/mysql.rs
  - resource: src/rs/vertebra-db/src/sqlite.rs
  - resource: src/rs/vertebra-db/src/oracle.rs
  - resource: src/rs/vertebra-db/src/tns.rs
  - resource: src/rs/vertebra-db/src/mssql.rs
  - resource: src/rs/vertebra-db/src/duckdb_engine.rs
  - resource: src/rs/vertebra-db/src/resp3.rs
  - resource: src/rs/vertebra-db/src/dialer.rs
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/vertebra/db/postgres.py
  - resource: src/py/vertebra/db/mysql.py
  - resource: src/py/vertebra/db/sqlite.py
  - resource: src/py/vertebra/db/oracle.py
  - resource: src/py/vertebra/db/mssql.py
  - resource: src/py/vertebra/db/duckdb.py
  - resource: src/py/vertebra/db/redis.py
  - resource: src/py/vertebra/db/valkey.py
  - resource: src/py/vertebra/db/dialer/cloudsql.py
  - resource: src/py/vertebra/db/dialer/alloydb.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
  - resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
---

# Database Drivers & Cloud Dialers

[`src/rs/vertebra-db`](../../../../../src/rs/vertebra-db/src/lib.rs) and [`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) implement Vertebra's Tier-1 shared-memory database and cache drivers, typed connection configs (`msgspec.Struct(frozen=True, gc=False, kw_only=True)`), zero-copy `ShmRegion` Arrow IPC cursors, and Cloud SQL / AlloyDB mTLS dialers. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

## 1. Registry (`_registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/_registry.py) maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgresConfig`, `mysql`/`mariadb` → `MysqlConfig`, `sqlite` → `SqliteConfig` (`:memory:` for an empty path), `oracle` → `OracleConfig`, `mssql`/`sqlserver` → `MssqlConfig`, `duckdb` → `DuckdbConfig`, `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `cloudsql` → `CloudSqlConfig`, `alloydb` → `AlloyDbConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_driver(name)` builds the `Async*Driver` or dialer that matches the config type.
- [`vertebra.extensions.sqlalchemy.dialects`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) integrates all six relational/analytical drivers into SQLAlchemy 2.1 via the `+vtb` dialect suffix (`{postgresql,oracle,mysql,sqlite,mssql,duckdb}+{vertebra,vtb}`).

## 2. Driver Implementations (`vertebra-db` & `vertebra.db`)

| Module | Config | Driver behavior |
|---|---|---|
| [`postgres.py`](../../../../../src/py/vertebra/db/postgres.py) / [`postgres.rs`](../../../../../src/rs/vertebra-db/src/postgres.rs) | `PostgresConfig`, `PoolerMode = Literal["direct", "transaction", "session"]` | Native v3 wire framing (`StartupMessage`, `SASLInitialResponse` / `SASLResponse` SCRAM-SHA-256, `Parse`/`Bind`/`Describe`/`Execute`/`Sync`, binary `DataRow`, `CopyOutResponse`). `strip_jsonb_header` validates the `0x01` JSONB version byte and returns a zero-copy `memoryview` slice. Supports `execute_arrow_shm()` into `ArrowCursor`. |
| [`mysql.py`](../../../../../src/py/vertebra/db/mysql.py) / [`mysql.rs`](../../../../../src/rs/vertebra-db/src/mysql.rs) | `MysqlConfig` | Handshake v10 packet parsing, `caching_sha2_password` / `mysql_native_password` auth, `COM_QUERY` and `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` dispatch, length-encoded integer and binary result row decoding, and `execute_arrow_shm()`. |
| [`sqlite.py`](../../../../../src/py/vertebra/db/sqlite.py) / [`sqlite.rs`](../../../../../src/rs/vertebra-db/src/sqlite.rs) | `SqliteConfig` | Opens WAL-mode connections (`journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout`, `mmap_size`, `temp_store=MEMORY`), serializes writes through `LeaseBroker` slots when `broker` is attached, and streams `execute_arrow_shm()` via `ArrowCursor`. |
| [`oracle.py`](../../../../../src/py/vertebra/db/oracle.py) / [`oracle.rs`](../../../../../src/rs/vertebra-db/src/oracle.rs) | `OracleConfig` | Pure-Rust Thin TNS/TTC framing (zero ODPI-C / Instant Client dependency), Autonomous Database mTLS `ewallet.pem` + `tnsnames.ora` zip extraction, `O5LOGON` verifier parsing, and native Oracle 26ai `VECTOR` (`FLOAT32`/`FLOAT64`/`INT8`/`BINARY`) and `OSON` binary JSON codecs (`encode_vector_binary`, `decode_vector_binary`, `encode_oson_frame`, `decode_oson_frame`). |
| [`mssql.py`](../../../../../src/py/vertebra/db/mssql.py) / [`mssql.rs`](../../../../../src/rs/vertebra-db/src/mssql.rs) | `MssqlConfig` | Pure-Rust `tiberius` TDS 7.4/8.0 wire protocol (`PRELOGIN` `0x12`, `LOGIN7` `0x10`, `SQL_BATCH` `0x01`, `RPC_REQUEST` `0x03` `sp_executesql`), automatic positional (`?`/`$1`) and named (`:name`) parameter rewriting to `@P1..@Pn` via `rewrite_sql_to_tds_params`, and Cloud SQL for SQL Server `:3307` TLS routing. |
| [`duckdb.py`](../../../../../src/py/vertebra/db/duckdb.py) / [`duckdb_engine.rs`](../../../../../src/rs/vertebra-db/src/duckdb_engine.rs) | `DuckdbConfig` | Single-writer / multi-reader SHM analytical architecture preventing `IO Error: Could not set lock on file` across multi-worker forks, DuckDB 2.0-safe startup pragmas (`threads`, `memory_limit`, `temp_directory`, `preserve_insertion_order`), and zero-copy Arrow `RecordBatch` streaming over `ShmRegion` (`execute_arrow_shm()`). |
| [`redis.py`](../../../../../src/py/vertebra/db/redis.py) / [`resp3.rs`](../../../../../src/rs/vertebra-db/src/resp3.rs) | `RedisConfig` | Rust `RESP2`/`RESP3` parser (`+`, `-`, `:`, `$`, `*`, `_`, `,`, `#`, `!`, `=`, `%`, `~`, `>`, `\|`), `HELLO 3` negotiation, `CLIENT TRACKING ON ... BCAST` invalidation push handling with bounded LRU cache, Sentinel master discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), and multiplexed Pub/Sub fan-out. |
| [`valkey.py`](../../../../../src/py/vertebra/db/valkey.py) | `ValkeyConfig` | Full Redis RESP3 compatibility plus Valkey 8.0+/9.0+ client tracking, Cluster slot routing, and GCP Memorystore IAM token authentication (`needs_iam_refresh` / `authenticate_iam`). |

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
