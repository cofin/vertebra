---
type: Guide
title: "Database Drivers, Cloud Dialers & Oracle Wallets"
description: "The `vertebra.db` driver facades, `DatabaseRegistry`, Cloud SQL and AlloyDB dialers, Oracle wallet and TNS handling, and the standalone `vertebra-db` Rust crate."
status: stable
tags: [domains, database, drivers, oracle, cloud-sql]
sources:
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/vertebra/db/postgres.py
  - resource: src/py/vertebra/db/mysql.py
  - resource: src/py/vertebra/db/sqlite.py
  - resource: src/py/vertebra/db/redis.py
  - resource: src/py/vertebra/db/valkey.py
  - resource: src/py/vertebra/db/dialer/cloudsql.py
  - resource: src/py/vertebra/db/dialer/alloydb.py
  - resource: src/py/vertebra/db/oracle/config.py
  - resource: src/py/vertebra/db/oracle/driver.py
  - resource: src/py/vertebra/db/oracle/wallet.py
  - resource: src/py/vertebra/db/oracle/containers.py
  - resource: src/rs/vertebra-db/src/lib.rs
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
---

# Database Drivers, Cloud Dialers & Oracle Wallets

[`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) holds typed connection configs (`msgspec.Struct(frozen=True, gc=False, kw_only=True)`), sync/async driver classes, cloud dialer helpers, and Oracle wallet tooling. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

> [!IMPORTANT]
> Only `SyncSqliteDriver` / `AsyncSqliteDriver` perform real I/O (through `sqlite3`). The Postgres, MySQL, Redis, Valkey, and Oracle drivers open no network connections. They provide config validation, wire-format helpers, and in-process placeholder state. Every `Async*Driver` wraps its sync counterpart and calls it directly without offloading.

## 1. Registry (`_registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/_registry.py) maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgresConfig`, `mysql`/`mariadb` → `MysqlConfig`, `sqlite` → `SqliteConfig` (`:memory:` for an empty path), `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `oracle` → `OracleConfig` (defaults `system@127.0.0.1:1521/FREEPDB1`), `cloudsql` → `CloudSqlConfig`, `alloydb` → `AlloyDbConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_driver(name)` builds the `Async*Driver` or dialer that matches the config type.
- `vertebra.extensions.sqlalchemy.dialects` is the only caller outside the package.

## 2. Driver Facades

| Module | Config | Driver behavior |
|---|---|---|
| [`postgres.py`](../../../../../src/py/vertebra/db/postgres.py) | `PostgresConfig`, `PoolerMode = Literal["direct", "transaction", "session"]` | `execute_tuples` returns `_in_memory_rows` (empty unless set). `strip_jsonb_header` validates the `0x01` JSONB version byte and returns a `memoryview` slice without copying. |
| [`mysql.py`](../../../../../src/py/vertebra/db/mysql.py) | `MysqlConfig` | `execute_tuples` returns `[]`. |
| [`sqlite.py`](../../../../../src/py/vertebra/db/sqlite.py) | `SqliteConfig` | Opens `sqlite3.connect(..., check_same_thread=False)`, applies `startup_pragmas()`, commits after each `execute_tuples`. |
| [`redis.py`](../../../../../src/py/vertebra/db/redis.py) | `RedisConfig` | `get`/`set`/`delete` use an in-process dict. `publish` returns `1`. `encode_command`, `encode_hello3_handshake`, `encode_pipeline` produce RESP bytes. |
| [`valkey.py`](../../../../../src/py/vertebra/db/valkey.py) | `ValkeyConfig` | Same as Redis, plus a client-side cache (`cached_keys`, `handle_invalidation_push`). `ValkeyConfig.build_handshake_commands` emits `HELLO 3` and `CLIENT TRACKING ON ... BCAST`. With `enable_gcp_memorystore_iam`, it authenticates as user `default` with the IAM token. |

`PostgresConfig` rules:

- `__post_init__` rejects invalid `min_size`/`max_size`.
- `effective_statement_cache_size` is `0` in `transaction` pooler mode unless `protocol_level_pooler_cache` is set. Otherwise it is `max_prepared_statements` or 256.
- `effective_direct_dsn` falls back to `dsn`. Use it for LISTEN/NOTIFY, replication, and migrations, which cannot run through a transaction pooler.
- `compute_worker_pool_budget` (static) divides `max_connections` minus reserved connections across replicas × workers, with a minimum of 1.
- `build_session_gucs` sets `application_name`, the timeouts, and `alloydb.enable_columnar_engine=on` when an AlloyDB URI or the columnar flag is set.
- `to_cloud_dialer_config` returns an `AlloyDbConfig` or `CloudSqlConfig` when an instance is configured. `MysqlConfig.to_cloud_dialer_config` does the same for Cloud SQL.

## 3. Cloud Dialers (`dialer/`)

| Symbol | Responsibility |
|---|---|
| [`CloudSqlInstanceUri`](../../../../../src/py/vertebra/db/dialer/cloudsql.py) | Parses `project:region:instance`, including domain-scoped `domain:project:region:instance`. `expected_server_cn` is `project:instance`. `generate_ephemeral_cert_url` builds the SQL Admin `v1beta4 ...:generateEphemeralCert` URL. |
| `CloudSqlConfig` / `CloudSqlDialer` | Port 3307. `needs_refresh` / `cache_iam_token` track IAM token expiry against `refresh_Lead_seconds` (default 240; the field name has a capital `L`). `build_tls_context` returns a TLS 1.3 context with `check_hostname=False`, because the server CN is not a hostname. It only enforces chain verification when `ca_pem` is passed (otherwise `CERT_NONE`). Passing client cert PEMs raises `NotImplementedError`. |
| [`AlloyDbClusterUri`](../../../../../src/py/vertebra/db/dialer/alloydb.py) | Parses `projects/<p>/locations/<l>/clusters/<c>/instances/<i>`. `generate_client_cert_url` builds the `v1beta ...:generateClientCertificate` URL. |
| `AlloyDbConfig` / `AlloyDbDialer` / `AlloyDbAuthType` | Port 5433. `encode_metadata_exchange` writes a 4-byte big-endian length followed by protobuf-style fields: user agent (tag `0x0A`), auth type (`0x10`, `BUILT_IN=1` / `IAM=2`), token (`0x1A`). `build_handshake_frame` picks IAM or built-in auth from `enable_iam_auth`. |

Neither dialer opens a socket or calls the admin APIs. They supply URIs, TLS contexts, token caching, and handshake bytes for a caller-supplied transport.

## 4. Oracle (`oracle/`)

| Symbol | Responsibility |
|---|---|
| [`OracleConnectDescriptor`](../../../../../src/py/vertebra/db/oracle/config.py) | Renders a full `(DESCRIPTION=...)` string with timeouts, retries, `LOAD_BALANCE`/`FAILOVER`, optional `SDU`, and the `SSL_SERVER_DN_MATCH` security clause. |
| `OracleConfig` | `build_effective_dsn` resolves in order: wallet TNS alias, then `dsn` (passed through wallet aliases), then a generated descriptor when `hosts`/`drcp`/`tac_profile`/`sdu` are set (TCPS when a wallet exists, SDU 65535 for `batch`/`exadata`), else `host:port/service`. `to_oracledb_connect_params` returns `oracledb` Thin-mode kwargs (`cclass` defaults to `VERTEBRA_{BIND_KEY}` under DRCP). `derive_pool` copies the config with merged `driver_features` so a secondary pool cannot change the primary's `fetch_lobs=False` default. |
| [`OracleWallet`](../../../../../src/py/vertebra/db/oracle/wallet.py) | `extract` unpacks a `Wallet_*.zip` (path or bytes) or copies a wallet directory into `<root>/vertebra-wallets/wallet-<sha16>` with `0o700` directories and `0o600` files. Zip members are flattened to basenames and must stay inside the target. `ewallet.p12` is converted to `ewallet.pem` when `cryptography` is importable. `os.environ["TNS_ADMIN"]` is never set, so pools with different wallets can coexist. `parse_tnsnames` handles comments, multi-alias entries, and nested parentheses, and upper-cases aliases. `build_ssl_context` builds a Thin-mode mTLS context. |
| [`SyncOracleDriver`](../../../../../src/py/vertebra/db/oracle/driver.py) / `AsyncOracleDriver` | Resolve the wallet at construction. `encode_vector` returns `array.array("f"/"d")` for `VECTOR` binds. `prepare_bind_params` converts numeric lists to vectors and dicts to JSON via `encode_json`. `fetch_df` wraps caller-supplied rows. Neither runs SQL. |
| `OracleDataFrame` | Column/row container with `to_dicts`, `to_arrow_table` (imports `pyarrow`), and `__arrow_c_stream__`. |
| `OraclePoolRegistry` | Named `AsyncOracleDriver` pools built with `derive_pool`. `get_pool("default")` registers lazily. |
| [`OracleContainer`](../../../../../src/py/vertebra/db/oracle/containers.py) / `OrdsContainer` | Container image settings for Autonomous Database Free (`adb-free:latest-23ai`, ports 1521/1522/8443, `env_vars`, `jdbc_thin_dsn`) and ORDS (`ords:latest`, port 8181, `ords_base_url`). They hold settings only and do not start containers. |

## 5. `vertebra-db` Rust Crate

[`src/rs/vertebra-db/src/lib.rs`](../../../../../src/rs/vertebra-db/src/lib.rs) is an `rlib` workspace member with no PyO3 binding and no dependents. It re-implements the same helpers in Rust: `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `AlloyDbAuthType`, `encode_alloydb_metadata_exchange`, `PoolerMode` (`parse` also accepts `pgbouncer_transaction`, `pgcat`, `pgdog`), `compute_worker_pool_budget`, `strip_pg_jsonb_header`, `parse_tnsnames_ora`, `resolve_tns_alias`, `OracleConnectDescriptor`, `encode_resp_command`, `encode_hello3_handshake`, and `DbError`. Unit tests live in the file's `mod tests`. Python code does not call it.
