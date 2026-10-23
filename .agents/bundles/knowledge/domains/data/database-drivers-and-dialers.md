---
type: Guide
title: "Database Drivers & Cloud Dialers"
description: "The `vertebra.db` driver facades, `DatabaseRegistry`, and Cloud SQL and AlloyDB dialers."
status: stable
tags: [domains, database, drivers, cloud-sql, alloydb]
sources:
  - resource: src/py/vertebra/db/__init__.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/vertebra/typing.py
  - resource: src/py/vertebra/db/postgres.py
  - resource: src/py/vertebra/db/mysql.py
  - resource: src/py/vertebra/db/sqlite.py
  - resource: src/py/vertebra/db/redis.py
  - resource: src/py/vertebra/db/valkey.py
  - resource: src/py/vertebra/db/dialer/cloudsql.py
  - resource: src/py/vertebra/db/dialer/alloydb.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
---

# Database Drivers & Cloud Dialers

[`vertebra.db`](../../../../../src/py/vertebra/db/__init__.py) holds typed connection configs (`msgspec.Struct(frozen=True, gc=False, kw_only=True)`), sync/async driver classes, and cloud dialer helpers. Query composition, CDC, and tenancy in the same package are covered in [query-pipeline-cdc-and-tenancy](query-pipeline-cdc-and-tenancy.md).

> [!NOTE]
> Database drivers provide both synchronous and asynchronous interfaces (`Sync*Driver` / `Async*Driver`). SQLite operates via native `sqlite3`. Postgres, MySQL, Redis, and Valkey implement concrete network protocol transports with socket I/O, handshake negotiation, authentication (including SCRAM, IAM, and TLS/mTLS), statement preparation, connection leasing, and binary/RESP wire serialization. Every `Async*Driver` wraps or mirrors its sync counterpart. Oracle relational connectivity, Autonomous Database wallet extraction, and 26ai hybrid vector/text search are delegated directly to `sqlspec>=0.68.0` (`sqlspec.adapters.oracledb` and `sqlspec.adapters.adbc`), while [`vertebra.extensions.apex`](../extensions/oracle-apex.md) and [`vertebra.framework.queues.oracle`](../framework/distributed-queues-and-email.md) provide Oracle APEX and AQ/TxEventQ integration.

## 1. Registry (`_registry.py`)

[`DatabaseRegistry`](../../../../../src/py/vertebra/db/_registry.py) maps names to configs and lazily built drivers.

- `parse_url(url, **overrides)` dispatches on the scheme prefix before `+`: `postgres`/`postgresql` → `PostgresConfig`, `mysql`/`mariadb` → `MysqlConfig`, `sqlite` → `SqliteConfig` (`:memory:` for an empty path), `redis`/`rediss` → `RedisConfig`, `valkey`/`valkeys` → `ValkeyConfig`, `cloudsql` → `CloudSqlConfig`, `alloydb` → `AlloyDbConfig`.
- `register(name, config_or_url)` stores a config and drops any cached driver. `get_driver(name)` builds the `Async*Driver` or dialer that matches the config type.
- `vertebra.extensions.sqlalchemy.dialects` integrates drivers into SQLAlchemy 2.1 via the `+vtb` dialect prefix.

## 2. Driver Implementations

| Module | Config | Driver behavior |
|---|---|---|
| [`postgres.py`](../../../../../src/py/vertebra/db/postgres.py) | `PostgresConfig`, `PoolerMode = Literal["direct", "transaction", "session"]` | Native protocol framing (`StartupMessage`, `PasswordMessage`, `Query`, `Parse`/`Bind`/`Execute`/`Sync`). `execute_tuples` negotiates SSL/TLS or cleartext sockets, authenticates via md5/cleartext, binds parameters, and decodes binary/text rows. `strip_jsonb_header` validates the `0x01` JSONB version byte and returns a `memoryview` slice without copying. |
| [`mysql.py`](../../../../../src/py/vertebra/db/mysql.py) | `MysqlConfig` | Handshake v10 packet parsing, `COM_QUERY` dispatch, and binary/text result row decoding. |
| [`sqlite.py`](../../../../../src/py/vertebra/db/sqlite.py) | `SqliteConfig` | Opens `sqlite3.connect(..., check_same_thread=False)`, applies `startup_pragmas()`, commits after each `execute_tuples`. |
| [`redis.py`](../../../../../src/py/vertebra/db/redis.py) | `RedisConfig` | Opens TCP/TLS streams, emits `RESP2`/`RESP3` frames (`encode_command`, `encode_pipeline`), and parses bulk strings, integers, arrays, and errors. Includes bounded `timeout` on socket closure. |
| [`valkey.py`](../../../../../src/py/vertebra/db/valkey.py) | `ValkeyConfig` | Full Redis compatibility plus client-side cache tracking (`CLIENT TRACKING ON ... BCAST`), push notification handling, and GCP Memorystore IAM token authentication. |

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
