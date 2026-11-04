---
type: Spec
flow_id: native-embedded-and-cache-drivers
title: Standalone SQLite, DuckDB, and Redis/Valkey Wheels in Rust
description: Standalone PyO3 crates src/rs/vertebra-sqlite (vertebra._sqlite), src/rs/vertebra-duckdb (vertebra._duckdb), and src/rs/vertebra-redis (vertebra._redis for [redis] and [valkey]) with zero-copy VTBARROW ShmCapsule results, cluster routing, pub/sub NotificationSource, and client-side tracking, removing sqlite3 fallbacks and memory:// modes.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-04T01:17:00Z
tags: [rust, sqlite, duckdb, redis, valkey, wheel]
---

# Flow Charter: Standalone SQLite, DuckDB, and Redis/Valkey Wheels in Rust

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **SQLite (`src/rs/vertebra-sqlite/` -> `vertebra._sqlite` + `vertebra-core[sqlite]`).** `rusqlite` 0.41.1 (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`) packaged in its own PyO3 wheel (`SyncSQLiteClient` over `SyncCompletionQueue` and `AsyncSQLiteClient` over `AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async` with `"_vertebra_sqlite_completion_drainer"`), one dedicated worker thread per connection, `sqlite3_interrupt()` cancellation, and direct `sqlite3_column_*` -> `VTBARROW` `ShmCapsule` `ArrowCursor` building.
- **DuckDB (`src/rs/vertebra-duckdb/` -> `vertebra._duckdb` + `vertebra-core[duckdb]`).** `duckdb` 1.20000.1 (`bundled`, `appender-arrow`, `arrow 60`) isolated in its own PyO3 wheel (`SyncDuckDBClient` and `AsyncDuckDBClient` via `SyncCompletionQueue` and `AsyncCompletionQueue` with `"_vertebra_duckdb_completion_drainer"`, `_duckdb.pyi`, `uv lock --offline`) so `vertebra-core` stays free of the ~35+ MB C++ DuckDB amalgamation and `arrow 60` never collides with `vertebra-oracle` (`arrow 59`), serializing `RecordBatch` streams directly to `VTBARROW` `ShmCapsule` `ArrowCursor` instances.
- **Redis / Valkey (`src/rs/vertebra-redis/` -> `vertebra._redis` + `vertebra-core[redis]` / `vertebra-core[valkey]`).** `redis` 1.8.1 (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) packaged in its own PyO3 wheel (`SyncRedisClient` and `AsyncRedisClient` via `SyncCompletionQueue` and `AsyncCompletionQueue` with `"_vertebra_redis_completion_drainer"`, `_redis.pyi`), shared by `[project.optional-dependencies].redis` and `[project.optional-dependencies].valkey` and benchmarked against `vertebra_db::resp3` hot-path frame codecs.

## Scope

1. **SQLite (`vertebra-sqlite`).** Rust SQLite engine behind `vertebra/db/adapters/sqlite/driver.py` (`SyncSQLiteDriver`, `AsyncSQLiteDriver` via `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")`) covering WAL/mmap/`busy_timeout` pragmas, prepared statement cache, `execute_many`, `sqlite3_interrupt()` `cancel()`, `DatabaseError` mapping, and `VTBARROW` `ShmCapsule` export/import, replacing Python stdlib `sqlite3`.
2. **DuckDB (`vertebra-duckdb`).** Rust DuckDB engine behind `vertebra/db/adapters/duckdb/driver.py` (`SyncDuckDBDriver`, `AsyncDuckDBDriver` via `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")`) covering `arrow 60` -> `VTBARROW` `ShmCapsule` `ArrowCursor`, Appender `load_from_arrow`, GCS ADC secret bootstrap (`DuckDBCore.build_gcs_secret_sql`, relocated from `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` with a delegating wrapper retained in `extensions/sqlspec/storage.py` until `sqlspec-adapter-parity` removes generic engine helpers), `interrupt()` `cancel()`, and `DatabaseError` mapping, removing the `sqlite3` fallback in `DuckDB` and activating `duckdb` in `test_driver_conformance.py`.
3. **Redis & Valkey (`vertebra-redis` + `vertebra/db/adapters/{redis,valkey}/`).**
   - Tokio + `rustls` (`ring`) RESP3 client (`HELLO 3`, ACL `AUTH`, GCP Memorystore IAM token refresh via `SyncGcpTokenCache` / `AsyncGcpTokenCache`), pipelining (`execute_pipeline`), `MULTI`/`EXEC`/`WATCH` transactions, streams, Sentinel discovery (`discover_sentinel_master` / `async_discover_sentinel_master`), Cluster CRC16 slot routing (`MOVED`/`ASK`), and RESP3 `CLIENT TRACKING` invalidation push handling.
   - Pub/sub and sharded pub/sub (`SSUBSCRIBE`) implementing `NotificationSource` in `vertebra/db/adapters/redis/notify.py` (re-exported for Valkey).
   - Replace Python raw socket / `asyncio.open_connection` code in `vertebra/db/adapters/redis/driver.py` and the Python RESP parser in `vertebra/db/adapters/redis/core.py` with `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")`, and remove `memory://` fake mode (and `"memory"` from `RedisConfig.url_schemes`).

## Public Seams

`vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`), `vertebra._duckdb` (`SyncDuckDBClient`, `AsyncDuckDBClient`), `vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`), `SyncSQLiteDriver`, `AsyncSQLiteDriver`, `SyncDuckDBDriver`, `AsyncDuckDBDriver`, `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, `AsyncValkeyDriver`, and `RedisNotificationSource` (`NotificationSource`).

## Dependencies

`db-layout-and-contract` (`refactor/sync-async-names @ 08ac03a`) and `integration-remediation/db-engine-primitives`.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for SQLite and DuckDB.
- Integration tests pass against Redis 7/8 and Valkey 8 (standalone and cluster) via `pytest-databases`.
- `mypy.stubtest vertebra._sqlite vertebra._duckdb vertebra._redis`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `_sqlite.pyi`, `[sqlite]` extra + `uv.lock`), `rusqlite` engine, `VTBARROW` `ShmCapsule` builder, `sqlite3_interrupt` cancel, and Python `sqlite` cutover.
- t02 Scaffold `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `_duckdb.pyi`, `[duckdb]` extra + `uv.lock`), `arrow 60` -> `VTBARROW` `ShmCapsule`, Appender load, `DuckDBCore.build_gcs_secret_sql` relocation, and Python `duckdb` cutover.
- t03 Scaffold `src/rs/vertebra-redis` (`vertebra._redis`, `_redis.pyi`, `[redis]` + `[valkey]` extras + `uv.lock`) and RESP3 client (`HELLO 3`, TLS, ACL/IAM auth, pipelining, `MULTI`/`EXEC`).
- t04 Redis/Valkey pub/sub `NotificationSource` + streams.
- t05 Cluster slot routing, `CLIENT TRACKING` invalidation, Python `redis`/`valkey` cutover, and `memory://` removal.
