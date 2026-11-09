---
type: Research
flow_id: native-data-platform
title: "Rust driver crate selection per engine"
description: "Primary-source comparison of Rust client crates for Postgres, MySQL, Oracle, MSSQL, SQLite, DuckDB, Redis/Valkey, and Google Cloud, with per-engine PyO3 wheel packaging."
status: stable
state: promoted
promoted_to: native-data-platform
created_at: "2026-11-03T04:25:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: crates-io
    resource: https://crates.io/api/v1/crates
    title: crates.io package metadata and version API
  - id: rust-postgres
    resource: https://github.com/rust-postgres/rust-postgres
    title: postgres-protocol 0.6.12 and tokio-postgres 0.7.18 upstream repository
  - id: rust-oracledb
    resource: https://github.com/oracle/rust-oracledb
    title: Official Oracle pure-Rust thin client repository
  - id: oracledb-docs
    resource: https://docs.rs/oracledb/latest/oracledb/guide/index.html
    title: oracledb crate guide and supported feature matrix
  - id: mssql-rs
    resource: https://github.com/microsoft/mssql-rs
    title: Microsoft experimental mssql-tds Rust repository
  - id: tiberius
    resource: https://github.com/prisma/tiberius
    title: tiberius 0.13.0 TDS 7.3/8.0 Rust client
  - id: duckdb-rs
    resource: https://github.com/duckdb/duckdb-rs
    title: Official DuckDB Rust bindings with bundled engine and Arrow 60
  - id: redis-rs
    resource: https://github.com/redis-rs/redis-rs
    title: redis 1.8.1 Tokio + rustls RESP3 client
  - id: gcp-rust
    resource: https://github.com/googleapis/google-cloud-rust
    title: Official Google Cloud Rust SDK workspace
tags:
  - research
  - rust
  - crates
  - drivers
---

# Rust Driver Crate Selection

<!-- truth: start -->
- **Per-Engine Wheel Isolation**: Every database engine ships in an optional `src/rs/vertebra-<engine>` (`vertebra._<engine>`) wheel standardized on `rustls` + `ring`, isolating heavy C++ (`duckdb`) and divergent `arrow` (`59` vs `60`) trees behind `VTBARROW` IPC v5 shared memory [^crates-io] [^duckdb-rs].
- **Selected Upstream Crates**: `postgres-protocol` `0.6.12` (+ `tokio-postgres` dev oracle) [^rust-postgres], `oracledb =26.0.0-beta.5` [^rust-oracledb] [^oracledb-docs], `mysql_async` `0.37.1`, `tiberius` `0.13.0` (over OpenSSL-locked `mssql-tds`) [^mssql-rs] [^tiberius], `rusqlite` `0.41.1`, `duckdb` `1.20000.1` [^duckdb-rs], `redis` `1.8.1` [^redis-rs], and `google-cloud-{spanner,bigquery}` [^gcp-rust].
<!-- truth: end -->

Survey date: 2026-11-03 (packaging & naming updated 2026-11-04). Popularity uses crates.io 90-day `recent_downloads`; GitHub stars were not collected. Workspace baseline: tokio 1.54, rustls 0.23 with `ring`, hyper 1, sha2 0.10.

User decision (2026-11-04): build on the official or best-in-class client for each engine, expose separate `Sync<Engine>Client` and `Async<Engine>Client` PyO3 classes over `SyncCompletionQueue` and `AsyncCompletionQueue`, and package **every** database engine in its own dedicated PyO3 extension crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, matching `vertebra-bundler` and `vertebra-storage`) paired with `[project.optional-dependencies].<engine>` (`vertebra-core[<engine>]`). Shared foundation primitives (`VTBARROW` SHM framing, `TransportStream`/`CancelHandle`/`Connection`, `dialer.rs`, optional `gcp.rs` auth/error helpers, optional `py_async.rs` completion bridge, and pure-CPU codecs) live in `vertebra-db` (`rlib`, `default = []`), keeping `vertebra-core` (`vertebra._vertebra`), `vertebra-db`, and `vertebra-http` free of engine wire, gRPC, or C++ client trees.

## Recommendations

| Engine | Crate, features, and PyO3 wheel | Version | Confidence | Vertebra fills |
| --- | --- | --- | --- | --- |
| Postgres | `postgres-protocol` (codecs, SASL incl. SCRAM-SHA-256-PLUS) + Vertebra connection in `src/rs/vertebra-postgres` (`vertebra._postgres`, `[postgres]` extra); `tokio-postgres` as dev test oracle; `tokio-postgres-rustls` (`ring`) for TLS | 0.6.12 / 0.7.18 / 0.14.0 | high | `SyncPostgresClient` (`SyncCompletionQueue`) & `AsyncPostgresClient` (`AsyncCompletionQueue`), single-`Sync` batch pipeline, statement cache, DataRow → `VTBARROW`, pgvector/JSONB/pgcopy codecs, Cloud SQL/AlloyDB IAM |
| MySQL/MariaDB | `mysql_async` (`default-features = false`, `default-rustls-ring`) in `src/rs/vertebra-mysql` (`vertebra._mysql`, `[mysql]` extra) | 0.37.1 | medium | `SyncMySQLClient` & `AsyncMySQLClient`, binary-protocol rows → Arrow, Cloud SQL IAM, MariaDB conformance |
| Oracle | `oracledb` (official pure-Rust thin, `arrow` feature), pinned `=26.0.0-beta.5` in `src/rs/vertebra-oracle` (`vertebra._oracle`, `[oracle]` extra) | 26.0.0-beta.5 | high (facts), medium (fit) | `SyncOracleClient` (`SyncCompletionQueue`) and full non-blocking `AsyncOracleClient` / `AsyncOracleDriver` (`AsyncCompletionQueue` + `Notifier`) over per-connection Rust worker threads, sequential `pipeline()` until upstream 26ai TTC pipelining lands, OSON/VECTOR fast paths, IAM/token auth, AQ/TxEventQ, `arrow-ipc 59` → `VTBARROW` + `__arrow_c_stream__` |
| MSSQL | `tiberius` (`default-features = false`, `tds80`, `rustls`, `chrono`, `rust_decimal`) in `src/rs/vertebra-mssql` (`vertebra._mssql`, `[mssql]` extra) | 0.13.0 | medium | `SyncMSSQLClient` & `AsyncMSSQLClient`, token stream → Arrow, ATTENTION cancellation, Entra ID (#68) |
| SQLite | `rusqlite` (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`), one thread per connection in `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `[sqlite]` extra) | 0.41.1 | high | `SyncSQLiteClient` (`SyncCompletionQueue`) & `AsyncSQLiteClient` (`AsyncCompletionQueue`), Arrow column building from `sqlite3_column_*` |
| DuckDB | `duckdb` (`bundled`, `appender-arrow`) in `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `[duckdb]` extra) | 1.20000.1 | high | `SyncDuckDBClient` (`SyncCompletionQueue`) & `AsyncDuckDBClient` (`AsyncCompletionQueue`), `arrow-ipc 60` `RecordBatch` → `VTBARROW` + `__arrow_c_stream__` |
| Redis/Valkey | `redis` (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) in `src/rs/vertebra-redis` (`vertebra._redis`, `[redis]`/`[valkey]` extras) | 1.8.1 | medium-high | `SyncRedisClient` & `AsyncRedisClient`, `resp3.rs` hot-path evaluation, SHM pub/sub fan-out |
| BigQuery/Spanner | `google-cloud-bigquery` (`=0.19.0`) in `src/rs/vertebra-bigquery` (`vertebra._bigquery`, `[bigquery]` extra) and `google-cloud-spanner` (`=0.34.8-preview`) in `src/rs/vertebra-spanner` (`vertebra._spanner`, `[spanner]` extra) (`default-features = false`, `tonic/tls-ring`, `tonic/tls-webpki-roots`) | 0.19.0 / 0.34.8-preview | high | `SyncBigQueryClient` / `AsyncBigQueryClient` and `SyncSpannerClient` / `AsyncSpannerClient`, `GcpTokenCredentials` bridge over `SyncGcpTokenCache` / `AsyncGcpTokenCache` (`vertebra-db` `gcp` feature), BigQuery REST `jobs.query` / `jobs.getQueryResults`, bounded `WriteRetryableErrors` attempt cap, `VTBARROW` SHM framing |

## Per-Engine Notes

### Postgres

**Candidates**
- **tokio-postgres 0.7.18** (2026-06-12), now under the `rust-postgres` org.
  - Binary COPY, `CancelToken`, LISTEN/NOTIFY.
  - Pipelines automatically when futures are polled concurrently.
  - Every request ends with its own `Sync` (`tokio-postgres/src/query.rs`), so a single-`Sync` batch cannot be built through the public API.
- **sqlx 0.9.0**: a toolkit layer with no user-controlled pipelining.
- **deadpool-postgres**: unnecessary, because the broker owns the pools.
- **psqlpy** builds on forks of tokio-postgres, postgres-protocol, and deadpool. That suggests upstream extension points are not enough for a Python driver.

**Decision**
- Use `postgres-protocol` plus a Vertebra-owned connection in `vertebra-postgres` (`vertebra._postgres`), reusing the Parse/Bind/Execute/Sync encoders in `vertebra-db/src/postgres/mod.rs`.
- Fallback if per-statement `Sync` is acceptable: wrap tokio-postgres directly, with raw bytes taken through a custom `FromSql`.

### MySQL
- `mysql_async` in `vertebra-mysql` (`vertebra._mysql`) is the only mature Tokio-native client. Single maintainer.
- The classic MySQL protocol has no pipelining.
- `sqlx-mysql` is a toolkit layer.

### Oracle

**Official crate**
- `oracledb` 26.0.0-beta.5 (2026-10-28), UPL-1.0 OR Apache-2.0, maintained by Oracle, packaged in `vertebra-oracle` (`vertebra._oracle`). The docs call it pre-release with APIs subject to change.
- Tested on 19c, 21c, and 26ai.
- The internal Rust socket API is synchronous (`connect()`, Iterator `Cursor`, sync `Pool`), with no Tokio dependency; `vertebra-oracle` runs per-connection Rust worker threads behind `SyncOracleClient` (`SyncCompletionQueue`) and `AsyncOracleClient` (`AsyncCompletionQueue` + `Notifier`) so Python gets both `SyncOracleDriver` and full non-blocking `AsyncOracleDriver`.
- rustls 0.23. Optional Arrow (arrow 59) with `query_arrow` and Arrow binds.
- Supports DRCP, statement caching, array DML, VECTOR/JSON/LOBs.
- Docs Appendix A lists these as unsupported: 26ai pipelining, token auth, AQ/TxEventQ, CQN, direct path loads, Application Continuity, TPC, implicit results.

**Alternatives**
- `oracle` (rust-oracle 0.6.3) uses ODPI-C and needs Instant Client.
- `sibyl` and `oracle-rs` were not verified.

### MSSQL
- **tiberius 0.13.0** in `vertebra-mssql` (`vertebra._mssql`): TDS 7.3/8.0, Tokio through a compat layer, `rustls` feature (default is native-tls). No pipelining or MARS.
- **mssql-tds 0.3.0** (Microsoft, `microsoft/mssql-rs`, created 2026-09):
  - Forces native-tls/OpenSSL on Linux.
  - Pre-1.0 and closed to external PRs.
  - Watch list only.
- `mssql-client` is a community crate, not Microsoft's.
- sqlx removed MSSQL in 0.7.

### SQLite
- `rusqlite` 0.41.1 bundled in `vertebra-sqlite` (`vertebra._sqlite`) is fast and self-contained.
- `turso` 0.8.3 is a pre-1.0 Rust rewrite with unverified parity.

### DuckDB
- `duckdb` 1.20000.1 is official and packaged in `vertebra-duckdb` (`vertebra._duckdb`).
- It has a hard dependency on arrow ^60 (`ffi`).
- `bundled` compiles the C++ engine: isolating it in `vertebra-duckdb` keeps `vertebra-core` and every other driver wheel free of the ~35+ MB C++ amalgamation.
- The mapping from crate version to engine version is unverified.

### Redis/Valkey
- **redis 1.8.1** (2026-10-30, BSD-3) in `vertebra-redis` (`vertebra._redis`): features include cluster-async, client-side caching (`cache-aio`), sentinel, and entra-id.
- **fred 10.1.0**: last release 2025-02-27, so stale.
- **glide-core**: not intended for direct use.

### Google Cloud
- `google-cloud-spanner =0.34.8-preview` (crates.io `default_version` still points to `0.33.0` from the prior owner, requiring `=` pre-release pinning) and `google-cloud-bigquery =0.19.0` are extracted into `src/rs/vertebra-spanner` (`vertebra._spanner`) and `src/rs/vertebra-bigquery` (`vertebra._bigquery`) in `db-layout-and-contract`, sharing `vertebra-db` (`features = ["gcp", "pyo3"]`).
- Both run with `default-features = false` (disabling `aws-lc-rs` and `builtin-metrics`), `tonic` `tls-ring` + `tls-webpki-roots`, and explicit `rustls::crypto::ring::default_provider()` installation.

## Risks

1. **Oracle internal Rust sync I/O and 26ai pipelining (full async Python preserved).** `oracledb =26.0.0-beta.5` uses blocking Rust sockets internally (`std::net::TcpStream`, no `tokio`) and lacks 26ai TTC pipelining. Python still gets full non-blocking `AsyncOracleDriver` (`asyncio` / `oracle+vertebra_async`): `AsyncOracleClient` dispatches requests with the GIL detached onto per-connection Rust worker threads and resolves Python `asyncio.Future` awaitables through `AsyncCompletionQueue` + `Notifier` (`vertebra-db/src/py_async.rs`), while `SyncOracleClient` waits on `SyncCompletionQueue` with the GIL detached, and out-of-band `connection.break_execution()` is wired to `_DbTaskCanceller` on task cancellation. The only cost vs a Tokio socket client is one OS thread per pooled Oracle connection in Rust (`pool_size`), and `SyncOraclePipeline` / `AsyncOraclePipeline` execute queued operations sequentially with a tracking test until upstream 26ai pipelining lands.
2. **Arrow `59` vs `60` crate versions (isolated per wheel, zero runtime conflict).** `vertebra-oracle` depends on pure-Rust `arrow 59` while `vertebra-duckdb` depends on `arrow 60`. Because each driver compiles as its own `cdylib` wheel (`vertebra._oracle` and `vertebra._duckdb`) and `VTBARROW` (`vertebra-db/src/arrow_shm.rs`) frames standard Arrow IPC v5 stream bytes in `/dev/shm` rather than passing Rust `arrow_array::RecordBatch` structs across crates, there is zero symbol collision or wire incompatibility even when both wheels are loaded in the same Python process (and workspace-wide `cargo test --workspace` resolves both cleanly under Cargo `resolver = "2"`).
3. **Postgres single-`Sync` batches** are impossible on the tokio-postgres `Client`. That is why `vertebra-postgres` uses `postgres-protocol` plus a Vertebra connection.
4. **Duplicate crypto stacks.** Spanner defaults to aws-lc-rs, mssql-tds forces OpenSSL, oracledb uses sha2 0.11 against the workspace's 0.10, and tiberius defaults to native-tls. Disable default features everywhere and standardize on rustls + ring.
5. **Maintenance.** fred is stale, mssql-tds is closed to PRs, oracledb is beta, and mysql_async has a single maintainer.
6. **No primary-source cross-crate benchmarks.** `make bench-check` decides each adoption.

[^crates-io]: crates.io package metadata and version API (https://crates.io/api/v1/crates).
[^rust-postgres]: postgres-protocol 0.6.12 and tokio-postgres 0.7.18 upstream repository (https://github.com/rust-postgres/rust-postgres).
[^rust-oracledb]: Official Oracle pure-Rust thin client repository (https://github.com/oracle/rust-oracledb).
[^oracledb-docs]: oracledb crate guide and supported feature matrix (https://docs.rs/oracledb/latest/oracledb/guide/index.html).
[^mssql-rs]: Microsoft experimental mssql-tds Rust repository (https://github.com/microsoft/mssql-rs).
[^tiberius]: tiberius 0.13.0 TDS 7.3/8.0 Rust client (https://github.com/prisma/tiberius).
[^duckdb-rs]: Official DuckDB Rust bindings with bundled engine and Arrow 60 (https://github.com/duckdb/duckdb-rs).
[^redis-rs]: redis 1.8.1 Tokio + rustls RESP3 client (https://github.com/redis-rs/redis-rs).
[^gcp-rust]: Official Google Cloud Rust SDK workspace (https://github.com/googleapis/google-cloud-rust).
