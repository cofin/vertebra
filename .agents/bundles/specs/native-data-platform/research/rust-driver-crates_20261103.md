---
type: Research
flow_id: native-data-platform
title: Rust driver crate selection per engine
description: Primary-source comparison of Rust client crates for Postgres, MySQL, Oracle, MSSQL, SQLite, DuckDB, Redis/Valkey, and Google Cloud, with one recommendation per engine and the gaps Vertebra fills itself.
status: stable
created_at: 2026-11-03T04:25:00Z
updated_at: 2026-11-03T04:25:00Z
tags: [research, rust, crates, drivers]
sources:
  - resource: https://crates.io/api/v1/crates
  - resource: https://github.com/rust-postgres/rust-postgres
  - resource: https://github.com/oracle/rust-oracledb
  - resource: https://docs.rs/oracledb/latest/oracledb/guide/index.html
  - resource: https://github.com/microsoft/mssql-rs
  - resource: https://github.com/prisma/tiberius
  - resource: https://github.com/duckdb/duckdb-rs
  - resource: https://github.com/redis-rs/redis-rs
  - resource: https://github.com/googleapis/google-cloud-rust
---

# Rust Driver Crate Selection

Survey date: 2026-11-03. Popularity uses crates.io 90-day `recent_downloads`; GitHub stars were not collected. Workspace baseline: tokio 1.54, rustls 0.23 with `ring`, hyper 1, sha2 0.10.

User decision (2026-11-03): build on the official or best-in-class client for each engine, extend it as needed, and require it as an optional dependency (Cargo feature + Python extra).

## Recommendations

| Engine | Crate and features | Version | Confidence | Vertebra fills |
| --- | --- | --- | --- | --- |
| Postgres | `postgres-protocol` (codecs, SASL incl. SCRAM-SHA-256-PLUS) under a Vertebra-owned connection state machine; `tokio-postgres` as reference/test oracle; `tokio-postgres-rustls` (`ring`) for TLS | 0.6.12 / 0.7.18 / 0.14.0 | high | Single-`Sync` batch pipeline and pipeline mode, statement cache, DataRow → VTBARROW decoding, pgvector/JSONB/pgcopy codecs (already in `postgres.rs`), Cloud SQL/AlloyDB IAM |
| MySQL/MariaDB | `mysql_async` (`default-features = false`, `default-rustls-ring`) | 0.37.1 | medium | Binary-protocol rows → Arrow (check raw row access in `mysql_common`), Cloud SQL IAM, MariaDB conformance |
| Oracle | `oracledb` (official, pure-Rust thin, `arrow` feature), pinned `=26.0.0-beta.5` | 26.0.0-beta.5 | high (facts), medium (fit) | Async bridge (sync API), 23ai/26ai TTC pipelining (crate lists it as unsupported), OSON/VECTOR fast paths, IAM/token auth, AQ/TxEventQ, Arrow 59 → VTBARROW via the C Data Interface |
| MSSQL | `tiberius` (`default-features = false`, `tds80`, `rustls`, `chrono`, `rust_decimal`) | 0.13.0 | medium | Token stream → Arrow, ATTENTION cancellation (unverified in tiberius), Entra ID (#68) |
| SQLite | `rusqlite` (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`), one thread per connection | 0.41.1 | high | Async bridge, Arrow column building from `sqlite3_column_*` |
| DuckDB | `duckdb` (`bundled`, `appender-arrow`) behind the existing `duckdb` Cargo feature | 1.20000.1 | high | Blocking-thread execution, Arrow 60 `RecordBatch` → VTBARROW through the FFI, lean default build |
| Redis/Valkey | `redis` (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) | 1.8.1 | medium-high | Decide whether `resp3.rs` decoding stays on the hot path; SHM pub/sub fan-out |
| BigQuery/Spanner | Keep the hand-written clients | — | medium | Revisit `google-cloud-spanner`/`google-cloud-bigquery` at 1.0 (disable `builtin-metrics` and `default-rustls-provider`) |

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
- Use `postgres-protocol` plus a Vertebra-owned connection, reusing the existing Parse/Bind/Execute/Sync encoders and `PostgresPool`.
- Fallback if per-statement `Sync` is acceptable: wrap tokio-postgres directly, with raw bytes taken through a custom `FromSql`.

### MySQL
- `mysql_async` is the only mature Tokio-native client. Single maintainer.
- The classic MySQL protocol has no pipelining.
- `sqlx-mysql` is a toolkit layer.

### Oracle

**Official crate**
- `oracledb` 26.0.0-beta.5 (2026-10-28), UPL-1.0 OR Apache-2.0, maintained by Oracle. The docs call it pre-release with APIs subject to change.
- Tested on 19c, 21c, and 26ai.
- The API is synchronous (`connect()`, Iterator `Cursor`, sync `Pool`), with no Tokio dependency.
- rustls 0.23. Optional Arrow (arrow 59) with `query_arrow` and Arrow binds.
- Supports DRCP, statement caching, array DML, VECTOR/JSON/LOBs.
- Docs Appendix A lists these as unsupported: 26ai pipelining, token auth, AQ/TxEventQ, CQN, direct path loads, Application Continuity, TPC, implicit results.

**Alternatives**
- `oracle` (rust-oracle 0.6.3) uses ODPI-C and needs Instant Client.
- `sibyl` and `oracle-rs` were not verified.

### MSSQL
- **tiberius 0.13.0**: TDS 7.3/8.0, Tokio through a compat layer, `rustls` feature (default is native-tls). No pipelining or MARS.
- **mssql-tds 0.3.0** (Microsoft, `microsoft/mssql-rs`, created 2026-09):
  - Forces native-tls/OpenSSL on Linux.
  - Pre-1.0 and closed to external PRs.
  - Watch list only.
- `mssql-client` is a community crate, not Microsoft's.
- sqlx removed MSSQL in 0.7.

### SQLite
- `rusqlite` 0.41.1 bundled is cheap to build.
- `turso` 0.8.3 is a pre-1.0 Rust rewrite with unverified parity.

### DuckDB
- `duckdb` 1.20000.1 is official.
- It has a hard dependency on arrow ^60 (`ffi`).
- `bundled` compiles the C++ engine: high build cost and large binary. The default links a system libduckdb.
- The mapping from crate version to engine version is unverified.

### Redis/Valkey
- **redis 1.8.1** (2026-10-30, BSD-3): features include cluster-async, client-side caching (`cache-aio`), sentinel, and entra-id.
- **fred 10.1.0**: last release 2025-02-27, so stale.
- **glide-core**: not intended for direct use.

### Google Cloud
- `google-cloud-spanner` 0.35.0 on `main`; crates.io still shows 0.33.0 from the previous owner.
- `google-cloud-bigquery` 0.19.0.
- Both are pre-1.0. Spanner defaults to aws-lc-rs plus a background metrics exporter.

## Risks

1. **Oracle contradicts the spec.** The official crate is synchronous and lacks 26ai pipelining, while the plan requires async on Tokio and 23ai pipelining. This is resolved in `native-oracle-driver` (decision pending).
2. **Arrow version skew.** oracledb wants arrow 59 and duckdb wants arrow 60. Both stay behind their Cargo features and bridge to VTBARROW through the Arrow C Data Interface; enabling both pulls two arrow trees.
3. **Postgres single-`Sync` batches** are impossible on the tokio-postgres `Client`. That is why the recommendation uses postgres-protocol plus a Vertebra connection.
4. **Duplicate crypto stacks.** Spanner defaults to aws-lc-rs, mssql-tds forces OpenSSL, oracledb uses sha2 0.11 against the workspace's 0.10, and tiberius defaults to native-tls. Disable default features everywhere and standardize on rustls + ring.
5. **Maintenance.** fred is stale, mssql-tds is closed to PRs, oracledb is beta, and mysql_async has a single maintainer.
6. **No primary-source cross-crate benchmarks.** `make bench-check` decides each adoption.
