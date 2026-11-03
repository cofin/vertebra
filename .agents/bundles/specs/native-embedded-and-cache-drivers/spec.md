---
type: Spec
flow_id: native-embedded-and-cache-drivers
title: SQLite, DuckDB, and Redis/Valkey in Rust
description: Rust-backed SQLite and DuckDB drivers with zero-copy Arrow, and a complete RESP3 Redis/Valkey client with cluster, pub/sub, and client tracking, removing sqlite fallbacks and memory:// modes.
state: draft
parent_prd: native-data-platform
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [rust, sqlite, duckdb, redis, valkey]
---

# Flow Charter: SQLite, DuckDB, and Redis/Valkey in Rust

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **SQLite.** `rusqlite` 0.41.1 (`bundled`, `hooks`, `serialize`, `blob`, `column_decltype`), one dedicated thread per connection. Vertebra builds the async bridge and Arrow column building.
- **DuckDB.** `duckdb` 1.20000.1 (`bundled`, `appender-arrow`), behind the existing `duckdb` Cargo feature. It carries a hard arrow ^60 dependency and a C++ build. Vertebra runs it on blocking threads and exports `RecordBatch` to VTBARROW through the Arrow FFI.
- **Redis/Valkey.** `redis` 1.8.1 (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`). `fred` is stale (last release 2025-02) and `glide-core` is not meant for direct use. Vertebra benchmarks its `resp3.rs` decoding against the crate's and builds the SHM pub/sub fan-out.
- **Packaging.** All are optional Cargo features and Python extras. The default build stays lean.

## Scope

1. **SQLite.** A Rust SQLite engine (bundled `rusqlite`) behind `db/adapters/sqlite/driver.py`. It covers WAL, busy timeout, prepared statement cache, `execute_many`, Arrow export/import, and a thread-offloaded async form. The existing `vertebra-db/src/sqlite.rs` lease/statement pieces are reused.
2. **DuckDB.** A Rust DuckDB engine (`duckdb` crate, bundled) behind `db/adapters/duckdb/driver.py`. It covers the zero-copy Arrow C data interface into `ArrowCursor`, secrets bootstrap (`duckdb_bootstrap_statements`), and extensions. The sqlite3 fallback is removed.
3. **Redis/Valkey.** A complete RESP3 client on `resp3.rs`:
   - connection, AUTH/ACL, TLS, and `HELLO 3`;
   - pipelining (existing `execute_pipeline`) and transactions (MULTI/EXEC/WATCH);
   - pub/sub and sharded pub/sub implementing `NotificationSource`;
   - streams, cluster slot routing (existing `ClusterRedirect`), and Valkey `CLIENT TRACKING` invalidation.
   - The Python socket code (`db/redis/driver.py::_create_raw_socket`, `asyncio.open_connection`) and the Python RESP parser (`db/redis/core.py::RedisCore._parse_view`) are replaced by the Rust client: Tokio + rustls I/O, a multiplexed pipelined connection, RESP3 push for tracking, and Sentinel.
   - The `memory://` mode is removed.
   - The DuckDB crate's C++ build cost (several minutes, roughly 30–50 MB, medium confidence) is measured. Arrow export passes through the Arrow C Data Interface, so no arrow-rs dependency is added.

## Public Seams

The SQLite, DuckDB, Redis, and Valkey sync/async drivers.

## Dependencies

`db-layout-and-contract`.

## Acceptance Boundary

- The conformance suite passes for SQLite and DuckDB.
- Redis 7/8 and Valkey 8 integration tests pass, including cluster mode.
- `make bench-check` shows no regression against stdlib `sqlite3` or the `duckdb` module.

## Draft Task Outline

- t01 Rust SQLite engine + cutover.
- t02 Rust DuckDB engine + Arrow C data interface + cutover.
- t03 RESP3 connection/TLS/ACL + transactions.
- t04 Pub/sub + streams + `NotificationSource`.
- t05 Cluster routing + client tracking + `memory://` removal.
