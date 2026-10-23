---
type: PRD
flow_id: shm-native-drivers-and-broker-proxy
title: "Two-Tier Shared-Memory Database & Storage Plane: Rust-Native SHM Drivers and Universal SQLSpec/Alchemy Broker Proxy"
state: closed
plan_revision: 2
plan_commit: null
state_revision: 1
current_task: null
last_operation: finish
operation_targets:
  - "1.1"
  - "1.2"
  - "2.1"
  - "2.2"
  - "2.3"
last_verified_checkpoint: "make rs-test && make py-test && make lint-all && make review-check"
created_at: 2026-10-23T20:18:00Z
updated_at: 2026-10-23T22:26:00Z
description: "Master PRD restoring and completing Vertebra's two-tier shared-memory data architecture: (1) Rust-native database (PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB), cache (Redis, Valkey), and cloud dialer drivers in vertebra-db integrated natively with shared-memory rings and Arrow IPC regions, and (2) the universal SHM broker proxy enabling any existing SQLSpec or Advanced Alchemy driver to run over shared memory."
tags: [prd, database, rust, shm, sqlspec, advanced-alchemy, arrow]
child_specs:
  - rust-native-shm-drivers
  - rust-native-enterprise-olap-drivers
  - shm-broker-driver-proxy
  - shm-database-broker
  - redis-valkey-resp3
research: []
---

# Master PRD: Two-Tier Shared-Memory Database & Storage Plane

**Flow ID:** `shm-native-drivers-and-broker-proxy`

## Specification

### Historical & Cross-Repo Architecture Synthesis

Across [`cofin/vertebra`](https://github.com/cofin/vertebra) (commits `809c8d77`, `5ca8e2ec`, `a5918811`, `6737c0c`), [`cofin/litecore`](https://github.com/cofin/litecore) (`mvp/4` / `feat/mvp-v3-post`), [`cofin/bungi`](https://github.com/cofin/bungi), and [`cofin/sqlspec`](https://github.com/cofin/sqlspec) (`feat/sqlbridge`, `feat/storage-driver-bridge`, `feat/cloudsql-connector`), Vertebra's data and storage plane was designed around **two complementary shared-memory tiers**:

1. **Tier 1 — Rust-Native Shared-Memory Drivers, Cloud Dialers & Storage (`vertebra-db` + `vertebra-storage` + `vertebra-core::broker` + `vertebra-py`)**:
   - **What happened**: In commit `6737c0c` ([PR #27](https://github.com/cofin/vertebra/pull/27)), `src/rs/vertebra-db` was introduced with Rust implementations of `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `encode_alloydb_metadata_exchange`, `PoolerMode`, `strip_pg_jsonb_header`, `encode_resp_command`, and `encode_hello3_handshake`. In commit `17ef2a3` ([PR #37](https://github.com/cofin/vertebra/pull/37)), `src/rs/vertebra-db` was mistakenly deleted because it was not yet wired into `vertebra-py`, and in `02efeac` ([PR #38](https://github.com/cofin/vertebra/pull/38)), pure-Python socket/pool wrappers were written in `src/py/vertebra/db/` instead.
   - **Target Architecture**:
     - Native Rust drivers in `src/rs/vertebra-db` for **PostgreSQL** (with zero-sidecar **Cloud SQL `:3307`** and **AlloyDB `:5433`** mTLS + `MetadataExchangeRequest` dialers, binary `COPY` -> Arrow, zero-copy binary `JSONB`), **MySQL** (with Cloud SQL `:3307` dialer), **SQLite** (WAL, mmap, busy timeout, multi-reader pool), **Redis**, and **Valkey** (RESP3 parser, `HELLO 3`, `CLIENT TRACKING` invalidation push frames, Sentinel discovery, Cluster CRC16 slot routing), alongside **`src/rs/vertebra-storage`** (Rust multi-cloud object storage for S3, GCS, Azure, OCI, R2, Local, and Memory).
     - The Rust supervisor/broker owns the physical TCP/TLS sockets, connection pools, and prepared statement caches. Python workers communicate over lock-free shared-memory rings (`SpscRing`/`MpmcRing`) for command dispatch and compact tuple results, and zero-copy `ShmRegion` Arrow IPC buffers (`VertebraArrowCursor` / `PyMemoryView`) for analytical and bulk result sets exceeding `arrow_threshold_rows` / `arrow_threshold_bytes`.
     - Exposed as first-class SQLSpec configs (`VertebraPostgresSyncConfig`, `VertebraPostgresAsyncConfig`, `VertebraMysqlSyncConfig`, `VertebraMysqlAsyncConfig`, `VertebraSqliteSyncConfig`, `VertebraSqliteAsyncConfig`), SQLAlchemy 2.1 `+vtb` / `+vertebra` dialects (`postgresql+vtb`, `mysql+vtb`, `sqlite+vtb` with `NullPool` over PEP 249 SHM DBAPI), and Advanced Alchemy configs (`VertebraAsyncAlchemyConfig`, `VertebraSyncAlchemyConfig`).

2. **Tier 2 — Universal Shared-Memory Broker Proxy for Any Existing SQLSpec or Advanced Alchemy Driver (`ProxySyncConfig[ConfigT]`, `ProxyAsyncConfig[ConfigT]`, `SqlSpecProvider`)**:
   - **What happened**: In `a5918811` (`src/py/vertebra/extensions/sqlspec/{proxy,provider}.py`) and `cofin/litecore` (`litecore-db-broker-prd`), `ProxySyncConfig[SyncConfigT]` and `ProxyAsyncConfig[AsyncConfigT]` wrapped **any** upstream `sqlspec` or `advanced-alchemy` config and routed queries over SHM request/response rings (`/{name}.provider.{pool}.req` / `.resp`) to a single `SqlSpecProvider` (`ProviderHost`) owning the upstream driver pool, spilling large result sets to a `ShmRegion` Arrow IPC buffer (`_write_arrow_region` / `_arrow_release`). In the current [`src/py/vertebra/extensions/sqlspec/plugin.py`](../../../../src/py/vertebra/extensions/sqlspec/plugin.py#L272-L452), `ProxySyncConfig` and `ProxyAsyncConfig` store `self.broker` as an unused attribute and delegate `provide_session()` / `provide_connection()` / `create_pool()` directly in-process.
   - **Target Architecture**:
     - Wire `ProxySyncConfig[SyncConfigT]` and `ProxyAsyncConfig[AsyncConfigT]` in `src/py/vertebra/extensions/sqlspec/` and `AlchemyPlugin` / `ShmSessionBroker` in `src/py/vertebra/extensions/alchemy/` to route `acquire`, `release`, `execute`, `execute_many`, `select_to_arrow`, `begin`, `commit`, and `rollback` over shared memory to the broker provider host.
     - Any existing driver supported in `sqlspec` (`oracledb`, `spanner`, `bigquery`, `duckdb`, `adbc`, `asyncpg`, `psycopg`, `psqlpy`, `aiosqlite`, etc.) or `advanced-alchemy` automatically gains cross-worker connection pooling, shared-memory statement caching, transaction-pinned leases, and zero-copy `ShmRegion` Arrow IPC result transfer without per-worker connection bloat.

### Child Specifications

1. **[`rust-native-shm-drivers`](../rust-native-shm-drivers/spec.md)** — Restore and expand `src/rs/vertebra-db` with native Rust PostgreSQL, MySQL, SQLite, and Cloud SQL (`:3307`) / AlloyDB (`:5433`) mTLS dialers, PyO3 bindings in `vertebra-py`, zero-copy SHM ring + Arrow IPC result transfer, and first-class `Vertebra*` SQLSpec, SQLAlchemy `+vtb`, and Advanced Alchemy adapters.
2. **[`rust-native-enterprise-olap-drivers`](../rust-native-enterprise-olap-drivers/spec.md)** — Implement Rust-native shared-memory drivers in `src/rs/vertebra-db` for **Oracle Database** (pure-Rust Thin TNS/TTC wire protocol, Autonomous Database Wallet extraction, 26ai `VECTOR`/`OSON`, zero-RTT TTC session attributes), **SQL Server / MSSQL** (pure-Rust `tiberius` TDS wire protocol with `@P1` parameter rewriting and Cloud SQL `:3307`), and **DuckDB** (SHM-brokered single-writer analytical engine streaming Arrow `RecordBatch` frames via `ShmRegion` Arrow IPC), with `Vertebra{Oracle,Mssql,Duckdb}*Config` and `{oracle,mssql,duckdb}+vtb` SQLAlchemy dialects.
3. **[`shm-broker-driver-proxy`](../shm-broker-driver-proxy/spec.md)** — Implement `SqlSpecProvider` and SHM proxy execution for `ProxySyncConfig[SyncConfigT]`, `ProxyAsyncConfig[AsyncConfigT]`, and `AlchemyPlugin` so any existing `sqlspec` or `advanced-alchemy` driver executes through the shared-memory broker with threshold-based `ShmRegion` Arrow IPC transfer.
4. **[`shm-database-broker`](../shm-database-broker/spec.md)** — Co-locate `ShmStatementCache` inside `ShmLeaseTable`'s single `ShmRegion` (`stmt_offset`), wire supervisor worker-exit lease reclamation (`reclaim_worker`), and back `ShmBrokerTransport.publish`/`drain_channel` with a lock-free cross-process SHM ring.
5. **[`redis-valkey-resp3`](../redis-valkey-resp3/spec.md)** — Complete Rust-native RESP3 wire framing, `HELLO 3` + `CLIENT TRACKING ON ... BCAST` invalidation push handling, Sentinel master discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), GCP Memorystore IAM token refresh, and multiplexed Pub/Sub in `vertebra-db` and `vertebra.db.{redis,valkey}`.

## Implementation Plan

### Phase 1: Shared-Memory Broker Core & Universal SQLSpec / Alchemy Proxy
- [x] 1.1 Complete [`shm-database-broker`](../shm-database-broker/spec.md) (co-located SHM lease + statement cache region, supervisor `reclaim_worker`, and cross-process broker channel ring)
- [x] 1.2 Complete [`shm-broker-driver-proxy`](../shm-broker-driver-proxy/spec.md) (`SqlSpecProvider` host, `ProxySyncDriver`/`ProxyAsyncDriver` SHM ring RPC, threshold-based `ShmRegion` Arrow IPC result transfer, and `ProxySyncConfig`/`ProxyAsyncConfig`/`AlchemyPlugin` wiring)

### Phase 2: Rust-Native Database, Cache & Cloud Dialer Engine (`vertebra-db`)
- [x] 2.1 Complete [`rust-native-shm-drivers`](../rust-native-shm-drivers/spec.md) (`src/rs/vertebra-db` crate, native Rust Postgres/MySQL/SQLite pools, Cloud SQL `:3307` & AlloyDB `:5433` mTLS dialers, SHM + Arrow IPC cursors, and `Vertebra*` SQLSpec/SQLAlchemy `+vtb` adapters)
- [x] 2.2 Complete [`rust-native-enterprise-olap-drivers`](../rust-native-enterprise-olap-drivers/spec.md) (Rust-native Oracle Thin TNS/TTC, SQL Server `tiberius` TDS, and SHM-brokered DuckDB Arrow IPC drivers in `src/rs/vertebra-db` + `Vertebra{Oracle,Mssql,Duckdb}*Config` and `{oracle,mssql,duckdb}+vtb` dialects)
- [x] 2.3 Complete [`redis-valkey-resp3`](../redis-valkey-resp3/spec.md) (Rust RESP3 parser/encoder in `vertebra-db`, `CLIENT TRACKING` invalidation, Sentinel/Cluster CRC16 routing, Memorystore IAM auth, and SHM cache integration)

## Continuity Snapshot

- **Active Flow**: `shm-native-drivers-and-broker-proxy` (`closed`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `make rs-test && make py-test && make lint-all && make review-check`
- **Decisions**:
  - Reconciled historical architecture across `cofin/vertebra`, `cofin/litecore`, `cofin/bungi`, and `cofin/sqlspec` into a two-tier shared-memory data plane: Tier 1 Rust-native SHM drivers (`vertebra-db` + `vertebra-storage`) for all 6 relational/analytical engines (PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB) plus Redis and Valkey, and Tier 2 universal SHM broker proxy (`ProxySyncConfig[ConfigT]`, `ProxyAsyncConfig[ConfigT]`, `SqlSpecProvider`) for any upstream `sqlspec` or `advanced-alchemy` driver.
- **Recent Discoveries**:
  - `src/rs/vertebra-db` was created in `6737c0c` (PR #27) and deleted in `17ef2a3` (PR #37) before being wired to PyO3 and SHM; `ProxySyncConfig` and `ProxyAsyncConfig` in `src/py/vertebra/extensions/sqlspec/plugin.py` currently bypass `self.broker` and execute in-process.
  - All 6 requested engines have viable Rust driver foundations in 2026: `tokio-postgres` (Postgres), `mysql_async` (MySQL), `rusqlite` bundled (SQLite), `oracledb` / `oracle-rs` pure-Rust Thin TNS/TTC (Oracle, zero Instant Client required), `tiberius` pure-Rust TDS (SQL Server, zero ODBC required), and optional feature-gated `duckdb` / Arrow C Data Interface (DuckDB).
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Execute Phase 1 starting with `shm-database-broker` Task `1.1`, or activate any of the 5 refined child specs (`shm-database-broker`, `shm-broker-driver-proxy`, `rust-native-shm-drivers`, `rust-native-enterprise-olap-drivers`, `redis-valkey-resp3`).
- **Plan Identity**: `plan_revision: 2`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
  - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
  - `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
