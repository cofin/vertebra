---
type: Spec
flow_id: native-data-platform
title: 'Master PRD: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)'
description: Master PRD for shared cross-worker connection pools via the shared-memory ring broker and feature-complete per-engine Rust PyO3 driver and object-storage wheels across all supported databases and caches.
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
parent_prd: null
research:
  - research/gap-analysis_20261103.md
  - research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 13
plan_commit: 473e1f2e
state_revision: 11
current_task: "native-data-platform:t15"
last_verified_checkpoint: 58e8db93
created_at: '2026-11-03T04:05:00Z'
updated_at: '2026-11-15T23:10:00Z'
generated:
  by: flow-plan
  at: '2026-11-12T23:45:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:10:00Z'
    commit: 473e1f2e
  - by: agent:flow-archive
    at: '2026-11-15T17:48:00Z'
    commit: 246d7d0e
  - by: agent:flow-completion
    at: '2026-11-14T21:38:00Z'
    commit: c526639f
  - by: agent:plan-generator
    at: '2026-11-12T23:45:00Z'
    commit: 18baadcd
tags:
  - prd
  - drivers
  - rust
  - broker
  - storage
  - postgres
  - oracle
  - mysql
  - redis
  - valkey
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: Shared-memory ring broker pool admission and worker lease primitives
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: VTBARROW shared-memory IPC result allocator
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: Polymorphic DatabaseRegistry and per-engine adapter resolution
  - id: fail-loud
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud driver, codec, and install-hint baseline
  - id: pg-native
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline
  - id: postgres-py
    resource: src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver
    title: Permanent native SyncPostgreSQLDriver over vertebra-postgres
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource
    title: SyncNotificationSource and AsyncNotificationSource capability protocols split in native-redis-driver
  - id: datafusion
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: Embedded in-process Arrow federation engine exempt from server-client driver metric
  - id: mysql
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: Standalone vertebra._mysql PyO3 module
  - id: mssql
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: Standalone vertebra._mssql PyO3 module
  - id: oracle
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: Standalone vertebra._oracle PyO3 module
---

# Master PRD: Native Data Platform

<!-- truth: start -->
- **Per-Engine Optional PyO3 Wheels**: Every database and cache engine compiles as an isolated optional PyO3 extension (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`, `vertebra-core[<engine>]`) sharing `vertebra-db` (`rlib`) primitives, keeping `vertebra-core` (`vertebra._vertebra`) and `vertebra-http` free of database and gRPC client SDKs [^db-registry].
- **Merged Engine & Storage Wheels (`main @ 473e1f2e`)**: BigQuery (`21c99f95`), Spanner (`21c99f95`), Object Storage (`067fa901` + `18baadcd` + archived `storage-integrity-and-streaming` `14b70b07`), DuckDB (`14aa83b8` + archived `driver-conformance-ci` GCS bearer rotation `1b191388`), MySQL (`e86d8daf`), SQL Server (`134546d4`), Oracle (`47da6820` + archived `driver-conformance-ci` notify liveness `1b191388`), and PostgreSQL (`native-postgres-driver` complete and archived at `7116ba13` + remediation `1b191388`) execute natively through standalone wheels [^pg-native] [^postgres-py] [^mysql] [^mssql] [^oracle].
- **Shared Broker Pools Complete; SQLite, Redis & Valkey Planned**: `broker-pool-sharing` (`C-ABI` `BrokerEngineCapsule`, SHM RPC + `VTBARROW`, `vertebra.db.dbapi`, `SQLAlchemy` `BrokerPool`, `0600` UDS `SCM_RIGHTS`, and crash `ROLLBACK` cleanup) is complete and archived at `246d7d0e` alongside `native-adapter-organization` (`c526639f`), `driver-conformance-ci` (`1b191388`), and `storage-integrity-and-streaming` (`14b70b07`); `native-sqlite-driver` (`vertebra-sqlite` bundled `rusqlite`), `native-redis-driver` (`vertebra-redis` RESP3 + hoisted `SyncNotificationSource`/`AsyncNotificationSource` split), and `native-valkey-driver` (`vertebra-valkey` RESP3 with Valkey 8.0+/9.0+ optimizations, fully decoupled `adapters/valkey/` and `queues/valkey.py`, and `vertebra_db::resp3` removal) remain planned [^core-broker] [^arrow-shm] [^notification-source].
- **Embedded `DataFusionEngine` Exemption**: `DataFusionEngine` (`datafusion` Python package, `mode="in_process"` only) is an in-process Arrow federation engine holding zero server connections and is exempt from the no-third-party-client driver metric [^datafusion].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Baseline & Problem Statement
Baseline is `main @ 473e1f2e`, paired with sibling Master PRD [integration-remediation](../integration-remediation/spec.md). Research dossiers live in [research/gap-analysis_20261103.md](research/gap-analysis_20261103.md) and [research/rust-driver-crates_20261103.md](research/rust-driver-crates_20261103.md).

This initiative delivers two non-negotiable platform commitments:
1. **Cross-Worker Shared Pools via the SHM Ring Broker** [^core-broker]: Any upstream or Vertebra Advanced Alchemy / SQLAlchemy or SQLSpec configuration routes through Vertebra's shared-memory ring broker so worker processes share one physical connection pool with zero-copy `VTBARROW` result transfer [^arrow-shm].
2. **Modular Per-Engine Rust PyO3 Wheels Across Every Engine** [^db-registry]: Standalone `#[pymodule(gil_used = false)]` wheels back PostgreSQL (`vertebra-postgres`), MySQL/MariaDB (`vertebra-mysql`), Oracle 26ai (`vertebra-oracle`), SQL Server (`vertebra-mssql`), SQLite (`vertebra-sqlite`), DuckDB (`vertebra-duckdb`), BigQuery (`vertebra-bigquery`), Spanner (`vertebra-spanner`), Redis (`vertebra-redis`), Valkey (`vertebra-valkey`), and Object Storage (`vertebra-storage`) with zero cross-adapter Python inheritance or helper sharing [^fail-loud].

Eight companion wheels (`_bigquery`, `_spanner`, `_storage`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`, `_postgres`) are live behind their permanent Python drivers (`native-postgres-driver` completed at `7116ba13`; `native-adapter-organization` archived at `c526639f`; `broker-pool-sharing` archived at `246d7d0e`) [^pg-native] [^postgres-py]. SQLite (`sqlite3`), Redis (`vertebra-redis`), and Valkey (`vertebra-valkey`) await `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver`.

### 1.2 Non-Goals & Deferred Backlog
- AWS RDS / Aurora DSQL IAM authentication ([#67](https://github.com/cofin/vertebra/issues/67)), Azure Entra ID database/cache authentication ([#68](https://github.com/cofin/vertebra/issues/68)), `SyncRedisQueueStore` / `AsyncRedisQueueStore` (`redis-queue-store`), and CockroachDB/AlloyDB custom wire extensions remain in the backlog until scheduled.

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` Per-Engine Wheel Isolation & `ShmCapsule` Sharing**: Each engine crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, `src/py/vertebra/_<engine>.pyi`, `[project.optional-dependencies].<engine>`) links `vertebra-db` (`rlib`: `connection.rs`, `dialer.rs`, `arrow_shm.rs`, `gcp.rs`, `py_async.rs`), imports `DatabaseError` / `GoogleCloudError` / `TaskPanicError` dynamically from `vertebra._vertebra`, keys completion drainers by `_vertebra_<engine>_completion_drainer`, and crosses `.so` boundaries strictly via C-ABI capsules (`ShmCapsule`, `GcpTokenCacheCapsule`, `BrokerEngineCapsule`). Redis (`vertebra-redis` -> `vertebra._redis`, `vertebra-core[redis]`) and Valkey (`vertebra-valkey` -> `vertebra._valkey`, `vertebra-core[valkey]`) compile as separate standalone wheels and self-contained Python adapters (`adapters/redis/` and `adapters/valkey/`, `queues/redis.py` and `queues/valkey.py`) so Valkey-specific optimizations (`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`, `CLIENT CAPA redirect`, `CLUSTER SHARDS`) evolve independently with zero cross-adapter imports.
- **`R2` Leading `Sync*` / `Async*` Driver & Capability Contracts**:
  - `SyncDatabaseDriver` / `AsyncDatabaseDriver` expose `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`), `cancel()`, and `close()` with zero `*_async` methods or aliases.
  - `native-redis-driver` (`ch03`) splits `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across PostgreSQL, Oracle, and Redis (`Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource`), and `native-valkey-driver` (`ch03`) delivers self-contained `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` ahead of `queues-tasks-and-channels`, while `queues-tasks-and-channels` splits `QueueStore` into `SyncQueueStore` / `AsyncQueueStore` [^notification-source].
- **`R3` Broker Pool Ownership & Neutral `vertebra.db.dbapi`**: Complete (`246d7d0e`) — `broker-pool-sharing` hosts physical Rust pools by `config.pool_key()` via `BrokerEngineCapsule`, streams results over `VTBARROW`, extracts framework-neutral PEP 249 `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`), and implements dead-worker lease reclamation (`reclaim_worker_leases`, `BrokerHost` crash `ROLLBACK`, `SLOT_STATE_RECLAIMING`).
- **`R4` Clean Adapter Module Organization & `DataFusionEngine` Boundary**: Complete (`c526639f`) — BigQuery Object Table helpers (`bigquery/object_tables.py`) and Spanner mutation/transaction modules (`spanner/mutations.py`, `spanner/transactions.py`) are separated from connection configs, Oracle/MySQL/MSSQL/DuckDB drivers follow `_sync.py`/`_async.py`/`pipeline.py` boundaries, redundant vendor wrappers are removed from `DataFusionEngine`, and `DataFusionConfig` accepts `mode="in_process"` only while remaining exempt from the database-client metric [^datafusion].
- **`R5` Live Conformance, Integrity & Companion Wheel Gates**: Every engine passes `test_driver_conformance.py` (`pytest-databases`) with zero skips in CI (`driver-conformance-ci`), and every companion wheel (`_storage`, `_bigquery`, `_spanner`, `_bundler`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`, `_postgres`, `_sqlite`, `_redis`, `_valkey`) enforces wheel/sdist payload isolation (`storage-integrity-and-streaming` `ch03:t01` + per-driver flows).

## 3. Documentation & OKF Knowledge Plan

Each child flow updates `docs/usage/{databases,analytics-and-cdc,storage,cloud-dialers}.md`, `docs/reference/native-stubs.md`, `.agents/skills/{pyo3-bridge,review-pyo3-bridge,review-core}/`, and `.agents/bundles/knowledge/domains/{data,storage}/` (`<= 200` lines per chapter, `<= 40` truth-block lines), retiring split-state caveats in `postgres-limitations.md`, `database-driver-limitations.md`, and `object-storage-limitations.md` as cutovers land.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Child Flow(s) | Primary Surface | Verification Gate |
| --- | --- | --- | --- |
| `R1` Per-engine wheels & PostgreSQL/Redis/Valkey cutover | `native-postgres-driver` (`7116ba13`), `native-sqlite-driver` (`49fb8150`, archived), `native-redis-driver` (`58e8db93`, archived), [`native-valkey-driver`](../native-valkey-driver/spec.md) | `src/rs/vertebra-{postgres,sqlite,redis,valkey}/**`, `src/py/vertebra/db/adapters/**` | `cargo test`, `mypy.stubtest`, `*-wheel-check`, `pytest` |
| `R2` `SyncNotificationSource` / `AsyncNotificationSource` split | `native-redis-driver` (`58e8db93`, archived), [`native-valkey-driver`](../native-valkey-driver/spec.md) (`ch03`) | `db/driver/capabilities.py`, `db/adapters/{postgres,oracle,redis,valkey}/notify.py` | Unit & wire notification suites |
| `R3` Shared broker pools & `vertebra.db.dbapi` | `broker-pool-sharing` (`246d7d0e`, archived) | `src/rs/vertebra-{core,db,py}/**`, `src/py/vertebra/{db/dbapi,extensions/broker}/**` | Multi-process SHM pool & crash-reclamation tests |
| `R4` Adapter module split & `DataFusionConfig` cleanup | `native-adapter-organization` (`c526639f`) | `src/py/vertebra/db/adapters/{bigquery,spanner,oracle,mysql,mssql,duckdb,datafusion}/**` | `pytest` + `mypy` + docstring leak gate |
| `R5` Live conformance, storage integrity & wheel checks | `driver-conformance-ci` (`1b191388`), `storage-integrity-and-streaming` (`14b70b07`) | `src/rs/vertebra-storage/**`, `src/py/tests/integration/**`, `Makefile`, `ci.yml` | `make db-conformance`, `make *-wheel-check`, `make rs-test` |

## 5. Implementation Plan

- [x] [21c99f9] t01 — `db-layout-and-contract` — Adapter package layout, standalone Google Cloud wheels, unified driver contract, and conformance harness
- [x] [af1b3d3] t02 — `driver-fail-loud` — Removal of `memory://` and `sqlite3` stand-ins, install-hint extras gate, and explicit conformance states
- [x] [067fa90] t03 — `storage-completeness` & `storage-remediation` — Multicloud `vertebra-storage`, `GcpTokenCacheCapsule`, zero-copy `PyBackedBytes`, bulk transfers, and readers/writers
- [x] [14aa83b] t04 — `native-duckdb-driver` — Standalone `vertebra-duckdb` wheel, `VTBARROW`, `Appender`, and `DuckDBSecretConfig`
- [x] [e86d8da] t05 — `native-mysql-driver` — Standalone `vertebra-mysql` wheel, binary prepared statements, Cloud SQL IAM, and `VTBARROW`
- [x] [134546d] t06 — `native-mssql-driver` — Standalone `vertebra-mssql` wheel, TDS 7.4/8.0 `sp_executesql`, `BulkLoad`, and `VTBARROW`
- [x] [47da682] t07 — `native-oracle-driver` — Standalone `vertebra-oracle` Thin 26ai wheel, wallets/DRCP, `VECTOR`/`OSON`, and TxEventQ notifications
- [x] [7116ba1] t08 — `native-postgres-driver` — Complete standalone `vertebra-postgres` wheel (`ch01`–`ch09`, `32` tasks): verified reconnect, `AsyncPostgresReplication`, CAS `PostgresChangeSource`, permanent `_sync.py`/`_async.py` drivers, `psycopg`/`asyncpg` removal + Google Cloud dialer fixes, PG14–18 conformance, wheel/benchmark gates, and docs
- [x] [14b70b0] t09 — `storage-integrity-and-streaming` — CRC32C/MD5/SHA-256 content-integrity receipts and verified streaming reads ([#99](https://github.com/cofin/vertebra/issues/99)), streaming Arrow/Parquet multipart writes ([#100](https://github.com/cofin/vertebra/issues/100)), `vertebra.typing.IntegrityError`, and parameterized `{storage,bigquery,spanner,bundler}-wheel-check` isolation gates
- [x] [1b19138] t10 — `driver-conformance-ci` — DuckDB GCS bearer secret rotation (`SyncGcsBearerSecretProvider`/`AsyncGcsBearerSecretProvider`, `(HTTP 401)`/`(HTTP 403)` invalidation, startup `_client` cleanup), Oracle `wait_timeout_seconds` `SELECT 1 FROM DUAL` liveness, `ConformanceGate` + `ServiceVersionMatrix`, live DuckDB/MySQL/MariaDB/MSSQL/Oracle suites, `make db-conformance`, `bench`-only PyPI `duckdb` isolation, and CI conformance lanes
- [x] [c526639] t11 — `native-adapter-organization` — BigQuery `object_tables.py`, Spanner `mutations.py`/`transactions.py`, Oracle/MySQL/MSSQL/DuckDB driver splits, and `DataFusionEngine` / `DataFusionConfig` (`pgwire_fdw` removal) cleanup (`t01`–`t04`)
- [x] [246d7d0] t12 — `broker-pool-sharing` — Cross-`.so` `BrokerEngineCapsule`, SHM RPC + `VTBARROW` lease execution, `vertebra.db.dbapi`, SQLAlchemy `BrokerPool`, and crash reclamation ([#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98); `ch01`–`ch04`, `16/16` tasks)
- [x] [49fb815] t13 — `native-sqlite-driver` — Standalone `vertebra-sqlite` (`rusqlite` bundled WAL multi-reader pool, `JSONB`, `VTBARROW`, delayed-commit pipelines, stdlib `sqlite3` removal; `ch01`–`ch03`, `12/12` tasks)
- [x] [58e8db9] t14 — `native-redis-driver` — Standalone `vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]`) RESP3 wheel for Redis `7.2+`/`8.0` (Cluster, Sentinel, client tracking, Google Cloud Memorystore IAM, Streams, Pub/Sub) plus hoisted `SyncNotificationSource`/`AsyncNotificationSource` split (`ch01`–`ch04`, `16/16` tasks)
- [ ] t15 — [`native-valkey-driver`](../native-valkey-driver/spec.md) — Standalone `vertebra-valkey` (`vertebra._valkey`, `vertebra-core[valkey]`) RESP3 wheel for Valkey `8.0+`/`9.0` (`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST` field TTLs, `CLIENT CAPA redirect`, `CLIENT TRACKING`, `CLUSTER SHARDS`, Sentinel, Google Cloud Memorystore for Valkey IAM, Streams, Pub/Sub), fully decoupled `adapters/valkey/` and `queues/valkey.py` (zero `adapters/redis/` imports), and `vertebra_db::resp3` removal (`ch01`–`ch04`, `16` tasks)

## 6. Current State & Remaining Plan Snapshot

- **Immediate Wave (Ready)**: `native-valkey-driver` (`ch01`–`ch04`), unlocking downstream [integration-remediation](../integration-remediation/spec.md) flows (`native-postgres-driver`, `storage-integrity-and-streaming`, `driver-conformance-ci`, `native-adapter-organization`, `broker-pool-sharing`, `native-sqlite-driver`, and `native-redis-driver` archived).

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory ring broker pool admission and worker leases.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named shared-memory allocator for `VTBARROW` IPC frames.
[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Polymorphic database registry and driver loader.
[^fail-loud]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud driver, codec, and install-hint baseline.
[^pg-native]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — Merged standalone `vertebra-postgres` wheel and bounded `ReplicationSession` baseline.
[^postgres-py]: `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` — Permanent native PostgreSQL driver over `vertebra._postgres`.
[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource` — `SyncNotificationSource` and `AsyncNotificationSource` capability protocols split in `native-redis-driver`.
[^datafusion]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — Embedded in-process Arrow federation engine exempt from server-client driver metric.
[^mysql]: `src/rs/vertebra-mysql/src/lib.rs` — Standalone `vertebra._mysql` PyO3 module.
[^mssql]: `src/rs/vertebra-mssql/src/lib.rs` — Standalone `vertebra._mssql` PyO3 module.
[^oracle]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` PyO3 module.
