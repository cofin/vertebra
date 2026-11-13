---
type: Spec
flow_id: native-data-platform
title: "Master PRD: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)"
description: "Master PRD for shared cross-worker connection pools via the shared-memory ring broker and feature-complete per-engine Rust PyO3 driver and object-storage wheels across all supported databases and caches."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: false
parent_prd: null
research:
  - research/gap-analysis_20261103.md
  - research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 12
plan_commit: 18baadcd
state_revision: 8
current_task: null
last_verified_checkpoint: 18baadcd
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
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
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Shared-memory ring broker pool admission and worker lease primitives"
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
    title: "VTBARROW shared-memory IPC result allocator"
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: "Polymorphic DatabaseRegistry and per-engine adapter resolution"
  - id: fail-loud
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Completed fail-loud driver, codec, and install-hint baseline"
  - id: pg-native
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline"
  - id: postgres-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: "Python PostgreSQL driver awaiting native cutover in native-postgres-driver ch05"
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "Notification capability protocol split into SyncNotificationSource and AsyncNotificationSource in native-redis-driver"
  - id: datafusion
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: "Embedded in-process Arrow federation engine exempt from server-client driver metric"
  - id: mysql
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: "Standalone vertebra._mysql PyO3 module"
  - id: mssql
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: "Standalone vertebra._mssql PyO3 module"
  - id: oracle
    resource: src/rs/vertebra-oracle/src/lib.rs
    title: "Standalone vertebra._oracle PyO3 module"
---

# Master PRD: Native Data Platform

<!-- truth: start -->
- **Per-Engine Optional PyO3 Wheels**: Every database and cache engine compiles as an isolated optional PyO3 extension (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`, `vertebra-core[<engine>]`) sharing `vertebra-db` (`rlib`) primitives, keeping `vertebra-core` (`vertebra._vertebra`) and `vertebra-http` free of database and gRPC client SDKs [^db-registry].
- **Merged Engine & Storage Wheels on `main @ 18baadcd`**: BigQuery (`21c99f95`), Spanner (`21c99f95`), Object Storage (`067fa901` + `18baadcd` create-only copy and streamed `put_multipart`), DuckDB (`14aa83b8`), MySQL (`e86d8daf`), SQL Server (`134546d4`), and Oracle (`47da6820`) execute natively through standalone wheels; `vertebra-postgres` (`c8722c76` + `18baadcd` bounded `pgoutput` v1 `ReplicationSession`) is on `main` with `9` consolidated chapters (`ch01`–`ch09`, `32` tasks) remaining for verified reconnect, Python handles, CDC, and public driver cutover [^pg-native] [^postgres-py] [^mysql] [^mssql] [^oracle].
- **Shared Broker Pools, SQLite, Redis & Adapter Hygiene Planned**: `broker-pool-sharing` (`C-ABI` `BrokerEngineCapsule`, SHM RPC + `VTBARROW`, `vertebra.db.dbapi`, SQLAlchemy `BrokerPool`), `native-sqlite-driver` (`vertebra-sqlite` bundled `rusqlite`), `native-redis-driver` (`vertebra-redis` RESP3 + hoisted `SyncNotificationSource`/`AsyncNotificationSource` split), `driver-conformance-ci`, `storage-integrity-and-streaming`, and `native-adapter-organization` are fully specified [^core-broker] [^arrow-shm] [^notification-source].
- **Embedded `DataFusionEngine` Exemption**: `DataFusionEngine` (`datafusion` Python package) is an in-process Arrow federation engine holding zero server connections and is exempt from the no-third-party-client driver metric; unimplemented `pgwire_fdw` config fields are removed in `native-adapter-organization` [^datafusion].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Baseline & Problem Statement
Baseline is `main @ 18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)), paired with sibling Master PRD [integration-remediation](../integration-remediation/spec.md). Research dossiers live in [research/gap-analysis_20261103.md](research/gap-analysis_20261103.md) and [research/rust-driver-crates_20261103.md](research/rust-driver-crates_20261103.md).

This initiative delivers two non-negotiable platform commitments:
1. **Cross-Worker Shared Pools via the SHM Ring Broker** [^core-broker]: Any upstream or Vertebra Advanced Alchemy / SQLAlchemy or SQLSpec configuration routes through Vertebra's shared-memory ring broker so worker processes share one physical connection pool with zero-copy `VTBARROW` result transfer [^arrow-shm].
2. **Modular Per-Engine Rust PyO3 Wheels Across Every Engine** [^db-registry]: Standalone `#[pymodule(gil_used = false)]` wheels back PostgreSQL (`vertebra-postgres`), MySQL/MariaDB (`vertebra-mysql`), Oracle 26ai (`vertebra-oracle`), SQL Server (`vertebra-mssql`), SQLite (`vertebra-sqlite`), DuckDB (`vertebra-duckdb`), BigQuery (`vertebra-bigquery`), Spanner (`vertebra-spanner`), Redis/Valkey (`vertebra-redis`), and Object Storage (`vertebra-storage`) [^fail-loud].

At `18baadcd`, seven companion wheels (`_bigquery`, `_spanner`, `_storage`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`) are live behind their Python drivers, and `_postgres` (`vertebra-postgres`) is merged through bounded single-generation `ReplicationSession`s while `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` await cutover in `native-postgres-driver` `ch05` (which also cleans up `cloudsql.py` and `alloydb.py` legacy helpers and TLS/refresh edge cases) [^pg-native] [^postgres-py]. SQLite (`sqlite3`) and Redis/Valkey (Python sockets + `redis.asyncio`) await `native-sqlite-driver` and `native-redis-driver`, broker slot admission awaits real cross-`.so` pool hosting in `broker-pool-sharing`, and BigQuery/Spanner/DataFusion config/helper separation is planned in `native-adapter-organization`.

### 1.2 Non-Goals & Deferred Backlog
- AWS RDS / Aurora DSQL IAM authentication ([#67](https://github.com/cofin/vertebra/issues/67)), Azure Entra ID database/cache authentication ([#68](https://github.com/cofin/vertebra/issues/68)), `SyncRedisQueueStore` / `AsyncRedisQueueStore` (`redis-queue-store`), and CockroachDB/AlloyDB custom wire extensions remain in the backlog until scheduled.

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` Per-Engine Wheel Isolation & `ShmCapsule` Sharing**: Each engine crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, `src/py/vertebra/_<engine>.pyi`, `[project.optional-dependencies].<engine>`) links `vertebra-db` (`rlib`: `connection.rs`, `dialer.rs`, `arrow_shm.rs`, `gcp.rs`, `py_async.rs`), imports `DatabaseError` / `GoogleCloudError` / `TaskPanicError` dynamically from `vertebra._vertebra`, keys completion drainers by `_vertebra_<engine>_completion_drainer`, and crosses `.so` boundaries strictly via C-ABI capsules (`ShmCapsule`, `GcpTokenCacheCapsule`, `BrokerEngineCapsule`).
- **`R2` Leading `Sync*` / `Async*` Driver & Capability Contracts**:
  - `SyncDatabaseDriver` / `AsyncDatabaseDriver` expose `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_to_arrow -> ArrowCursor`, `stream_arrow_batches`, `load_from_arrow`, `transaction()`, `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`), `cancel()`, and `close()` with zero `*_async` methods or aliases.
  - `native-redis-driver` (`ch05`) splits `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across PostgreSQL, Oracle, Redis, and Valkey (`Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource`) ahead of `queues-tasks-and-channels`, while `queues-tasks-and-channels` splits `QueueStore` into `SyncQueueStore` / `AsyncQueueStore` [^notification-source].
- **`R3` Broker Pool Ownership & Neutral `vertebra.db.dbapi`**: `broker-pool-sharing` hosts physical Rust pools by `config.pool_key()`, streams results over `VTBARROW`, extracts framework-neutral PEP 249 `vertebra.db.dbapi` (`SyncDBAPI*` / `AsyncDBAPI*`), and implements dead-worker lease reclamation (`reclaim_worker`).
- **`R4` Clean Adapter Module Organization & `DataFusionEngine` Boundary**: `native-adapter-organization` separates BigQuery Object Table helpers (`bigquery/object_tables.py`) and Spanner mutation structs (`spanner/mutations.py`) from connection configs, removes redundant vendor wrappers from `DataFusionEngine`, and deletes unimplemented `mode="pgwire_fdw"` fields from `DataFusionConfig` while keeping embedded `DataFusionEngine` exempt from the database-client metric [^datafusion].
- **`R5` Live Conformance, Integrity & Companion Wheel Gates**: Every engine passes `test_driver_conformance.py` (`pytest-databases`) with zero skips in CI (`driver-conformance-ci`), and every companion wheel (`_storage`, `_bigquery`, `_spanner`, `_bundler`, `_duckdb`, `_mysql`, `_mssql`, `_oracle`, `_postgres`, `_sqlite`, `_redis`) enforces wheel/sdist payload isolation (`storage-integrity-and-streaming` `ch03:t01` + per-driver flows).

## 3. Documentation & OKF Knowledge Plan

Each child flow updates `docs/usage/{databases,analytics-and-cdc,storage,cloud-dialers}.md`, `docs/reference/native-stubs.md`, `.agents/skills/{pyo3-bridge,review-pyo3-bridge,review-core}/`, and `.agents/bundles/knowledge/domains/{data,storage}/` (`<= 200` lines per chapter, `<= 40` truth-block lines), retiring split-state caveats in `postgres-limitations.md`, `database-driver-limitations.md`, and `object-storage-limitations.md` as cutovers land.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Child Flow(s) | Primary Surface | Verification Gate |
| --- | --- | --- | --- |
| `R1` Per-engine wheels & PostgreSQL cutover | [`native-postgres-driver`](../native-postgres-driver/spec.md), [`native-sqlite-driver`](../native-sqlite-driver/spec.md), [`native-redis-driver`](../native-redis-driver/spec.md) | `src/rs/vertebra-{postgres,sqlite,redis}/**`, `src/py/vertebra/db/adapters/**` | `cargo test`, `mypy.stubtest`, `*-wheel-check`, `pytest` |
| `R2` `SyncNotificationSource` / `AsyncNotificationSource` split | [`native-redis-driver`](../native-redis-driver/spec.md) (`ch05`) | `db/driver/capabilities.py`, `db/adapters/{postgres,oracle,redis,valkey}/notify.py` | Unit & wire notification suites |
| `R3` Shared broker pools & `vertebra.db.dbapi` | [`broker-pool-sharing`](../broker-pool-sharing/spec.md) | `src/rs/vertebra-{core,db,py}/**`, `src/py/vertebra/{db/dbapi,extensions/broker}/**` | Multi-process SHM pool & crash-reclamation tests |
| `R4` Adapter module split & `DataFusionConfig` cleanup | [`native-adapter-organization`](../native-adapter-organization/spec.md) | `src/py/vertebra/db/adapters/{bigquery,spanner,datafusion}/**` | `pytest` + `mypy` + docstring leak gate |
| `R5` Live conformance, storage integrity & wheel checks | [`driver-conformance-ci`](../driver-conformance-ci/spec.md), [`storage-integrity-and-streaming`](../storage-integrity-and-streaming/spec.md) | `src/rs/vertebra-storage/**`, `src/py/tests/integration/**`, `Makefile`, `ci.yml` | `make db-conformance`, `make *-wheel-check`, `make rs-test` |

## 5. Implementation Plan

- [x] [21c99f95] `db-layout-and-contract` — Adapter package layout, standalone GCP wheels, unified driver contract, and conformance harness
- [x] [af1b3d30] `driver-fail-loud` — Removal of `memory://` and `sqlite3` stand-ins, install-hint extras gate, and explicit conformance states
- [x] [067fa901] `storage-completeness` & `storage-remediation` — Multicloud `vertebra-storage`, `GcpTokenCacheCapsule`, zero-copy `PyBackedBytes`, bulk transfers, and readers/writers
- [x] [14aa83b8] `native-duckdb-driver` — Standalone `vertebra-duckdb` wheel, `VTBARROW`, `Appender`, and `DuckDBSecretConfig`
- [x] [e86d8daf] `native-mysql-driver` — Standalone `vertebra-mysql` wheel, binary prepared statements, Cloud SQL IAM, and `VTBARROW`
- [x] [134546d4] `native-mssql-driver` — Standalone `vertebra-mssql` wheel, TDS 7.4/8.0 `sp_executesql`, `BulkLoad`, and `VTBARROW`
- [x] [47da6820] `native-oracle-driver` — Standalone `vertebra-oracle` Thin 26ai wheel, wallets/DRCP, `VECTOR`/`OSON`, and TxEventQ notifications
- [~] [`native-postgres-driver`](../native-postgres-driver/spec.md) — Wheel and `ReplicationSession` merged (`c8722c76`, `18baadcd`); remaining `ch01`–`ch09` (`32` tasks) deliver verified reconnect, Python handles, `PostgresChangeSource`, permanent drivers, atomic `psycopg`/`asyncpg` removal + GCP dialer fixes, PG14–18 conformance, benchmarks, and docs
- [~] [`storage-integrity-and-streaming`](../storage-integrity-and-streaming/spec.md) — `ch01` create-only copy and streamed `put_multipart` merged (`18baadcd`); remaining `ch01`–`ch03` (`9` tasks) deliver checksum receipts/verified reads ([#99](https://github.com/cofin/vertebra/issues/99)), streaming Arrow writes ([#100](https://github.com/cofin/vertebra/issues/100)), and companion wheel isolation checks
- [ ] [`native-adapter-organization`](../native-adapter-organization/spec.md) — BigQuery `object_tables.py`, Spanner `mutations.py`, and `DataFusionEngine` / `DataFusionConfig` (`pgwire_fdw` removal) cleanup (`t01`–`t04`)
- [ ] [`driver-conformance-ci`](../driver-conformance-ci/spec.md) — DuckDB GCS bearer secret rotation, Oracle `wait_timeout_seconds` liveness, live MySQL/MariaDB/MSSQL/Oracle suites, and `bench`-only `duckdb` isolation (`ch01`–`ch04`, `13` tasks)
- [ ] [`broker-pool-sharing`](../broker-pool-sharing/spec.md) — Cross-`.so` `BrokerEngineCapsule`, SHM RPC + `VTBARROW` lease execution, `vertebra.db.dbapi`, SQLAlchemy `BrokerPool`, and crash reclamation ([#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96), [#97](https://github.com/cofin/vertebra/issues/97), [#98](https://github.com/cofin/vertebra/issues/98); `ch01`–`ch08`, `27` tasks)
- [ ] [`native-sqlite-driver`](../native-sqlite-driver/spec.md) — Standalone `vertebra-sqlite` (`rusqlite` bundled WAL multi-reader pool, `JSONB`, `VTBARROW`, delayed-commit pipelines, stdlib `sqlite3` removal; `ch01`–`ch05`, `19` tasks)
- [ ] [`native-redis-driver`](../native-redis-driver/spec.md) — Standalone `vertebra-redis` RESP3 wheel (Cluster, Sentinel, client tracking, GCP Memorystore IAM, Streams, Pub/Sub) plus hoisted `SyncNotificationSource`/`AsyncNotificationSource` split (`ch01`–`ch08`, `28` tasks)

## 6. Current State & Remaining Plan Snapshot

- **Immediate Wave (Independent on `main @ 18baadcd`)**: `native-postgres-driver` (`ch01`–`ch09`), `storage-integrity-and-streaming` (`ch01`–`ch03`), and `native-adapter-organization` (`t01`–`t04`).
- **Second Wave (Post-`native-postgres-driver`)**: `driver-conformance-ci` (`ch01`–`ch04`), `broker-pool-sharing` (`ch01`–`ch08`), and `native-redis-driver` (`ch01`–`ch08`), followed by `native-sqlite-driver` (`ch01`–`ch05`), unlocking downstream [integration-remediation](../integration-remediation/spec.md) flows.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs` — Shared-memory ring broker pool admission and worker leases.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Named shared-memory allocator for `VTBARROW` IPC frames.
[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — Polymorphic database registry and driver loader.
[^fail-loud]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud driver, codec, and install-hint baseline.
[^pg-native]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — Merged standalone `vertebra-postgres` wheel and bounded `ReplicationSession` baseline.
[^postgres-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Python PostgreSQL driver awaiting native cutover in `native-postgres-driver` `ch05`.
[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — Notification capability protocol split into `SyncNotificationSource` and `AsyncNotificationSource` in `native-redis-driver`.
[^datafusion]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — Embedded in-process Arrow federation engine exempt from server-client driver metric.
[^mysql]: `src/rs/vertebra-mysql/src/lib.rs` — Standalone `vertebra._mysql` PyO3 module.
[^mssql]: `src/rs/vertebra-mssql/src/lib.rs` — Standalone `vertebra._mssql` PyO3 module.
[^oracle]: `src/rs/vertebra-oracle/src/lib.rs` — Standalone `vertebra._oracle` PyO3 module.
