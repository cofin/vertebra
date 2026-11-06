---
type: Spec
flow_id: native-data-platform
title: Native Data Platform (Shared Broker Pools, Rust Drivers, Storage)
description: Master PRD for shared connection pools across worker processes through the shared-memory ring broker and feature-complete native Rust database and object-storage drivers packaged as per-driver PyO3 wheels across every supported engine and framework integration.
state: planned
parent_prd: null
research:
  - research/gap-analysis_20261103.md
  - research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 6
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 1
current_task: null
last_verified_checkpoint: "80ff8a65e43a2bcef9e48a7763e89a7352118232"
created_at: 2026-11-03T04:05:00Z
updated_at: "2026-11-06T06:30:00Z"
tags: [prd, drivers, rust, broker, storage, postgres, oracle, mysql, mssql]
---

# Master PRD: Native Data Platform

Baseline: `main @ 80ff8a65` (incorporating completed flows `critical-correctness-fixes` [`364c7cbe`], `gcp-wire-protocol-correctness` [`984ce732`], `gcp-driver-resilience` [`abd33ab6`], the `Sync*` / `Async*` class split [`f7417015`], `db-layout-and-contract` [`21c99f95`], `db-engine-primitives` [`80ff8a65`], and the post-merge review remediation on `fix/post-merge-review-and-plan-sync`). Sibling PRD: [integration-remediation](../integration-remediation/spec.md), which consumes this platform.

## 1. North Star Goal & Problem Framing

### Objective
Two product commitments, both feature complete:

1. **Shared pools through the ring broker.** Any existing Advanced Alchemy / SQLAlchemy config or SQLSpec config, including upstream configs such as `sqlspec.adapters.asyncpg.AsyncpgConfig` and Advanced Alchemy `SQLAlchemyAsyncConfig`, routes through Vertebra's shared-memory ring broker so every worker process shares one connection pool.
2. **Vertebra's own high-performance Rust drivers packaged as modular per-engine wheels.** Rust drivers back every engine: PostgreSQL, MySQL, Oracle, MSSQL, SQLite, DuckDB, BigQuery, Spanner, and Redis/Valkey. Each database engine compiles as its own optional PyO3 extension crate (`src/rs/vertebra-<engine>` -> `vertebra._<engine>`) and install extra (`vertebra-core[<engine>]`), keeping `vertebra-core` (`vertebra._vertebra`), shared `vertebra-db` (`rlib`), and `vertebra-http` free of engine wire, gRPC, or C++ client trees. Object storage (`vertebra-storage` / `vertebra-core[storage]`) and the Arrow/SHM data plane are equally complete, and every integration (SQLSpec adapters, Advanced Alchemy / SQLAlchemy dialects, Django backends, queues, channels, stores) runs on these drivers.

### Current state and limits
- **PostgreSQL.** Rust `vertebra-db/src/postgres.rs` encodes frames (`encode_parse_frame`, `encode_bind_frame`, `encode_execute_frame`, `encode_sync_frame`, `pgcopy`, `pgvector`, JSONB) and holds an admission-only `PostgresPool`, with no socket connection, authentication, TLS, or result decoding. The Python driver executes through `psycopg_pool` (sync), duck-typed `asyncpg` (async, where `_ensure_async_pool` returns the module), and `memory://` rows.
- **MySQL.** `mysql.rs` has packet framing, the `caching_sha2` scramble, `COM_QUERY`/`COM_STMT_PREPARE` encoders, and `MysqlPool` leases. The Python driver uses `pymysql` and `memory://` rows.
- **Oracle.** `oracle.rs` and `tns.rs` have TTC piggyback, OSON, VECTOR, and TNS descriptor codecs. The Python driver executes against `sqlite3.connect(":memory:")`.
- **MSSQL.** `mssql.rs` has TDS packet/SQL-batch encoders and `MssqlPool` leases. The Python driver executes against `sqlite3.connect(":memory:")`.
- **SQLite, DuckDB, and DataFusion.** SQLite uses Python `sqlite3` (`native-sqlite-driver`). DuckDB native cutover (`native-duckdb-driver`) and Redis/Valkey native cutover (`native-redis-driver`) ship as dedicated single-engine flows. `DataFusionEngine` requires DataFusion for execution and propagates missing-dependency and query failures.
- **BigQuery and Spanner.** `vertebra._bigquery` and `vertebra._spanner` provide standalone client bindings with official Google Cloud Rust SDKs, Arrow SHM transfer, panic/cancel-safe completion, and canonical `GoogleCloudError` / `DatabaseError` exceptions. The [database foundation](../../knowledge/domains/data/database-drivers-and-dialers.md) defines the shared contract, and [ADR-004](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) records the wheel and ownership boundary.
- **Redis/Valkey.** RESP3 wire codec in `resp3.rs`; the Python driver still uses Python sockets/parsers and has a `memory://` mode.
- **Broker.** `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs`, `extensions/broker/transport.py::{SyncSHMBrokerTransport, AsyncSHMBrokerTransport}`, and `extensions/sqlspec/plugin.py::{ProxySyncConfig, ProxyAsyncConfig}` perform slot admission without owning server connections or serving `BrokerRpcRequest` frames across workers (addressed in Chapter 3 `broker-pool-sharing`).

### What is new and why it succeeds
- Each engine gets a complete Rust client in its own PyO3 crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, sharing `vertebra-db` `rlib` primitives for `connection.rs`, `dialer.rs`, `arrow_shm.rs`, `gcp`, and `py_async.rs`) behind one unified Python `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract.
- Separate `Sync*` and `Async*` PyO3 and Python classes (`Sync<Engine>Client` / `Async<Engine>Client`, `Sync<Engine>Driver` / `Async<Engine>Driver`, `SyncCompletionQueue` / `AsyncCompletionQueue`, `SyncObjectStore` / `AsyncObjectStore`) expose plain un-suffixed method names in each execution mode without mixed `*_async` methods or runtime awaitable inspection.
- The broker process owns the real connections per `BaseDatabaseConfig.pool_key()`, and workers lease sessions over shared memory with `VTBARROW` zero-copy result transfer.
- Upstream configs are wrapped rather than reimplemented, so users keep existing Advanced Alchemy / SQLSpec configurations and gain cross-worker pool sharing and Rust I/O.

### Success metrics
- No production driver imports a third-party Python database client or opens SQLite for a non-SQLite engine.
- `vertebra-core` (`vertebra._vertebra`), `vertebra-db` (`default = []`), and `vertebra-http` carry zero database client or gRPC SDK dependencies; installing `vertebra-core[<engine>]` installs `vertebra-<engine>` (`vertebra._<engine>`).
- Each engine passes `src/py/tests/integration/db/test_driver_conformance.py` against real servers (`pytest-databases`).
- An unmodified upstream `AsyncpgConfig`, `OracleAsyncConfig`, or Advanced Alchemy `SQLAlchemyAsyncConfig` wrapped by Vertebra runs across N workers with one broker-owned pool, verified by server-side connection counts.
- `driver.pipeline()` sends N statements in one round trip on PostgreSQL; on Oracle the same API executes operations sequentially until `oracledb` supports 26ai pipelining.
- `make bench-check` confirms Rust drivers meet or exceed the Python clients they replace.

### Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Oracle TTC thin client (`oracledb =26.0.0-beta.5`) is synchronous and lacks 26ai pipelining | Run connections on dedicated Rust worker threads bridged through `SyncCompletionQueue` (`SyncOracleClient`) and `AsyncCompletionQueue` + `Notifier` (`AsyncOracleClient`), expose sequential `SyncOraclePipeline` / `AsyncOraclePipeline` behind `SyncPipelineCapable` / `AsyncPipelineCapable` with an upstream-support tracking test, and serialize `arrow-ipc 59` batches into `VTBARROW` |
| PyO3 `cdylib` symbol isolation across per-driver `.so` extensions | Share pure-Rust helpers via `vertebra-db` (`rlib`, `pyo3` feature), dynamically import `DatabaseError` / `GoogleCloudError` / `TaskPanicError` classes registered once on `vertebra._vertebra`, and key each extension's event-loop completion drainer attribute uniquely (`_vertebra_<engine>_completion_drainer`) |
| MSSQL TDS 7.4/8.0 breadth (login7, TLS-in-TDS, token stream, RPC) | Build on `tiberius` (`rustls`) in `vertebra-mssql` (`vertebra._mssql`) with golden wire fixtures and `pytest-databases` conformance |
| Behavior differences versus mature Python clients | Shared cross-engine conformance suite (`test_driver_conformance.py`) plus engine-specific edge suites (`NULL`s, `Decimal` precision, time zones, LOBs, Unicode) |
| Broker single point of failure | Supervisor restart plus worker crash lease reclamation (`reclaim_worker`) with server-side transaction rollback tests |

## 2. Locked Architectural Seams

1. **Layout & per-driver wheels**.
   - Python engine code lives in `vertebra/db/adapters/<engine>/` (`config.py`, `core.py`, `driver.py`, plus engine-local primitives `pipeline.py`, `notify.py`, `queue.py`, `change_stream.py`, `embeddings.py`), loading `vertebra._<engine>` lazily via `vertebra.utils.module_loader.ensure_module("vertebra._<engine>", package="vertebra-core[<engine>]", extra="<engine>")`.
   - Engine-agnostic Python code stays directly under `vertebra/db/` (`driver/`, `registry.py`, `arrow.py`, `cdc.py`, `rls.py`, `dialer/`, and the framework-neutral `dbapi/` extracted in `broker-pool-sharing`).
   - Shared Rust primitives and pure-CPU wire/type codecs live in `vertebra-db` (`rlib`: `<engine>/mod.rs`, `arrow_shm.rs`, `connection.rs`, `dialer.rs`, optional `gcp.rs`, optional `py_async.rs`), while live engine I/O and PyO3 classes (`Sync<Engine>Client`, `Async<Engine>Client`, plus `#[pymodule(gil_used = false)]`) live in dedicated workspace crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`, `src/py/vertebra/_<engine>.pyi`) paired with `[project.optional-dependencies].<engine>` (`valkey` installing `vertebra-redis`, `storage` installing `vertebra-storage`).
2. **Driver contract & `Sync*` / `Async*` split**.
   - Every dual-mode primitive splits into explicit `Sync*` and `Async*` types (`SyncDatabaseDriver` / `AsyncDatabaseDriver`, `SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `Sync<Engine>Transaction` / `Async<Engine>Transaction`, `Sync<Engine>Pipeline` / `Async<Engine>Pipeline`, `SyncPipelineCapable` / `AsyncPipelineCapable`, `SyncNotificationSource` / `AsyncNotificationSource`, `SyncChangeSource` / `AsyncChangeSource`, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer`, `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, `SyncObjectStore` / `AsyncObjectStore`) with plain method names and zero compatibility aliases.
   - Methods on `SyncDatabaseDriver` and `AsyncDatabaseDriver` in `vertebra/db/driver/`:
     - `execute` (`-> int`), `execute_many` (`-> int`), `execute_tuples`, `execute_with_description` (`-> (rows, col_names)`), `select`, `select_one` (raises `NotFoundError`), `select_one_or_none`, `select_value` (raises `NotFoundError`), `select_to_arrow` (`-> ArrowCursor`), `stream_arrow_batches`, `load_from_arrow`
     - `transaction()` context manager, `begin()`, `commit()`, `rollback()`
     - optional `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`)
     - `cancel()` and `close()`
     - typed `DatabaseError` hierarchy registered on `vertebra._vertebra` carrying `sqlstate`, `vendor_code`, and `is_transient`
3. **Broker ownership.**
   - The broker process owns real Rust connections partitioned per `BaseDatabaseConfig.pool_key()` (loading engine pools from installed `vertebra-<engine>` crates on demand).
   - Workers lease connections over SHM rings (`SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport`) for session/transaction scopes, streaming result sets zero-copy via `VTBARROW`.
   - Upstream SQLSpec configs wrap via `ProxySyncConfig`/`ProxyAsyncConfig`; Advanced Alchemy / SQLAlchemy configs use `BrokerPool` + `creator` over `vertebra.db.dbapi` (`SyncDBAPIModule` / `AsyncDBAPIModule`), extracted directly from the existing dialect module in Chapter 3 `broker-pool-sharing`. SQLAlchemy-specific async wrapping lives in `extensions/sqlalchemy/async_adapter.py`; Django later consumes the neutral package without a SQLAlchemy dependency. The shared DB-API consumes generic registry/driver/lease contracts and contains no engine dispatch, upward imports, or SQLite fallback. Existing native execution, DB-API cursor, and Arrow/lease contracts remain required; unresolved metadata and async-connect details are researched before Chapter 3 task refinement rather than replaced with a new result API or policy registry.
4. **Pipelining.** PostgreSQL implements `SyncPipelineCapable.pipeline()` / `AsyncPipelineCapable.pipeline()` natively as an extended-protocol batch with a single `Sync`. Oracle exposes `SyncOraclePipeline` / `AsyncOraclePipeline` executing sequentially until `oracledb` ships 26ai pipelining.
5. **Storage.** `vertebra-storage` (`vertebra-core[storage]`) `SyncObjectStore` and `AsyncObjectStore` (`gs`, `s3`, `r2`, `oci`, `az`, `file`) are the single implementation behind SQLSpec `ObjectStorageProtocol`, Advanced Alchemy `StorageBackend`, Django `Storage`, Litestar static/file responses, and Arrow Parquet import/export, sharing `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`.

## 3. Master Roadmap (Ordered Child Flows)

| Ch | Flow ID | Title | Key Deliverables | Depends on | State |
| --- | --- | --- | --- | --- | --- |
| 1 | `db-layout-and-contract` | Adapter package layout, GCP wheel extraction, driver contract, and conformance suite | Package move (`db/adapters/<engine>`, `vertebra-db/src/<engine>`), `connection.rs` + `py_async.rs`, `DatabaseError` hierarchy on `_vertebra`, standalone `vertebra-bigquery` & `vertebra-spanner` wheels + `[bigquery]`/`[spanner]`/`[storage]` extras, unified `Sync*`/`Async*` driver contract & capability protocols, polymorphic `DatabaseRegistry` + `provide_driver`, import gate, conformance suite | integration-remediation Ch1, Ch2, Ch9 | completed |
| 2 | `native-postgres-driver` | Rust PostgreSQL client (`vertebra-postgres`) | `src/rs/vertebra-postgres` (`vertebra._postgres`, `[postgres]` extra): Startup/SCRAM-SHA-256/TLS/IAM, extended protocol, binary codecs, statement cache, `COPY`, live `LISTEN`/`NOTIFY` (`SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`), single-`Sync` `pipeline()`, `CancelRequest`, `VTBARROW`; `psycopg`/`asyncpg` removed | 1; integration-remediation `db-engine-primitives` | planned |
| 3 | `broker-pool-sharing` | Shared pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config | Broker-owned pools, `dbrpc` SHM frames, session/transaction affinity, `VTBARROW` streaming, `ProxySync/AsyncConfig` for upstream configs, `db/dbapi/` (`SyncDBAPI*`/`AsyncDBAPI*`) with SQLAlchemy-owned `extensions/sqlalchemy/{async_adapter,pool}.py`, crash reclamation | 1, 2; integration-remediation `db-engine-primitives` | draft |
| 4 | `native-oracle-driver` | Rust Oracle thin client (`vertebra-oracle`) | `src/rs/vertebra-oracle` (`vertebra._oracle`, `[oracle]` extra): `oracledb =26.0.0-beta.5` on worker threads over `SyncCompletionQueue`/`AsyncCompletionQueue`, TNS/wallets/TLS, TTC binds/defines, LOBs, OSON/VECTOR, DRCP, sequential `pipeline()`, `arrow-ipc 59` -> `VTBARROW`, TxEventQ `NotificationSource`; `sqlite3` stand-in removed | 1; integration-remediation `db-engine-primitives` | draft |
| 5 | `native-mysql-driver` | Rust MySQL and MariaDB client (`vertebra-mysql`) | `src/rs/vertebra-mysql` (`vertebra._mysql`, `[mysql]` extra, `mysql_async` rustls + `ring`, binary protocol, Cloud SQL IAM, `KILL QUERY` cancel, `VTBARROW`); `pymysql` and `memory://` removed | 1; integration-remediation `db-engine-primitives` | draft |
| 6 | `native-mssql-driver` | Rust SQL Server client (`vertebra-mssql`) | `src/rs/vertebra-mssql` (`vertebra._mssql`, `[mssql]` extra, `tiberius` rustls, TDS 7.4/8.0, `sp_executesql` RPC, `BulkLoad`, `ATTENTION` cancel, `VTBARROW`); `sqlite3` stand-in removed | 1; integration-remediation `db-engine-primitives` | draft |
| 7 | `native-sqlite-driver` | Rust SQLite client (`vertebra-sqlite`) | `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `[sqlite]` extra, bundled `rusqlite`, worker threads, WAL/mmap pragmas, `sqlite3_interrupt` cancel, `VTBARROW`); stdlib `sqlite3` removed | 1; integration-remediation `db-engine-primitives` | draft |
| 8 | `native-duckdb-driver` | Rust DuckDB client (`vertebra-duckdb`) | `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `[duckdb]` extra, bundled `duckdb` + `arrow-ipc 60` -> `VTBARROW`, `Appender`, `DuckDBCore.build_gcs_secret_sql`); `sqlite3` fallback removed | 1; integration-remediation `db-engine-primitives` | draft |
| 9 | `native-redis-driver` | Rust Redis and Valkey client (`vertebra-redis`) | `src/rs/vertebra-redis` (`vertebra._redis`, `[redis]`/`[valkey]` extras, `redis` 1.8 RESP3 client with cluster, Sentinel, pub/sub `NotificationSource`, client tracking); Python sockets and `memory://` removed | 1; integration-remediation `db-engine-primitives` | draft |
| 10 | `storage-completeness` | Feature-complete storage (`vertebra-storage`) | `SyncObjectStore`/`AsyncObjectStore` and `SyncReadableFile`/`AsyncReadableFile` streaming/pagination/signing behind SQLSpec, Advanced Alchemy, Django, and Litestar contracts, unified `_vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`, strict `allocate_shm_region` | 1; integration-remediation `db-engine-primitives` | planned |

### Cross-PRD execution order
1. **Completed on `main @ 80ff8a65`**: `integration-remediation` `critical-correctness-fixes` (`364c7cbe`), `gcp-wire-protocol-correctness` (`984ce732`), `gcp-driver-resilience` (`abd33ab6`), `Sync*` / `Async*` split (`f7417015`), `db-layout-and-contract` (`21c99f95`), and `db-engine-primitives` (`80ff8a65`).
2. **Wave 1 (Planned & Ready)**: `runtime-structure-and-throughput`, `mcp-webmcp-core`, `storage-completeness` (Ch10), and `native-postgres-driver` (Ch2), followed immediately by `broker-pool-sharing` (Ch3).
3. **Per-Engine Driver Stack (1 PR per engine)**: `native-oracle-driver` (Ch4), `native-mysql-driver` (Ch5), `native-mssql-driver` (Ch6), `native-sqlite-driver` (Ch7), `native-duckdb-driver` (Ch8), and `native-redis-driver` (Ch9), followed by `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity`, and `queues-tasks-and-channels`.

## 4. Global Constraints

The global constraints from [integration-remediation](../integration-remediation/spec.md) apply. In addition:

- No third-party Python database client or SQLite stand-in in production driver paths once an engine's chapter completes.
- Every Rust driver crate (`src/rs/vertebra-<engine>`) exposes `#[pymodule(gil_used = false)]`, ships a complete `.pyi` stub verified by `mypy.stubtest vertebra._<engine>`, registers `[project.optional-dependencies].<engine>`, `[tool.uv.sources]`, and `[[tool.bumpversion.files]]`, and passes golden wire/stub tests plus `pytest-databases` integration coverage in `test_driver_conformance.py`.
- `unsafe` blocks carry `// SAFETY:` comments explaining pointer validity, alignment, and lifetime invariants.
- Async Rust never calls `Python::attach` on Tokio or driver worker threads; completion delivery always routes through `AsyncCompletionQueue` + `Notifier` (`vertebra-db/src/py_async.rs`).

## 5. Backlog (Deferred)

- **Adapter organization follow-up**: `/flow:plan native-adapter-organization` reconciles concrete consumers before separating BigQuery Object Table operations from connection settings, Spanner mutation values from connection configuration, and redundant DataFusion vendor registration wrappers. Preserve existing public engine values, native errors, Arrow ownership, optional dependencies, and wire behavior. This is bounded planning scope scheduled independently of the PostgreSQL and broker cutovers.

- AWS RDS/Aurora DSQL IAM ([vertebra#67](https://github.com/cofin/vertebra/issues/67)) and Azure Entra ID ([vertebra#68](https://github.com/cofin/vertebra/issues/68)) auth in the native clients.
- CockroachDB- and AlloyDB-specific protocol extensions beyond PostgreSQL wire compatibility.

## Open Questions

- **Resolved Q5** *(user, 2026-11-04)*: Build each native driver on the official or best-in-class Rust client for its engine ([crate research](research/rust-driver-crates_20261103.md)) and package **every** database engine as its own standalone PyO3 extension crate `src/rs/vertebra-<engine>` (`vertebra._<engine>`, matching `vertebra-bundler` and `vertebra-storage`) paired with `[project.optional-dependencies].<engine>` (`vertebra-core[<engine>]`), keeping `vertebra-core` (`vertebra._vertebra`), shared `vertebra-db` (`rlib`), and `vertebra-http` lightweight and free of database client SDKs:

  | Engine | Crate & Optional Wheel Packaging |
  | --- | --- |
  | BigQuery | `google-cloud-bigquery =0.19.0` (`rustls` + `ring`) + Vertebra BigQuery REST in `src/rs/vertebra-bigquery` (`vertebra._bigquery`) + `[bigquery]` extra (extracted in Ch1 `db-layout-and-contract`) |
  | Spanner | `google-cloud-spanner =0.34.8-preview` (`rustls` + `ring`) in `src/rs/vertebra-spanner` (`vertebra._spanner`) + `[spanner]` extra (extracted in Ch1 `db-layout-and-contract`) |
  | PostgreSQL | `postgres-protocol` 0.6.12 + Vertebra connection in `src/rs/vertebra-postgres` (`vertebra._postgres`) + `[postgres]` extra (`tokio-postgres` as dev test oracle) |
  | Oracle | `oracledb =26.0.0-beta.5` (`arrow`) in `src/rs/vertebra-oracle` (`vertebra._oracle`) + `[oracle]` extra (worker threads over `SyncCompletionQueue`/`AsyncCompletionQueue`; sequential `pipeline()` until upstream 26ai pipelining) |
  | MySQL | `mysql_async` 0.37.1 (`default-rustls-ring`) in `src/rs/vertebra-mysql` (`vertebra._mysql`) + `[mysql]` extra |
  | MSSQL | `tiberius` 0.13.0 (`rustls`) in `src/rs/vertebra-mssql` (`vertebra._mssql`) + `[mssql]` extra |
  | SQLite | `rusqlite` 0.41.1 (`bundled`) in `src/rs/vertebra-sqlite` (`vertebra._sqlite`) + `[sqlite]` extra |
  | DuckDB | `duckdb` 1.20000.1 (`bundled`, `appender-arrow`) in `src/rs/vertebra-duckdb` (`vertebra._duckdb`) + `[duckdb]` extra |
  | Redis/Valkey | `redis` 1.8.1 (`tokio-rustls-comp`, `cluster-async`, `cache-aio`) in `src/rs/vertebra-redis` (`vertebra._redis`) + `[redis]` and `[valkey]` extras |
  | Object Storage | `object_store` in `src/rs/vertebra-storage` (`vertebra._storage`) + `[storage]` extra |

- **Resolved Q6** *(user, 2026-11-03)*: Oracle ships on the official `oracledb` crate with each connection on a dedicated Rust worker thread (`SyncCompletionQueue` for `SyncOracleClient`, `AsyncCompletionQueue` + `Notifier` for `AsyncOracleClient`); `pipeline()` executes sequentially until `oracledb` adds 26ai pipelining.
- **Resolved Q7** *(user, 2026-11-04)*: All dual-mode Rust and Python primitives use leading `Sync*` and `Async*` prefixes (`SyncCompletionQueue` / `AsyncCompletionQueue`, `Sync<Engine>Client` / `Async<Engine>Client`, `SyncObjectStore` / `AsyncObjectStore`, `Sync<Engine>Dialect` / `Async<Engine>Dialect`) and split into separate sync and async classes with plain method names and zero backward-compatibility aliases (preserving upstream `sqlspec` `<Engine>SyncConfig` / `<Engine>AsyncConfig` infix naming in `vertebra.extensions.sqlspec`).

## Implementation Plan

- [x] [21c99f95] db-layout-and-contract — Adapter layout, standalone GCP wheels, unified contract, and conformance suite (archived in `log.md`).
- [ ] native-postgres-driver — Rust PostgreSQL client (../native-postgres-driver/spec.md)
- [ ] broker-pool-sharing — Shared pools for any SQLSpec, Advanced Alchemy, or SQLAlchemy config (../broker-pool-sharing/spec.md)
- [ ] native-oracle-driver — Rust Oracle thin client (../native-oracle-driver/spec.md)
- [ ] native-mysql-driver — Rust MySQL and MariaDB client (../native-mysql-driver/spec.md)
- [ ] native-mssql-driver — Rust SQL Server client (../native-mssql-driver/spec.md)
- [ ] native-sqlite-driver — Rust SQLite client (../native-sqlite-driver/spec.md)
- [ ] native-duckdb-driver — Rust DuckDB client (../native-duckdb-driver/spec.md)
- [ ] native-redis-driver — Rust Redis and Valkey client (../native-redis-driver/spec.md)
- [ ] storage-completeness — Feature-complete storage (../storage-completeness/spec.md)

## Continuity Snapshot

- **Flow ID:** `native-data-platform` (`state: planned`, `plan_revision: 6`, `plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"`)
- **Active Focus:** `db-layout-and-contract` (`21c99f95`) and `db-engine-primitives` (`80ff8a65`) are merged on `main`. `storage-completeness` and `native-postgres-driver` are planned and ready for stacked implementation, with each remaining engine delivered as its own standalone PR.
- **Key decisions** *(user, 2026-11-03 – 2026-11-04)*:
  - Shared broker pools for Advanced Alchemy / SQLSpec configs and native Rust drivers across all engines remain required acceptance targets for their owning child flows.
  - Layout: `vertebra/db/adapters/<engine>/` for Python engine code, `vertebra/db/*` for engine-agnostic code, shared `vertebra-db` (`rlib`) for Rust primitives/codecs, and standalone PyO3 extension crates `src/rs/vertebra-<engine>` (`vertebra._<engine>`) + `[project.optional-dependencies].<engine>` per database engine.
  - Leading `Sync*` / `Async*` class split across Rust and Python with zero compatibility aliases.
- **Research:** [gap-analysis_20261103.md](research/gap-analysis_20261103.md) and [rust-driver-crates_20261103.md](research/rust-driver-crates_20261103.md).
