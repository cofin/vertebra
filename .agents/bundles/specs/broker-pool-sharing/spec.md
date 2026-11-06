---
type: Spec
flow_id: broker-pool-sharing
title: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config
description: Route any upstream SQLSpec config or Advanced Alchemy/SQLAlchemy config through the shared-memory ring broker so worker processes share one broker-owned connection pool per pool_key with session affinity, transactions, VTBARROW streaming, and crash reclamation.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/gap-analysis_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-04T01:15:00Z
tags: [broker, shm, pool, sqlspec, advanced-alchemy, sqlalchemy]
---

# Flow Charter: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

## Charter

Users keep existing configurations such as `sqlspec.adapters.asyncpg.AsyncpgConfig`, `sqlspec.adapters.oracledb.OracleAsyncConfig`, or `advanced_alchemy.config.SQLAlchemyAsyncConfig`, wrap them with Vertebra, and share one broker-owned connection pool across all worker processes over POSIX shared-memory rings. Where a native Vertebra driver crate (`vertebra-<engine>`) is installed for the engine, the broker dynamically registers that engine's native pool handler and executes I/O directly in Rust without forcing `vertebra-core` to depend on optional database crates.

## Scope

1. **Broker-owned pools & `dbrpc` transport.**
   - Create `vertebra-core/src/dbrpc/` defining versioned `msgspec` `array_like=True` RPC frames (`execute`, `execute_many`, `begin`, `commit`, `rollback`, `savepoint`, `fetch_chunk`, `cancel`, `close`) over `vertebra-core/src/transport/ring_pair.rs` plus a dynamic `BrokerEngineBackend` registration trait in `vertebra-db` so installed `vertebra-<engine>` wheels register native Rust pools at startup.
   - Partition `vertebra-core/src/broker.rs::ShmLeaseTable` per `BaseDatabaseConfig.pool_key()` using deterministic cross-process hashing (`xxh3`/FNV, never seeded Python `hash()`), lease heartbeats instead of blind 30s TTL reaping, and standalone broker daemon startup for external `uvicorn`/`granian` workers alongside `vertebra-http` supervisor spawning.
   - Delete `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, `extensions/alchemy/broker.py::StatementCache`, and unused admission-only `*Pool` structs in `vertebra-db` (`SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport` over `SyncSHMSessionBroker` / `AsyncSHMSessionBroker` and `SyncConnectionLease` / `AsyncConnectionLease` run locally on Linux and macOS via `ShmRegion`/`memfd` without external services).
2. **Data path & session affinity.**
   - Stream result sets back to workers as `VTBARROW` SHM frames (`ArrowCursor`) or compact row frames, supporting chunked cursor iteration and `cancel()` propagation across `SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport`.
   - Pin leased connections (`SyncConnectionLease` / `AsyncConnectionLease`) across transactions, temporary tables, prepared statements, and session GUCs (`SET LOCAL` / RLS context).
3. **Upstream SQLSpec proxy.**
   - `extensions/sqlspec/plugin.py::{ProxySyncConfig,ProxyAsyncConfig}` and `extensions/sqlspec/proxy.py::{ProxySyncDriver,ProxyAsyncDriver}` subclass `SyncDriverAdapterBase` / `AsyncDriverAdapterBase`, preserving the wrapped upstream adapter's SQL compiler, parameter style, and stores while routing connection execution through `SyncConnectionLease` / `AsyncConnectionLease` without `inspect.signature` duck-typing or suppressed rollback errors.
4. **SQLAlchemy & Advanced Alchemy broker pool.**
   - Extract `extensions/sqlalchemy/dbapi.py` (`SyncDBAPIModule`, `AsyncDBAPIModule`, `SyncDBAPIConnection`, `AsyncDBAPIConnection`, `SyncDBAPICursor`, `AsyncDBAPICursor`) from `extensions/sqlalchemy/dialects.py` (`Sync<Engine>Dialect` / `Async<Engine>Dialect`) and add `extensions/sqlalchemy/pool.py` (`BrokerPool` + `creator` helper) so any SQLAlchemy or Advanced Alchemy sync/async config shares broker-owned pools cleanly (consumed downstream by `sqlalchemy-and-django-integration`).
5. **Crash reclamation & observability.**
   - Reclaim crashed worker leases (`reclaim_worker`), issue server-side `ROLLBACK` before returning connections to the pool, enforce idle/abandoned lease timeouts, and expose per-pool metrics via `BrokerPlugin.snapshot_metrics` (`zpages`) and accurate documentation in `docs/usage/extensions.md` and `docs/architecture/pyo3-and-workers.md`.

## Public Seams

`ProxySyncConfig`, `ProxyAsyncConfig`, `BrokerPool`, `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, `SyncConnectionLease`, `AsyncConnectionLease`, `vertebra.extensions.sqlalchemy.dbapi`, `BrokerPlugin`, and `BrokerConfig`.

## Dependencies

`db-layout-and-contract` (`main @ f741701`), `integration-remediation/db-engine-primitives`, and `native-postgres-driver` (first native Rust pool hosted by the broker).

## Acceptance Boundary

- Integration tests verify an unmodified upstream `AsyncpgConfig` and an Advanced Alchemy `SQLAlchemyAsyncConfig` running across 4 worker processes open a single shared pool in `pg_stat_activity`, isolate concurrent transactions per lease, and roll back + release leases when a worker process `SIGKILL`s mid-transaction.
- `make lint-all`, `make rs-test`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 Per-`pool_key` `ShmLeaseTable` partitions, deterministic hashing, lease heartbeats, and dead `*Pool`/`SyncInMemoryBrokerTransport`/`AsyncInMemoryBrokerTransport`/`StatementCache` cleanup.
- t02 `vertebra-core/src/dbrpc/` ring RPC protocol + `BrokerEngineBackend` dynamic pool registration + `VTBARROW` streaming + cancellation.
- t03 Session/transaction affinity (`SyncConnectionLease` / `AsyncConnectionLease`) and standalone + supervisor broker host lifecycle.
- t04 `ProxySyncConfig`/`ProxyAsyncConfig` and `ProxySyncDriver`/`ProxyAsyncDriver` on `SyncDriverAdapterBase`/`AsyncDriverAdapterBase`.
- t05 Extract `extensions/sqlalchemy/dbapi.py` (`SyncDBAPI*` / `AsyncDBAPI*`) + implement `extensions/sqlalchemy/pool.py::BrokerPool` for SQLAlchemy & Advanced Alchemy.
- t06 Worker crash reclamation (`reclaim_worker` + server-side `ROLLBACK`) and multi-process integration tests.
- t07 ZPages pool metrics, `docs/`, and OKF `broker-and-shm-leases.md` update.
