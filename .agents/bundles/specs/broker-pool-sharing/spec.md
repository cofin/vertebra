---
type: Spec
flow_id: broker-pool-sharing
title: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config
description: Let any existing upstream SQLSpec config or Advanced Alchemy/SQLAlchemy config route through the shared-memory ring broker so all worker processes share one broker-owned pool, with session affinity, transactions, streaming results, and crash reclamation.
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
tags: [broker, shm, pool, sqlspec, advanced-alchemy, sqlalchemy]
---

# Flow Charter: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

## Charter

A user keeps their existing configuration, for example `sqlspec.adapters.asyncpg.AsyncpgConfig`, `sqlspec.adapters.oracledb.OracleAsyncConfig`, or `advanced_alchemy.config.SQLAlchemyAsyncConfig`. They wrap it with Vertebra, and every worker process shares one broker-owned pool over the shared-memory ring. Where a native Vertebra driver exists for the engine, the broker uses it for I/O.

## Scope

1. **Broker-owned pools.**
   - The broker process (`supervisor/`) owns one pool per `pool_key()`.
   - Workers acquire session- or transaction-scoped leases through `extensions/broker/transport.py::SHMBrokerTransport`, backed by `vertebra-core/src/broker.rs`.
   - `InMemoryBrokerTransport` is limited to tests and single-process use, and is never selected silently in production.
2. **Data path.**
   - Statements and parameters go to the broker over the ring, and results return as VTBARROW SHM frames or msgpack rows.
   - Session affinity covers transactions, temp tables, prepared statements, and `SET`/RLS GUCs.
   - Streaming covers large results, cursor iteration, and cancellation propagation.
3. **Upstream SQLSpec.**
   - `extensions/sqlspec/plugin.py::ProxySyncConfig`/`ProxyAsyncConfig` wrap any upstream `SyncDatabaseConfig`/`AsyncDatabaseConfig`. They keep the upstream adapter's SQL pipeline, parameter style, and stores, and replace only connection acquisition with broker leases.
   - `proxy.py::ProxySyncDriver`/`ProxyAsyncDriver` implement the upstream driver contract.
4. **AA / SQLAlchemy.**
   - A broker-backed SQLAlchemy `Pool` subclass and `creator` built on the Vertebra DB-API (`extensions/sqlalchemy/dbapi.py`), so any AA config gains broker sharing by setting `pool_class`/`creator` through a Vertebra helper.
   - Async SQLAlchemy is supported through the async DB-API.
5. **Reliability.**
   - The broker reclaims a crashed worker's leases (`reclaim_worker`), rolls back abandoned transactions, and enforces lease timeouts.
   - Metrics are exported through zpages (`extensions/broker/plugin.py::BrokerPlugin.snapshot_metrics`).
6. **Docs.** Usage docs show both wrapping patterns, and the verification guide covers server-side connection counts.

The exact current gaps (fake transports, duck-typing, missing async/affinity/streaming) are recorded in `research/` once the gap analysis lands.

## Public Seams

`ProxySyncConfig`, `ProxyAsyncConfig`, the broker SQLAlchemy pool helper, `BrokerPlugin`, and `BrokerConfig`.

## Dependencies

`db-layout-and-contract` and `native-postgres-driver` (the first native engine behind the broker).

## Acceptance Boundary

- Integration tests: an unmodified upstream `AsyncpgConfig` and an AA `SQLAlchemyAsyncConfig` run across 4 workers, `pg_stat_activity` shows one pool's connections, transactions stay isolated per lease, and a worker crash releases its leases.
- `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Broker-owned pools and lease scopes.
- t02 Ring data path with VTBARROW results and streaming.
- t03 Session affinity and transactions.
- t04 Upstream SQLSpec proxy configs/drivers on the upstream contract.
- t05 SQLAlchemy broker pool and async DB-API.
- t06 Crash reclamation and lease timeouts.
- t07 Metrics and docs.
