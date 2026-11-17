---
type: Gotcha
title: Shared-Memory Broker & Cross-Process Limitations
description: "Platform, ABI, capacity, security, and cloud-driver boundaries governing Vertebra's shared-memory connection broker and standalone DB-API."
status: stable
tags:
- data
- broker
- shm
- dbapi
- limitations
generated:
  by: flow/0.30.1
  at: "2026-11-15T00:45:00Z"
verified:
- by: agent:flow-executor
  at: "2026-11-17T01:28:00Z"
  commit: 5a833832
- by: agent:flow-archive
  at: "2026-11-15T17:48:00Z"
  commit: 246d7d0e
- by: agent:flow-executor
  at: "2026-11-15T00:45:00Z"
  commit: 1b191388
sources:
- id: broker-engines
  resource: src/py/vertebra/extensions/broker/engines.py::BrokerEngineDiscovery
  title: Native engine capsule module discovery list
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport
  title: Async SHM broker transport, AsyncConnectionLease, and AsyncBrokerStream contracts
- id: broker-capsule
  resource: src/rs/vertebra-db/src/broker_capsule.rs::BrokerEngineCapsule
  title: Versioned C-ABI vtable and ABI version constant for native engine capsules
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::MAX_POOL_PARTITIONS
  title: Compile-time SHM lease partition table limit and dialect tags
- id: core-shm
  resource: src/rs/vertebra-core/src/shm.rs::ShmOwnerHeader
  title: Named POSIX SHM owner header and macOS PSHMNAMLEN length limit
- id: core-cleanup
  resource: src/rs/vertebra-core/src/cleanup.rs::MAX_OWNED_SEGMENTS
  title: Async-signal-safe owned SHM segment tracking limit
- id: core-attach
  resource: src/rs/vertebra-core/src/broker_attach.rs::AttachError
  title: Unix UDS SCM_RIGHTS permission and secret authentication errors
- id: dbapi-sync
  resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
  title: Statement-autocommit enforcement on standalone DB-API connections
---

# Shared-Memory Broker & Cross-Process Limitations

<!-- truth: start -->
- `BrokerEngineDiscovery` loads `broker_engine_capsule()` (`BrokerEngineCapsule`, ABI version `1`) only from `vertebra._postgres`, `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, `vertebra._duckdb`, and `vertebra._sqlite`; `vertebra._bigquery` and `vertebra._spanner` export no native capsule because their Rust clients run over multiplexed HTTP/2 gRPC and REST transports rather than stateful TCP session pools [^broker-engines] [^broker-capsule].
- `AsyncSHMBrokerTransport` exposes strictly asynchronous operations (`stream_dbrpc(...)` returns `AsyncBrokerStream` and blocking SHM calls offload through `SyncPool.default().submit_async(...)`) with all `*_sync` methods removed; `AsyncConnectionLease.release_sync()` calls `_release_sync` / `release_sync` when backed by a sync-capable transport and raises `InterfaceError` when the transport is async-only (never creating or closing an unawaited coroutine) [^broker-transport].
- Standalone `SyncDBAPIConnection` and `AsyncDBAPIConnection` raise PEP 249 `NotSupportedError` when `autocommit=False` is requested on `bigquery`, `spanner`, or `spangres` configurations because those engines commit per statement or RPC boundary [^dbapi-sync].
- `PostgresBrokerBackend` supports per-`(pool_id, lease_slot)` pinned server transactions, whereas MySQL, Oracle, MSSQL, DuckDB, and SQLite `BrokerEngineCapsule` backends serialize or pool on capsule-owned handles without per-slot pinned server transactions across interleaved workers; multi-worker transactional isolation tests on non-PostgreSQL capsules set `pool_size=1` per partition [^broker-capsule].
- `BrokerAttachServer`, `BrokerAttachClient`, `ShmRegion::open_or_create_named`, `LeaseBroker::open_or_create_named`, and `ShmStatementCache::open_or_create_named` are Unix-only (`#[cfg(unix)]`); UDS `SCM_RIGHTS` attachment returns `AttachError::UnsupportedPlatform` on non-Unix hosts [^core-shm] [^core-attach].
- macOS XNU bounds POSIX `shm_open` names (`PSHMNAMLEN`) to `31` bytes including the leading `/`, constraining named broker segments on Darwin hosts [^core-shm].
- `ShmLeaseTable` caps distinct pool partitions at `MAX_POOL_PARTITIONS = 64`, and async-signal-safe panic/signal segment reaping caps simultaneously tracked owned SHM names per process at `MAX_OWNED_SEGMENTS = 256` [^core-broker] [^core-cleanup].
- Authenticated UDS attachment fails closed with `AttachError::InsecurePermissions` (`BrokerAuthenticationError` / `BrokerAttachError`) unless the socket parent directory is non-symlinked UID-matched mode `0700` and the socket itself is a non-symlinked UID-matched mode `0600` Unix socket [^core-attach].
<!-- truth: end -->

## Related Context

- Broker design, supervision, and SQLAlchemy pool architecture: [Shared-Memory Connection Broker & Statement Cache](broker-and-shm-leases.md).
- PostgreSQL-specific runtime limits: [PostgreSQL Native Driver Limitations](postgres-limitations.md).
- Driver wheel and GCS secret caveats: [Database Driver Limitations](database-driver-limitations.md).

[^broker-engines]: `src/py/vertebra/extensions/broker/engines.py::BrokerEngineDiscovery` — Native engine capsule module discovery list.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` — Async SHM broker transport, `AsyncConnectionLease`, and `AsyncBrokerStream` contracts.
[^broker-capsule]: `src/rs/vertebra-db/src/broker_capsule.rs::BrokerEngineCapsule` — Versioned C-ABI vtable (`BROKER_ENGINE_ABI_VERSION = 1`).
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::MAX_POOL_PARTITIONS` — Compile-time SHM lease partition limit (`64`).
[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmOwnerHeader` — Named POSIX SHM owner header and macOS `31`-byte `PSHMNAMLEN` limit.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::MAX_OWNED_SEGMENTS` — Async-signal-safe owned SHM segment tracking limit (`256`).
[^core-attach]: `src/rs/vertebra-core/src/broker_attach.rs::AttachError` — Unix UDS `SCM_RIGHTS` permission and secret authentication errors.
[^dbapi-sync]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection` — Statement-autocommit enforcement on standalone DB-API connections.
