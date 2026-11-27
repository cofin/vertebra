---
type: Concept
title: "Shared-Memory Connection Broker & Statement Cache"
description: "Cross-process shared-memory connection lease tables, seqlock statement caches, PyO3 broker bindings, and loop-sharded sync thread pools."
status: stable
tags:
  - domains
  - data
  - broker
  - shm
  - ipc
  - database
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-27T21:12:00Z"
    commit: 4f64c6e5
  - by: agent:flow-executor
    at: "2026-11-21T01:18:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-archive
    at: "2026-11-15T17:48:00Z"
    commit: 246d7d0e
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-broker-rs
    resource: src/rs/vertebra-core/src/broker.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/broker.rs)"
  - id: src-vertebra-py-src-lib-rs
    resource: src/rs/vertebra-py/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/lib.rs)"
  - id: src-extensions-broker-transport-py
    resource: src/py/vertebra/extensions/broker/transport.py
    title: "Python implementation module (src/py/vertebra/extensions/broker/transport.py)"
  - id: src-extensions-broker-host-py
    resource: src/py/vertebra/extensions/broker/host.py
    title: "Python implementation module (src/py/vertebra/extensions/broker/host.py)"
  - id: src-extensions-broker-engines-py
    resource: src/py/vertebra/extensions/broker/engines.py
    title: "Python implementation module (src/py/vertebra/extensions/broker/engines.py)"
  - id: src-extensions-broker-plugin-py
    resource: src/py/vertebra/extensions/broker/plugin.py
    title: "Python implementation module (src/py/vertebra/extensions/broker/plugin.py)"
  - id: src-extensions-sqlalchemy-pool-py
    resource: src/py/vertebra/extensions/sqlalchemy/pool.py
    title: "Python implementation module (src/py/vertebra/extensions/sqlalchemy/pool.py)"
  - id: src-extensions-sqlalchemy-async-adapter-py
    resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py
    title: "Python implementation module (src/py/vertebra/extensions/sqlalchemy/async_adapter.py)"
  - id: src-db-dbapi-sync-py
    resource: src/py/vertebra/db/dbapi/_sync.py
    title: "Python implementation module (src/py/vertebra/db/dbapi/_sync.py)"
  - id: src-db-registry-py
    resource: src/py/vertebra/db/registry.py
    title: "Python implementation module (src/py/vertebra/db/registry.py)"
  - id: src-db-driver-common-py
    resource: src/py/vertebra/db/driver/_common.py
    title: "Python implementation module (src/py/vertebra/db/driver/_common.py)"
  - id: src-utils-sync-tools-py
    resource: src/py/vertebra/utils/sync_tools.py
    title: "Python implementation module (src/py/vertebra/utils/sync_tools.py)"
  - id: src-tests-unit-db-test-db-capabilities-and-purity-gates-py
    resource: src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py
    title: "Driver capability protocols, pool_key isolation, provide_driver, and AST purity gates"
  - id: src-tests-unit-db-test-db-drivers-channels-and-dialers-py
    resource: src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py
    title: "Native database drivers, Cloud SQL and AlloyDB dialers, ChannelBus, and ArrowCursor tests"
---

# Shared-Memory Connection Broker & Statement Cache

<!-- truth: start -->
- `ShmLeaseTable` co-locates lock-free CAS connection lease slots (`LeaseState::{Free,Leased,Quarantined}`), a seqlock `ShmStatementCache` (256 B `StatementSlot`), and a `ShmChannelSlot` pub/sub ring (`64 x 384 B`) in one shared-memory region.
- Expired or crashed worker leases are reclaimed via monotonic `expiry_ns` deadlines and PID-tagged `reclaim_worker_leases` sweeps into `LeaseState::Quarantined` until explicit server rollback and `dispose_quarantined`.
- `BrokerConfig` exposes millisecond fields (`acquire_timeout_ms = 5_000`, `lease_ttl_ms = 30_000`); lease `release()` marks `_released = True` only after native release succeeds, and `AsyncConnectionLease.release_sync()` delegates synchronously or raises `InterfaceError`.
- `AsyncSHMBrokerTransport` exposes `AsyncBrokerStream` from `stream_dbrpc(...)`, has no `*_sync` methods, coordinates contended `acquire()` via per-event-loop FIFO waiter deques with direct local handoff and a single `LeaseBroker.wait_for_wake_async()` watcher (using `SyncPool.default()` only for DBRPC/reclaim or non-native test doubles), and pairs with `BrokerHost` disposal-error tracking and `BrokerEngineDiscovery`.
- Network database pools take one broker lease at connection checkout and return it at release (never per query or statement); embedded engines (SQLite, DuckDB) take no lease. A Rust guard owns the lease and returns it on drop.
<!-- truth: end -->

Vertebra coordinates cross-process database connection leases, statement caching, and pub/sub notifications via [`src/rs/vertebra-core/src/broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs)[^src-vertebra-core-src-broker-rs] and [`src/py/vertebra/extensions/broker/transport.py`](../../../../../src/py/vertebra/extensions/broker/transport.py)[^src-extensions-broker-transport-py].

## 1. Rust SHM Lease Table (`broker.rs`)

[`ShmLeaseTable`](../../../../../src/rs/vertebra-core/src/broker.rs) packs all broker structures into one fixed-size POSIX shared-memory segment (`VERTEBRA_BROKER_SHM_FD` / `VERTEBRA_BROKER_SHM_SIZE`):

- **Co-Located Layout**: `ShmLeaseHeader`, `LeaseSlot` array (64 B each), up to `MAX_POOL_PARTITIONS = 64` `ShmPartitionEntry` slots (64 B each, resolved via `lookup_partition`), `ShmChannelSlot` ring (64 x 384 B), and `ShmStatementCache` (256 B slots). All slot structs are cache-line aligned to prevent false sharing. `LeaseBroker.statement_cache()` returns a handle bound to the same segment.
- **Slot State Machine, Single-Waiter Wake & Async Wait**: Each `LeaseSlot` tracks atomic `state_and_pid` (`Free`, `Leased`, `Quarantined`, plus transient claiming/reclaiming states), `generation`, `acquired_ns`, `expiry_ns`, and `bind_key_hash`. Single-slot `release()` and `dispose_quarantined()` call `wake_one_waiter_seqcst()` (`wake_one()` when `partition_count <= 1`, falling back to `wake_all()` when `partition_count > 1`, paired with `Ordering::Release` on `register_partition`), while multi-slot `reap_expired()` and `reclaim_worker()` call `wake_all()`. `ShmLeaseTable::{wake_seq, partition_count, record_timeout, wait_for_wake}` preserve Dekker `SeqCst` waiter ordering and bridge to Python via `LeaseBroker::{wake_seq, partition_count, record_timeout, wait_for_wake_async}` over Tokio `spawn_blocking` and `TaskAwaitable`.
- **Crash Reclamation & Quarantine**: On worker exit or `expiry_ns` expiration, `reclaim_worker_leases(pid)` moves slots into `LeaseState::Quarantined` and returns `ReclaimedLease` records. `BrokerHost.on_worker_exit(pid)` runs server `ROLLBACK` cleanup and calls `dispose_quarantined` to restore capacity (unpartitioned `reclaim_worker` / `reap_expired` reset directly to `Free`).
- **Cross-Process Channel Ring**: `publish_channel` and `drain_channel` pass messages across workers via atomic `CHAN_SLOT_FREE` -> `WRITING` -> `READY` transitions.

## 2. Shared Statement Cache (`ShmStatementCache`)

[`ShmStatementCache`](../../../../../src/rs/vertebra-core/src/broker.rs) shares compiled SQL plans across workers:

- **Deterministic Key Hashing**: Hashes SQL text and partition keys with unseeded XXH3-64 (`stable_hash64`) plus a `u16` dialect tag (`DIALECT_TAG_*`).
- **Seqlock Storage & Eviction**: Four-way set-associative 256 B `StatementSlot` entries store up to 224 B of compiled SQL, parameter counts, and MsgPack parameter plans under an optimistic seqlock, using clock eviction (`CLOCK_REF_BIT`) over a power-of-two capacity.

## 3. Python Broker Layer (`vertebra.extensions.broker`)

Key modules: [`transport.py`](../../../../../src/py/vertebra/extensions/broker/transport.py), [`host.py`](../../../../../src/py/vertebra/extensions/broker/host.py)[^src-extensions-broker-host-py], [`engines.py`](../../../../../src/py/vertebra/extensions/broker/engines.py)[^src-extensions-broker-engines-py], and [`plugin.py`](../../../../../src/py/vertebra/extensions/broker/plugin.py)[^src-extensions-broker-plugin-py].

- **Config & Lease Teardown**: `BrokerConfig` defines `lease_ttl_ms` (30,000) and `acquire_timeout_ms` (5,000). `SyncConnectionLease.release()` and `AsyncConnectionLease.release()` set `_released = True` only after native release succeeds. `AsyncConnectionLease.release_sync()` calls the transport's sync teardown hook for non-greenlet paths or raises `InterfaceError`.
- **SHM Transports & DBRPC Streams**: `SyncSHMBrokerTransport` and `AsyncSHMBrokerTransport` attach to `LeaseBroker` via named SHM (`ShmOwnerHeader`) or `0600` UDS `SCM_RIGHTS` (`BrokerAttachClient` / `BrokerAttachServer`). `AsyncSHMBrokerTransport` caches `_DEFAULT_BIND_KEY_HASH`, skips partition locks when `partition_count() == 0`, queues contended `acquire()` callers in per-event-loop FIFO `_PartitionWaitQueue` deques (`WeakKeyDictionary` with no strong loop reference in values) woken directly on local `_release_impl` / `_dispose_quarantined_impl` (`_on_slot_freed` skipping closed loops) or by a single background `_watch_broker_wakes` task (spawned outside `_waiter_lock` for `eager_task_factory` safety with `watcher_seq`-bounded `local_wakes` accounting), records async timeouts via `record_timeout()`, falls back to `SyncPool.default()` only for DBRPC/reclaim or non-native doubles, and returns `AsyncBrokerStream` from `stream_dbrpc(...)` (`DataError` on malformed frames).
- **Host Supervision & Engine Discovery**: `BrokerHost` (`vtb broker serve`) manages server pools and `BrokerEngineCapsule` (ABI v1) vtables discovered via `BrokerEngineDiscovery.register_installed()` across `_postgres`, `_mysql`, `_oracle`, `_mssql`, `_duckdb`, and `_sqlite`. Disposal failures during `_recover_single_lease` increment `ShutdownReport.disposal_errors`, record `last_disposal_error`, and fire `on_disposal_error` callbacks.
- **DB-API, SQLAlchemy & Diagnostics**: Standalone PEP 249 wrappers ([`dbapi/_sync.py`](../../../../../src/py/vertebra/db/dbapi/_sync.py)[^src-db-dbapi-sync-py]) run without SQLAlchemy. `SQLAlchemyAsyncDBAPI` ([`async_adapter.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/async_adapter.py)[^src-extensions-sqlalchemy-async-adapter-py]) pairs with `SyncBrokerPool` / `AsyncBrokerPool` ([`pool.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/pool.py)[^src-extensions-sqlalchemy-pool-py]), quarantining leases on pool-return errors (`LeaseQuarantinedError`). `BrokerPlugin` wires DI, `/brokerz` ZPages, and OpenTelemetry `DbPoolMetricSample` counters (see [SHM Broker Limitations](shm-broker-limitations.md)).

## 4. Driver Lease Contract, Registry & Sync Pools

- **Checkout-Scoped Driver Leases**: [`CommonDriverMixin`](../../../../../src/py/vertebra/db/driver/_common.py)[^src-db-driver-common-py] acquires one broker lease at connection checkout (`_attach_checkout_lease`) for network adapters (`postgres`, `mysql`, `mssql`, `oracle`) and returns it on release via a Rust drop guard. Single-client drivers hold their lease while open; embedded SQLite and DuckDB take no lease.
- **Shared Pool Registry**: [`DatabaseRegistry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py] deduplicates pools by `BaseDatabaseConfig.pool_key()` (`None` isolates `:memory:` SQLite/DuckDB). `DatabaseRegistry.default()`, `default_broker()`, and `provide_driver()` let Django, Litestar/SQLSpec, SQLAlchemy `+vtb`, and native apps share the same underlying pools, cloud dialers, and SHM `LeaseBroker` unless `share_pool=False`.
- **Thread Pools & Loop-Sharded Resources**: [`SyncPool`](../../../../../src/py/vertebra/utils/sync_tools.py)[^src-utils-sync-tools-py] runs a `ThreadPoolExecutor` (`cpu_count * 2` default), propagates `contextvars`, recreates after `fork`, and coalesces `submit_async` completions per event loop via `_AsyncCompletionQueue`. `SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]` shard non-thread-safe primitives per event-loop thread.

Provenance sources: [^src-vertebra-py-src-lib-rs], [^src-tests-unit-db-test-db-capabilities-and-purity-gates-py], [^src-tests-unit-db-test-db-drivers-channels-and-dialers-py].

[^src-vertebra-core-src-broker-rs]: `src/rs/vertebra-core/src/broker.rs` — Rust implementation module (src/rs/vertebra-core/src/broker.rs).
[^src-vertebra-py-src-lib-rs]: `src/rs/vertebra-py/src/lib.rs` — Rust implementation module (src/rs/vertebra-py/src/lib.rs).
[^src-extensions-broker-transport-py]: `src/py/vertebra/extensions/broker/transport.py` — Python implementation module (src/py/vertebra/extensions/broker/transport.py).
[^src-extensions-broker-host-py]: `src/py/vertebra/extensions/broker/host.py` — Python implementation module (src/py/vertebra/extensions/broker/host.py).
[^src-extensions-broker-engines-py]: `src/py/vertebra/extensions/broker/engines.py` — Python implementation module (src/py/vertebra/extensions/broker/engines.py).
[^src-extensions-broker-plugin-py]: `src/py/vertebra/extensions/broker/plugin.py` — Python implementation module (src/py/vertebra/extensions/broker/plugin.py).
[^src-extensions-sqlalchemy-pool-py]: `src/py/vertebra/extensions/sqlalchemy/pool.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/pool.py).
[^src-extensions-sqlalchemy-async-adapter-py]: `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/async_adapter.py).
[^src-db-dbapi-sync-py]: `src/py/vertebra/db/dbapi/_sync.py` — Python implementation module (src/py/vertebra/db/dbapi/_sync.py).
[^src-db-registry-py]: `src/py/vertebra/db/registry.py` — Python implementation module (src/py/vertebra/db/registry.py).
[^src-db-driver-common-py]: `src/py/vertebra/db/driver/_common.py` — Python implementation module (src/py/vertebra/db/driver/_common.py).
[^src-utils-sync-tools-py]: `src/py/vertebra/utils/sync_tools.py` — Python implementation module (src/py/vertebra/utils/sync_tools.py).
[^src-tests-unit-db-test-db-capabilities-and-purity-gates-py]: `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py` — Driver capability protocols, `pool_key` isolation, `provide_driver`, and AST purity gates.
[^src-tests-unit-db-test-db-drivers-channels-and-dialers-py]: `src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py` — Native database drivers, Cloud SQL and AlloyDB dialers, `ChannelBus`, and `ArrowCursor` tests.
