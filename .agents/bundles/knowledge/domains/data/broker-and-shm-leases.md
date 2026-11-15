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
  - id: src-tests-unit-db-test-native-db-drivers-py
    resource: src/py/tests/unit/db/test_native_db_drivers.py
    title: "Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py)"
---

# Shared-Memory Connection Broker & Statement Cache

<!-- truth: start -->
- `ShmLeaseTable` co-locates lock-free CAS connection lease slots (`LeaseState::{Free,Leased,Quarantined}`), a seqlock `ShmStatementCache` (256 B `StatementSlot`), and a cross-worker `ShmChannelSlot` pub/sub ring (`DEFAULT_CHANNEL_CAPACITY = 64` × 384 B) inside a single shared-memory region.
- Expired or crashed worker leases are reclaimed via monotonic `expiry_ns` deadlines and PID-tagged `reclaim_worker_leases` sweeps into `SLOT_STATE_QUARANTINED` (`LeaseState::Quarantined`) until explicit server rollback and `dispose_quarantined`.
<!-- truth: end -->

Vertebra provides cross-process shared-memory database connection management and statement caching via [`src/rs/vertebra-core/src/broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs)[^src-vertebra-core-src-broker-rs] and [`src/py/vertebra/extensions/broker/transport.py`](../../../../../src/py/vertebra/extensions/broker/transport.py)[^src-extensions-broker-transport-py].

## 1. Rust SHM Lease Table (`broker.rs`)

[`ShmLeaseTable`](../../../../../src/rs/vertebra-core/src/broker.rs) manages pooled connection slots, a co-located statement cache, and a cross-process pub/sub ring in a single fixed-size POSIX shared memory region:

- **Single-Region Co-location**: `ShmLeaseTable::init_in_region_with_statements(region, pool_size, max_overflow, stmt_capacity)` places the cache-line-aligned `ShmLeaseHeader` (including `stmt_offset`, `stmt_capacity`, `channel_offset`, and `channel_capacity`) with `write_seq` tracked on `ShmChannelHeader`, the `LeaseSlot` array (`64` B each), up to `MAX_POOL_PARTITIONS = 64` `ShmPartitionEntry` slots (`64` B each, resolved via `lookup_partition`), the `ShmChannelSlot` ring (`DEFAULT_CHANNEL_CAPACITY = 64` × `384` B), and the `ShmStatementCache` inside one `ShmRegion` (`VERTEBRA_BROKER_SHM_FD` / `VERTEBRA_BROKER_SHM_SIZE`). `LeaseBroker.statement_cache()` returns a `StatementCache` handle bound to the co-located statement cache in the same region.
- **Slot State Machine**: Each slot stores an atomic `state_and_pid` (`SLOT_STATE_FREE`, `SLOT_STATE_PRIMARY`, `SLOT_STATE_OVERFLOW`, `SLOT_STATE_QUARANTINED` mapped publicly to `LeaseState::{Free, Leased, Quarantined}`), `generation`, `acquired_ns`, `expiry_ns`, and `bind_key_hash`.
- **Atomic Acquisition**: Workers acquire slots using compare-and-swap (`CAS`) without global locks, blocking on `wake_seq` (`WaitWord`) when exhausted.
- **Heartbeats & Crash Reclamation**: If a worker process exits or fails to release its slot before `expiry_ns` elapses, `ShmLeaseTable::reclaim_worker_leases(pid)` (returning `ReclaimedLease` with `slot_index`, `generation`, and `pool_id`) and `BrokerHost.on_worker_exit(pid)` transition active slots owned by `pid` into `SLOT_STATE_QUARANTINED` (`LeaseState::Quarantined`) for server `ROLLBACK` cleanup before `dispose_quarantined` returns the slot to `SLOT_STATE_FREE`, whereas unpartitioned `reclaim_worker(pid)` and `reap_expired` reset stale slots directly to `SLOT_STATE_FREE` and `RingFactory::shutdown_workers` terminates worker processes.
- **Cross-Process Channel Ring (`ShmChannelSlot`)**: `publish_channel(channel, payload, timestamp_ns, lease_id)` and `drain_channel(channel)` provide lock-free cross-process pub/sub messaging via atomic `slot.state` CAS transitions (`CHAN_SLOT_FREE` / `CHAN_SLOT_WRITING` / `CHAN_SLOT_READY`).
- **Slot Alignment**: Lease slots (`64` B), `ShmPartitionEntry` (`64` B), `StatementSlot` (`256` B), and `ShmChannelSlot` (`384` B) are cache-line aligned to prevent false sharing across concurrent worker cores.

## 2. Shared Statement Cache (`ShmStatementCache`)

[`ShmStatementCache`](../../../../../src/rs/vertebra-core/src/broker.rs) provides cross-worker statement compilation caching:

- **Key Hashing**: SQL statements and partition keys are hashed across processes with deterministic unseeded XXH3-64 (`stable_hash64` via `twox_hash::XxHash3_64::oneshot`) and a `u16` dialect tag (`DIALECT_TAG_*`).
- **Prepared Statement Metadata**: Stores compiled SQL bytes (`STMT_SLOT_PAYLOAD_BYTES = 224` payload within `256` B `StatementSlot`), parameter counts, and MsgPack-encoded parameter plans inside optimistic seqlock-guarded four-way set-associative slots.
- **Eviction**: Clock eviction policy bounded by power-of-two `capacity` (`CLOCK_REF_BIT`).

## 3. Python Broker Layer (`vertebra.extensions.broker`)

[`vertebra.extensions.broker.transport`](../../../../../src/py/vertebra/extensions/broker/transport.py), [`vertebra.extensions.broker.host`](../../../../../src/py/vertebra/extensions/broker/host.py)[^src-extensions-broker-host-py], [`vertebra.extensions.broker.engines`](../../../../../src/py/vertebra/extensions/broker/engines.py)[^src-extensions-broker-engines-py], and [`vertebra.extensions.broker.plugin`](../../../../../src/py/vertebra/extensions/broker/plugin.py)[^src-extensions-broker-plugin-py] integrate the native SHM tables into Python runtimes:

- **`SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport`**: Coordinate lease requests (resolving worker partitions via `lookup_partition` when unregistered locally) and statement plan caching across workers via `LeaseBroker`, `StatementCache`, named POSIX SHM (`ShmOwnerHeader` generation/PID reclamation), or authenticated Unix domain socket `SCM_RIGHTS` attachment (`BrokerAttachClient` / `BrokerAttachServer`, failing closed on insecure directory/socket modes).
- **`BrokerHost`, `BrokerEngineDiscovery` & Quarantine Supervision**: `BrokerHost` (`vtb broker serve`, `ShutdownReport`) hosts server pools and `BrokerEngineCapsule` (`BROKER_ENGINE_ABI_VERSION = 1`) vtables across `vertebra._postgres`, `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, and `vertebra._duckdb` (with per-`(pool_id, lease_slot)` pinned connection transactions in `PostgresBrokerBackend`); worker exit/`SIGKILL` transitions slots into `LeaseState::Quarantined` via `reclaim_worker_leases`, executes server `ROLLBACK` cleanup, and clears capacity via `dispose_quarantined`.
- **`vertebra.db.dbapi` & SQLAlchemy `BrokerPool`**: Standalone PEP 249 `SyncDBAPIConnection` / `AsyncDBAPIConnection` (`SyncDBAPIModule`, `AsyncDBAPIModule`, `is_row_returning_sql` in [`_sync.py`](../../../../../src/py/vertebra/db/dbapi/_sync.py)[^src-db-dbapi-sync-py]) run without SQLAlchemy or adapter-package imports, while `SQLAlchemyAsyncDBAPI` ([`async_adapter.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/async_adapter.py)[^src-extensions-sqlalchemy-async-adapter-py]) alongside `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` ([`pool.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/pool.py)[^src-extensions-sqlalchemy-pool-py]) share SHM partitions across SQLAlchemy and Advanced Alchemy engines; session release or pool return failures quarantine the lease and raise `LeaseQuarantinedError`.
- **`BrokerPlugin` & `DbPoolMetricSample`**: Install broker state, DI providers, ZPages (`/brokerz`) diagnostics, and OpenTelemetry `DbPoolMetricSample` counters (`quarantined`, `total_reclaimed`, `total_quarantined`, `total_disposed`); see [Shared-Memory Broker & Cross-Process Limitations](shm-broker-limitations.md).
- **`DatabaseRegistry.default()`, `BaseDatabaseConfig.pool_key()`, & `provide_driver`** ([`vertebra.db.registry`](../../../../../src/py/vertebra/db/registry.py)[^src-db-registry-py], [`vertebra.db.driver._common`](../../../../../src/py/vertebra/db/driver/_common.py)[^src-db-driver-common-py]): Every adapter config overrides `BaseDatabaseConfig.pool_key()` (returning `None` for `:memory:` SQLite and DuckDB instances so ephemeral in-memory databases never cross-pollinate), and `DatabaseRegistry.pool_fingerprint(config_or_url)` delegates directly to `config.pool_key()`. `DatabaseRegistry.default()`, `DatabaseRegistry.default_broker()`, and `provide_driver(name_or_config, *, sync=False, registry=None)` allow native database adapters declared independently across stacked `Django` (`DatabaseWrapperMixin`), `Litestar` (`SQLSpec` `SyncAdapterConfig` / `AsyncAdapterConfig`), `SQLAlchemy` (`+vtb` `SyncDBAPIConnection` / `AsyncDBAPIConnection`), and `Vertebra Native` apps to reuse the exact same underlying connection pool, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer` / `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, and process-wide SHM `LeaseBroker` (unless `share_pool=False` is set).
- **`SyncPool` & `SyncLoopShardedResource` / `AsyncLoopShardedResource`** ([`vertebra.utils.sync_tools`](../../../../../src/py/vertebra/utils/sync_tools.py)[^src-utils-sync-tools-py]): `SyncPool` sizes its `ThreadPoolExecutor` to `max(1, max_workers if max_workers is not None else cpu_count * 2)` workers, propagates `contextvars`, recreates the executor across `fork`, and coalesces `submit_async` completion wakeups per event loop via `_AsyncCompletionQueue` (`_wake_armed` flag and `loop.call_soon_threadsafe(self.drain)`). `SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]` shards non-thread-safe async primitives per event-loop thread.

Provenance sources: [^src-vertebra-py-src-lib-rs], [^src-tests-unit-db-test-native-db-drivers-py].

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
[^src-tests-unit-db-test-native-db-drivers-py]: `src/py/tests/unit/db/test_native_db_drivers.py` — Python implementation module (src/py/tests/unit/db/test_native_db_drivers.py).
