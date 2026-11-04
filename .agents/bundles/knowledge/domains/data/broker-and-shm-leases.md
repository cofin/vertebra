---
type: Concept
title: "Shared-Memory Connection Broker & Statement Cache"
description: "The cross-process SHM connection lease broker (`ShmLeaseTable` and `ShmStatementCache` in `vertebra-core/src/broker.rs`), the native PyO3 bindings (`LeaseBroker`, `StatementCache`), and Python broker transports (`SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin`, `SyncPool`, `SyncLoopShardedResource`, `AsyncLoopShardedResource`)."
status: stable
tags: [domains, data, broker, shm, ipc, database]
sources:
  - resource: src/rs/vertebra-core/src/broker.rs
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/py/vertebra/extensions/broker/transport.py
  - resource: src/py/vertebra/extensions/broker/plugin.py
  - resource: src/py/vertebra/extensions/alchemy/broker.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
---

# Shared-Memory Connection Broker & Statement Cache

Vertebra provides cross-process shared-memory database connection management and statement caching via [`src/rs/vertebra-core/src/broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs) and [`src/py/vertebra/extensions/broker/transport.py`](../../../../../src/py/vertebra/extensions/broker/transport.py).

## 1. Rust SHM Lease Table (`broker.rs`)

[`ShmLeaseTable`](../../../../../src/rs/vertebra-core/src/broker.rs) manages pooled connection slots, a co-located statement cache, and a cross-process pub/sub ring in a single fixed-size POSIX shared memory region:

- **Single-Region Co-location**: `ShmLeaseTable::init_in_region_with_statements(region, pool_size, max_overflow, stmt_capacity)` places the 64-byte `LeaseTableHeader` (including `stmt_offset`, `stmt_capacity`, `channel_write_seq`, and `channel_capacity`), the `LeaseSlot` array, the `ShmChannelSlot` ring (`DEFAULT_CHANNEL_CAPACITY = 256` × 256 B), and the `ShmStatementCache` inside one `ShmRegion` (`VERTEBRA_BROKER_SHM_FD` / `VERTEBRA_BROKER_SHM_SIZE`). `LeaseBroker.statement_cache()` returns a `StatementCache` handle bound to the co-located statement cache in the same region.
- **Slot State Machine**: Each slot contains an atomic `state_and_pid` (`SLOT_STATE_FREE`, `SLOT_STATE_PRIMARY`, `SLOT_STATE_OVERFLOW`), `generation`, `acquired_ns`, `expiry_ns`, and `bind_key_hash`.
- **Atomic Acquisition**: Workers acquire slots using compare-and-swap (`CAS`) without global locks, blocking on `wake_seq` (`WaitWord`) when exhausted.
- **Heartbeats & Crash Reclamation**: If a worker process exits or fails to release its slot before `expiry_ns` elapses, `reap_expired` and `reclaim_worker(pid)` (invoked automatically by `WorkerLifecycle::reclaim_worker_leases` and `RingFactory::shutdown_workers` in [`workers/factory.rs`](../../../../../src/rs/vertebra-http/src/workers/factory.rs)) transition all slots owned by `pid` back to `SLOT_STATE_FREE` and wake waiting workers.
- **Cross-Process Channel Ring (`ShmChannelSlot`)**: `publish_channel(channel, payload, sender_pid, timestamp_ns)` and `drain_channel(channel, cursor)` provide lock-free seqlock-guarded cross-process pub/sub messaging across forked workers.
- **Slot Alignment**: Lease slots (64 B) and channel/statement slots (256 B) are cache-line aligned to eliminate false sharing across concurrent worker cores.

## 2. Shared Statement Cache (`ShmStatementCache`)

[`ShmStatementCache`](../../../../../src/rs/vertebra-core/src/broker.rs) provides cross-worker statement compilation caching:

- **Key Hashing**: SQL statements are hashed with FNV-1a and a dialect tag into 64-bit fingerprint keys.
- **Prepared Statement Metadata**: Stores compiled SQL bytes, parameter counts, and MsgPack-encoded parameter plans inside optimistic seqlock-guarded 4-way set-associative slots (`StatementSlot`, 256 B).
- **Eviction**: Clock eviction policy bounded by power-of-two `capacity` (`CLOCK_REF_BIT`).

## 3. Python Broker Layer (`vertebra.extensions.broker`)

[`vertebra.extensions.broker.transport`](../../../../../src/py/vertebra/extensions/broker/transport.py) and [`vertebra.extensions.broker.plugin`](../../../../../src/py/vertebra/extensions/broker/plugin.py) integrate the native SHM tables into Python runtimes:

- **`SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` & `SyncInMemoryBrokerTransport` / `AsyncInMemoryBrokerTransport`**: Coordinate lease requests and statement plan caching across workers via `LeaseBroker` and `StatementCache`, with an in-process thread-safe fallback.
- **`BrokerPlugin` & `SyncSHMSessionBroker` / `AsyncSHMSessionBroker`**: Install broker state, DI providers, ZPages diagnostics, OpenTelemetry `DbPoolRecorder` metrics, and SQLAlchemy `checkout`/`checkin` pool event listeners.
- **`DatabaseRegistry.default()` & Cross-Framework Shared Pools** ([`vertebra.db._registry`](../../../../../src/py/vertebra/db/_registry.py)): `DatabaseRegistry.default()`, `DatabaseRegistry.default_broker()`, and `DatabaseRegistry.pool_fingerprint(config_or_url)` normalize `(vendor, host_or_path, port, database, user, pooler_mode, cloud_sql_instance, alloydb_instance_uri)` so native database adapters declared independently across stacked `Django` (`DatabaseWrapperMixin`), `Litestar` (`SQLSpec` `SyncAdapterConfig` / `AsyncAdapterConfig`), `SQLAlchemy` (`+vtb` `SyncDBAPIConnection` / `AsyncDBAPIConnection`), and `Vertebra Native` apps reuse the exact same underlying connection pool, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer` / `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, and process-wide SHM `LeaseBroker` (unless `share_pool=False` is set).
- **`SyncPool` & `SyncLoopShardedResource` / `AsyncLoopShardedResource`** ([`vertebra.utils.sync_tools`](../../../../../src/py/vertebra/utils/sync_tools.py)): `SyncPool` right-sizes its thread pool (`max(4, min(32, cpu_count * 2))` on GIL builds, `cpu_count * 4` on `3.14t`/`3.15t`) and coalesces `submit_async` completion wakeups per event loop via `_LoopCompletionQueue` (`0 -> 1` transition triggers a single `loop.call_soon_threadsafe(_drain_completions)` wakeup per burst). `SyncLoopShardedResource[T]` / `AsyncLoopShardedResource[T]` shards non-thread-safe async primitives per event-loop thread.
