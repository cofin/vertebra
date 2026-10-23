---
type: Concept
title: "Shared-Memory Connection Broker & Statement Cache"
description: "The cross-process SHM connection lease broker (`ShmLeaseTable` and `ShmStatementCache` in `vertebra-core/src/broker.rs`), the native PyO3 bindings (`NativeLeaseBroker`, `NativeStatementCache`), and Python broker transports (`ShmBrokerTransport`, `InMemoryBrokerTransport`, `BrokerPlugin`, `SyncPool`, `LoopShardedResource`)."
status: stable
tags: [domains, data, broker, shm, ipc, database]
sources:
  - resource: src/rs/vertebra-core/src/broker.rs
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/py/vertebra/extensions/broker/transport.py
  - resource: src/py/vertebra/extensions/broker/plugin.py
  - resource: src/py/vertebra/extensions/alchemy/broker.py
  - resource: src/py/vertebra/utils/sync_tools.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
---

# Shared-Memory Connection Broker & Statement Cache

Vertebra provides cross-process shared-memory database connection management and statement caching via [`src/rs/vertebra-core/src/broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs) and [`src/py/vertebra/extensions/broker/transport.py`](../../../../../src/py/vertebra/extensions/broker/transport.py).

## 1. Rust SHM Lease Table (`broker.rs`)

[`ShmLeaseTable`](../../../../../src/rs/vertebra-core/src/broker.rs) manages pooled connection slots in a fixed-size POSIX shared memory ring:

- **Slot State Machine**: Each slot contains an atomic `state_and_pid` (`SLOT_STATE_FREE`, `SLOT_STATE_PRIMARY`, `SLOT_STATE_OVERFLOW`), `generation`, `acquired_ns`, `expiry_ns`, and `bind_key_hash`.
- **Atomic Acquisition**: Workers acquire slots using compare-and-swap (`CAS`) without global locks, blocking on `wake_seq` (`WaitWord`) when exhausted.
- **Heartbeats & Stale Reclamation**: If a worker process exits or fails to release its slot before `expiry_ns` elapses, `reap_expired` and `reclaim_worker` transition the slot back to `SLOT_STATE_FREE` and wake waiting workers.
- **Slot Alignment**: Slots are padded to 64-byte cache lines to eliminate false sharing across concurrent worker cores.

## 2. Shared Statement Cache (`ShmStatementCache`)

[`ShmStatementCache`](../../../../../src/rs/vertebra-core/src/broker.rs) provides cross-worker statement compilation caching:

- **Key Hashing**: SQL statements are hashed with FNV-1a and a dialect tag into 64-bit fingerprint keys.
- **Prepared Statement Metadata**: Stores compiled SQL bytes, parameter counts, and MsgPack-encoded parameter plans inside optimistic seqlock-guarded 4-way set-associative slots (`StatementSlot`, 256 B).
- **Eviction**: Clock eviction policy bounded by power-of-two `capacity` (`CLOCK_REF_BIT`).

## 3. Python Broker Layer (`vertebra.extensions.broker`)

[`vertebra.extensions.broker.transport`](../../../../../src/py/vertebra/extensions/broker/transport.py) and [`vertebra.extensions.broker.plugin`](../../../../../src/py/vertebra/extensions/broker/plugin.py) integrate the native SHM tables into Python runtimes:

- **`ShmBrokerTransport` & `InMemoryBrokerTransport`**: Coordinate lease requests and statement plan caching across workers via `NativeLeaseBroker` and `NativeStatementCache`, with an in-process thread-safe fallback.
- **`BrokerPlugin` & `ShmSessionBroker`**: Install broker state, DI providers, ZPages diagnostics, OpenTelemetry `DbPoolRecorder` metrics, and SQLAlchemy `checkout`/`checkin` pool event listeners.
- **`SyncPool` & `LoopShardedResource`** ([`vertebra.utils.sync_tools`](../../../../../src/py/vertebra/utils/sync_tools.py)): Offload blocking operations without event-loop starvation and shard resources per event-loop thread in multi-loop or free-threaded (Python 3.14t) topologies.
