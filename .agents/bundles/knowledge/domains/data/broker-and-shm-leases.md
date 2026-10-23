---
type: Concept
title: "Shared-Memory Connection Broker & Statement Cache"
description: "The cross-process SHM connection lease broker (`ShmLeaseTable` and `ShmStatementCache` in `vertebra-core/src/broker.rs`), the native PyO3 binding (`NativeConnectionBroker`), and Python pool abstractions (`ConnectionBroker`, `SyncPool`, `LoopShardedResource`)."
status: stable
tags: [domains, data, broker, shm, ipc, database]
sources:
  - resource: src/rs/vertebra-core/src/broker.rs
  - resource: src/rs/vertebra-py/src/lib.rs
  - resource: src/py/vertebra/db/broker.py
  - resource: src/py/vertebra/db/_registry.py
  - resource: src/py/tests/unit/db/test_native_db_drivers.py
---

# Shared-Memory Connection Broker & Statement Cache

Vertebra provides cross-process shared-memory database connection management and statement caching via [`src/rs/vertebra-core/src/broker.rs`](../../../../../src/rs/vertebra-core/src/broker.rs) and [`src/py/vertebra/db/broker.py`](../../../../../src/py/vertebra/db/broker.py).

## 1. Rust SHM Lease Table (`broker.rs`)

[`ShmLeaseTable`](../../../../../src/rs/vertebra-core/src/broker.rs) manages pooled connection slots in a fixed-size POSIX shared memory ring:

- **Slot State Machine**: Each slot contains an atomic `state` (`IDLE`, `ACQUIRED`, `DRAINING`, `DEAD`), `owner_pid`, `acquired_at_epoch_ms`, and `lease_timeout_ms`.
- **Atomic Acquisition**: Workers acquire slots using compare-and-swap (`CAS`) without global locks.
- **Heartbeats & Stale Reclamation**: If a worker process exits or fails to touch its slot before `lease_timeout_ms` elapses, supervisory sweeps transition the slot back to `IDLE` or invoke cleanup routines.
- **Slot Alignment**: Slots are padded to 64-byte or 128-byte cache lines to eliminate false sharing across concurrent worker cores.

## 2. Shared Statement Cache (`ShmStatementCache`)

[`ShmStatementCache`](../../../../../src/rs/vertebra-core/src/broker.rs) provides cross-worker statement compilation caching:

- **Key Hashing**: SQL statements are hashed with XXH3/FNV-1a into 64-bit fingerprint keys.
- **Prepared Statement Metadata**: Stores parameterized metadata, input type codes, column names, and parsed AST tokens so child worker processes skip redundant statement preparation and SQL parser passes.
- **Eviction**: Clock/LRU eviction policy bounded by `max_cached_statements` (default 256).

## 3. Python Broker Layer (`vertebra.db.broker`)

[`vertebra.db.broker`](../../../../../src/py/vertebra/db/broker.py) integrates the native SHM tables into Python runtimes:

- **`ConnectionBroker`**: High-level manager coordinating lease requests across workers. Dispatches to `NativeConnectionBroker` when the Rust extension is present, with a transparent pure-Python memory fallback.
- **`SyncPool` & `AsyncPool`**: Thread-safe and task-safe pool wrappers with min/max bounds, connection health checks, and automatic eviction on disconnects.
- **`LoopShardedResource`**: In multi-loop or free-threaded (Python 3.14t) topologies, shards pooled resources per event-loop thread to prevent cross-thread lock contention and GIL-release thrashing.
- **`SyncConnectionDescriptor`**: Frozen metadata struct describing backend type, host, port, database, user, pool limits, and active lease handles.
