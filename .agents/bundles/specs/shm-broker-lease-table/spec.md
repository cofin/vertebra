---
type: Spec
flow_id: shm-broker-lease-table
title: "Shared-Memory Atomic Lease Table, Futex Wait Queue & Supervisor Reclamation"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:40:00Z
updated_at: 2026-10-22T04:40:00Z
description: "Implement the cache-line-isolated shared-memory LeaseSlot array, WaitWord futex wait/wake admission control, supervisor memfd/shm_open FD inheritance and worker-crash reclamation, and GIL-detached PyO3 NativeLeaseBroker bindings."
tags: [feature, rust, python, ipc, database]
parent_prd: vertebra-shm-connection-broker
research: []
---

# Shared-Memory Atomic Lease Table, Futex Wait Queue & Supervisor Reclamation

## Problem & Goal

In `src/py/vertebra/extensions/alchemy/broker.py:167-360`, database connection lease admission is bound to a single Python process (`InMemoryBrokerTransport`), and `ShmBrokerTransport` (`:318-360`) allocates an unused 64 KiB `ShmRegion` without cross-process synchronization. When running $N$ Python workers behind `vertebra-http` (`src/rs/vertebra-http/src/workers/{factory,supervisor}.rs`), upstream database pools cannot enforce a global `pool_size + max_overflow` ceiling across processes, and crashed workers cannot have their in-flight connection slots reclaimed by the supervisor.

This chapter builds the Rust shared-memory lease table (`ShmLeaseTable`), supervisor lifecycle wiring (`WorkerFactory` and `WorkerLifecycle`), and GIL-detached PyO3 bindings (`NativeLeaseBroker`, `NativeLeaseGrant`, `NativeBrokerStats`):

1. **`ShmLeaseTable` & `LeaseSlot` (`src/rs/vertebra-core/src/broker.rs`)**:
   - Defines `BROKER_MAGIC: u64 = u64::from_le_bytes(*b"VTBBRKR1")` and `BROKER_ABI_VERSION: u32 = 1`.
   - Defines `#[repr(C, align(64))] pub struct ShmLeaseHeader` (256 bytes) storing immutable capacity (`pool_size`, `max_overflow`, `stmt_capacity`) and cache-line-isolated (`CacheLine<T>` from `src/rs/vertebra-core/src/layout.rs`) atomic counters: `active_leases: CacheLine<AtomicU32>`, `waiters: CacheLine<AtomicU32>`, `wake_seq: CacheLine<WaitWord>`, `rr_cursor: CacheLine<AtomicU32>`, `total_acquired: AtomicU64`, `total_released: AtomicU64`, `total_expired: AtomicU64`, `total_timeouts: AtomicU64`, and `total_reclaimed: AtomicU64`.
   - Defines `#[repr(C, align(64))] pub struct LeaseSlot` (64 bytes per slot) with `state_and_pid: AtomicU64` (`0` = free, `(1 << 32) | pid` = primary, `(2 << 32) | pid` = overflow), `generation: AtomicU64`, `acquired_ns: AtomicU64`, `expiry_ns: AtomicU64`, `bind_key_hash: AtomicU64`, and `_pad: [u8; 24]`.
   - Implements lock-free CAS acquisition (`acquire`, `try_acquire`), generation-checked `release`, TTL expiration reaping (`reap_expired`), futex wait/wake (`wake_seq.wait_timeout` / `wake_one` via `WaitWord` in `src/rs/vertebra-core/src/platform/mod.rs`), and `reclaim_worker(worker_pid: u32) -> u32`.
2. **Supervisor FD Inheritance & Crash Reclamation (`src/rs/vertebra-http/src/workers/{factory,supervisor}.rs`, `config.rs`, `main.rs`)**:
   - Extends `WorkerFactory` to allocate a shared `ShmLeaseTable` region (`ShmRegion::create`), clear `FD_CLOEXEC` via `vertebra_core::platform::process::make_inheritable`, pass `VERTEBRA_BROKER_SHM_FD`, `VERTEBRA_BROKER_SHM_SIZE`, `VERTEBRA_BROKER_POOL_SIZE`, and `VERTEBRA_BROKER_MAX_OVERFLOW` to each spawned worker, and invoke `broker_table.reclaim_worker(dead_pid)` inside `WorkerFactory` / `WorkerLifecycle` crash recovery before respawning a dead worker.
3. **PyO3 `NativeLeaseBroker` Bindings (`src/rs/vertebra-py/src/broker.rs`, `lib.rs`, `_vertebra.pyi`)**:
   - Exposes `NativeLeaseBroker`, `NativeLeaseGrant`, and `NativeBrokerStats` in `vertebra._vertebra` with `py.detach(|| ...)` around blocking futex waits so neither the Python 3.12 GIL nor Python 3.14t thread state is held while waiting for a connection slot.

---

## Requirements Traceability

| Requirement ID | Description | Tasks |
| :--- | :--- | :--- |
| `BRK-R1` | Cache-line-isolated `LeaseSlot` array enforcing `pool_size + max_overflow` across processes. | `1.1`, `1.4` |
| `BRK-R2` | Zero-spin futex/ulock wait-wake (`WaitWord`) on lease exhaustion and release. | `1.1`, `1.4` |
| `BRK-R3` | Monotonic nanosecond TTL expiration (`expiry_ns`) and `generation` ABA protection. | `1.1`, `1.4` |
| `BRK-R4` | Supervisor `VERTEBRA_BROKER_SHM_FD` inheritance and `reclaim_worker(dead_pid)` on worker exit. | `1.2`, `1.4` |
| `BRK-R5` | GIL-detached PyO3 `NativeLeaseBroker` bindings and `_vertebra.pyi` type stubs. | `1.3`, `1.4` |

---

## Task Breakdown

- [ ] `shm-broker-lease-table:1.1` — Implement `ShmLeaseTable`, `LeaseSlot`, TTL Reaper & Futex Wait Queue in `vertebra-core` (`verification_strategy: behavior_tdd`, `depends_on: []`)
- [ ] `shm-broker-lease-table:1.2` — Wire Supervisor Broker SHM Inheritance & Crash Lease Reclamation in `vertebra-http` (`verification_strategy: integration_acceptance`, `depends_on: [shm-broker-lease-table:1.1]`)
- [ ] `shm-broker-lease-table:1.3` — Expose GIL-Detached `NativeLeaseBroker` PyO3 Bindings & Stubs in `vertebra-py` (`verification_strategy: behavior_tdd`, `depends_on: [shm-broker-lease-table:1.1]`)
- [ ] `shm-broker-lease-table:1.4` — Multi-Process Contention, Futex Latency & Worker Crash Reclamation Tests (`verification_strategy: integration_acceptance`, `depends_on: [shm-broker-lease-table:1.2, shm-broker-lease-table:1.3]`)
