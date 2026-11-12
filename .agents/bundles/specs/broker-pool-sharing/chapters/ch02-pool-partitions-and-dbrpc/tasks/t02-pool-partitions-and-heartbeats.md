---
type: Task
id: "broker-pool-sharing:ch02:t02"
chapter_id: ch02-pool-partitions-and-dbrpc
title: Per-pool_key partitions and lease heartbeats
description: Partition ShmLeaseTable by stable pool_key hash with per-partition capacity, add heartbeat-based expiry, and expose register_partition/try_acquire_in/heartbeat through LeaseBroker.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch01:t03"
  - "broker-pool-sharing:ch02:t01"
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/vertebra/_vertebra.pyi
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-core/tests/it/broker.rs
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
estimated_loc: 240
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: lease-header
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseHeader
    title: Single-pool header
  - id: lease-slot
    resource: src/rs/vertebra-core/src/broker.rs::LeaseSlot
    title: Lease slot layout
tags:
  - broker
  - partitions
---

# Task t02: Per-pool_key Partitions and Lease Heartbeats

## Objective & Scope Boundary

1. Add a partition directory after `ShmLeaseHeader` [^lease-header]: `[PoolPartition; MAX_POOL_PARTITIONS]` with `pool_key_hash: AtomicU64`, `pool_size`, `max_overflow`, `slot_base`, `slot_count`, `active: AtomicU32`. Bump the ABI version constant.
2. `register_partition(spec)` claims a directory entry by CAS on `pool_key_hash` (idempotent for the same hash and identical capacities; `BrokerError::PartitionMismatch` for different capacities; `PartitionsExhausted` when full or when slots run out).
3. `try_acquire_in` / `acquire_in` restrict slot search to the partition range; existing `try_acquire` / `acquire` map to partition `0` (the default partition created at construction).
4. Add `last_heartbeat_ns: AtomicU64` to `LeaseSlot` [^lease-slot]; `heartbeat()` refreshes it after generation check; `reap_expired` compares against `last_heartbeat_ns + ttl_ns` rather than acquisition time.
5. PyO3: `LeaseBroker.register_partition(pool_key_hash, pool_size, max_overflow) -> int`, `try_acquire_in(partition, worker_pid, ttl_ns, bind_key_hash)`, `acquire_in(...)`, `heartbeat(slot_index, generation)`; stubs follow in `ch03 / t01` stub pass (record).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseHeader`, `LeaseSlot`, `ShmLeaseTable::{try_acquire,acquire,release,reap_expired,reclaim_worker}`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker`.
- `src/rs/vertebra-core/tests/it/broker.rs`.
- `src/py/vertebra/_vertebra.pyi`.

## Symbol & Signature Contracts

- See chapter §2.2; `BrokerError::{PartitionsExhausted, PartitionMismatch { pool_key_hash: u64 }}`.
- `LeaseGrant` gains `partition: u16`.

## Edge Cases & Error Semantics

- Partition with `pool_size = 0`: `BrokerError::InvalidCapacity`.
- Heartbeat with stale generation: `BrokerError::StaleGeneration`.
- Release across partitions: release uses the slot index; partition `active` counter decremented from the slot's partition.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/broker.rs`. Dependency category: in-process SHM.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Isolation | Partitions A(1,0), B(1,0); acquire A twice | Second A `None`; B acquire `Some` |
| Idempotent | `register_partition(h, 2, 0)` twice | Same index |
| Mismatch | `register_partition(h, 2, 0)` then `(h, 3, 0)` | `PartitionMismatch` |
| Exhausted | 65 distinct hashes | 65th `PartitionsExhausted` |
| Heartbeat keeps lease | ttl 50 ms, heartbeat every 20 ms for 200 ms, then `reap_expired()` | `0` reaped |
| Stalled lease | ttl 50 ms, no heartbeat, sleep 80 ms | `reap_expired() == 1` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker::`.
- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q` (existing default-partition behavior unchanged).

## Acceptance Criteria

- [ ] Partitions isolate admission per pool_key (R5).
- [ ] Reaping is heartbeat-based (R5).

## Notes & Discoveries

- [2026-11-12 01:40] `LeaseBroker.try_acquire` already accepts `bind_key_hash`; partitioning is orthogonal to bind affinity.

[^lease-header]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseHeader` — Header layout.
[^lease-slot]: `src/rs/vertebra-core/src/broker.rs::LeaseSlot` — Slot layout.
