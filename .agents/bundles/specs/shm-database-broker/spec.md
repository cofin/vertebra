---
type: Spec
flow_id: shm-database-broker
title: "Shared-Memory Database Broker Supervisor Reaping, Unified Region & Pub/Sub Ring"
state: closed
plan_revision: 1
plan_commit: null
state_revision: 1
current_task: null
last_operation: finish
operation_targets:
  - "1.1"
  - "1.2"
  - "2.1"
last_verified_checkpoint: "make rs-test && make py-test && make lint-all"
created_at: 2026-10-23T19:34:00Z
updated_at: 2026-10-23T22:23:00Z
description: "Complete cross-process shared-memory database broker integration: co-locate ShmStatementCache inside ShmLeaseTable's single ShmRegion via stmt_offset, wire supervisor worker-exit lease reclamation into RingFactory/WorkerLifecycle, and back ShmBrokerTransport.publish/drain_channel with a cross-process SHM ring."
tags: [feature, ipc, shm, database, broker]
parent_prd: shm-native-drivers-and-broker-proxy
research: []
---

# Flow: Shared-Memory Database Broker Supervisor Reaping, Unified Region & Pub/Sub Ring

**Flow ID:** `shm-database-broker`

## Specification

### Code Analysis Summary

- [`src/rs/vertebra-core/src/broker.rs`](../../../../src/rs/vertebra-core/src/broker.rs): Implements `ShmLeaseTable` (`ShmLeaseHeader` + `LeaseSlot`) and `ShmStatementCache` (`ShmStatementHeader` + 256-byte `StatementSlot` with optimistic seqlock and 4-way set-associative CLOCK eviction). `ShmLeaseHeader` already defines `stmt_capacity: u32` and `stmt_offset: u64`, and `ShmStatementCache::from_raw_ptrs` exists to attach a sub-cache inside `ShmLeaseTable`, but `ShmLeaseTable::init_in_region` currently sets `stmt_capacity = 0` and `stmt_offset = 0`, requiring `ShmBrokerTransport` to allocate a second separate SHM region (`{shm_name}-stmt`).
- [`src/py/vertebra/extensions/broker/transport.py`](../../../../src/py/vertebra/extensions/broker/transport.py): `ShmBrokerTransport` wraps `LeaseBroker` and `StatementCache`, but `publish` and `drain_channel` store `ChannelMessage` items in a process-local `self._channels: dict[str, list[ChannelMessage]]` rather than a cross-process shared-memory broadcast ring.
- [`src/rs/vertebra-http/src/ring_factory.rs`](../../../../src/rs/vertebra-http/src/ring_factory.rs): Spawns and supervises Python SHM worker processes, but does not invoke `ShmLeaseTable::reclaim_worker(worker_pid)` when a worker process exits or crashes, leaving orphaned database connection leases until their TTL expires.

### Relevant Patterns

- [Lock-Free Shared Memory & Sharded Concurrency](../../knowledge/patterns/rust/lock-free-shm-and-sharding.md)
- [PyO3 0.30.1 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md)
- [Shared-Memory Connection Broker & Statement Cache](../../knowledge/domains/data/broker-and-shm-leases.md)

### Requirements

1. **Co-located Single-Region `ShmLeaseTable` + `ShmStatementCache`**:
   - Populate `stmt_capacity` and `stmt_offset` in `ShmLeaseTable::init_in_region_with_statements` so a single `ShmRegion` (and single file descriptor passed via `VERTEBRA_BROKER_SHM_FD`) hosts both the lease slots and the 4-way set-associative statement cache.
2. **Supervisor Crash Reclamation**:
   - Wire `ShmLeaseTable::reclaim_worker(worker_pid)` into `WorkerLifecycle` / `RingFactory` worker exit handling so crashed or restarted workers immediately release their held connection slots and wake blocked waiters without waiting for `lease_ttl_ms`.
3. **Cross-Process Broker Channel Ring**:
   - Back `ShmBrokerTransport.publish` and `ShmBrokerTransport.drain_channel` with a shared-memory ring buffer so invalidation and transaction events published on one worker are visible to all workers attached to the broker.

## Implementation Plan

### Phase 1: Unified SHM Region & Supervisor Worker Reclamation

- [x] 1.1 Co-locate `ShmStatementCache` inside `ShmLeaseTable` (`stmt_offset` / `stmt_capacity`) and expose unified `LeaseBroker` attachment in `vertebra-py`
- [x] 1.2 Wire `ShmLeaseTable::reclaim_worker(pid)` into `vertebra-http` SHM worker lifecycle teardown and respawn hooks

### Phase 2: Cross-Process Broker Pub/Sub Ring

- [x] 2.1 Back `ShmBrokerTransport.publish` and `drain_channel` with a lock-free cross-process SHM channel buffer and multi-process tests

## Continuity Snapshot

- **Active Flow**: `shm-database-broker` (`closed`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `make rs-test && make py-test && make lint-all`
- **Decisions**:
  - Created a clean `planned` Flow spec for the remaining cross-process SHM broker work (co-located statement cache region, supervisor `reclaim_worker` hook, and cross-process `ChannelMessage` ring) instead of reopening archived specs.
- **Recent Discoveries**:
  - `ShmLeaseHeader` in `src/rs/vertebra-core/src/broker.rs` already reserves `stmt_capacity` and `stmt_offset` fields and `ShmStatementCache::from_raw_ptrs`, making single-FD co-location straightforward without breaking the binary header layout.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Activate flow and claim Task `1.1` when ready to implement SHM broker enhancements.
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
  - `.agents/bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md`
