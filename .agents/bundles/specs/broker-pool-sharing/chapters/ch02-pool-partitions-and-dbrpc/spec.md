---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch02-pool-partitions-and-dbrpc
title: "Chapter 2: Pool Partitions and dbrpc Frames"
description: Stable XXH3 cross-process keys, per-pool_key ShmLeaseTable partitions with lease heartbeats, versioned dbrpc RPC frames over ring pairs, and deletion of in-memory broker transports and the alchemy StatementCache.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-shm-segment-ownership
replan_trigger: null
current_task: null
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: transport-hash
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._get_or_compile_plan
    title: Process-seeded hash() statement and bind keys
  - id: lease-table
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Single-pool lease table with TTL reaping
  - id: ring-pair
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: Ring pair transport
  - id: inmem
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseInMemoryBrokerTransport
    title: In-memory broker transports
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/broker.py::StatementCache
    title: Python-side alchemy statement cache
tags:
  - broker
  - dbrpc
  - hashing
---

# Chapter 2: Pool Partitions and dbrpc Frames

<!-- truth: start -->
- `vertebra_core::broker::stable_hash64` (XXH3-64, seed `0`) and `vertebra._vertebra.stable_hash64` derive every cross-process statement and bind key [^transport-hash].
- `ShmLeaseTable` holds up to `MAX_POOL_PARTITIONS = 64` partitions keyed by `stable_hash64(pool_key)`, each with its own `pool_size`/`max_overflow` and slot range [^lease-table].
- Leases record `last_heartbeat_ns`; reaping frees only leases whose heartbeat is older than `lease_ttl_ns` [^lease-table].
- `vertebra_core::dbrpc` defines versioned array-like frames mirrored by `vertebra.extensions.broker.dbrpc` msgspec structs [^ring-pair].
- In-memory transports and `extensions/alchemy/broker.py::StatementCache` are removed [^inmem] [^alchemy-cache].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Statement plans use `hash((dialect_tag, raw_sql))` and bind keys use `hash(bind_key)`; under hash randomization each worker computes different keys for the same SQL, so the shared statement cache never hits across processes and bind affinity breaks [^transport-hash]. `ShmLeaseTable` is a single pool, so two configs share one admission limit [^lease-table]. There is no RPC frame contract for broker-hosted execution, and in-memory transports duplicate the SHM path [^inmem].

### 1.2 Non-Goals

- Native engine execution behind frames (`ch03`, `ch04`); quarantine states (`ch05 / t01`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Hash sites | `transport.py::_BaseInMemoryBrokerTransport._get_or_compile_plan`, `::_BaseSHMBrokerTransport._get_or_compile_plan`, `::_BaseSHMBrokerTransport._acquire_sync`, `::AsyncSHMBrokerTransport._acquire_async` [^transport-hash] | In-process. |
| Lease table | `broker.rs::{ShmLeaseHeader,LeaseSlot,ShmLeaseTable::try_acquire,acquire,release}` [^lease-table] | SHM shared table. |
| Frames | `src/rs/vertebra-core/src/transport/ring_pair.rs` [^ring-pair] | Ring transport. |
| Dead surface | `transport.py::{SyncInMemoryBrokerTransport,AsyncInMemoryBrokerTransport}`, `extensions/broker/plugin.py::BrokerPlugin`, `extensions/alchemy/broker.py::StatementCache` [^inmem] [^alchemy-cache] | Python. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Transport and Rings](../../../../knowledge/domains/ipc/transport-and-rings.md); `.agents/skills/runtime-ipc/SKILL.md`, `msgspec`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R4**: Stable hashing (`t01`). **R5**: Partitions and heartbeats (`t02`). **R6**: `dbrpc` frames (`t03`). **R7**: Dead transports removed (`t04`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
pub fn stable_hash64(bytes: &[u8]) -> u64;
pub const MAX_POOL_PARTITIONS: usize = 64;
pub struct PoolPartitionSpec { pub pool_key_hash: u64, pub pool_size: u32, pub max_overflow: u32 }
impl ShmLeaseTable {
    pub fn register_partition(&self, spec: PoolPartitionSpec) -> Result<u16, BrokerError>;
    pub fn try_acquire_in(&self, partition: u16, worker_pid: u32, ttl_ns: u64, bind_key_hash: u64) -> Result<Option<LeaseGrant>, BrokerError>;
    pub fn heartbeat(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
}
pub mod dbrpc {
    pub const DBRPC_VERSION: u16 = 1;
    #[repr(u8)] pub enum DbRpcOp { Execute = 1, ExecuteMany = 2, Begin = 3, Commit = 4, Rollback = 5, Savepoint = 6, FetchChunk = 7, Cancel = 8, Close = 9 }
    pub struct DbRpcHeader { pub version: u16, pub op: DbRpcOp, pub request_id: u64, pub lease_slot: u32, pub lease_generation: u64 }
    pub fn encode_header(h: &DbRpcHeader, out: &mut Vec<u8>);
    pub fn decode_header(buf: &[u8]) -> Result<(DbRpcHeader, &[u8]), DbRpcError>;
}
```

```python
def stable_hash64(data: bytes | str) -> int: ...
class DbRpcRequest(msgspec.Struct, frozen=True, gc=False, array_like=True):
    version: int
    op: int
    request_id: int
    lease_slot: int
    lease_generation: int
    sql: str | None = None
    params: list[Any] | None = None
    savepoint: str | None = None
    chunk_rows: int = 0
class DbRpcResponse(msgspec.Struct, frozen=True, gc=False, array_like=True):
    request_id: int
    status: int
    rowcount: int = -1
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    arrow_shm: str | None = None
    lastrowid: int | None = None
    error_class: str | None = None
    error_message: str | None = None
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `_BaseSHMBrokerTransport._get_or_compile_plan` → `stable_hash64(dialect_tag.to_bytes(2,"little") + raw_sql.encode())`.
- `_acquire_sync` → `stable_hash64(bind_key)` → `LeaseBroker.try_acquire_in(partition, ...)`.
- `BrokerPlugin.on_app_init` → `LeaseBroker.register_partition(stable_hash64(repr(config.pool_key())), pool_size, max_overflow)`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Different hash seeds | `PYTHONHASHSEED=1` vs `2` | Same `stable_hash64` | Cross-process keys |
| Partition overflow | 65th distinct pool_key | `BrokerError::PartitionsExhausted` → `BrokerAttachError` | Fail loud |
| Same pool_key twice | Two registrations | Same partition index | Idempotent |
| Stalled worker | No heartbeat for `ttl` | Lease reaped, `total_expired` +1 | No blind reaping of active leases |
| Unknown frame version | `version=2` | `DbRpcError::Version` → response `status=2` | Versioned protocol |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given two subprocesses with different `PYTHONHASHSEED`, when both compute a plan key for `SELECT 1`, then the keys are equal and the second sees a statement-cache hit.
- Given partitions A (`pool_size=1`) and B (`pool_size=1`), when A is exhausted, then B still grants a lease.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Hash | XXH3-64 via `xxhash-rust` | FNV-1a: weaker distribution for long SQL; `hashlib`: slower and Python-only. |
| Partitions | Fixed partition table in one segment | One segment per pool: multiplies attach/auth and cleanup work. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in `ch09`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R4 | [t01](tasks/t01-stable-cross-process-hash.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_broker_transport.py` |
| R5 | [t02](tasks/t02-pool-partitions-and-heartbeats.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R6 | [t03](tasks/t03-dbrpc-frames.md) | `behavior_tdd` | `src/rs/vertebra-core/tests/it/broker.rs` |
| R7 | [t04](tasks/t04-remove-in-memory-transports.md) | `regression_tdd` | `src/py/tests/unit/extensions/test_broker_transport.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch02:t01 — Stable cross-process statement and bind hash (tasks/t01-stable-cross-process-hash.md)
- [ ] broker-pool-sharing:ch02:t02 — Per-pool_key partitions and lease heartbeats (tasks/t02-pool-partitions-and-heartbeats.md)
- [ ] broker-pool-sharing:ch02:t03 — Versioned dbrpc frames (tasks/t03-dbrpc-frames.md)
- [ ] broker-pool-sharing:ch02:t04 — Remove in-memory transports and alchemy StatementCache (tasks/t04-remove-in-memory-transports.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker && uv run --no-sync pytest src/py/tests/unit/extensions -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed).
- **Active Invariants**: No Python `hash()` in cross-process keys.

[^transport-hash]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._get_or_compile_plan` — `hash()` keys.
[^lease-table]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Lease table.
[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Ring pair transport.
[^inmem]: `src/py/vertebra/extensions/broker/transport.py::_BaseInMemoryBrokerTransport` — In-memory transports.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Alchemy statement cache.
