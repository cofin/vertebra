---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch02-broker-ipc-identity-and-wire
title: "Chapter 02: Cross-Process Hash Identity, Pool Partitions, and dbrpc Wire Frames"
description: "Replace PYTHONHASHSEED-salted hash() with XXH3-64 stable_hash64, partition ShmLeaseTable per pool_key with heartbeats, add versioned dbrpc frames, and delete in-memory broker transports."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - broker
  - dbrpc
  - hashing
  - shm
sources:
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Type stubs for vertebra._vertebra"
  - id: ring-pair
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Bidirectional SHM ring pair transport"
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/broker.py::StatementCache
    title: "Unused Python StatementCache in alchemy broker module"
---

# Chapter 02: Cross-Process Hash Identity, Pool Partitions, and dbrpc Wire Frames

<!-- truth: start -->
- `vertebra_core::broker::stable_hash64` (XXH3-64, seed `0`) and `vertebra._vertebra.stable_hash64` replace salted Python `hash()` in `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` [^core-broker] [^broker-transport].
- `ShmLeaseTable` supports `MAX_POOL_PARTITIONS = 64` keyed by `stable_hash64(pool_key)` with independent `pool_size`/`max_overflow` and `last_heartbeat_ns` expiry [^core-broker].
- `vertebra_core::dbrpc` and `vertebra.extensions.broker.dbrpc` define versioned `DbRpcHeader`, `DbRpcOp`, `DbRpcRequest`, and `DbRpcResponse` (`array_like=True`) frames [^ring-pair].
- `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, and dead `alchemy/broker.py::StatementCache` are removed [^broker-transport] [^broker-plugin] [^alchemy-cache].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` in `src/py/vertebra/extensions/broker/transport.py` derive statement and bind keys with `PYTHONHASHSEED`-salted `hash()`, preventing cross-worker cache hits and session bind affinity [^broker-transport]. `ShmLeaseTable` models a single unpartitioned pool, and unused in-memory broker transports plus `alchemy/broker.py::StatementCache` duplicate state [^core-broker] [^alchemy-cache].

**Non-Goals:** Cross-cdylib engine capsule registration (`ch03`); UDS attach authentication (`ch05`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Stable hash | `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs` [^core-broker] [^py-broker] | `stable_hash64(bytes | str) -> int` (XXH3-64 seed `0`) |
| Partitions | `vertebra-core/src/broker.rs::ShmLeaseTable` [^core-broker] | `PoolPartitionSpec`, `register_partition`, `try_acquire_in`, `heartbeat` |
| RPC frames | `vertebra-core/src/dbrpc/mod.rs`, `extensions/broker/dbrpc.py` [^ring-pair] | Versioned `DbRpcOp` request/response framing |
| Cleanup | `extensions/broker/{transport,plugin}.py`, `extensions/alchemy/broker.py` [^broker-transport] [^alchemy-cache] | SHM-only transport and dead `StatementCache` removal |

**Knowledge Anchors:** [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Transport and Rings](../../../../knowledge/domains/ipc/transport-and-rings.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R4a**: Implement `vertebra_core::broker::stable_hash64` (XXH3-64, seed `0`) and expose `vertebra._vertebra.stable_hash64` (`t01`).
- **R5**: Partition `ShmLeaseTable` into up to `MAX_POOL_PARTITIONS = 64` per-`pool_key` slot ranges and gate TTL reaping on `last_heartbeat_ns` (`t02`).
- **R6**: Define `DBRPC_VERSION = 1`, `DbRpcOp` (`Execute`, `ExecuteMany`, `Begin`, `Commit`, `Rollback`, `Savepoint`, `FetchChunk`, `Cancel`, `Close`), `DbRpcRequest`, and `DbRpcResponse` (`t03`).
- **R4b / R7**: Wire `stable_hash64` and partition registration into `transport.py` / `plugin.py`, and delete `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `use_shm=False`, and `alchemy/broker.py::StatementCache` (`t04`).

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
    #[repr(u8)]
    pub enum DbRpcOp { Execute = 1, ExecuteMany = 2, Begin = 3, Commit = 4, Rollback = 5, Savepoint = 6, FetchChunk = 7, Cancel = 8, Close = 9 }
    pub struct DbRpcHeader { pub version: u16, pub op: DbRpcOp, pub request_id: u64, pub lease_slot: u32, pub lease_generation: u64 }
}
```

```python
class DbRpcRequest(msgspec.Struct, frozen=True, gc=False, array_like=True):
    version: int
    op: int
    request_id: int
    lease_slot: int
    lease_generation: int
    sql: str | None = None
    params: tuple[Any, ...] | None = None
    savepoint: str | None = None
    chunk_rows: int = 0

class DbRpcResponse(msgspec.Struct, frozen=True, gc=False, array_like=True):
    request_id: int
    status: int
    rowcount: int = -1
    columns: tuple[str, ...] | None = None
    rows: tuple[tuple[Any, ...], ...] | None = None
    arrow_shm: str | None = None
    lastrowid: int | None = None
    error_class: str | None = None
    error_message: str | None = None
```

**Integration Flow:**
- `get_or_compile_statement` -> `stable_hash64(dialect_tag_le + sql_utf8)` -> deterministic cross-worker `ShmStatementCache` lookup.
- `BrokerPlugin.on_app_init` -> `LeaseBroker.register_partition(stable_hash64(pool_key), pool_size, max_overflow)`.
- `_acquire_sync` / `AsyncSHMBrokerTransport.acquire` -> `stable_hash64(bind_key)` -> `try_acquire_in(partition, ...)`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Hash randomization | Workers with distinct `PYTHONHASHSEED` | Identical 64-bit `stable_hash64` digest | Cross-worker cache hits |
| Partition cap | 65th distinct `pool_key_hash` | `BrokerError::PartitionsExhausted` -> `BrokerAttachError` | Bounded SHM header |
| Active long query | Periodic `heartbeat(slot, gen)` refreshes `last_heartbeat_ns` | Reaper skips active lease; reaps stalled worker after `ttl_ns` | No premature reaping |
| Protocol mismatch | `DbRpcHeader.version != 1` | `DbRpcError::UnsupportedVersion` | Fail-loud wire check |

## 3. Documentation & OKF Knowledge Plan

Removal of `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, and `use_shm=False` from `docs/` executes in `ch08:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4a` | `t01` | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `pytest src/py/tests/unit/extensions/test_broker_transport.py` |
| `R5` | `t02` | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `cargo test -p vertebra-core --test it broker` |
| `R6` | `t03` | `src/rs/vertebra-core/src/dbrpc/mod.rs`, `src/py/vertebra/extensions/broker/dbrpc.py` | `cargo test -p vertebra-core --test it broker` |
| `R4b`, `R7` | `t04` | `src/py/vertebra/extensions/broker/{transport,plugin}.py`, `extensions/alchemy/broker.py` | `pytest src/py/tests/unit/extensions/test_broker_transport.py` |

## 5. Implementation Plan

- [ ] [t01-stable-cross-process-hash.md](tasks/t01-stable-cross-process-hash.md) — Add stable_hash64 (XXH3-64 seed 0) in vertebra-core and expose vertebra._vertebra.stable_hash64.
- [ ] [t02-pool-partitions-and-heartbeats.md](tasks/t02-pool-partitions-and-heartbeats.md) — Partition ShmLeaseTable into up to 64 independent pool_key slot ranges with heartbeat-gated TTL reaping.
- [ ] [t03-dbrpc-frames.md](tasks/t03-dbrpc-frames.md) — Add vertebra_core::dbrpc header codecs and matching array-like DbRpcRequest/DbRpcResponse structs.
- [ ] [t04-shm-transport-keys-and-inmem-removal.md](tasks/t04-shm-transport-keys-and-inmem-removal.md) — Use stable_hash64 in get_or_compile_statement and acquire paths, and remove in-memory broker transports and alchemy StatementCache.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Built-in Python `hash()` is never used for statement keys, pool keys, or bind affinity hashes.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-py && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py`

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Bidirectional SHM ring pair transport.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Unused Python StatementCache in alchemy broker module.
