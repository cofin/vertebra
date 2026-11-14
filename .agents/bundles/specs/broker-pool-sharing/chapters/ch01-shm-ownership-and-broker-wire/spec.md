---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-broker-wire
title: 'Chapter 01: Atomic SHM Ownership, Supervisor Lifecycle, Pool Identity, and DBRPC Wire Protocol'
description: Enforce O_CREAT | O_EXCL SHM ownership, OwnedShmRegistry crash/shutdown unlink, explicit owner/worker broker roles, cross-process FNV-1a pool identity, partitioned pools/heartbeats, and binary DBRPC frames.
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T23:20:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- shm
- broker
- ownership
- cleanup
- dbrpc
- hashing
sources:
- id: core-shm
  resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
  title: Named POSIX shared-memory regions
- id: shm-it
  resource: src/rs/vertebra-core/tests/it/shm.rs::live_owner_blocks_create_named_with_in_use_and_reaps_dead_owner
  title: SHM ownership and cleanup integration test suite
- id: core-cleanup
  resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
  title: Process SHM cleanup hook
- id: worker-factory
  resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
  title: Supervisor worker spawning, panic hook, and stale SHM reaper
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Type stubs for vertebra._vertebra
- id: ring-pair
  resource: src/rs/vertebra-core/src/transport/ring_pair.rs
  title: Bidirectional SHM ring pair transport
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
- id: alchemy-cache
  resource: src/py/vertebra/extensions/alchemy/broker.py::SyncSHMSessionBroker
  title: Alchemy session broker after StatementCache removal
---

# Chapter 01: Atomic SHM Ownership, Supervisor Lifecycle, Pool Identity, and DBRPC Wire Protocol

<!-- truth: start -->
- `ShmRegion::open_or_create_named` opens first, creates with `O_CREAT | O_EXCL` only on `ENOENT`, and never pre-unlinks a live segment ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^shm-it].
- `ShmOwnerHeader` records `init_state`, `owner_pid`, and `owner_generation`; orphan segments are reclaimed only when `kill(owner_pid, 0)` reports `ESRCH` [^core-shm].
- `cleanup::{register_owned, unregister_owned, cleanup_all, reap_dead_owner_segments}` replace the empty `cleanup_all()` and pattern-only `cleanup_stale_shm` ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].
- Python `_BaseSHMBrokerTransport` enforces explicit `role: Literal["owner", "worker"]` and raises `BrokerSegmentInUseError` / `BrokerAttachError` instead of falling back to `create_named` [^py-broker] [^broker-transport].
- `vertebra_core::broker::stable_hash64` (XXH3-64, seed `0`) and `vertebra._vertebra.stable_hash64` replace salted Python `hash()` in `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` [^core-broker] [^broker-transport].
- `ShmLeaseTable` supports `MAX_POOL_PARTITIONS = 64` keyed by `stable_hash64(pool_key)` with independent `pool_size`/`max_overflow` and `last_heartbeat_ns` expiry [^core-broker].
- `vertebra_core::dbrpc` and `vertebra.extensions.broker.dbrpc` define versioned `DbRpcHeader`, `DbRpcOp`, `DbRpcRequest`, and `DbRpcResponse` (`array_like=True`) frames [^ring-pair].
- `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, and dead `alchemy/broker.py::StatementCache` are removed [^broker-transport] [^broker-plugin] [^alchemy-cache] [^vertebra-pyi].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`ShmRegion::create_named` unconditionally calls `shm_unlink` before `shm_open(..., O_CREAT | O_EXCL)`, and `_BaseSHMBrokerTransport.__init__` catches bare `Exception` on `LeaseBroker.open_named` to invoke `create_named`, destroying or splitting live pools ([#95](https://github.com/cofin/vertebra/issues/95)) [^core-shm] [^broker-transport]. `cleanup::cleanup_all` is a no-op while `vertebra-http` `cleanup_stale_shm` sweeps `/dev/shm` by prefix without checking PID liveness ([#96](https://github.com/cofin/vertebra/issues/96)) [^core-cleanup] [^worker-factory].

**Non-Goals:** Unix socket attach authentication (`ch03`); Lease quarantine state (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| SHM regions | `src/rs/vertebra-core/src/shm.rs::ShmRegion` [^core-shm] | Atomic POSIX `/dev/shm` ownership header and liveness probes |
| Cleanup hooks | `src/rs/vertebra-core/src/cleanup.rs`, `vertebra-http/src/workers/factory.rs` [^core-cleanup] [^worker-factory] | Async-signal-safe owned-name table + PID-aware startup reaper |
| Broker attach | `src/rs/vertebra-py/src/broker.rs`, `extensions/broker/transport.py` [^py-broker] [^broker-transport] | Typed PyO3 `open_or_create_named` + Python `role` enforcement |

**Knowledge Anchors:** [Ring Buffers and SHM](../../../../knowledge/architecture/ipc/ring-buffers-and-shm.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md).

At `18baadcd`, `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` in `src/py/vertebra/extensions/broker/transport.py` derive statement and bind keys with `PYTHONHASHSEED`-salted `hash()`, preventing cross-worker cache hits and session bind affinity [^broker-transport]. `ShmLeaseTable` models a single unpartitioned pool, and unused in-memory broker transports plus `alchemy/broker.py::StatementCache` duplicate state [^core-broker] [^alchemy-cache].

**Non-Goals:** Cross-cdylib engine capsule registration (`ch03`); UDS attach authentication (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Stable hash | `vertebra-core/src/broker.rs`, `vertebra-py/src/broker.rs` [^core-broker] [^py-broker] | `stable_hash64(bytes | str) -> int` (XXH3-64 seed `0`) |
| Partitions | `vertebra-core/src/broker.rs::ShmLeaseTable` [^core-broker] | `PoolPartitionSpec`, `register_partition`, `try_acquire_in`, `heartbeat` |
| RPC frames | `vertebra-core/src/dbrpc/mod.rs`, `extensions/broker/dbrpc.py` [^ring-pair] | Versioned `DbRpcOp` request/response framing |
| Cleanup | `extensions/broker/{transport,plugin}.py`, `extensions/alchemy/broker.py` [^broker-transport] [^alchemy-cache] | SHM-only transport and dead `StatementCache` removal |

**Knowledge Anchors:** [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Transport and Rings](../../../../knowledge/domains/ipc/transport-and-rings.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Implement `ShmOwnerHeader`, `NamedAttach`, `ShmRegion::open_or_create_named`, `owner_pid()`, `owner_alive()`, and `ShmError::{InUse, InitTimeout}` without pre-unlinking live segments (`t01`).
- **R2**: Back `cleanup_all()` with a fixed-capacity async-signal-safe owned-segment table, wire SIGTERM/SIGINT + panic hooks, and replace prefix-only sweeping with `reap_dead_owner_segments` (`t02`).
- **R3a**: Expose `ShmLeaseTable::open_or_create_with_statements`, `LeaseBroker.open_or_create_named`, and `StatementCache.open_or_create_named` with matching `_vertebra.pyi` stubs (`t03`).
- **R3b**: Define `BrokerAttachError`, `BrokerSegmentInUseError`, `BrokerAuthenticationError`, and `LeaseQuarantinedError` in `extensions/broker/errors.py` and enforce explicit `owner`/`worker` roles in `transport.py` and `plugin.py` (`t04`).

```rust
pub const SHM_INIT_PENDING: u32 = 0;
pub const SHM_INIT_READY: u32 = 1;
#[repr(C)]
pub struct ShmOwnerHeader {
    pub init_state: AtomicU32,
    pub owner_pid: AtomicU32,
    pub owner_generation: AtomicU64,
}
pub enum NamedAttach { Created(ShmRegion), Attached(ShmRegion) }
impl ShmRegion {
    pub fn open_or_create_named(
        name: &str,
        size: usize,
        init: impl FnOnce(&mut ShmRegion) -> Result<(), ShmError>,
    ) -> Result<NamedAttach, ShmError>;
    pub fn owner_pid(&self) -> Option<u32>;
    pub fn owner_alive(&self) -> bool;
}
pub fn register_owned(name: &str);
pub fn unregister_owned(name: &str);
pub fn cleanup_all();
pub fn install_signal_cleanup() -> std::io::Result<()>;
pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32;
```

```python
class BrokerAttachError(VertebraError): ...
class BrokerSegmentInUseError(BrokerAttachError): ...
class BrokerAuthenticationError(BrokerAttachError): ...
class LeaseQuarantinedError(VertebraError):
    slot_index: int
    generation: int
```

**Integration Flow:**
- `LeaseBroker.open_or_create_named` -> `ShmLeaseTable::open_or_create_with_statements` -> `ShmRegion::open_or_create_named` -> `cleanup::register_owned` on `Created`.
- Supervisor startup -> `reap_dead_owner_segments(&["/vertebra-", "/vtb-"])` -> `install_panic_cleanup_hook` + `install_signal_cleanup`.
- `_BaseSHMBrokerTransport(role="worker")` -> `LeaseBroker.open_named` only (raises `BrokerAttachError` on `ENOENT` or ABI mismatch without calling `create_named`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Concurrent start | 8 processes call `open_or_create_named` | 1 `Created`, 7 `Attached`; single `/dev/shm` inode | Shared admission cap |
| Live owner ABI mismatch | Header version differs while `owner_alive()` is true | `ShmError::InUse` -> `BrokerSegmentInUseError` | Live segment preserved |
| Dead owner orphan | `kill(owner_pid, 0) == ESRCH` | Unlink stale inode and recreate cleanly | SIGKILL recovery |
| Supervisor panic/signal | `panic!` or `SIGTERM` with owned segments | `cleanup_all()` unlinks owned names only | Peer segments untouched |

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

User and OKF documentation updates execute in `ch04:t04` (`t01`–`t02`).

Removal of `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, and `use_shm=False` from `docs/` executes in `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Enforce Atomic O_CREAT | O_EXCL SHM Creation and Supervisor OwnedShmRegistry | `src/rs/vertebra-core/src/shm.rs`, `src/rs/vertebra-core/src/cleanup.rs`, `src/rs/vertebra-http/src/workers/factory.rs` | `src/rs/vertebra-core/tests/it/shm.rs` |
| `t02` | Expose PyO3 Lease Broker Attach and Explicit Owner vs Worker Server Roles | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` (+3 more) | `src/py/tests/unit/extensions/test_shm_lease_broker.py` |
| `t03` | Implement Deterministic Cross-Process Pool Hash, Partitioned Lease Slots, and Worker Heartbeats | `src/rs/vertebra-core/src/broker.rs`, `src/rs/vertebra-py/src/broker.rs`, `src/py/vertebra/_vertebra.pyi` | `src/py/tests/unit/extensions/test_broker_transport.py`, `src/rs/vertebra-core/tests/it/broker.rs` |
| `t04` | Implement Binary DBRPC Wire Frames, SHM Transport Keys, and In-Memory Fallback Removal | `src/rs/vertebra-core/src/dbrpc/mod.rs`, `src/rs/vertebra-core/src/lib.rs`, `src/py/vertebra/extensions/broker/dbrpc.py` (+3 more) | `src/rs/vertebra-core/tests/it/broker.rs`, `src/py/tests/unit/extensions/test_broker_transport.py` |

## 5. Implementation Plan

- [x] [936110fc] [t01-atomic-shm-and-owned-segment-registry.md](tasks/t01-atomic-shm-and-owned-segment-registry.md) — Separate create_exclusive vs attach_existing SHM constructors and track owned segments in OwnedShmRegistry for deterministic unlink.
- [x] [936110fc] [t02-pyo3-lease-attach-and-owner-worker-roles.md](tasks/t02-pyo3-lease-attach-and-owner-worker-roles.md) — Bind ShmLeaseTable.attach in vertebra-py and enforce explicit owner vs worker broker roles in Python server startup.
- [x] [936110fc] [t03-stable-hash-partitions-and-heartbeats.md](tasks/t03-stable-hash-partitions-and-heartbeats.md) — Replace DefaultHasher with deterministic XXH3-64 pool_key hashing and partition ShmLeaseTable slots with atomic worker heartbeats.
- [x] [936110fc] [t04-dbrpc-frames-and-shm-transport-keys.md](tasks/t04-dbrpc-frames-and-shm-transport-keys.md) — Define binary DBRPC request/response frames, propagate deterministic SHM transport keys, and remove silent in-memory broker fallbacks.

## 6. Current State & Remaining Plan Snapshot

- **Current (`936110fc`)**: `state: completed`; `4/4` tasks completed (`t01`–`t04`).
- **Active Invariants**: Built-in Python `hash()` is never used for statement keys, pool keys, or bind affinity hashes.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-py && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py`

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Existing SHM integration test suite.
[^core-cleanup]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Process SHM cleanup hook.
[^worker-factory]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Supervisor worker spawning, panic hook, and stale SHM reaper.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Bidirectional SHM ring pair transport.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Unused Python StatementCache in alchemy broker module.
