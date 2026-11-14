---
type: Task
id: broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t03
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-broker-wire
title: Implement Deterministic Cross-Process Pool Hash, Partitioned Lease Slots, and Worker Heartbeats
description: Replace DefaultHasher with deterministic 64-bit FNV-1a pool_key hashing and partition ShmLeaseTable slots with atomic worker heartbeats.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t02
files:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
tests:
- src/py/tests/unit/extensions/test_broker_transport.py
- src/rs/vertebra-core/tests/it/broker.rs
read_set:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
- src/py/tests/unit/extensions/test_broker_transport.py
- src/rs/vertebra-core/tests/it/broker.rs
estimated_loc: 235
tags:
- hashing
- broker
- pyo3
- shm
- partitions
sources:
- id: core-broker
  resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
  title: Shared-memory lease table, statement cache, dialect tags, and worker reclamation
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Type stubs for vertebra._vertebra
---

# Task `t03`: Implement Deterministic Cross-Process Pool Hash, Partitioned Lease Slots, and Worker Heartbeats

## Objective & Scope Boundary

Implement `stable_hash64` in `vertebra-core` and expose `vertebra._vertebra.stable_hash64(data: bytes | str) -> int` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: Python transport call-site wiring (`t04`).

Add `MAX_POOL_PARTITIONS = 64`, `PoolPartitionSpec`, `register_partition`, `try_acquire_in`, and `heartbeat` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: quarantine states (`ch03:t01`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — Current statement hash computation in `ShmStatementCache`.
- `src/rs/vertebra-py/src/broker.rs` — PyO3 broker module exports.
- `src/py/vertebra/_vertebra.pyi` — Stub declarations for `_vertebra`.
- `src/py/tests/unit/extensions/test_broker_transport.py` — Broker transport key tests.

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseHeader`, `LeaseSlot`, and `ShmLeaseTable`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker` PyO3 methods.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` stub signatures.
- `src/rs/vertebra-core/tests/it/broker.rs` — Broker integration tests.

## Contracts & Edge Cases

- Compute unseeded XXH3-64 (`seed = 0`) over UTF-8 bytes or raw `bytes` in `stable_hash64` and use it inside `ShmStatementCache`.
- Export `stable_hash64` on `_vertebra` and declare `def stable_hash64(data: bytes | str) -> int: ...` in `_vertebra.pyi`.
- **Edge Case**: Empty `b""` and `""` produce identical deterministic `u64` digests across subprocesses with distinct `PYTHONHASHSEED`.
- **Edge Case**: Full unsigned 64-bit range converts losslessly to Python `int`.

- Maintain idempotent `register_partition(PoolPartitionSpec)` mapping `pool_key_hash` to partition `0..63` with isolated `pool_size + max_overflow` bounds.
- Record `last_heartbeat_ns: AtomicU64` on `LeaseSlot`; reap expired leases only when `now_ns - last_heartbeat_ns > ttl_ns`.
- **Edge Case**: Registering a 65th distinct `pool_key_hash` returns `BrokerError::PartitionsExhausted`.
- **Edge Case**: `heartbeat` with a stale `generation` returns `BrokerError::StaleGeneration` without mutating the slot.

## Test Matrix

- Subprocess test with `PYTHONHASHSEED=1` vs `PYTHONHASHSEED=999` asserts identical `stable_hash64` digests and `StatementCache` hits.
- `mypy.stubtest vertebra._vertebra` passes.

- Exhausting partition A (`pool_size=1, max_overflow=0`) still permits immediate lease acquisition in partition B.
- Heartbeated lease survives past initial `ttl_ns`, whereas un-heartbeated lease is reaped once `ttl_ns` elapses.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker
```

## Acceptance Criteria

- [ ] `stable_hash64` produces identical digests across randomized Python interpreters.
- [ ] `ShmStatementCache` keys match `stable_hash64`.
- [ ] Up to 64 `pool_key` partitions enforce independent admission limits in one SHM segment.
- [ ] `LeaseBroker` partition and heartbeat bindings match `_vertebra.pyi`.
- [ ] Zero regressions in the fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
