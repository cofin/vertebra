---
type: Task
id: "broker-pool-sharing:ch02:t02"
flow_id: broker-pool-sharing
chapter_id: ch02-broker-ipc-identity-and-wire
title: "Per-pool_key Lease Table Partitions and Heartbeat Expiry"
description: "Partition ShmLeaseTable into up to 64 independent pool_key slot ranges with heartbeat-gated TTL reaping."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch01:t03"
  - "broker-pool-sharing:ch02:t01"
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/rs/vertebra-core/tests/it/broker.rs
estimated_loc: 235
tags:
  - broker
  - shm
  - partitions
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
---

# Task `t02`: Per-pool_key Lease Table Partitions and Heartbeat Expiry

## Objective & Scope Boundary

Add `MAX_POOL_PARTITIONS = 64`, `PoolPartitionSpec`, `register_partition`, `try_acquire_in`, and `heartbeat` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: quarantine states (`ch05:t01`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseHeader`, `LeaseSlot`, and `ShmLeaseTable`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker` PyO3 methods.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` stub signatures.
- `src/rs/vertebra-core/tests/it/broker.rs` — Broker integration tests.

## Contracts & Edge Cases

- Maintain idempotent `register_partition(PoolPartitionSpec)` mapping `pool_key_hash` to partition `0..63` with isolated `pool_size + max_overflow` bounds.
- Record `last_heartbeat_ns: AtomicU64` on `LeaseSlot`; reap expired leases only when `now_ns - last_heartbeat_ns > ttl_ns`.
- **Edge Case**: Registering a 65th distinct `pool_key_hash` returns `BrokerError::PartitionsExhausted`.
- **Edge Case**: `heartbeat` with a stale `generation` returns `BrokerError::StaleGeneration` without mutating the slot.

## Test Matrix

- Exhausting partition A (`pool_size=1, max_overflow=0`) still permits immediate lease acquisition in partition B.
- Heartbeated lease survives past initial `ttl_ns`, whereas un-heartbeated lease is reaped once `ttl_ns` elapses.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker
```

## Acceptance Criteria

- [ ] Up to 64 `pool_key` partitions enforce independent admission limits in one SHM segment.
- [ ] `LeaseBroker` partition and heartbeat bindings match `_vertebra.pyi`.
- [ ] Zero regressions in the chapter fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
