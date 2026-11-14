---
type: Task
id: broker-pool-sharing:ch03-lease-safety-and-uds-attach:t01
flow_id: broker-pool-sharing
chapter_id: ch03-lease-safety-and-uds-attach
title: Implement Lease Quarantine State Machine and Dead-Worker Slot Reclamation
description: Add Quarantine/Resetting slot states and generation-fenced reclamation in ShmLeaseTable.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch02-engine-capsules-and-discovery:t04
files:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
tests:
- src/rs/vertebra-core/tests/it/broker.rs
- src/py/tests/unit/extensions/test_shm_lease_broker.py
read_set:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
estimated_loc: 195
tags:
- quarantine
- broker
- shm
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

# Task `t01`: Implement Lease Quarantine State Machine and Dead-Worker Slot Reclamation

## Objective & Scope Boundary

Implement `LeaseState::Quarantined`, `quarantine`, `dispose_quarantined`, `quarantined_count`, and `reclaim_worker_leases` per [`../spec.md` §2](../spec.md) ([#98](https://github.com/cofin/vertebra/issues/98)) [^core-broker] [^py-broker] [^vertebra-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `LeaseSlot`, `release`, and `reclaim_worker`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker` PyO3 methods.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` type stubs.

## Contracts & Edge Cases

- Transition `Leased -> Quarantined` while keeping `active_leases` admission reserved until `dispose_quarantined` marks the slot `Free`.
- Return `Vec<ReclaimedLease>` in `Quarantined`/pending-rollback state from `reclaim_worker_leases(worker_pid)` so the host runs `ROLLBACK` before slots return to `Free`.
- **Edge Case**: `dispose_quarantined` on a `Free` or `Leased` slot returns `BrokerError::InvalidState`.
- **Edge Case**: Stale `generation` on `quarantine` returns `BrokerError::StaleGeneration`.

## Test Matrix

- Pool of size 1 with 1 quarantined slot blocks new `try_acquire_in` until `dispose_quarantined` runs.
- `reclaim_worker_leases` isolates dead-PID slots without freeing live-PID slots.

## Verification

```bash
make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q
```

## Acceptance Criteria

- [ ] Quarantined slots cannot be re-leased before explicit disposal.
- [ ] `mypy.stubtest vertebra._vertebra` passes.
- [ ] Zero regressions in the fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
