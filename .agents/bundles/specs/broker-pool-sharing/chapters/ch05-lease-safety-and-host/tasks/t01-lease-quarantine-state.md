---
type: Task
id: "broker-pool-sharing:ch05:t01"
chapter_id: ch05-lease-safety-and-host
title: Lease quarantine state
description: Add LeaseState::Quarantined to LeaseSlot with quarantine/dispose_quarantined/quarantined_count in ShmLeaseTable and LeaseBroker, with matching _vertebra stubs.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch02:t02"
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/vertebra/_vertebra.pyi
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 180
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
  - id: lease-slot
    resource: src/rs/vertebra-core/src/broker.rs::LeaseSlot
    title: Lease slot layout
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 broker bindings
tags:
  - broker
---

# Task t01: Lease Quarantine State

## Objective & Scope Boundary

Implement the Rust and binding half of [#98](https://github.com/cofin/vertebra/issues/98):

1. In `src/rs/vertebra-core/src/broker.rs` [^lease-slot], add `#[repr(u32)] pub enum LeaseState { Free = 0, Leased = 1, Quarantined = 2 }` stored in `LeaseSlot.state: AtomicU32`; `release` refuses `Quarantined` slots (`BrokerError::InvalidState`); `quarantine(slot, gen)` CAS `Leased → Quarantined`; `dispose_quarantined(slot, gen)` CAS `Quarantined → Free`, bumps generation, decrements `active_leases`, increments `total_quarantined_disposed`; `quarantined_count()` scans slots. `reap_expired` and `reclaim_worker` skip `Quarantined` slots (the host disposes them).
2. In `src/rs/vertebra-py/src/broker.rs` [^py-broker], expose `quarantine`, `dispose_quarantined`, `quarantined_count`, and add `quarantined` / `total_quarantined_disposed` to `BrokerStats`.
3. Add the matching stubs to `src/py/vertebra/_vertebra.pyi`. The Python `LeaseQuarantinedError` lands in `t02` with the other broker error types.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `LeaseSlot`, `ShmLeaseTable::{release,reap_expired,reclaim_worker}`, `LeaseTableStats`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker`, `BrokerStats`.
- `src/rs/vertebra-core/tests/it/broker.rs`, `src/py/vertebra/_vertebra.pyi`.

## Symbol & Signature Contracts

- Chapter §2.2 Rust contracts; `BrokerError::InvalidState { expected: LeaseState, found: LeaseState }`.
- Python `LeaseBroker.quarantine(slot_index: int, generation: int) -> None`, `dispose_quarantined(slot_index: int, generation: int) -> None`, `quarantined_count() -> int`; `BrokerStats.quarantined: int`, `BrokerStats.total_quarantined_disposed: int`.

## Edge Cases & Error Semantics

- Stale generation on quarantine: `BrokerError::StaleGeneration`.
- Quarantine of `Free`: `InvalidState`.
- Worker crash while slot quarantined: `reclaim_worker` leaves it for disposal and counts it in its return value as not reclaimed.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-core/tests/it/broker.rs`, `src/py/tests/unit/extensions/test_shm_lease_broker.py`. Dependency category: in-process SHM.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Quarantine blocks capacity | `pool_size=1`; acquire, `quarantine`, `try_acquire` | `None`; `quarantined_count() == 1` |
| Dispose returns capacity | then `dispose_quarantined` | `try_acquire` returns `Some` with generation + 1 |
| Release refused | `release` on quarantined slot | `InvalidState` (Python `RuntimeError`) |
| Reaper skips | quarantined slot, ttl elapsed, `reap_expired()` | `0` |
| Reclaim skips | `reclaim_worker(pid)` with one quarantined slot | Slot still `Quarantined` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker::`.
- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q -k quarantine`.
- `make lint` (stubtest for `vertebra._vertebra`).

## Acceptance Criteria

- [ ] Quarantined slots are never re-leased and count against admission until disposed (R11).

## Notes & Discoveries

- [2026-11-12 01:40] No `quarantin` or `poison` symbol exists in `vertebra-core` or `extensions` at HEAD.

[^lease-slot]: `src/rs/vertebra-core/src/broker.rs::LeaseSlot` — Lease slot layout.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 broker bindings.
