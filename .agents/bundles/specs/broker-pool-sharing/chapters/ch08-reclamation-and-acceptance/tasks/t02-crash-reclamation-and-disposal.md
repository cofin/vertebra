---
type: Task
id: "broker-pool-sharing:ch08:t02"
chapter_id: ch08-reclamation-and-acceptance
title: Reclaim crashed worker leases with ROLLBACK and dispose quarantined connections
description: Add ShmLeaseTable::reclaim_worker_leases returning reclaimed coordinates, run server-side ROLLBACK on the owning connection thread before freeing, quarantine and dispose on failure, and report undisposed connections from bounded shutdown.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch05:t01"
  - "broker-pool-sharing:ch05:t03"
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/vertebra/extensions/broker/host.py
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch08-reclamation-and-acceptance/spec.md
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/py/vertebra/extensions/broker/host.py
  - src/rs/vertebra-core/tests/it/broker.rs
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
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
  - id: reclaim
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::reclaim_worker
    title: PID-based slot reclamation without connection cleanup
  - id: active-leases
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::active_leases_for_worker
    title: Active lease enumeration per worker
  - id: host
    resource: src/py/vertebra/extensions/broker/host.py::BrokerHost
    title: Broker host lifecycle (ch05 t03)
tags:
  - reclamation
  - quarantine
  - broker
---

# Task t02: Reclaim Crashed Worker Leases with ROLLBACK and Dispose Quarantined Connections

## Objective & Scope Boundary

1. `src/rs/vertebra-core/src/broker.rs`: add `ReclaimedLease` and `ShmLeaseTable::reclaim_worker_leases(&self, worker_pid: u32) -> Vec<ReclaimedLease>` that CAS-transitions each slot owned by `worker_pid` from `Leased` to a `Reclaiming` hold (generation bumped, slot still counted against admission) and returns the coordinates; `reclaim_worker` becomes a thin caller that frees only slots whose connection the caller already cleaned. Add `ShmLeaseTable::complete_reclaim(slot_index, generation)` to free a slot after successful `ROLLBACK` [^reclaim] [^active-leases].
2. `src/py/vertebra/extensions/broker/host.py` (`BrokerHost` from `ch05 / t03`) [^host]: `on_worker_exit(pid)` posts, per reclaimed lease, a job to the connection's owning thread that runs `ROLLBACK`; success → `complete_reclaim`; failure → `quarantine` → close → `dispose_quarantined`. `shutdown(timeout: float) -> ShutdownReport` disposes quarantined connections and lists any left.
3. Tests in `tests/it/broker.rs` and `test_shm_lease_broker.py`.

Out of scope: metrics (`t04`), acceptance (`t03`).

## Bounded Read Manifest

- `broker.rs` — `ShmLeaseTable::{reclaim_worker, active_leases_for_worker, quarantine, dispose_quarantined}`, `LeaseState` [^reclaim].
- `tests/it/broker.rs` — reclamation tests.
- `host.py` — `BrokerHost` [^host].
- `test_shm_lease_broker.py`.

## Symbol & Signature Contracts

```rust
pub struct ReclaimedLease { pub slot_index: u32, pub generation: u64, pub pool_id: u16 }
impl ShmLeaseTable {
    pub fn reclaim_worker_leases(&self, worker_pid: u32) -> Vec<ReclaimedLease>;
    pub fn complete_reclaim(&self, slot_index: u32, generation: u64) -> Result<(), BrokerError>;
}
```

```python
class BrokerHost:
    def on_worker_exit(self, pid: int) -> int: ...
    def shutdown(self, timeout: float) -> ShutdownReport: ...
```

## Edge Cases & Error Semantics

- Reclaiming a PID with no leases → empty `Vec`; `on_worker_exit` returns `0`.
- `complete_reclaim` with stale generation → `BrokerError::StaleGeneration`.
- Reclaimed slots stay counted against admission until `complete_reclaim` or `dispose_quarantined`.
- ROLLBACK raises → quarantine, close, dispose; the backend PID never returns to the pool.
- Shutdown timeout elapses with a blocked dispose → `ShutdownReport.undisposed` contains `(pool_name, slot_index, generation)`; shutdown still returns.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `reclaim_worker_leases_holds_capacity` (Rust) | pool size 2, worker holds 2, reclaim | 2 coords returned; `try_acquire` fails until `complete_reclaim` |
| `complete_reclaim_stale_generation` (Rust) | wrong generation | `Err(StaleGeneration)` |
| `reclaim_unknown_pid_empty` (Rust) | unused PID | empty vec |
| `test_on_worker_exit_rolls_back_then_frees` | fake connection records SQL | `["ROLLBACK"]` executed on owning thread id; slot free |
| `test_on_worker_exit_rollback_failure_disposes` | rollback raises | connection closed; `dispose_quarantined` called; slot free after dispose |
| `test_shutdown_reports_undisposed` | dispose blocks past `timeout=0.05` | `report.undisposed == (("default", 0, gen),)` |

## Verification

- `cargo test -p vertebra-core --features tls,compression,gcp --test it broker`
- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q`

## Acceptance Criteria

- No reclaimed slot becomes free before `ROLLBACK` succeeds or the connection is disposed; matrix tests pass.

## Notes & Discoveries

- At HEAD `reclaim_worker` frees slots directly with a CAS to `0` and wakes waiters; the new hold state prevents a waiter from receiving a connection with an open transaction.
