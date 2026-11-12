---
type: Task
id: "broker-pool-sharing:ch01:t03"
chapter_id: ch01-shm-segment-ownership
title: Fail-loud Python broker attach and caller audit
description: Route ShmLeaseTable and LeaseBroker/StatementCache named construction through open_or_create_named, expose LeaseBroker.open_or_create_named with typed attach errors, and delete the except-Exception create_named fallback in the SHM broker transport.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch01:t01"
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/errors.py
tests:
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
  - id: create-with-statements
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::create_with_statements
    title: Lease table named creation
  - id: py-lease-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 create_named/open_named bindings
  - id: shm-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: except-Exception create_named fallback
tags:
  - broker
  - attach
---

# Task t03: Fail-Loud Python Broker Attach and Caller Audit

## Objective & Scope Boundary

1. `src/rs/vertebra-core/src/broker.rs` [^create-with-statements]: add `ShmLeaseTable::open_or_create_with_statements(name, pool_size, max_overflow, stmt_capacity) -> Result<(Self, ShmStatementCache, bool), BrokerError>` over `ShmRegion::open_or_create_named`, validating magic/ABI/capacities on attach and mapping mismatches with a live owner to `BrokerError::SegmentInUse { owner_pid }`.
2. `src/rs/vertebra-py/src/broker.rs` [^py-lease-broker]: add `#[staticmethod] LeaseBroker::open_or_create_named(name, pool_size, max_overflow, stmt_capacity)`; map `BrokerError::SegmentInUse` to `vertebra.extensions.broker.errors.BrokerSegmentInUseError` and other attach failures to `BrokerAttachError` (imported lazily via `PyModule::import` at error time). `StatementCache::create_named` uses `open_or_create_named`.
3. `src/py/vertebra/extensions/broker/transport.py` [^shm-transport]: replace the `try: open_named / except Exception: create_named` block with one `LeaseBroker.open_or_create_named(...)` call, and define `BrokerAttachError(VertebraError)` and `BrokerSegmentInUseError(BrokerAttachError)` in a new `src/py/vertebra/extensions/broker/errors.py` (created by this task; it replaces the `.pyi` edit in the owned-file budget, and `ch03 / t02` adds the `LeaseBroker.open_or_create_named` stub with the other new `_vertebra` symbols).
4. Audit the other `create_named` callers (`src/rs/vertebra-core/src/vector.rs`, `src/rs/vertebra-db/src/arrow_shm.rs`): they create single-owner, uniquely named regions; record in Notes that they keep `create_named` semantics from `t01` (fail on live owner) with no further change.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseTable::{create_with_statements,open_with_statements}`, `BrokerError`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker::{create_named,open_named,from_env}`, `broker_err_to_py`, `StatementCache::{create_named,open_named}`.
- `src/py/vertebra/extensions/broker/transport.py` — `_BaseSHMBrokerTransport.__init__`.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` stub.
- `src/py/tests/unit/extensions/test_shm_lease_broker.py`.

## Symbol & Signature Contracts

- Rust: `BrokerError::SegmentInUse { owner_pid: u32 }`.
- PyO3: `LeaseBroker.open_or_create_named(name: str, pool_size: int, max_overflow: int = 0, stmt_capacity: int = 0) -> LeaseBroker`, plus read-only `LeaseBroker.created: bool`.
- Python: `class BrokerAttachError(VertebraError)`, `class BrokerSegmentInUseError(BrokerAttachError)` with attribute `owner_pid: int`.

## Edge Cases & Error Semantics

- Capacity mismatch with live owner: `BrokerSegmentInUseError("broker segment '/vtb-x' is owned by live pid 1234 with pool_size=8; requested 4")`.
- `VERTEBRA_BROKER_SHM_FD` set: `LeaseBroker.from_env()` precedence unchanged.
- Permission denied: `BrokerAttachError` wrapping the OS error text.
- No `shm_name`: anonymous `LeaseBroker(...)` path unchanged.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_lease_broker.py`. Dependency category: local OS resource.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Two transports one name | `SyncSHMBrokerTransport(BrokerConfig(shm_name="/vtb-t-<uuid8>", pool_size=2))` twice | Second `created is False`; acquiring 3 leases across both times out (shared admission) |
| Capacity mismatch live | First `pool_size=2`, second `pool_size=4` | `BrokerSegmentInUseError` with `owner_pid == os.getpid()` |
| Multi-process race | `multiprocessing` spawn 4 processes constructing the transport behind `Barrier(4)` | Exactly one reports `created`; `stats().total_capacity == 2` in all |
| No fallback | Monkeypatch-free: `git grep -n "except Exception" -- src/py/vertebra/extensions/broker/transport.py` around attach | No match in `__init__` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/extensions/test_broker_transport.py -q`.
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker::`.
- `uv run --no-sync python -m mypy.stubtest vertebra._vertebra` via `make py-test` stubtest step.

## Acceptance Criteria

- [ ] Python attach never replaces a live segment and raises typed errors (R3).
- [ ] Every `create_named` caller is audited and documented (R3).

## Notes & Discoveries

- [2026-11-12 01:40] `vertebra-py/src/broker.rs` exposes both `LeaseBroker::create_named` and `StatementCache::create_named`; both route through the new path.

[^create-with-statements]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable::create_with_statements` — Lease table named creation.
[^py-lease-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 bindings.
[^shm-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Attach fallback.
