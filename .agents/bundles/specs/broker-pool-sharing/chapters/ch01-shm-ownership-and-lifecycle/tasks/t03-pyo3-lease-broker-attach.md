---
type: Task
id: "broker-pool-sharing:ch01:t03"
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-lifecycle
title: "PyO3 LeaseBroker and StatementCache Atomic Attach Bindings"
description: "Expose open_or_create_named and owner_pid on LeaseBroker and StatementCache with synchronized _vertebra.pyi stubs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch01:t01"
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 175
tags:
  - pyo3
  - broker
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
---

# Task `t03`: PyO3 LeaseBroker and StatementCache Atomic Attach Bindings

## Objective & Scope Boundary

Add `ShmLeaseTable::open_or_create_with_statements` and PyO3 `LeaseBroker.open_or_create_named` / `StatementCache.open_or_create_named` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: Python transport role config (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseTable::create_with_statements` and `open_with_statements`.
- `src/rs/vertebra-py/src/broker.rs` — PyO3 `LeaseBroker` and `StatementCache` constructors.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` and `StatementCache` type stubs.
- `src/py/tests/unit/extensions/test_shm_lease_broker.py` — Unit tests for named SHM lease broker.

## Contracts & Edge Cases

- Add `ShmLeaseTable::open_or_create_with_statements` returning `(Self, bool)` (`created` flag) and registering owned names via `cleanup::register_owned`.
- Expose `open_or_create_named` and `owner_pid` on `LeaseBroker` and `StatementCache` inside `py.detach`, updating `src/py/vertebra/_vertebra.pyi`.
- **Edge Case**: `ShmError::InUse` surfaces distinct owner PID metadata so Python maps it to `BrokerSegmentInUseError`.
- **Edge Case**: Non-`ENOENT` `open_named` errors (`EACCES`, ABI mismatch) propagate without mutating the segment.

## Test Matrix

- Parameterized `test_shm_lease_broker.py` checks `open_or_create_named` returns `created=True` first and `created=False` on second attach.
- `mypy.stubtest vertebra._vertebra` passes with zero signature drift.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q
```

## Acceptance Criteria

- [ ] `LeaseBroker.open_or_create_named` and `StatementCache.open_or_create_named` match `_vertebra.pyi`.
- [ ] Owned segments unregister cleanly when `LeaseBroker.unlink()` is called by the owner.
- [ ] Zero regressions in the chapter fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
