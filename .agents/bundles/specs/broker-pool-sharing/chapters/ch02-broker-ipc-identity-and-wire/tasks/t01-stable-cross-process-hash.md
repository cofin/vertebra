---
type: Task
id: "broker-pool-sharing:ch02:t01"
flow_id: broker-pool-sharing
chapter_id: ch02-broker-ipc-identity-and-wire
title: "Deterministic XXH3-64 stable_hash64 Primitive and PyO3 Binding"
description: "Add stable_hash64 (XXH3-64 seed 0) in vertebra-core and expose vertebra._vertebra.stable_hash64."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/extensions/test_broker_transport.py
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_broker_transport.py
estimated_loc: 95
tags:
  - hashing
  - broker
  - pyo3
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

# Task `t01`: Deterministic XXH3-64 stable_hash64 Primitive and PyO3 Binding

## Objective & Scope Boundary

Implement `stable_hash64` in `vertebra-core` and expose `vertebra._vertebra.stable_hash64(data: bytes | str) -> int` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: Python transport call-site wiring (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — Current statement hash computation in `ShmStatementCache`.
- `src/rs/vertebra-py/src/broker.rs` — PyO3 broker module exports.
- `src/py/vertebra/_vertebra.pyi` — Stub declarations for `_vertebra`.
- `src/py/tests/unit/extensions/test_broker_transport.py` — Broker transport key tests.

## Contracts & Edge Cases

- Compute unseeded XXH3-64 (`seed = 0`) over UTF-8 bytes or raw `bytes` in `stable_hash64` and use it inside `ShmStatementCache`.
- Export `stable_hash64` on `_vertebra` and declare `def stable_hash64(data: bytes | str) -> int: ...` in `_vertebra.pyi`.
- **Edge Case**: Empty `b""` and `""` produce identical deterministic `u64` digests across subprocesses with distinct `PYTHONHASHSEED`.
- **Edge Case**: Full unsigned 64-bit range converts losslessly to Python `int`.

## Test Matrix

- Subprocess test with `PYTHONHASHSEED=1` vs `PYTHONHASHSEED=999` asserts identical `stable_hash64` digests and `StatementCache` hits.
- `mypy.stubtest vertebra._vertebra` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py -q
```

## Acceptance Criteria

- [ ] `stable_hash64` produces identical digests across randomized Python interpreters.
- [ ] `ShmStatementCache` keys match `stable_hash64`.
- [ ] Zero regressions in the chapter fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
