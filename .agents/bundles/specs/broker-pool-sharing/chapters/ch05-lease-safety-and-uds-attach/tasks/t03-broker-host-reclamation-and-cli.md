---
type: Task
id: "broker-pool-sharing:ch05:t03"
flow_id: broker-pool-sharing
chapter_id: ch05-lease-safety-and-uds-attach
title: "PyO3 Attach Bindings and BrokerHost Crash Reclamation Service"
description: "Expose BrokerAttachServer/Client in _vertebra and implement BrokerHost with server-side ROLLBACK reclamation, owning-thread disposal, and vtb broker serve."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch05:t02"
  - "broker-pool-sharing:ch03:t02"
files:
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/extensions/broker/host.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/rs/vertebra-core/src/broker_attach.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/extensions/broker/errors.py
estimated_loc: 240
tags:
  - broker
  - host
  - reclamation
  - pyo3
sources:
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Type stubs for vertebra._vertebra"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Task `t03`: PyO3 Attach Bindings and BrokerHost Crash Reclamation Service

## Objective & Scope Boundary

Expose `BrokerAttachServer` and `BrokerAttachClient` on `vertebra._vertebra` and implement `BrokerHost`, `ShutdownReport`, and the `broker` Click command group (`vtb broker serve`) in `host.py` per [`../spec.md` §2](../spec.md) [^py-broker] [^vertebra-pyi] [^core-broker].

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker_attach.rs` — `BrokerAttachServer` and `BrokerAttachClient` from `t02`.
- `src/rs/vertebra-py/src/broker.rs` — `LeaseBroker.from_fd` and PyO3 module bindings.
- `src/py/vertebra/_vertebra.pyi` — `_vertebra` type stubs.
- `src/py/vertebra/extensions/broker/errors.py` — `BrokerAuthenticationError` and `LeaseQuarantinedError`.

## Contracts & Edge Cases

- Map `AttachError::AuthFailed` to `BrokerAuthenticationError` and update `src/py/vertebra/_vertebra.pyi`.
- `BrokerHost.on_worker_exit(pid)` calls `reclaim_worker_leases(pid)`, dispatches `ROLLBACK` on each connection's owning thread before releasing, and quarantines + disposes connections whose `ROLLBACK` fails.
- **Edge Case**: `BrokerHost.stop(timeout)` bounds disposal wait and returns `ShutdownReport(undisposed=...)` if any close blocks past `timeout`.
- **Edge Case**: `BrokerConfig.__repr__` redacts attach secrets.

## Test Matrix

- Simulated worker death with open transaction runs `ROLLBACK` before freeing the slot; simulated `ROLLBACK` error disposes the connection on its owning thread.
- UDS `BrokerAttachClient.attach` with wrong secret raises `BrokerAuthenticationError`; `mypy.stubtest vertebra._vertebra` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q
```

## Acceptance Criteria

- [ ] `BrokerHost` shares one lifecycle across supervisor mode and `vtb broker serve`.
- [ ] Reclaimed leases never return to the free pool without a clean `ROLLBACK`.
- [ ] Zero regressions in the chapter fast verification gate.

[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
