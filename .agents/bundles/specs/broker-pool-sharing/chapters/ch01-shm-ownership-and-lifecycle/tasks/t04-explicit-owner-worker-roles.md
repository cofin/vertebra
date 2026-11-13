---
type: Task
id: "broker-pool-sharing:ch01:t04"
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-lifecycle
title: "Typed Broker Errors and Explicit Owner/Worker Transport Roles"
description: "Add BrokerAttachError hierarchy and enforce explicit owner/worker roles in _BaseSHMBrokerTransport and BrokerPlugin."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch01:t03"
files:
  - src/py/vertebra/extensions/broker/errors.py
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
tests:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/typing.py
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 165
tags:
  - python
  - broker
  - errors
sources:
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
---

# Task `t04`: Typed Broker Errors and Explicit Owner/Worker Transport Roles

## Objective & Scope Boundary

Create `vertebra.extensions.broker.errors` and eliminate the silent `open_named` -> `create_named` fallback in `_BaseSHMBrokerTransport` per [`../spec.md` §2](../spec.md) ([#95](https://github.com/cofin/vertebra/issues/95)) [^broker-transport] [^broker-plugin]. Out of scope: UDS secret handshake (`ch05`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — `_BaseSHMBrokerTransport.__init__` fallback to `create_named`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerConfig` and `BrokerPlugin` initialization.
- `src/py/vertebra/typing.py` — `VertebraError` public exception base.
- `src/py/tests/unit/extensions/test_shm_lease_broker.py` — Broker attach unit tests.

## Contracts & Edge Cases

- Define `BrokerAttachError`, `BrokerSegmentInUseError`, `BrokerAuthenticationError`, and `LeaseQuarantinedError(slot_index, generation)` in `errors.py`.
- Add `role: Literal["owner", "worker"] = "owner"` to `BrokerConfig` and `_BaseSHMBrokerTransport`: `owner` uses `open_or_create_named`; `worker` uses `open_named` / `from_fd` only and raises `BrokerAttachError` on failure.
- **Edge Case**: Worker role facing `FileNotFoundError` (`ENOENT`) raises `BrokerAttachError` without creating an isolated segment.
- **Edge Case**: Live segment with mismatched capacity/ABI raises `BrokerSegmentInUseError` naming segment and `owner_pid`.

## Test Matrix

- `@pytest.mark.parametrize` over `ENOENT`, `PermissionError`, and ABI mismatch verifies `role="worker"` raises `BrokerAttachError` / `BrokerSegmentInUseError` and leaves `/dev/shm` clean.
- Multi-worker `role="owner"` + `role="worker"` attach shares one underlying `LeaseBroker` capacity.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/broker && uv run --no-sync mypy src/py/vertebra/extensions/broker
```

## Acceptance Criteria

- [ ] Bare `except Exception:` fallback to `LeaseBroker.create_named` is completely removed.
- [ ] Ruff, Mypy, and `test_shm_lease_broker.py` pass with zero inline `#` comments.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
