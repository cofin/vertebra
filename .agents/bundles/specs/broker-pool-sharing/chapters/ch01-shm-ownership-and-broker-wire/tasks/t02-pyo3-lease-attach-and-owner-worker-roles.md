---
type: Task
id: broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t02
flow_id: broker-pool-sharing
chapter_id: ch01-shm-ownership-and-broker-wire
title: Expose PyO3 Lease Broker Attach and Explicit Owner vs Worker Server Roles
description: Bind ShmLeaseTable.attach in vertebra-py and enforce explicit owner vs worker broker roles in Python server startup.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t01
files:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/extensions/broker/errors.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/plugin.py
tests:
- src/py/tests/unit/extensions/test_shm_lease_broker.py
read_set:
- src/rs/vertebra-core/src/broker.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
- src/py/tests/unit/extensions/test_shm_lease_broker.py
- src/py/vertebra/extensions/broker/transport.py
- src/py/vertebra/extensions/broker/plugin.py
estimated_loc: 220
tags:
- pyo3
- broker
- shm
- python
- errors
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
- id: broker-transport
  resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
  title: Python SHM broker transports and lease objects
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
---

# Task `t02`: Expose PyO3 Lease Broker Attach and Explicit Owner vs Worker Server Roles

## Objective & Scope Boundary

Add `ShmLeaseTable::open_or_create_with_statements` and PyO3 `LeaseBroker.open_or_create_named` / `StatementCache.open_or_create_named` per [`../spec.md` §2](../spec.md) [^core-broker] [^py-broker] [^vertebra-pyi]. Out of scope: Python transport role config (`t04`).

Create `vertebra.extensions.broker.errors` and eliminate the silent `open_named` -> `create_named` fallback in `_BaseSHMBrokerTransport` per [`../spec.md` §2](../spec.md) ([#95](https://github.com/cofin/vertebra/issues/95)) [^broker-transport] [^broker-plugin]. Out of scope: UDS secret handshake (`ch03`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/broker.rs` — `ShmLeaseTable::create_with_statements` and `open_with_statements`.
- `src/rs/vertebra-py/src/broker.rs` — PyO3 `LeaseBroker` and `StatementCache` constructors.
- `src/py/vertebra/_vertebra.pyi` — `LeaseBroker` and `StatementCache` type stubs.
- `src/py/tests/unit/extensions/test_shm_lease_broker.py` — Unit tests for named SHM lease broker.

- `src/py/vertebra/extensions/broker/transport.py` — `_BaseSHMBrokerTransport.__init__` fallback to `create_named`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerConfig` and `BrokerPlugin` initialization.
- `src/py/vertebra/typing.py` — `VertebraError` public exception base.
- `src/py/tests/unit/extensions/test_shm_lease_broker.py` — Broker attach unit tests.

## Contracts & Edge Cases

- Add `ShmLeaseTable::open_or_create_with_statements` returning `(Self, bool)` (`created` flag) and registering owned names via `cleanup::register_owned`.
- Expose `open_or_create_named` and `owner_pid` on `LeaseBroker` and `StatementCache` inside `py.detach`, updating `src/py/vertebra/_vertebra.pyi`.
- **Edge Case**: `ShmError::InUse` surfaces distinct owner PID metadata so Python maps it to `BrokerSegmentInUseError`.
- **Edge Case**: Non-`ENOENT` `open_named` errors (`EACCES`, ABI mismatch) propagate without mutating the segment.

- Define `BrokerAttachError`, `BrokerSegmentInUseError`, `BrokerAuthenticationError`, and `LeaseQuarantinedError(slot_index, generation)` in `errors.py`.
- Add `role: Literal["owner", "worker"] = "owner"` to `BrokerConfig` and `_BaseSHMBrokerTransport`: `owner` uses `open_or_create_named`; `worker` uses `open_named` / `from_fd` only and raises `BrokerAttachError` on failure.
- **Edge Case**: Worker role facing `FileNotFoundError` (`ENOENT`) raises `BrokerAttachError` without creating an isolated segment.
- **Edge Case**: Live segment with mismatched capacity/ABI raises `BrokerSegmentInUseError` naming segment and `owner_pid`.

## Test Matrix

- Parameterized `test_shm_lease_broker.py` checks `open_or_create_named` returns `created=True` first and `created=False` on second attach.
- `mypy.stubtest vertebra._vertebra` passes with zero signature drift.

- `@pytest.mark.parametrize` over `ENOENT`, `PermissionError`, and ABI mismatch verifies `role="worker"` raises `BrokerAttachError` / `BrokerSegmentInUseError` and leaves `/dev/shm` clean.
- Multi-worker `role="owner"` + `role="worker"` attach shares one underlying `LeaseBroker` capacity.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/broker && uv run --no-sync mypy src/py/vertebra/extensions/broker
```

## Acceptance Criteria

- [ ] `LeaseBroker.open_or_create_named` and `StatementCache.open_or_create_named` match `_vertebra.pyi`.
- [ ] Owned segments unregister cleanly when `LeaseBroker.unlink()` is called by the owner.
- [ ] Bare `except Exception:` fallback to `LeaseBroker.create_named` is completely removed.
- [ ] Ruff, Mypy, and `test_shm_lease_broker.py` pass with zero inline `#` comments.
- [ ] Zero regressions in the fast verification gate.

[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
