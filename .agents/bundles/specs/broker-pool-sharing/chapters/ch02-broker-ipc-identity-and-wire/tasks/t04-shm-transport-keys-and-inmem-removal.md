---
type: Task
id: "broker-pool-sharing:ch02:t04"
flow_id: broker-pool-sharing
chapter_id: ch02-broker-ipc-identity-and-wire
title: "Wire stable_hash64 in SHM Transports and Delete In-Memory Transports"
description: "Use stable_hash64 in get_or_compile_statement and acquire paths, and remove in-memory broker transports and alchemy StatementCache."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch02:t01"
  - "broker-pool-sharing:ch02:t02"
files:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/alchemy/broker.py
tests:
  - src/py/tests/unit/extensions/test_broker_transport.py
  - src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/alchemy/broker.py
  - src/py/tests/unit/extensions/test_broker_transport.py
estimated_loc: 205
tags:
  - broker
  - python
  - debloat
sources:
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/broker.py::StatementCache
    title: "Unused Python StatementCache in alchemy broker module"
---

# Task `t04`: Wire stable_hash64 in SHM Transports and Delete In-Memory Transports

## Objective & Scope Boundary

Replace salted `hash()` in `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire` with `stable_hash64`, and delete `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `use_shm=False`, and `alchemy/broker.py::StatementCache` per [`../spec.md` §2](../spec.md) [^broker-transport] [^broker-plugin] [^alchemy-cache].

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — `get_or_compile_statement`, `_acquire_sync`, `AsyncSHMBrokerTransport.acquire`, and `_BaseInMemoryBrokerTransport`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin` `use_shm` branch and partition registration.
- `src/py/vertebra/extensions/alchemy/broker.py` — Dead `StatementCache` (`self._cache`) class.
- `src/py/tests/unit/extensions/test_broker_transport.py` — Existing transport unit tests.

## Contracts & Edge Cases

- Route all statement, `pool_key`, and `bind_key` hashes in `transport.py` and `plugin.py` through `vertebra._vertebra.stable_hash64`.
- Delete `_BaseInMemoryBrokerTransport`, `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `use_shm=False`, and `alchemy/broker.py::StatementCache` with zero aliases.
- **Edge Case**: Passing `bind_key=None` uses `0` without hashing `"None"`.
- **Edge Case**: Attempting `SyncInMemoryBrokerTransport` import raises `ImportError` (`PY-07` clean break).

## Test Matrix

- `@pytest.mark.parametrize` verifies deterministic keys across `get_or_compile_statement`, `_acquire_sync`, and `AsyncSHMBrokerTransport.acquire`.
- Confirm zero occurrences of `hash(` in `src/py/vertebra/extensions/broker/transport.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py src/py/tests/unit/extensions/test_shm_broker.py -q && uv run --no-sync ruff check src/py/vertebra/extensions && uv run --no-sync mypy src/py/vertebra/extensions
```

## Acceptance Criteria

- [ ] Zero salted `hash()` calls remain in `extensions/broker/transport.py`.
- [ ] In-memory broker transports and `alchemy/broker.py::StatementCache` are deleted cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Unused Python StatementCache in alchemy broker module.
