---
type: Task
id: "broker-pool-sharing:ch03:t02"
flow_id: broker-pool-sharing
chapter_id: ch03-engine-capsules-core
title: "BrokerEngineDiscovery Startup Probing via module_loader"
description: "Discover installed vertebra._<engine> wheels via import_optional and register capsules on BrokerPlugin init."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch03:t01"
files:
  - src/py/vertebra/extensions/broker/engines.py
  - src/py/vertebra/extensions/broker/plugin.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
read_set:
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 135
tags:
  - broker
  - python
  - discovery
sources:
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Type stubs for vertebra._vertebra"
---

# Task `t02`: BrokerEngineDiscovery Startup Probing via module_loader

## Objective & Scope Boundary

Create `BrokerEngineDiscovery` in `src/py/vertebra/extensions/broker/engines.py` and invoke `register_installed()` from `BrokerPlugin.on_app_init` per [`../spec.md` §2](../spec.md) [^broker-plugin] [^vertebra-pyi].

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin.on_app_init` startup hook.
- `src/py/vertebra/utils/module_loader.py` — `import_optional` cached loader (`PY-08`).
- `src/py/vertebra/_vertebra.pyi` — `BrokerEngineRegistry` signatures.
- `src/py/tests/unit/extensions/test_shm_broker.py` — SHM broker unit tests.

## Contracts & Edge Cases

- Define `BrokerEngineDiscovery.ENGINE_MODULES = ("vertebra._postgres", "vertebra._mysql", "vertebra._oracle", "vertebra._mssql", "vertebra._duckdb")`.
- Use `vertebra.utils.module_loader.import_optional` (never raw `importlib.import_module`, enforcing `PY-08`); when a module is installed, call `broker_engine_capsule()` and `BrokerEngineRegistry.register`.
- **Edge Case**: Missing optional wheels (`import_optional(...) is None`) are skipped without error.
- **Edge Case**: An installed engine module missing `broker_engine_capsule` raises `AttributeError` immediately.

## Test Matrix

- `@pytest.mark.parametrize` with simulated installed/missing engine modules verifies selective registration and fail-loud missing-symbol handling.
- `make review-check` confirms `PY-08` `module_loader` compliance.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q && make review-check
```

## Acceptance Criteria

- [ ] `BrokerEngineDiscovery.register_installed()` registers every installed native relational/OLAP wheel.
- [ ] Zero raw `importlib.import_module` calls appear in `src/py/vertebra/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
