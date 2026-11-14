---
type: Task
id: broker-pool-sharing:ch02-engine-capsules-and-discovery:t01
flow_id: broker-pool-sharing
chapter_id: ch02-engine-capsules-and-discovery
title: Define BrokerEngineCapsule ABI v1, BrokerEngineRegistry, and Python Discovery
description: Implement BrokerPoolBackend + PyCapsule ABI v1 in vertebra-db/vertebra-py and Python BrokerEngineDiscovery.register_installed().
status: stable
state: completed
commit: 673051f1
completed_at: '2026-11-14T23:42:28Z'
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch01-shm-ownership-and-broker-wire:t04
files:
- src/rs/vertebra-db/src/lib.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/extensions/broker/engines.py
- src/py/vertebra/extensions/broker/plugin.py
tests:
- src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs
- src/py/tests/unit/extensions/test_shm_broker.py
read_set:
- src/rs/vertebra-db/src/lib.rs
- src/rs/vertebra-core/src/gcp_auth.rs
- src/rs/vertebra-py/src/broker.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/extensions/broker/plugin.py
- src/py/vertebra/utils/module_loader.py
estimated_loc: 245
tags:
- capsule
- ffi
- pyo3
- broker
- python
- discovery
sources:
- id: db-registry
  resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
  title: Per-cdylib static BrokerEngineBackend registry in vertebra-db
- id: gcp-capsule
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
  title: Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs
- id: py-broker
  resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
  title: PyO3 LeaseBroker and StatementCache bindings
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Type stubs for vertebra._vertebra
- id: broker-plugin
  resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
  title: BrokerPlugin lifecycle and metric snapshot integration
---

# Task `t01`: Define BrokerEngineCapsule ABI v1, BrokerEngineRegistry, and Python Discovery

## Objective & Scope Boundary

Implement `#[repr(C)] BrokerEngineCapsule`, `BrokerPoolBackend`, `new_broker_engine_pycapsule`, and `BrokerEngineRegistry`, create `BrokerEngineDiscovery` in `src/py/vertebra/extensions/broker/engines.py`, and invoke `register_installed()` from `BrokerPlugin.on_app_init` per [`../spec.md` §2](../spec.md) [^db-registry] [^gcp-capsule] [^py-broker] [^vertebra-pyi] [^broker-plugin]. Out of scope: engine-specific pool backends (`t02`–`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/lib.rs` — Current `BrokerEngineBackend` and `BROKER_ENGINE_BACKENDS` static map.
- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCacheCapsule` C-ABI vtable and refcount pattern.
- `src/rs/vertebra-py/src/broker.rs` — `_vertebra` broker bindings and `PyCapsule` registration.
- `src/py/vertebra/_vertebra.pyi` — Type stubs for `vertebra._vertebra` including `BrokerEngineRegistry`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin.on_app_init` startup hook.
- `src/py/vertebra/utils/module_loader.py` — `import_optional` cached loader (`PY-08`).

## Contracts & Edge Cases

- Delete `BROKER_ENGINE_BACKENDS`, `register_broker_engine_backend`, and `lookup_broker_engine_backend` from `src/rs/vertebra-db/src/lib.rs`.
- Wrap every `extern "C"` vtable trampoline in `std::panic::catch_unwind`, validate `abi_version == 1`, and add `// SAFETY:` comments (`RT-01`).
- Define `BrokerEngineDiscovery.ENGINE_MODULES = ("vertebra._postgres", "vertebra._mysql", "vertebra._oracle", "vertebra._mssql", "vertebra._duckdb")`.
- Use `vertebra.utils.module_loader.import_optional` (never raw `importlib.import_module`, enforcing `PY-08`); when a module is installed, call `broker_engine_capsule()` and `BrokerEngineRegistry.register`.
- **Edge Case**: Re-registering an existing `dialect_tag` atomically swaps the handle and invokes `drop_ctx_fn` once on the replaced handle.
- **Edge Case**: Passing a non-capsule or wrong-named `PyCapsule` raises `TypeError`; `abi_version != 1` raises `RuntimeError`.
- **Edge Case**: Missing optional wheels (`import_optional(...) is None`) are skipped without error, whereas an installed engine module missing `broker_engine_capsule` raises `AttributeError` immediately.

## Test Matrix

- Rust integration test (`broker_engine_capsule_tests.rs`) verifies `clone_ctx_fn`/`drop_ctx_fn` reference balance, `abi_version` mismatch rejection, and `catch_unwind` panic containment.
- `@pytest.mark.parametrize` with simulated installed/missing engine modules in `test_shm_broker.py` verifies selective registration, fail-loud missing-symbol handling, and wrong-capsule `TypeError`.
- `mypy.stubtest vertebra._vertebra` and `make review-check` (`PY-08`) pass.

## Verification

```bash
make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --test broker_engine_capsule_tests && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q && make review-check
```

## Acceptance Criteria

- [x] Static `BROKER_ENGINE_BACKENDS` is removed from `vertebra-db`.
- [x] `BrokerEngineRegistry` safely registers, invokes, replaces, and drops C-ABI capsules.
- [x] `BrokerEngineDiscovery.register_installed()` registers every installed native relational/OLAP wheel.
- [x] Zero raw `importlib.import_module` calls appear in `src/py/vertebra/`.
- [x] Zero regressions in the fast verification gate.

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
