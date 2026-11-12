---
type: Task
id: "broker-pool-sharing:ch03:t02"
chapter_id: ch03-engine-registry-capsule
title: Python engine discovery at broker startup
description: Add vertebra.extensions.broker.engines.BrokerEngineDiscovery, call it from BrokerPlugin.on_app_init, route statement normalization through BrokerEngineRegistry, and add the _vertebra stubs for the new broker symbols.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch03:t01"
read_set:
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/py/vertebra/extensions/broker/engines.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 160
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
  - id: on-app-init
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.on_app_init
    title: Broker plugin startup hook
  - id: stubs
    resource: src/py/vertebra/_vertebra.pyi::LeaseBroker
    title: _vertebra broker stubs
tags:
  - discovery
  - registry
---

# Task t02: Python Engine Discovery at Broker Startup

## Objective & Scope Boundary

1. Create `src/py/vertebra/extensions/broker/engines.py` with `BrokerEngineDiscovery` (chapter §2.2): for each name in `ENGINE_MODULES`, `importlib.util.find_spec(name)`; when the spec exists import the module and call `module.broker_engine_capsule()`, then `BrokerEngineRegistry.register(capsule)`; return the registered dialect tags in order.
2. `BrokerPlugin.on_app_init` [^on-app-init] calls `BrokerEngineDiscovery.register_installed()` once per process (guarded by a class-level flag) before transports start.
3. Add stubs to `src/py/vertebra/_vertebra.pyi` [^stubs]: `stable_hash64`, `BrokerEngineRegistry`, `LeaseBroker.open_or_create_named`, `LeaseBroker.created`, `register_partition`, `try_acquire_in`, `acquire_in`, `heartbeat`.

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin`.
- `src/py/vertebra/extensions/broker/transport.py` — statement plan compilation sites.
- `src/py/vertebra/_vertebra.pyi` — existing `LeaseBroker`, `StatementCache` stubs.
- `src/py/tests/unit/extensions/test_shm_broker.py`.

## Symbol & Signature Contracts

- `class BrokerEngineDiscovery` with `ENGINE_MODULES: ClassVar[tuple[str, ...]]` and `@classmethod register_installed(cls) -> list[int]`.
- Module-level imports only: `importlib`, `importlib.util`, `from vertebra._vertebra import BrokerEngineRegistry`.

## Edge Cases & Error Semantics

- Module missing: skipped.
- Module installed but `broker_engine_capsule` missing: `AttributeError` propagates naming the module.
- Capsule ABI mismatch: `RuntimeError` from `register` propagates.
- Repeated `on_app_init`: discovery runs once.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: in-process; module presence controlled with `monkeypatch.setattr(BrokerEngineDiscovery, "ENGINE_MODULES", (...))`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Absent module | `ENGINE_MODULES = ("vertebra._does_not_exist",)` | `[]` |
| Installed MySQL | `ENGINE_MODULES = ("vertebra._mysql",)` after `ch04 / t01`; `pytest.importorskip("vertebra._mysql")` | `[2]`; `BrokerEngineRegistry.normalize(2, "SELECT :a")` returns `("SELECT ?", 1)` |
| Missing export | Fake module object in `sys.modules["vertebra._fake"]` with a `__spec__` and no capsule fn | `AttributeError` |
| Once per process | Call `on_app_init` twice with a spy on `register_installed` | One call |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q`.
- `make lint` (stubtest for `vertebra._vertebra`).

## Acceptance Criteria

- [ ] Installed engine capsules register with `_vertebra` at broker startup (R9).
- [ ] `_vertebra.pyi` matches the runtime surface under stubtest (R8, R9).

## Notes & Discoveries

- [2026-11-12 01:40] The "Installed MySQL" case becomes active once `ch04 / t01` lands; until then it skips through `importorskip` on the missing attribute check inside the test.

[^on-app-init]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.on_app_init` — Startup hook.
[^stubs]: `src/py/vertebra/_vertebra.pyi::LeaseBroker` — Broker stubs.
