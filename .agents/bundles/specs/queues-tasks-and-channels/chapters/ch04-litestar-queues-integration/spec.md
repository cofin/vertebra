---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch04-litestar-queues-integration
title: "Chapter 04: litestar-queues 0.14+ StoreQueueBackend, SHMQueueBackend & Scoped DI"
description: "Bump litestar-queues>=0.14.0, implement slotted SHMQueueBackend and engine-neutral StoreQueueBackend, wire queue_store_class / events_store_class, and bridge Container DI."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - litestar-queues
  - queues
  - shm
  - di
sources:
  - id: pyproject-lq
    resource: pyproject.toml
    title: "litestar-queues dependency floor"
  - id: lq-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: "litestar-queues backend module"
  - id: lq-cfg
    resource: src/py/vertebra/extensions/litestar/queues/config.py
    title: "litestar-queues config helpers"
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: "Vertebra Container DI"
---

# Chapter 04: litestar-queues 0.14+ StoreQueueBackend, SHMQueueBackend & Scoped DI

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `pyproject.toml` sets `litestar-queues>=0.12.1`, `extensions/litestar/queues/backend.py` contains a broken `litestar_queues.records` import masked by Mypy overrides, and no generic `AsyncQueueStore` adapter exists [^pyproject-lq] [^lq-backend].
- **Target Architecture**: `litestar-queues>=0.14.0` runs over slotted `SHMQueueBackend` (`@queue_backend('vertebra_shm')`), engine-neutral `StoreQueueBackend` (`@queue_backend('vertebra_store')`), `SQLSpecBackendConfig` (`queue_store_class` + `events_store_class`), and `ContainerTaskDependencyProvider` [^lq-cfg] [^fw-di].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter upgrades Vertebra's `litestar-queues` integration to `0.14.0+`, providing single-host Rust-ring `SHMQueueBackend`, multi-node `StoreQueueBackend` over any `AsyncQueueStore`, SQLSpec `queue_store_class`/`events_store_class` wiring, and `Container` task DI [^lq-backend] [^lq-cfg].

## 2. Requirements, Concrete Contracts & Architecture

- **R7 (`litestar-queues>=0.14.0` Backends & DI)**: Slotted `SHMQueueBackend` and engine-neutral `StoreQueueBackend`, `queue_store_class`/`events_store_class` support, and `ContainerTaskDependencyProvider`.

```python
@queue_backend("vertebra_store")
class StoreQueueBackendConfig:
    store: AsyncQueueStore
    notification_source: AsyncNotificationSource | None = None
    terminal_ttl: timedelta | None = None
    def create_backend(self, config: QueueConfig) -> StoreQueueBackend: ...

class StoreQueueBackend(BaseQueueBackend):
    __slots__ = ("_notification_source", "_store", "_terminal_ttl")

class SHMQueueBackend(BaseQueueBackend):
    __slots__ = ("_ring", "_segment_name", "_terminal_ttl")

class ContainerTaskDependencyProvider:
    def __init__(self, container: Container) -> None: ...
    def __call__(self, task: Any, record: Any, context: Any) -> AsyncContextManager[Mapping[str, Any]]: ...
```

## 3. Documentation & OKF Knowledge Plan

`litestar-queues` configuration and backend selection are documented in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R7` `litestar-queues>=0.14.0` & `SHMQueueBackend` | `t01` | `pyproject.toml`, `extensions/litestar/queues/shm_backend.py` | `pytest test_litestar_ecosystem_queues.py` |
| `R7` Generic `StoreQueueBackend` | `t02` | `extensions/litestar/queues/backend.py` | `pytest test_litestar_ecosystem_queues.py` |
| `R7` `queue_store_class` & DI provider | `t03` | `extensions/litestar/queues/{config,__init__}.py` | `pytest test_litestar_ecosystem_queues.py` |

## 5. Implementation Plan

- [ ] [t01-litestar-queues-bump-and-shm-backend.md](tasks/t01-litestar-queues-bump-and-shm-backend.md) — Bump `litestar-queues>=0.14.0` and implement slotted Rust-ring `SHMQueueBackend(BaseQueueBackend)`.
- [ ] [t02-generic-store-queue-backend.md](tasks/t02-generic-store-queue-backend.md) — Implement engine-neutral `StoreQueueBackend(BaseQueueBackend)` over `AsyncQueueStore`.
- [ ] [t03-sqlspec-queue-stores-and-di-provider.md](tasks/t03-sqlspec-queue-stores-and-di-provider.md) — Wire settings-driven `queue_store_class` / `events_store_class` selection and `ContainerTaskDependencyProvider`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch04-litestar-queues-integration` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

[^pyproject-lq]: `pyproject.toml` — litestar-queues dependency floor.
[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues backend module.
[^lq-cfg]: `src/py/vertebra/extensions/litestar/queues/config.py` — litestar-queues config helpers.
[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container DI.
