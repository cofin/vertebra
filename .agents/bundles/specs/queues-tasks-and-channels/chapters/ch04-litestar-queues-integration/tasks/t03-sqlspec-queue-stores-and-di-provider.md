---
type: Task
id: "queues-tasks-and-channels:ch04-litestar-queues-integration:t03"
flow_id: queues-tasks-and-channels
chapter_id: ch04-litestar-queues-integration
title: "Wire Settings-Driven litestar-queues Config (queue_store_class, events_store_class) & Container DI"
description: "Wire queue_store_class / events_store_class settings selection and ContainerTaskDependencyProvider in extensions/litestar/queues/."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t02
files:
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/__init__.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
read_set:
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/__init__.py
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/queues/shm_backend.py
  - src/py/vertebra/framework/di.py
estimated_loc: 190
tags:
  - litestar-queues
  - di
  - sqlspec
sources:
  - id: lq-cfg
    resource: src/py/vertebra/extensions/litestar/queues/config.py
    title: "litestar-queues configuration builders"
  - id: lq-init
    resource: src/py/vertebra/extensions/litestar/queues/__init__.py
    title: "Public litestar-queues extension exports"
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: "Vertebra Container scoped dependency provider"
---

# Task `t03`: Wire Settings-Driven litestar-queues Config (queue_store_class, events_store_class) & Container DI

## Objective & Scope Boundary

Wire settings-driven `litestar-queues` selection (`StoreQueueBackendConfig`, `SHMQueueBackendConfig`, and `SQLSpecBackendConfig` honoring `queue_store_class` & `events_store_class`) plus `ContainerTaskDependencyProvider` per [`../spec.md`](../spec.md) [^lq-cfg] [^lq-init] [^fw-di].

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/queues/{config,__init__}.py` — backend builders and DI wiring.
- `src/py/vertebra/framework/di.py` — `Container.scope()` context manager.

## Contracts & Edge Cases

- Implement `ContainerTaskDependencyProvider(container)` matching `litestar_queues.config.TaskDependencyProvider` (`(task, record, context) -> AsyncContextManager[Mapping[str, Any]]`) entering `async with container.scope():` per task execution.
- Support settings-driven selection between `StoreQueueBackend` (`postgres` / `oracle`), `SHMQueueBackend` (`shm`), and `SQLSpecBackendConfig` (attaching dialect `queue_store_class` and `events_store_class` onto Vertebra SQLSpec configs).

## Test Matrix

- Parametrized tests verifying settings selection (`postgres`, `oracle`, `shm`, `sqlspec`), `queue_store_class` / `events_store_class` resolution in `litestar-queues==0.14.0`, and per-task `ContainerTaskDependencyProvider` injection.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

## Acceptance Criteria

- [ ] `litestar-queues` resolves `StoreQueueBackend`, `SHMQueueBackend`, `queue_store_class`/`events_store_class`, and `ContainerTaskDependencyProvider` cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^lq-cfg]: `src/py/vertebra/extensions/litestar/queues/config.py` — litestar-queues configuration builders.
[^lq-init]: `src/py/vertebra/extensions/litestar/queues/__init__.py` — Public litestar-queues extension exports.
[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container scoped dependency provider.
