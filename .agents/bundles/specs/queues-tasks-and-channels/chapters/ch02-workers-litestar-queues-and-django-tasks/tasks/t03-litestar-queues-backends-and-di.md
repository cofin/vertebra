---
type: Task
id: queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t03
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: Bump litestar-queues>=0.14.0 and Implement SHMQueueBackend, StoreQueueBackend & DI Provider
description: Raise litestar-queues>=0.14.0 and wire slotted SHMQueueBackend, StoreQueueBackend(BaseQueueBackend), queue_store_class/events_store_class, and ContainerTaskDependencyProvider.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t02
files:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/queues/shm_backend.py
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/__init__.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/queues/config.py
  - src/py/vertebra/extensions/litestar/queues/__init__.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/framework/di.py
estimated_loc: 245
tags:
  - litestar-queues
  - shm
  - queues
  - store-backend
  - di
  - sqlspec
sources:
  - id: pyproject-lq
    resource: pyproject.toml
    title: litestar-queues version floor in queues extra and test dependency group
  - id: lq-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: Existing litestar-queues backend module with broken litestar_queues.records import
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py
    title: AsyncQueueStore and AsyncNotificationSource protocols
  - id: lq-cfg
    resource: src/py/vertebra/extensions/litestar/queues/config.py
    title: litestar-queues configuration builders
  - id: lq-init
    resource: src/py/vertebra/extensions/litestar/queues/__init__.py
    title: Public litestar-queues extension exports
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: Vertebra Container scoped dependency provider
verification_strategy: regression_tdd
---

# Task `t03`: Bump litestar-queues>=0.14.0 and Implement SHMQueueBackend, StoreQueueBackend & DI Provider

## Objective & Scope Boundary

Bump `litestar-queues>=0.14.0` in `pyproject.toml`, implement slotted `SHMQueueBackend(BaseQueueBackend)` (`@queue_backend('vertebra_shm')`) + `SHMQueueBackendConfig` (`backend_name = 'vertebra_shm'`) in `shm_backend.py` and engine-neutral slotted `StoreQueueBackend(BaseQueueBackend)` (`@queue_backend('vertebra_store')`) + `StoreQueueBackendConfig` (`backend_name = 'vertebra_store'`) in `backend.py`, and wire `SQLSpecBackendConfig` (`queue_store_class` & `events_store_class`) plus `ContainerTaskDependencyProvider` per [`../spec.md`](../spec.md) [^pyproject-lq] [^lq-backend] [^queue-cap] [^lq-cfg] [^lq-init] [^fw-di].

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies].queues` and `[dependency-groups].test` (`litestar-queues>=0.12.0`).
- `src/py/vertebra/extensions/litestar/queues/backend.py` — existing `SHMQueueBackend` and stale `litestar_queues.records` import to re-point at `litestar_queues.models`.
- `src/py/vertebra/extensions/litestar/queues/config.py` — backend builders and `SQLSpecBackendConfig` wiring.
- `src/py/vertebra/extensions/litestar/queues/__init__.py` — public `litestar-queues` extension exports.
- `src/py/vertebra/db/driver/capabilities.py` — `AsyncQueueStore` and `AsyncNotificationSource` protocols.
- `src/py/vertebra/framework/di.py` — `Container.scope()` context manager.

## Contracts & Edge Cases

- Raise `litestar-queues>=0.14.0` in `pyproject.toml` (`queues` extra and `test` dependency group) and re-point the broken `from litestar_queues.records import QueuedTaskRecord` import to `from litestar_queues.models import QueuedTaskRecord`; in `0.14.0` `@queue_backend(name)` registers `BaseQueueBackend` subclasses while typed config objects carry `backend_name: ClassVar[str]`.
- Implement `__slots__`-equipped `SHMQueueBackend(BaseQueueBackend)` + `SHMQueueBackendConfig` over `vertebra._vertebra` SHM rings and futex/ulock wakeups (zero `asyncio.Queue` fake), and engine-neutral `__slots__`-equipped `StoreQueueBackend(BaseQueueBackend)` importing zero database engine packages and adapting any `AsyncQueueStore` + optional `AsyncNotificationSource` (`list_pending` / `list_completed_by_task` / `list_running_external` pagination, `(status='running', retry_count)` fencing, `touch_heartbeats`, `requeue_stale_running`, `cleanup_terminal` with `terminal_ttl`, `get_statistics`, uniqueness reservations, maintenance leases, and external Cloud Tasks / Cloud Run dispatch reservation hooks).
- Implement `ContainerTaskDependencyProvider(container)` matching `litestar_queues.config.TaskDependencyProvider` (`(task, record, context) -> AsyncContextManager[Mapping[str, Any]]`) entering `async with container.scope():` per task execution, and support settings-driven selection between `StoreQueueBackend`, `SHMQueueBackend`, and `SQLSpecBackendConfig` (`queue_store_class` / `events_store_class`).

## Test Matrix

- Parametrized `litestar-queues==0.14.0` `BaseQueueBackend` contract tests covering `SHMQueueBackend` and `StoreQueueBackend` (`enqueue`, fenced `claim_many`/`complete_task`/`fail_task`, `list_pending` pagination, `terminal_ttl`, futex wakeups, `AsyncPostgresQueueStore` / `AsyncOracleQueueStore` test harnesses, AST check forbidding vendor imports in `backend.py`), settings selection (`postgres`, `oracle`, `shm`, `sqlspec`), `queue_store_class`/`events_store_class` resolution, and per-task `ContainerTaskDependencyProvider` injection.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

## Acceptance Criteria

- [ ] `pyproject.toml` requires `litestar-queues>=0.14.0` and `SHMQueueBackend` removes broken `litestar_queues.records` imports while passing `mypy` and `BaseQueueBackend` contract checks.
- [ ] `StoreQueueBackend` passes `litestar-queues>=0.14.0` lifecycle, fencing, maintenance, and dispatch reservation tests with zero engine imports.
- [ ] `litestar-queues` resolves `StoreQueueBackend`, `SHMQueueBackend`, `queue_store_class`/`events_store_class`, and `ContainerTaskDependencyProvider` cleanly.
- [ ] Zero regressions in the fast verification gate.

[^pyproject-lq]: `pyproject.toml` — litestar-queues version floor in queues extra and test dependency group.
[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Existing litestar-queues backend module with broken litestar_queues.records import.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — AsyncQueueStore and AsyncNotificationSource protocols.
[^lq-cfg]: `src/py/vertebra/extensions/litestar/queues/config.py` — litestar-queues configuration builders.
[^lq-init]: `src/py/vertebra/extensions/litestar/queues/__init__.py` — Public litestar-queues extension exports.
[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container scoped dependency provider.
