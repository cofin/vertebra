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
plan_commit: 1b191388
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
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/adapters/postgres/queue.py
- src/py/vertebra/db/adapters/oracle/queue.py
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
  title: litestar-queues version floor in litestar extra
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
---

# Task `t03`: Bump litestar-queues>=0.14.0 and Implement SHMQueueBackend, StoreQueueBackend & DI Provider

## Objective & Scope Boundary

Bump `litestar-queues>=0.14.0` in `pyproject.toml` and implement slotted `SHMQueueBackend(BaseQueueBackend)` + `SHMQueueBackendConfig` (`@queue_backend('vertebra_shm')`) in `shm_backend.py` per [`../spec.md`](../spec.md) [^pyproject-lq] [^lq-backend].

Implement engine-neutral slotted `StoreQueueBackend(BaseQueueBackend)` + `StoreQueueBackendConfig` (`@queue_backend('vertebra_store')`) in `extensions/litestar/queues/backend.py` per [`../spec.md`](../spec.md) [^lq-backend] [^queue-cap].

Wire settings-driven `litestar-queues` selection (`StoreQueueBackendConfig`, `SHMQueueBackendConfig`, and `SQLSpecBackendConfig` honoring `queue_store_class` & `events_store_class`) plus `ContainerTaskDependencyProvider` per [`../spec.md`](../spec.md) [^lq-cfg] [^lq-init] [^fw-di].

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies].litestar`.
- `src/py/vertebra/extensions/litestar/queues/backend.py` — existing `SHMQueueBackend` and broken `litestar_queues.records` import.

- `src/py/vertebra/extensions/litestar/queues/backend.py` — remove broken `from litestar_queues.records import QueuedTaskRecord` import and engine-specific code.
- `src/py/vertebra/db/driver/capabilities.py` — `AsyncQueueStore` and `AsyncNotificationSource`.

- `src/py/vertebra/extensions/litestar/queues/{config,__init__}.py` — backend builders and DI wiring.
- `src/py/vertebra/framework/di.py` — `Container.scope()` context manager.

## Contracts & Edge Cases

- Raise `litestar-queues>=0.14.0` in `pyproject.toml`.
- Implement `__slots__`-equipped `SHMQueueBackend(BaseQueueBackend)` + `SHMQueueBackendConfig` over `vertebra._vertebra` SHM rings and futex/ulock wakeups (zero `asyncio.Queue` fake), supporting `list_tasks` pagination, `(status='running', retry_count)` fencing, `terminal_ttl` cleanup, uniqueness reservations, and maintenance leases.

- `StoreQueueBackend(BaseQueueBackend)` declares `__slots__`, imports zero database engine packages, and adapts any `AsyncQueueStore` (`AsyncPostgresQueueStore`, `AsyncOracleQueueStore`, `AsyncSpannerQueueStore`) + optional `AsyncNotificationSource` to `litestar-queues>=0.14.0`.
- Implement claim fencing (`retry_count == expected_retry_count`), `touch_heartbeats`, `requeue_stale_running`, `cleanup_terminal`, `list_tasks`, `get_statistics`, maintenance leases, uniqueness reservations, and external Cloud Tasks / Cloud Run dispatch reservation hooks.

- Implement `ContainerTaskDependencyProvider(container)` matching `litestar_queues.config.TaskDependencyProvider` (`(task, record, context) -> AsyncContextManager[Mapping[str, Any]]`) entering `async with container.scope():` per task execution.
- Support settings-driven selection between `StoreQueueBackend` (`postgres` / `oracle`), `SHMQueueBackend` (`shm`), and `SQLSpecBackendConfig` (attaching dialect `queue_store_class` and `events_store_class` onto Vertebra SQLSpec configs).

## Test Matrix

- Parametrized `litestar-queues==0.14.0` `BaseQueueBackend` contract tests covering `enqueue`, fenced `claim_many`/`complete_task`/`fail_task`, `list_tasks` pagination, `terminal_ttl`, and futex wakeups.

- End-to-end `litestar-queues` worker execution tests over `StoreQueueBackend` wrapping `AsyncPostgresQueueStore` and `AsyncOracleQueueStore` test harnesses, plus AST check confirming zero vendor imports in `backend.py`.

- Parametrized tests verifying settings selection (`postgres`, `oracle`, `shm`, `sqlspec`), `queue_store_class` / `events_store_class` resolution in `litestar-queues==0.14.0`, and per-task `ContainerTaskDependencyProvider` injection.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

## Acceptance Criteria

- [ ] `pyproject.toml` requires `litestar-queues>=0.14.0` and `SHMQueueBackend` passes strict `mypy` (zero `ignore_missing_imports` masking) and `BaseQueueBackend` contract checks.
- [ ] `StoreQueueBackend` passes `litestar-queues>=0.14.0` lifecycle, fencing, maintenance, and dispatch reservation tests with zero engine imports.
- [ ] `litestar-queues` resolves `StoreQueueBackend`, `SHMQueueBackend`, `queue_store_class`/`events_store_class`, and `ContainerTaskDependencyProvider` cleanly.
- [ ] Zero regressions in the fast verification gate.

[^pyproject-lq]: `pyproject.toml` — litestar-queues version floor in litestar extra.
[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Existing litestar-queues backend module with broken litestar_queues.records import.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — AsyncQueueStore and AsyncNotificationSource protocols.
[^lq-cfg]: `src/py/vertebra/extensions/litestar/queues/config.py` — litestar-queues configuration builders.
[^lq-init]: `src/py/vertebra/extensions/litestar/queues/__init__.py` — Public litestar-queues extension exports.
[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container scoped dependency provider.
