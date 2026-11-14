---
type: Task
id: queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t02
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: Wire Starlette, FastAPI & Flask Queue Extensions onto QueueService & Container DI
description: Replace standalone deque stubs in Starlette/FastAPI/Flask queue modules with unified QueueService, QueueBackendSettings, and Container DI.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t01
files:
- src/py/vertebra/extensions/starlette/queues.py
- src/py/vertebra/extensions/flask/queues.py
- src/py/vertebra/extensions/fastapi/plugin.py
tests:
- src/py/tests/unit/extensions/test_starlette_plugin.py
- src/py/tests/unit/extensions/test_fastapi_plugin.py
- src/py/tests/unit/extensions/test_flask_plugin.py
read_set:
- src/py/vertebra/extensions/starlette/queues.py
- src/py/vertebra/extensions/flask/queues.py
- src/py/vertebra/extensions/fastapi/plugin.py
- src/py/vertebra/framework/queues/worker.py
- src/py/vertebra/server/lanes.py
estimated_loc: 195
tags:
- queues
- starlette
- fastapi
- flask
sources:
- id: starlette-q
  resource: src/py/vertebra/extensions/starlette/queues.py
  title: Starlette queue integration
- id: flask-q
  resource: src/py/vertebra/extensions/flask/queues.py
  title: Flask synchronous queue integration
- id: fastapi-plug
  resource: src/py/vertebra/extensions/fastapi/plugin.py
  title: FastAPI VertebraPlugin
---

# Task `t02`: Wire Starlette, FastAPI & Flask Queue Extensions onto QueueService & Container DI

## Objective & Scope Boundary

Wire Starlette, FastAPI, and Flask queue integrations onto unified `QueueService`, `QueueBackendSettings`, and `Container` DI per [`../spec.md`](../spec.md) [^starlette-q] [^flask-q] [^fastapi-plug].

## Bounded Read Manifest

- `src/py/vertebra/extensions/starlette/queues.py` — ASGI queue lifespan and DI wiring.
- `src/py/vertebra/extensions/flask/queues.py` — WSGI synchronous `SyncQueueStore` enqueue and worker dispatch.
- `src/py/vertebra/extensions/fastapi/plugin.py` — FastAPI `VertebraPlugin` queue registration and lifespan.
- `src/py/vertebra/framework/queues/worker.py` — `WorkerRunner` execution and drain contracts.
- `src/py/vertebra/server/lanes.py` — `MultiLaneWorkerRunner` lane supervision.

## Contracts & Edge Cases

- Delegate Starlette/FastAPI async `enqueue` and worker lifespans to `QueueService` + `WorkerRunner`.
- Delegate Flask synchronous `enqueue` directly to `SyncQueueStore` / `QueueService.enqueue_sync` without spinning transient event loops.

## Test Matrix

- Unit tests verifying Starlette/FastAPI lifespan startup/shutdown and Flask synchronous enqueue -> worker DI execution.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py src/py/vertebra/extensions/fastapi/plugin.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py src/py/vertebra/extensions/fastapi/plugin.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py
```

## Acceptance Criteria

- [ ] Starlette, FastAPI, and Flask queue extensions execute through `QueueService` and `Container` DI without standalone in-memory deque fakes.
- [ ] Zero regressions in the fast verification gate.

[^starlette-q]: `src/py/vertebra/extensions/starlette/queues.py` — Starlette queue integration.
[^flask-q]: `src/py/vertebra/extensions/flask/queues.py` — Flask synchronous queue integration.
[^fastapi-plug]: `src/py/vertebra/extensions/fastapi/plugin.py` — FastAPI VertebraPlugin.
