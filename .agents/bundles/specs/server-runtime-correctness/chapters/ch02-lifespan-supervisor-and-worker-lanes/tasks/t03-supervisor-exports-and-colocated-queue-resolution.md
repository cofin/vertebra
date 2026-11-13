---
type: Task
id: "server-runtime-correctness:ch02:t03"
flow_id: server-runtime-correctness
chapter_id: ch02-lifespan-supervisor-and-worker-lanes
title: Lazily Export vertebra.supervisor and Bind QueueService on Worker Lanes
description: Implement PEP 562 lazy exports on vertebra.supervisor and add WorkerRunner.resolve_queue_service wired into colocated worker startup.
status: stable
state: completed
commit: d2da8fa0
last_verified_commit: d2da8fa0
last_verified_at: "2026-11-13T04:56:47Z"
verification_evidence: "ruff + mypy (44 files) + fresh supervisor import + pytest (183 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch02:t01"
files:
  - src/py/vertebra/supervisor/__init__.py
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/server/worker.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
  - src/py/tests/unit/extensions/test_worker_lanes.py
read_set:
  - src/py/vertebra/supervisor/__init__.py
  - src/py/vertebra/supervisor/_supervisor.py
  - src/py/vertebra/server/__init__.py
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/server/worker.py
  - src/py/tests/unit/extensions/test_worker_lanes.py
estimated_loc: 175
tags:
  - supervisor
  - workers
  - queues
  - python
sources:
  - id: supervisor-init-py
    resource: src/py/vertebra/supervisor/__init__.py
    title: Empty supervisor package __init__
  - id: lanes-py
    resource: "src/py/vertebra/server/lanes.py::WorkerRunner"
    title: Multi-lane WorkerRunner
---

# Task `t03`: Lazily Export vertebra.supervisor and Bind QueueService on Worker Lanes

## Objective & Scope Boundary

Close [#83](https://github.com/cofin/vertebra/issues/83) and the colocated worker binding half of [#85](https://github.com/cofin/vertebra/issues/85) per [`../spec.md` §2](../spec.md) [^supervisor-init-py] [^lanes-py].

## Bounded Read Manifest

- `src/py/vertebra/server/__init__.py` — canonical `lazy_exports` pattern.
- `src/py/vertebra/supervisor/_supervisor.py` & `src/py/vertebra/server/lanes.py` — launcher symbols and `WorkerRunner`.

## Contracts & Edge Cases

- `vertebra.supervisor` lazily exports `build_server_command`, `find_repo_root`, `find_server_binary`, `run_supervisor`, and `worker_main` (`_worker.main`) with `TYPE_CHECKING` imports so `python -m vertebra.supervisor._worker` emits zero `runpy` warnings.
- `WorkerRunner.resolve_queue_service(target)` accepts a `QueueService` directly or reads `target.state['vertebra_queue_service']` (mapping or attribute), raising `ImproperConfigurationError` when missing or wrong-typed.
- `Worker.run` binds the resolved `QueueService` when starting colocated `--with-workers` lanes.

## Test Matrix

- Unit tests in `test_cli_commands.py` (all 5 exports, `python -m vertebra.supervisor._worker --help` warning-free) and `test_worker_lanes.py` (direct `QueueService`, `app.state` lookup, fail-loud `ImproperConfigurationError`).

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/supervisor src/py/vertebra/server && uv run --no-sync mypy src/py/vertebra/supervisor src/py/vertebra/server && uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/extensions/test_worker_lanes.py -q
```

## Acceptance Criteria

- [x] All 5 `vertebra.supervisor` exports resolve lazily and type-check cleanly.
- [x] `WorkerRunner.resolve_queue_service` fails loud when no `QueueService` is configured.

[^supervisor-init-py]: `src/py/vertebra/supervisor/__init__.py` — Empty supervisor package __init__.
[^lanes-py]: `src/py/vertebra/server/lanes.py::WorkerRunner` — Multi-lane WorkerRunner.
