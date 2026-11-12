---
type: Task
id: "server-runtime-correctness:ch05:t02"
chapter_id: ch05-supervisor-exports-and-worker-lanes
title: Bind a resolved QueueService to every worker-lane runner and keep vtb workers run alive
description: Add WorkerRunner.resolve_queue_service (target is a QueueService or exposes one under app state key vertebra_queue_service, else ImproperConfigurationError), bind it in execute_worker_cli and the colocated runner in Worker.run, and pass run_until_stopped=True from vtb workers run.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch05:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch05-supervisor-exports-and-worker-lanes/spec.md
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/cli/workers.py
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/tests/unit/extensions/test_worker_lanes.py
files:
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/cli/workers.py
  - src/py/vertebra/server/worker.py
tests:
  - src/py/tests/unit/extensions/test_worker_lanes.py
  - src/py/tests/unit/cli/test_run_with_workers.py
estimated_loc: 150
plan_revision: 1
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: lanes-py
    resource: src/py/vertebra/server/lanes.py::WorkerRunner
    title: WorkerRunner constructor, process_once, run, resolve_job
  - id: workers-cli-py
    resource: src/py/vertebra/cli/workers.py::execute_worker_cli
    title: vtb workers run implementation
  - id: queue-service-py
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: First-party QueueService
  - id: worker-py
    resource: src/py/vertebra/server/worker.py::Worker
    title: Colocated WorkerRunner construction in Worker.run
tags:
  - workers
  - queues
  - fail-loud
---

# Task t02: Bind a Resolved QueueService to Every Worker-Lane Runner and Keep vtb workers run Alive

## Objective & Scope Boundary

Add `WorkerRunner.resolve_queue_service` and `QUEUE_SERVICE_STATE_KEY` to `src/py/vertebra/server/lanes.py`; make `execute_worker_cli` (non-oneshot) build `WorkerRunner(lanes, queue_service=WorkerRunner.resolve_queue_service(app), lame_duck=...)`; make `run_workers` pass `run_until_stopped=True`; and make the colocated runner in `Worker.run` resolve the service from `import_app(os.environ["VERTEBRA_APP"])`. Out of scope: worker-only mode (`t03`) and how applications construct `QueueService`.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/server/lanes.py::{WorkerRunner.__init__,WorkerRunner.process_once,WorkerRunner.run,WorkerRunner.resolve_job,parse_lanes}` [^lanes-py].
- `src/py/vertebra/cli/workers.py::{execute_worker_cli,run_workers}` [^workers-cli-py].
- `src/py/vertebra/server/worker.py::Worker.run` — `has_colocated_workers` block constructing `WorkerRunner(lanes, lame_duck=self._lame_duck)` [^worker-py].
- `src/py/vertebra/framework/queues/base.py::QueueService` [^queue-service-py].
- `src/py/tests/unit/extensions/test_worker_lanes.py::test_vtb_workers_run_and_vtb_run_oneshot_cli`.

## Symbol & Signature Contracts

- `src/py/vertebra/server/lanes.py`:
  - `QUEUE_SERVICE_STATE_KEY: Final = "vertebra_queue_service"` in `__all__`.
  - `@staticmethod def resolve_queue_service(target: Any) -> QueueService`: returns `target` when `isinstance(target, QueueService)`; else reads `state = getattr(target, "state", None)`, looks up `state.get(QUEUE_SERVICE_STATE_KEY)` when `state` has `get`, otherwise `getattr(state, QUEUE_SERVICE_STATE_KEY, None)`; returns it when it is a `QueueService`; else raises `ImproperConfigurationError(f"worker lanes need a QueueService: pass one as the target or set app.state[{QUEUE_SERVICE_STATE_KEY!r}] (target type {type(target).__name__})")`.
- `src/py/vertebra/cli/workers.py::execute_worker_cli`: for the lanes path, `queue_service = WorkerRunner.resolve_queue_service(app)` before entering the lifespan; `run_workers` passes `run_until_stopped=True`.
- `src/py/vertebra/server/worker.py::Worker.run`: colocated branch builds `WorkerRunner(lanes, queue_service=WorkerRunner.resolve_queue_service(import_app(os.environ["VERTEBRA_APP"])), lame_duck=self._lame_duck)`; a missing `VERTEBRA_APP` raises `ImproperConfigurationError("VERTEBRA_WITH_WORKERS=1 requires VERTEBRA_APP")`.
- Imports at module top: `ImproperConfigurationError` from `vertebra._typing`, `QueueService` from `vertebra.framework.queues.base` (already imported in `lanes.py` under `TYPE_CHECKING`; move to runtime import because `isinstance` needs it), `import_app` from `vertebra.utils.module_loader` in `worker.py`.

## Edge Cases & Error Semantics

- Litestar apps expose `State` with `get`; Vertebra `App.state` and plain dicts also expose `get`; Starlette `State` exposes attributes — all supported.
- A `QueueService` subclass instance is accepted.
- `--oneshot` keeps working without a queue service.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_worker_lanes.py` (resolution), `src/py/tests/unit/cli/test_run_with_workers.py` (CLI lifetime, colocated runner); dependency category `unit`. Use `@pytest.mark.parametrize` for target shapes.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Direct target | `svc = QueueService(QueueConfig(backend_url="memory://"))`; `resolve_queue_service(svc)` | Returns `svc` |
| Mapping state | `SimpleNamespace(state={"vertebra_queue_service": svc})` | Returns `svc` |
| Attribute state | `SimpleNamespace(state=SimpleNamespace(vertebra_queue_service=svc))` | Returns `svc` |
| Missing | `SimpleNamespace(state={})` | `ImproperConfigurationError` mentioning `"vertebra_queue_service"` and `"SimpleNamespace"` |
| Wrong type | `SimpleNamespace(state={"vertebra_queue_service": object()})` | `ImproperConfigurationError` |
| Workers run stays alive | Monkeypatch `WorkerRunner.run` to record kwargs; `CliRunner().invoke(cli, ["workers", "run", "tests_app:app"])` with a resolvable service | `run` called once with `install_signal_handlers=True` |
| Workers run missing service | Same with an app lacking the state key | Non-zero exit; output contains `"vertebra_queue_service"` |
| Colocated binding | Existing `test_worker_starts_and_drains_colocated_worker_runner` with `VERTEBRA_APP` pointing at an app exposing the service | Runner's `queue_service is svc` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_worker_lanes.py src/py/tests/unit/cli/test_run_with_workers.py -q`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/extensions/test_worker_lanes.py src/py/tests/unit/cli/test_run_with_workers.py src/py/tests/unit/server/test_runner.py -q
  uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/cli
  uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/cli
  ```
- Planned local commit: `fix(workers): bind QueueService to worker lanes and keep workers run alive`

## Acceptance Criteria

- [ ] `WorkerRunner.resolve_queue_service` resolves direct, mapping-state, and attribute-state targets and fails loud otherwise (R12).
- [ ] `vtb workers run` runs lanes until signalled with a bound service (R12).
- [ ] Colocated `--with-workers` runners bind the same service (R12).

## Notes & Discoveries

[^lanes-py]: `src/py/vertebra/server/lanes.py::WorkerRunner` — `WorkerRunner` constructor, `process_once`, `run`, `resolve_job`.
[^workers-cli-py]: `src/py/vertebra/cli/workers.py::execute_worker_cli` — `vtb workers run` implementation.
[^queue-service-py]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party `QueueService`.
[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Colocated `WorkerRunner` construction in `Worker.run`.
