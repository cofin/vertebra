---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch05-supervisor-exports-and-worker-lanes
title: "Chapter 05: Supervisor Package Exports, QueueService-Bound Worker Lanes, and Worker-Only Mode"
description: Lazily export the vertebra.supervisor launcher API, resolve and bind a QueueService for every WorkerRunner built by the CLI and colocated workers, keep vtb workers run alive until signalled, and implement vtb run --worker-only with a HealthProbeServer instead of the unread VERTEBRA_WORKER_ONLY flag.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-lifespan-fail-loud
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: supervisor-init-py
    resource: src/py/vertebra/supervisor/__init__.py
    title: Docstring-only supervisor package
  - id: server-init-py
    resource: src/py/vertebra/server/__init__.py
    title: lazy_exports pattern for vertebra.server
  - id: lanes-py
    resource: src/py/vertebra/server/lanes.py::WorkerRunner
    title: WorkerRunner with optional QueueService and process_once short-circuit
  - id: workers-cli-py
    resource: src/py/vertebra/cli/workers.py::execute_worker_cli
    title: vtb workers run entry that exits without running lanes
  - id: run-cli-py
    resource: src/py/vertebra/cli/run.py::run
    title: vtb run with unread --worker-only
  - id: platform-py
    resource: src/py/vertebra/server/platform.py::ZPagesRegistry
    title: z-page payload renderer and LameDuckController readiness
tags:
  - supervisor
  - workers
  - cli
  - health
---

# Chapter 05: Supervisor Package Exports, QueueService-Bound Worker Lanes, and Worker-Only Mode

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/py/vertebra/supervisor/__init__.py` contains only a docstring naming imports it never performs ([#83](https://github.com/cofin/vertebra/issues/83)) [^supervisor-init-py]. Worker lanes never run work: `WorkerRunner.process_once` returns `0` when `queue_service is None`, and every constructor call (`execute_worker_cli`, the colocated runner in `Worker.run`) omits it [^lanes-py]. `run_workers` calls `execute_worker_cli` without `run_until_stopped=True`, so `vtb workers run` enters and leaves the lifespan immediately [^workers-cli-py]. `vtb run --worker-only` sets `VERTEBRA_WORKER_ONLY`, read nowhere, and still launches `vertebra-server` ([#85](https://github.com/cofin/vertebra/issues/85)) [^run-cli-py]. `ZPagesRegistry.handle_zpage` renders `/healthz`, `/statusz`, `/varz`, `/rpcz`, and `/brokerz` payloads but nothing serves them outside the HTTP server [^platform-py].

### 1.2 Non-Goals

- Building `QueueService` instances or registering tasks (owned by `queues-tasks-and-channels`).
- A general-purpose HTTP server; `HealthProbeServer` answers `GET` probes only.

### 1.3 Upstream Baseline & Code Analysis

`vertebra.server/__init__.py` uses `lazy_exports(__name__, __all__, _EXPORT_MAP)` with a `TYPE_CHECKING` block [^server-init-py]. Importing `vertebra.supervisor._worker` from the package `__init__` would make `python -m vertebra.supervisor._worker` emit a `runpy` `RuntimeWarning`, so worker exports must be lazy. `WorkerRunner.run(install_signal_handlers=True)` installs SIGTERM/SIGINT handlers that enter lame-duck and stop.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R11** (`t01`): Lazy `vertebra.supervisor` exports.
- **R12** (`t02`): `WorkerRunner.resolve_queue_service`, bound runners in `execute_worker_cli` and colocated `Worker.run`, and `vtb workers run` lifetime.
- **R13** (`t03`): `HealthProbeServer` and `vtb run --worker-only`.

### 2.2 Concrete Type & Signature Contracts

```python
# src/py/vertebra/supervisor/__init__.py
__all__ = ("build_server_command", "find_repo_root", "find_server_binary", "run_supervisor", "worker_main")

# src/py/vertebra/server/lanes.py
QUEUE_SERVICE_STATE_KEY: Final = "vertebra_queue_service"
class WorkerRunner:
    @staticmethod
    def resolve_queue_service(target: Any) -> QueueService: ...

# src/py/vertebra/cli/workers.py
def execute_worker_cli(
    app_path: str, *, queues: str = "default", concurrency: str = "10", drain_timeout_s: float = 30.0,
    run_until_stopped: bool = False, oneshot_job: str | None = None, payload: Any = None,
    health_probe: tuple[str, int] | None = None,
) -> int: ...

# src/py/vertebra/server/platform.py
class HealthProbeServer:
    def __init__(self, zpages: ZPagesRegistry, *, host: str, port: int) -> None: ...
    @property
    def bound_port(self) -> int: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `from vertebra.supervisor import run_supervisor` | `lazy_exports` | `vertebra.supervisor._supervisor`, `vertebra.supervisor._worker.main` |
| `vtb workers run APP` | `execute_worker_cli(run_until_stopped=True)` | `WorkerRunner.resolve_queue_service(app)` → `WorkerRunner(lanes, queue_service=...)` → `run(install_signal_handlers=True)` |
| `Worker.run` colocated lanes | `WorkerRunner.resolve_queue_service(import_app(VERTEBRA_APP))` | `WorkerRunner.run` on the primary loop |
| `vtb run --worker-only` | `execute_worker_cli(..., health_probe=(host, port))` | `HealthProbeServer` + `WorkerRunner.run`; no `build_server_command` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| Target object is a `QueueService` | Used directly. |
| App with `state["vertebra_queue_service"]` (mapping or attribute) holding a `QueueService` | Used. |
| Neither, or the state value is not a `QueueService` | `ImproperConfigurationError` naming the target type and the state key. |
| `--worker-only` with `--with-workers` or `--oneshot-job` | `CLIError(..., exit_code=EXIT_CONFIG_ERROR)`. |
| Health port already bound | `OSError` propagates and the command exits non-zero. |
| SIGTERM during worker-only | Lame-duck → `/healthz` and `/readyz` return `503`, lanes drain, probe server stops, exit `0`. |
| Probe request other than `GET` | `405` with `allow: GET`. |
| Unknown probe path | `404` JSON from `ZPagesRegistry.handle_zpage`. |

### 2.5 Alternatives Considered

Eagerly importing `_worker` in the package `__init__` was rejected (runpy warning on every SHM worker spawn and an import-time dependency on `vertebra.framework`). Launching `vertebra-server` for worker-only mode was rejected because it embeds the HTTP app and Python worker threads the mode is meant to avoid. Silently running lanes without a queue service was rejected as a fail-loud violation.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch05]`; documented in `docs/reference/cli.md`, `docs/reference/build-and-wasm.md`, `docs/usage/server-and-workers.md` (`ch10:t01`) and `worker-lanes.md`, `worker-and-protocols.md` (`ch10:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R11 | [t01](tasks/t01-supervisor-package-exports.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_cli_commands.py` |
| R12 | [t02](tasks/t02-queue-service-resolution.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_worker_lanes.py`, `src/py/tests/unit/cli/test_run_with_workers.py` |
| R13 | [t03](tasks/t03-worker-only-health-probes.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_run_with_workers.py` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch05:t01 — Lazily export the vertebra.supervisor launcher API (tasks/t01-supervisor-package-exports.md)
- [ ] server-runtime-correctness:ch05:t02 — Bind a resolved QueueService to every worker-lane runner and keep vtb workers run alive (tasks/t02-queue-service-resolution.md)
- [ ] server-runtime-correctness:ch05:t03 — Implement vtb run --worker-only with HealthProbeServer (tasks/t03-worker-only-health-probes.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/cli src/py/tests/unit/extensions/test_worker_lanes.py src/py/tests/unit/server -q && uv run --no-sync ruff check src/py/vertebra && uv run --no-sync mypy src/py/vertebra` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t03` (`0/3`).
- **Downstream Dependents**: `ch06:t02` (`_supervisor.py`), `ch07:t02` (smoke test exercises `--worker-only`).

[^supervisor-init-py]: `src/py/vertebra/supervisor/__init__.py` — Docstring-only supervisor package.
[^server-init-py]: `src/py/vertebra/server/__init__.py` — `lazy_exports` pattern for `vertebra.server`.
[^lanes-py]: `src/py/vertebra/server/lanes.py::WorkerRunner` — `WorkerRunner` with optional `QueueService` and `process_once` short-circuit.
[^workers-cli-py]: `src/py/vertebra/cli/workers.py::execute_worker_cli` — `vtb workers run` entry that exits without running lanes.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — `vtb run` with unread `--worker-only`.
[^platform-py]: `src/py/vertebra/server/platform.py::ZPagesRegistry` — z-page payload renderer and `LameDuckController` readiness.
