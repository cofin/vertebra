---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch02-lifespan-supervisor-and-worker-lanes
title: "Chapter 02: Fail-Loud Lifespan, Supervisor Exports, and Worker Lanes"
description: "Abort worker startup on lifespan or RSGI init failures with exit code 78, export vertebra.supervisor symbols, bind QueueService on worker lanes, and serve health probes in --worker-only mode."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-python-stream-submission-and-flush
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - lifespan
  - supervisor
  - workers
  - cli
  - server
sources:
  - id: worker-py
    resource: "src/py/vertebra/server/worker.py::Worker"
    title: Worker lifespan ExitStack and RSGI mount initialization
  - id: supervisor-rs
    resource: "src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor"
    title: SHM worker polling and respawn loop
  - id: supervisor-init-py
    resource: src/py/vertebra/supervisor/__init__.py
    title: Docstring-only supervisor package
  - id: lanes-py
    resource: "src/py/vertebra/server/lanes.py::WorkerRunner"
    title: Multi-lane worker runner
  - id: run-cli-py
    resource: "src/py/vertebra/cli/run.py::run"
    title: vtb run CLI entry point with unread --worker-only
---

# Chapter 02: Fail-Loud Lifespan, Supervisor Exports, and Worker Lanes

<!-- truth: start -->
- ASGI lifespan failures, `30s` startup timeouts, and RSGI `__rsgi_init__` exceptions raise `WorkerStartupError(prefix, phase)`, unwind started mounts, exit SHM workers with `EXIT_STARTUP_FAILURE = 78`, and stop `Supervisor::poll_workers` without respawn ([#84](https://github.com/cofin/vertebra/issues/84)) [^worker-py] [^supervisor-rs].
- `vertebra.supervisor` lazily exports `build_server_command`, `find_repo_root`, `find_server_binary`, `run_supervisor`, and `worker_main` ([#83](https://github.com/cofin/vertebra/issues/83)) [^supervisor-init-py].
- `WorkerRunner.resolve_queue_service` binds a `QueueService` across `vtb workers run` (`run_until_stopped=True`) and `--with-workers`, while `vtb run --worker-only` serves `ZPagesRegistry` probes through `HealthProbeServer` without launching `vertebra-server` ([#85](https://github.com/cofin/vertebra/issues/85)) [^lanes-py] [^run-cli-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`Worker.run` currently catches and logs lifespan and `__rsgi_init__` exceptions, serving traffic without initialized dependencies, while SHM `Supervisor::poll_workers` respawns exiting startup workers ([#84](https://github.com/cofin/vertebra/issues/84)) [^worker-py] [^supervisor-rs]. `vertebra.supervisor/__init__.py` exports nothing ([#83](https://github.com/cofin/vertebra/issues/83)) [^supervisor-init-py], `vtb workers run` exits immediately without a `QueueService`, and `vtb run --worker-only` sets unread `VERTEBRA_WORKER_ONLY` ([#85](https://github.com/cofin/vertebra/issues/85)) [^lanes-py] [^run-cli-py].

## 2. Requirements, Concrete Contracts & Architecture

- **`R4a` (`t01`)**: Define `WorkerStartupError(VertebraError, RuntimeError)` in `worker.py`, cancel timed-out lifespan tasks in `AsgiLifespan.startup`, unwind `ExitStack` on multi-mount failure, and exit `supervisor._worker.main` with `EXIT_STARTUP_FAILURE = 78`.
- **`R4b` (`t02`)**: Record `last_exit_code(&self) -> Option<i32>` on `WorkerLifecycle` (`EXIT_STARTUP_FAILURE: i32 = 78`), mark `78` workers `Dead` in `Supervisor::poll_workers`, store `startup_failed = true`, cancel server shutdown, and return `Err` from `run_server`.
- **`R5a` (`t03`)**: Export `("build_server_command", "find_repo_root", "find_server_binary", "run_supervisor", "worker_main")` lazily in `supervisor/__init__.py` (`PY-06`), add `QUEUE_SERVICE_STATE_KEY = "vertebra_queue_service"` + `WorkerRunner.resolve_queue_service(target)`, and bind colocated `--with-workers` lanes in `Worker.run`.
- **`R5b` (`t04`)**: Add `HealthProbeServer(zpages, *, host, port)` (`bound_port`, `start`, `stop`) in `server/platform.py`, pass `run_until_stopped=True` in `vtb workers run`, and route `vtb run --worker-only` through `execute_worker_cli(..., health_probe=(host, port))` rejecting `--with-workers` / `--oneshot-job` with `CLIError` (`exit_code=2`).

## 3. Documentation & OKF Knowledge Plan

Runtime startup, supervisor exports, and worker-only CLI semantics are documented in `ch07` (`docs/usage/server-and-workers.md`, `docs/reference/cli.md`, `worker-and-protocols.md`, `server-runtime-limitations.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R4a` | `t01` | `server/worker.py`, `server/_asgi/_runner.py`, `supervisor/_worker.py` | `pytest test_runner.py test_asgi.py test_cli_commands.py` |
| `R4b` | `t02` | `vertebra-http/src/workers/{factory,supervisor}.rs`, `main.rs` | `cargo test -p vertebra-http --lib workers` + `test_vertebra_server.py` |
| `R5a` | `t03` | `supervisor/__init__.py`, `server/lanes.py`, `server/worker.py` | `pytest test_cli_commands.py test_worker_lanes.py` |
| `R5b` | `t04` | `server/platform.py`, `cli/workers.py`, `cli/run.py` | `pytest test_run_with_workers.py` |

## 5. Implementation Plan

- [x] [d2da8fa0] [t01-worker-startup-error-and-shm-exit-code.md](tasks/t01-worker-startup-error-and-shm-exit-code.md) — Raise `WorkerStartupError` on lifespan/RSGI failures and exit SHM workers with code `78`
- [x] [d2da8fa0] [t02-rust-supervisor-startup-failure.md](tasks/t02-rust-supervisor-startup-failure.md) — Halt Rust SHM supervisor respawn on exit code `78` and terminate `vertebra-server` non-zero
- [x] [d2da8fa0] [t03-supervisor-exports-and-colocated-queue-resolution.md](tasks/t03-supervisor-exports-and-colocated-queue-resolution.md) — Export `vertebra.supervisor` lazily and bind `WorkerRunner.resolve_queue_service`
- [x] [d2da8fa0] [t04-worker-cli-lifetime-and-health-probes.md](tasks/t04-worker-cli-lifetime-and-health-probes.md) — Keep `vtb workers run` alive until signalled and implement `vtb run --worker-only` with `HealthProbeServer`

## 6. Current State & Remaining Plan Snapshot

- **Implemented (`4/4` tasks at `d2da8fa0`)**: `WorkerStartupError`, SHM exit code `78` supervisor halt, lazy `vertebra.supervisor` exports, `WorkerRunner.resolve_queue_service`, `vtb workers run` lifetime, and `vtb run --worker-only` `HealthProbeServer` are verified (`14` Rust `workers` tests, `183` Python unit/integration tests, `clippy`, `ruff`, and `mypy` clean).
- **Fast Chapter Gate (`< 35s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib workers && uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/supervisor src/py/vertebra/cli && uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/supervisor src/py/vertebra/cli && uv run --no-sync pytest src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_asgi.py src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/cli/test_run_with_workers.py src/py/tests/unit/extensions/test_worker_lanes.py -q`.

[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Worker lifespan ExitStack and RSGI mount initialization.
[^supervisor-rs]: `src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor` — SHM worker polling and respawn loop.
[^supervisor-init-py]: `src/py/vertebra/supervisor/__init__.py` — Docstring-only supervisor package.
[^lanes-py]: `src/py/vertebra/server/lanes.py::WorkerRunner` — Multi-lane worker runner.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — vtb run CLI entry point with unread --worker-only.
