---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch04-lifespan-fail-loud
title: "Chapter 04: Fail-Loud Lifespan Startup, SHM Startup Exit Code, and Supervisor Stop"
description: Raise WorkerStartupError from Worker.run for ASGI lifespan startup failures, startup timeouts, and RSGI __rsgi_init__ failures, exit SHM workers with code 78 on startup failure, and make the Rust supervisor stop respawning and shut vertebra-server down with a non-zero exit.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch03-asgi-rsgi-incremental-flush
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
  - id: worker-py
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker.run lifespan ExitStack and _instantiate_mount_runners RSGI init
  - id: lifespan-py
    resource: src/py/vertebra/server/_asgi/_runner.py::AsgiLifespan
    title: ASGI lifespan state machine and startup timeout
  - id: supervisor-worker-py
    resource: src/py/vertebra/supervisor/_worker.py::main
    title: SHM worker process entry point
  - id: rust-supervisor-rs
    resource: src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor
    title: Worker exit polling, crash backoff, and circuit breaker
  - id: main-rs
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: Server shutdown token, embedded worker failure flag, and SHM supervisor spawn
tags:
  - lifespan
  - fail-loud
  - supervisor
---

# Chapter 04: Fail-Loud Lifespan Startup, SHM Startup Exit Code, and Supervisor Stop

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`Worker.run` enters `enter_litestar_server_lifespan(app_obj)` and `AsgiLifespan(app_obj, loop=...)` for every ASGI mount inside `try: ... except Exception: logger.exception("ASGI lifespan startup failed for mount %r", _prefix)`, then serves requests without the state startup was meant to create [^worker-py]. `_instantiate_mount_runners` logs and ignores `__rsgi_init__` failures. `AsgiLifespan.startup` raises `TimeoutError` from `asyncio.wait_for` without cancelling the lifespan task [^lifespan-py]. In SHM topology a worker that dies at startup is respawned by `Supervisor::poll_workers` with exponential backoff until the circuit breaker trips, while the server keeps accepting traffic with dead workers [^rust-supervisor-rs]. This chapter closes [#84](https://github.com/cofin/vertebra/issues/84).

### 1.2 Non-Goals

- Lifespan shutdown failures: `lifespan.shutdown.failed` stays logged at error level during teardown.
- Accept-loop readiness gating (requests queue until the worker loop starts).

### 1.3 Upstream Baseline & Code Analysis

In embedded topology, an exception escaping `run_worker` makes the worker thread in `spawn_python_workers` set `worker_failed`, cancel the shutdown token, and `run_server` returns `Err("embedded Python worker thread terminated unexpectedly")`, so the binary exits non-zero once the exception propagates [^main-rs]. `WorkerLifecycle::check_alive` calls `child.try_wait()` and discards the `ExitStatus`. `Supervisor::spawn` receives only the factory, dispatcher, and shutdown token.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R9** (`t01`): `WorkerStartupError` raised from `Worker.run` and `_instantiate_mount_runners`; lifespan timeout cancels the task.
- **R10a** (`t02`): `vertebra.supervisor._worker.main` exits with `EXIT_STARTUP_FAILURE = 78` on `WorkerStartupError`.
- **R10b** (`t03`): Rust supervisor treats exit code `78` as fatal startup failure and stops the server non-zero.

### 2.2 Concrete Type & Signature Contracts

```python
# src/py/vertebra/server/worker.py
class WorkerStartupError(VertebraError, RuntimeError):
    """Raised when a mount's lifespan startup or RSGI init fails before the worker serves requests."""

    def __init__(self, prefix: str, phase: Literal["lifespan", "rsgi_init"]) -> None: ...
    prefix: str
    phase: Literal["lifespan", "rsgi_init"]

# src/py/vertebra/supervisor/_worker.py
EXIT_STARTUP_FAILURE: Final = 78
```

```rust
// src/rs/vertebra-http/src/workers/factory.rs
pub const EXIT_STARTUP_FAILURE: i32 = 78;
impl WorkerLifecycle { pub fn last_exit_code(&self) -> Option<i32>; }
// src/rs/vertebra-http/src/workers/supervisor.rs
impl Supervisor {
    pub fn spawn(factory: Arc<Mutex<RingFactory>>, shm: Arc<ShmDispatcher>, shutdown: CancellationToken,
        startup_failed: Arc<AtomicBool>) -> tokio::task::JoinHandle<()>;
    pub async fn poll_workers(factory: &Arc<Mutex<RingFactory>>, shm: &Arc<ShmDispatcher>,
        shutdown: &CancellationToken, startup_failed: &AtomicBool);
}
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `run_worker` → `Worker.run` | lifespan `ExitStack.enter_context` | `enter_litestar_server_lifespan`, `AsgiLifespan.__enter__` → `WorkerStartupError(prefix, "lifespan") from exc` |
| `Worker.run` → `_instantiate_mount_runners` | `__rsgi_init__(primary_loop)` | `WorkerStartupError(prefix, "rsgi_init") from exc` |
| Embedded `spawn_python_workers` thread | `run_worker` exception | existing `worker_failed` → non-zero exit |
| SHM `python -m vertebra.supervisor._worker` | `main` handler | `sys.exit(78)` → `WorkerLifecycle::check_alive` → `Supervisor::poll_workers` → `startup_failed.store(true)` + `shutdown.cancel()` → `run_server` returns `Err` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| `lifespan.startup.failed` with message `"db offline"` | `WorkerStartupError` whose `__cause__` is `RuntimeError("db offline")`; message names the prefix. |
| Lifespan raises `ValueError` after receiving startup | Same, cause `ValueError`. |
| Lifespan exceeds `30s` | Lifespan task cancelled and awaited; `TimeoutError` becomes the cause. |
| App ignores lifespan (raises before receiving startup) | Treated as unsupported (unchanged); no error. |
| Second of two mounts fails | First mount's lifespan shutdown runs (ExitStack unwind) before the error propagates. |
| `__rsgi_init__` raises | `WorkerStartupError(prefix, "rsgi_init")`. |
| SHM worker exits `1` (runtime crash) | Backoff respawn unchanged. |
| SHM worker exits `78` | No respawn; worker marked `Dead`; server cancels and exits non-zero. |

### 2.5 Alternatives Considered

Letting the raw exception escape was rejected because the SHM supervisor cannot tell a misconfigured startup from a transient crash; a typed error with a dedicated exit code (`EX_CONFIG`) gives both a clear Python traceback and a stop signal. Treating the circuit breaker as the stop signal was rejected: it waits through `10` crashes and leaves the server accepting traffic.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch04]`; documented in `docs/usage/server-and-workers.md` (`ch10:t01`), `worker-and-protocols.md` (`ch10:t04`), and `server-and-dispatch.md` (`ch10:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R9 | [t01](tasks/t01-worker-startup-error-propagation.md) | `behavior_tdd` | `src/py/tests/unit/server/test_runner.py`, `src/py/tests/unit/server/test_asgi.py` |
| R10a | [t02](tasks/t02-shm-worker-startup-exit-code.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_cli_commands.py`, `src/py/tests/integration/server/test_vertebra_server.py` |
| R10b | [t03](tasks/t03-rust-supervisor-startup-failure.md) | `behavior_tdd` | `src/rs/vertebra-http/src/workers/supervisor.rs` `mod tests` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch04:t01 — Raise WorkerStartupError for lifespan and RSGI init failures and cancel timed-out lifespans (tasks/t01-worker-startup-error-propagation.md)
- [ ] server-runtime-correctness:ch04:t02 — Exit SHM workers with code 78 on startup failure and verify embedded non-zero exit (tasks/t02-shm-worker-startup-exit-code.md)
- [ ] server-runtime-correctness:ch04:t03 — Stop the Rust supervisor and server on worker startup failure (tasks/t03-rust-supervisor-startup-failure.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib workers && make dev && uv run --no-sync pytest src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_asgi.py src/py/tests/unit/cli/test_cli_commands.py src/py/tests/integration/server -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t03` (`0/3`).
- **Downstream Dependents**: `ch10:t01`, `ch10:t03`, `ch10:t04`.

[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — `Worker.run` lifespan `ExitStack` and `_instantiate_mount_runners` RSGI init.
[^lifespan-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiLifespan` — ASGI lifespan state machine and startup timeout.
[^supervisor-worker-py]: `src/py/vertebra/supervisor/_worker.py::main` — SHM worker process entry point.
[^rust-supervisor-rs]: `src/rs/vertebra-http/src/workers/supervisor.rs::Supervisor` — Worker exit polling, crash backoff, and circuit breaker.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::run_server` — Server shutdown token, embedded worker failure flag, and SHM supervisor spawn.
