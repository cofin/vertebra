---
type: Task
id: "server-runtime-correctness:ch04:t01"
chapter_id: ch04-lifespan-fail-loud
title: Raise WorkerStartupError for lifespan and RSGI init failures and cancel timed-out lifespans
description: Replace the swallowing except blocks in Worker.run and _instantiate_mount_runners with WorkerStartupError raised from the original exception, unwind already-started lifespans, and make AsgiLifespan.startup cancel and await its task on timeout.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch03:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch04-lifespan-fail-loud/spec.md
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/_typing.py
  - src/py/tests/unit/server/test_runner.py
  - src/py/tests/unit/server/test_asgi.py
files:
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_asgi/_runner.py
tests:
  - src/py/tests/unit/server/test_runner.py
  - src/py/tests/unit/server/test_asgi.py
estimated_loc: 140
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
  - id: worker-py
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker.run lifespan ExitStack and _instantiate_mount_runners
  - id: lifespan-py
    resource: src/py/vertebra/server/_asgi/_runner.py::AsgiLifespan
    title: AsgiLifespan.startup wait_for and _run_app
  - id: typing-py
    resource: src/py/vertebra/_typing.py::VertebraError
    title: Base Vertebra exception type
tags:
  - lifespan
  - fail-loud
  - python
---

# Task t01: Raise WorkerStartupError for Lifespan and RSGI Init Failures and Cancel Timed-Out Lifespans

## Objective & Scope Boundary

Define `WorkerStartupError` in `src/py/vertebra/server/worker.py`, raise it from `Worker.run` when entering any ASGI mount lifespan fails and from `_instantiate_mount_runners` when `__rsgi_init__` fails, and make `AsgiLifespan.startup` cancel and await the lifespan task before re-raising `TimeoutError`. Out of scope: process exit codes (`t02`) and the Rust supervisor (`t03`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/server/worker.py::{Worker.run,_instantiate_mount_runners,__all__}` [^worker-py].
- `src/py/vertebra/server/_asgi/_runner.py::{AsgiLifespan.startup,AsgiLifespan._run_app,AsgiLifespan.__enter__}` [^lifespan-py].
- `src/py/vertebra/_typing.py::VertebraError` [^typing-py].
- `src/py/tests/unit/server/test_runner.py` — `Worker` construction with mounts and `max_batches`.
- `src/py/tests/unit/server/test_asgi.py::test_asgi_lifespan_startup_shutdown_state_and_unsupported_app`.

## Symbol & Signature Contracts

- `src/py/vertebra/server/worker.py`:
  - `class WorkerStartupError(VertebraError, RuntimeError)` with `__init__(self, prefix: str, phase: Literal["lifespan", "rsgi_init"]) -> None` storing `prefix` and `phase` and calling `super().__init__(f"{phase} startup failed for mount {prefix or '/'!r}")`; added to `__all__`.
  - `Worker.run`: inside the lifespan loop, `try: ... except Exception as exc: raise WorkerStartupError(_prefix, "lifespan") from exc`; the surrounding `try/finally` still closes `lifespan_stack`, so already-entered lifespans shut down in reverse order; the `zpages` `worker_state` var becomes `"failed"` before raising.
  - `_instantiate_mount_runners`: `init_fn(primary_loop)` failures raise `WorkerStartupError(prefix, "rsgi_init") from exc` (remove `logger.exception`).
  - Import `VertebraError` from `vertebra._typing` and `Literal` from `typing` at module top.
- `src/py/vertebra/server/_asgi/_runner.py::AsgiLifespan.startup`: wrap `asyncio.wait_for(self._startup_complete.wait(), timeout=self._timeout)` so that on `TimeoutError` the task is cancelled and awaited (suppressing `CancelledError` only) before re-raising the `TimeoutError` with `add_note(f"ASGI lifespan startup exceeded {self._timeout}s")`.

## Edge Cases & Error Semantics

- Apps that do not support lifespan (raise before receiving `lifespan.startup`) keep `supported == False` and do not fail startup.
- `lifespan.startup.failed` produces `RuntimeError(message)` inside `AsgiLifespan`, which becomes `__cause__`.
- Lifespan shutdown errors in `finally` stay logged by the existing `logger.exception("ASGI lifespan shutdown error")` and do not mask the startup error.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/server/test_runner.py` (`Worker.run` with `max_batches=0`, in-process transport), `src/py/tests/unit/server/test_asgi.py` (`AsgiLifespan` timeout); dependency category `unit`. Use `@pytest.mark.parametrize` over failure modes.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| startup.failed | Mount `("/api", app, "asgi")` whose lifespan sends `{"type": "lifespan.startup.failed", "message": "db offline"}`; `Worker({}, mounts=[...], max_batches=0).run()` | `WorkerStartupError` with `prefix == "/api"`, `phase == "lifespan"`, `str(exc.__cause__) == "db offline"` |
| startup raises | Lifespan raises `ValueError("bad config")` after receiving startup | `WorkerStartupError`, `isinstance(exc.__cause__, ValueError)` |
| ordered unwind | Two mounts `"/a"` (records `"a-shutdown"` on shutdown) and `"/b"` (fails startup) | `WorkerStartupError(prefix="/b")`; recorded events `["a-startup", "a-shutdown"]` |
| unsupported app | HTTP-only app that raises `KeyError` before receiving startup | `run()` completes with `max_batches=0`; no error |
| RSGI init failure | Mount with `__rsgi_init__` raising `OSError("no socket")` and `__rsgi__` defined | `WorkerStartupError(phase="rsgi_init")` from `_instantiate_mount_runners` |
| lifespan timeout | `AsgiLifespan(app_that_never_completes, timeout=0.05)` used as context manager | `TimeoutError` with note containing `"0.05s"`; the app coroutine observed `CancelledError` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_asgi.py -q -k "startup or lifespan"`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_asgi.py -q
  uv run --no-sync ruff check src/py/vertebra/server
  uv run --no-sync mypy src/py/vertebra/server
  ```
- Planned local commit: `fix(server): fail worker startup on lifespan and RSGI init errors`

## Acceptance Criteria

- [ ] Lifespan startup failure, exception, and timeout raise `WorkerStartupError` from `Worker.run` with ordered unwind (R9).
- [ ] RSGI `__rsgi_init__` failures raise `WorkerStartupError` (R9).
- [ ] `AsgiLifespan.startup` cancels its task on timeout (R9).

## Notes & Discoveries

[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — `Worker.run` lifespan `ExitStack` and `_instantiate_mount_runners`.
[^lifespan-py]: `src/py/vertebra/server/_asgi/_runner.py::AsgiLifespan` — `AsgiLifespan.startup` `wait_for` and `_run_app`.
[^typing-py]: `src/py/vertebra/_typing.py::VertebraError` — Base Vertebra exception type.
