---
type: Task
id: "server-runtime-correctness:ch02:t01"
flow_id: server-runtime-correctness
chapter_id: ch02-lifespan-supervisor-and-worker-lanes
title: Raise WorkerStartupError on Lifespan/RSGI Failures and Exit SHM Workers with Code 78
description: "Replace swallowed lifespan and __rsgi_init__ exceptions with WorkerStartupError, cancel timed-out lifespans, and exit SHM _worker.main with EXIT_STARTUP_FAILURE = 78."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/supervisor/_worker.py
tests:
  - src/py/tests/unit/server/test_runner.py
  - src/py/tests/unit/server/test_asgi.py
  - src/py/tests/unit/cli/test_cli_commands.py
read_set:
  - src/py/vertebra/server/worker.py
  - src/py/vertebra/server/_asgi/_runner.py
  - src/py/vertebra/supervisor/_worker.py
  - src/py/tests/unit/server/test_runner.py
  - src/py/tests/unit/cli/test_cli_commands.py
estimated_loc: 190
tags:
  - lifespan
  - fail-loud
  - worker
  - supervisor
sources:
  - id: worker-py
    resource: "src/py/vertebra/server/worker.py::Worker"
    title: Worker.run lifespan ExitStack and mount initialization
  - id: supervisor-worker-py
    resource: "src/py/vertebra/supervisor/_worker.py::main"
    title: SHM worker entrypoint
---

# Task `t01`: Raise WorkerStartupError on Lifespan/RSGI Failures and Exit SHM Workers with Code 78

## Objective & Scope Boundary

Make ASGI lifespan startup, lifespan timeouts, and RSGI `__rsgi_init__` fail loud ([#84](https://github.com/cofin/vertebra/issues/84)) per [`../spec.md` §2](../spec.md) [^worker-py] [^supervisor-worker-py].

## Bounded Read Manifest

- `src/py/vertebra/server/worker.py` — `Worker.run` and `_instantiate_mount_runners`.
- `src/py/vertebra/server/_asgi/_runner.py` — `AsgiLifespan.startup` timeout cleanup.
- `src/py/vertebra/supervisor/_worker.py` — `main()` exit code handling.

## Contracts & Edge Cases

- `WorkerStartupError(prefix: str, phase: Literal['lifespan', 'rsgi_init'])` chains `__cause__`, unwinds already-entered mounts via `ExitStack`, and leaves unsupported-lifespan mounts (raising before `lifespan.startup` receive) non-fatal.
- `AsgiLifespan.startup` cancels and awaits the background lifespan task when `asyncio.wait_for` times out.
- `supervisor._worker.main` catches `WorkerStartupError`, logs traceback, and calls `sys.exit(EXIT_STARTUP_FAILURE)` (`78`).

## Test Matrix

- Parameterized tests in `test_runner.py`, `test_asgi.py`, and `test_cli_commands.py` covering `lifespan.startup.failed`, raised exceptions, startup timeout cancellation, multi-mount reverse unwind, `__rsgi_init__` failure, and SHM exit code `78`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server src/py/vertebra/supervisor/_worker.py && uv run --no-sync mypy src/py/vertebra/server src/py/vertebra/supervisor/_worker.py && uv run --no-sync pytest src/py/tests/unit/server/test_runner.py src/py/tests/unit/server/test_asgi.py src/py/tests/unit/cli/test_cli_commands.py -q
```

## Acceptance Criteria

- [ ] `Worker.run` raises `WorkerStartupError` and unwinds prior mounts on startup failure.
- [ ] `_worker.main` exits with `78` on `WorkerStartupError`.

[^worker-py]: `src/py/vertebra/server/worker.py::Worker` — Worker.run lifespan ExitStack and mount initialization.
[^supervisor-worker-py]: `src/py/vertebra/supervisor/_worker.py::main` — SHM worker entrypoint.
