---
type: Concept
title: "Multi-Lane Worker Runner"
description: "Admission-controlled background task worker lanes (LaneConfig, WorkerRunner) and the vtb workers run CLI."
status: stable
tags: [domains, server, worker-lanes, queues, cli]
sources:
  - resource: src/py/vertebra/cli/workers.py
  - resource: src/py/vertebra/extensions/litestar/plugin.py
---

# Multi-Lane Worker Runner

[`vertebra.cli.workers`](../../../../../src/py/vertebra/cli/workers.py) provides an admission-controlled background worker runner (`LaneConfig`, `WorkerRunner`) with per-queue `asyncio.Semaphore` isolation and oneshot job execution.

## 1. Domain Types (`cli/workers.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`LaneConfig`](../../../../../src/py/vertebra/cli/workers.py) | `vertebra.cli.workers` | Frozen `msgspec.Struct(gc=False, kw_only=True)` defining a queue lane's `queue_name`, `concurrency: int = 10`, and `drain_timeout_s: float = 30.0`. |
| [`WorkerRunner`](../../../../../src/py/vertebra/cli/workers.py) | `vertebra.cli.workers` | Manages per-lane `asyncio.Semaphore` pools and in-flight task tracking (`submit`, `stop`, `parse_lanes`, `resolve_job`, `run_oneshot`, `execute_cli`). |

## 2. Admission Control, Draining & Oneshot Execution

- **Lane Parsing (`WorkerRunner.parse_lanes`)**: Splits comma-separated `--queues` (default `"default"`) and `--concurrency` (default `"10"`) strings into a tuple of `LaneConfig` instances, clamping each lane's concurrency to `>= 1` and repeating the final concurrency limit when fewer limits than queues are provided.
- **Semaphore-Guarded Submission (`submit`)**: `runner.submit(queue_name, job_factory)` acquires the lane's `asyncio.Semaphore` (lazily creating `asyncio.Semaphore(10)` for unconfigured queue names) before invoking `job_factory()` and awaiting the result if awaitable. Submissions while stopping raise `RuntimeError`.
- **Graceful Drain (`stop`)**: Sets `_stopping = True` and waits up to `max(lane.drain_timeout_s)` (default `30.0` seconds) for all in-flight `asyncio.Task` instances to finish.
- **Oneshot Job Execution (`resolve_job`, `run_oneshot`)**: Resolves a named callable from `app.state["vertebra_jobs"]` or `app.jobs`, enters `app.lifespan()` when callable, awaits the job, and returns exit code `0` on success or `1` on missing job / unhandled exception.

## 3. CLI Integration (`cli/workers.py`)

- **[`vtb workers run APP`](../../../../../src/py/vertebra/cli/workers.py)** (`WorkerRunner.execute_cli`): Sets `LITESTAR_APP` and `VERTEBRA_APP`, imports `APP` via `import_app`, attaches `WorkerRunner` to `app.state["vertebra_worker_runner"]` when `app.state` is a `dict`, enters [`LitestarPlugin.enter_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py), and drains all lanes via `runner.stop()` on exit.
