---
type: Concept
title: "Multi-Lane Worker Runner"
description: "Admission-controlled background task worker lanes (LaneConfig, WorkerRunner) and the vtb workers run CLI."
status: stable
tags:
  - domains
  - server
  - worker-lanes
  - queues
  - cli
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
sources:
  - id: src-server-lanes-py
    resource: src/py/vertebra/server/lanes.py
    title: "Python implementation module (src/py/vertebra/server/lanes.py)"
  - id: src-cli-workers-py
    resource: src/py/vertebra/cli/workers.py
    title: "Python implementation module (src/py/vertebra/cli/workers.py)"
  - id: src-cli-run-py
    resource: src/py/vertebra/cli/run.py
    title: "Python implementation module (src/py/vertebra/cli/run.py)"
  - id: src-cli-dev-py
    resource: src/py/vertebra/cli/dev.py
    title: "Python implementation module (src/py/vertebra/cli/dev.py)"
  - id: src-extensions-litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)"
---

# Multi-Lane Worker Runner

<!-- truth: start -->
- **Canonical Server Module (`vertebra.server.lanes`)**: [`LaneConfig`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py] and [`WorkerRunner`](../../../../../src/py/vertebra/server/lanes.py) live in `vertebra.server.lanes` and are re-exported by `vertebra.server` and `vertebra.cli.workers`.
- **Co-Located & Standalone Worker Execution**: `vtb run` and `vtb dev` support `--with-workers / --no-workers`, `--queues`, `--lane-concurrency` (e.g. `high:16,default:8,embeddings:2`), and `[tool.vertebra.workers]` in `pyproject.toml` to co-locate `WorkerRunner` inside the server lifecycle, while `vtb workers run --oneshot <job> --payload <json>` runs standalone or Cloud Run Jobs / Kubernetes Job executions.
<!-- truth: end -->

[`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py) (re-exported by [`vertebra.cli.workers`](../../../../../src/py/vertebra/cli/workers.py)[^src-cli-workers-py]) provides an admission-controlled background worker runner (`LaneConfig`, `WorkerRunner`) with per-queue `asyncio.Semaphore` isolation, co-located HTTP worker execution, and oneshot job execution.

## 1. Domain Types (`server/lanes.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`LaneConfig`](../../../../../src/py/vertebra/server/lanes.py) | `vertebra.server.lanes` | Frozen `msgspec.Struct(gc=False, kw_only=True)` defining a queue lane's `queue_name`, `concurrency: int = 10`, and `drain_timeout_s: float = 30.0`. |
| [`WorkerRunner`](../../../../../src/py/vertebra/server/lanes.py) | `vertebra.server.lanes` | Manages per-lane `asyncio.Semaphore` pools and in-flight task tracking (`submit`, `stop`, `parse_lanes`, `parse_lane_concurrency`, `resolve_job`, `run_oneshot`, `execute_cli`). |

## 2. Admission Control, Draining & Oneshot Execution

- **Lane Parsing (`WorkerRunner.parse_lanes`)**: Splits comma-separated `--queues` (default `"default"`) and `--concurrency` (default `"10"`) strings into a tuple of `LaneConfig` instances, clamping each lane's concurrency to `>= 1` and repeating the final concurrency limit when fewer limits than queues are provided.
- **Semaphore-Guarded Submission (`submit`)**: `runner.submit(queue_name, job_factory)` acquires the lane's `asyncio.Semaphore` (lazily creating `asyncio.Semaphore(10)` for unconfigured queue names) before invoking `job_factory()` and awaiting the result if awaitable. Submissions while stopping raise `RuntimeError`.
- **Graceful Drain (`stop`)**: Sets `_stopping = True` and waits up to `max(lane.drain_timeout_s)` (default `30.0` seconds) for all in-flight `asyncio.Task` instances to finish.
- **Oneshot Job Execution (`resolve_job`, `run_oneshot`)**: Resolves a named callable from `app.state["vertebra_jobs"]` or `app.jobs`, enters `app.lifespan()` when callable, awaits the job, and returns exit code `0` on success or `1` on missing job / unhandled exception.

## 3. CLI Integration (`cli/workers.py`)

- **[`vtb workers run APP`](../../../../../src/py/vertebra/cli/workers.py)** (`WorkerRunner.execute_cli`): Sets `LITESTAR_APP` and `VERTEBRA_APP`, imports `APP` via `import_app`, attaches `WorkerRunner` to `app.state["vertebra_worker_runner"]` when `app.state` is a `dict`, enters [`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py], and drains all lanes via `runner.stop()` on exit.

Provenance sources: [^src-cli-run-py], [^src-cli-dev-py].

[^src-server-lanes-py]: `src/py/vertebra/server/lanes.py` — Python implementation module (src/py/vertebra/server/lanes.py).
[^src-cli-workers-py]: `src/py/vertebra/cli/workers.py` — Python implementation module (src/py/vertebra/cli/workers.py).
[^src-cli-run-py]: `src/py/vertebra/cli/run.py` — Python implementation module (src/py/vertebra/cli/run.py).
[^src-cli-dev-py]: `src/py/vertebra/cli/dev.py` — Python implementation module (src/py/vertebra/cli/dev.py).
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Python implementation module (src/py/vertebra/extensions/litestar/plugin.py).
