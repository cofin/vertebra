---
type: Task
id: "server-runtime-correctness:ch02:t04"
flow_id: server-runtime-correctness
chapter_id: ch02-lifespan-supervisor-and-worker-lanes
title: Keep vtb workers run Alive and Implement vtb run --worker-only Health Probes
description: "Bind QueueService and run_until_stopped=True in vtb workers run, add HealthProbeServer on ZPagesRegistry, and wire vtb run --worker-only without launching vertebra-server."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch02:t03"
files:
  - src/py/vertebra/server/platform.py
  - src/py/vertebra/cli/workers.py
  - src/py/vertebra/cli/run.py
tests:
  - src/py/tests/unit/cli/test_run_with_workers.py
read_set:
  - src/py/vertebra/server/platform.py
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/cli/workers.py
  - src/py/vertebra/cli/run.py
  - src/py/tests/unit/cli/test_run_with_workers.py
estimated_loc: 230
tags:
  - cli
  - workers
  - health
  - server
sources:
  - id: platform-py
    resource: "src/py/vertebra/server/platform.py::ZPagesRegistry"
    title: Z-page health and readiness renderer
  - id: workers-cli-py
    resource: "src/py/vertebra/cli/workers.py::execute_worker_cli"
    title: Worker CLI execution helper
  - id: run-cli-py
    resource: "src/py/vertebra/cli/run.py::run"
    title: vtb run CLI command
---

# Task `t04`: Keep vtb workers run Alive and Implement vtb run --worker-only Health Probes

## Objective & Scope Boundary

Complete [#85](https://github.com/cofin/vertebra/issues/85) across `vtb workers run` and `vtb run --worker-only` per [`../spec.md` §2](../spec.md) [^platform-py] [^workers-cli-py] [^run-cli-py].

## Bounded Read Manifest

- `src/py/vertebra/server/platform.py` — `ZPagesRegistry` and `LameDuckController`.
- `src/py/vertebra/cli/workers.py` & `src/py/vertebra/cli/run.py` — worker and server CLI commands.

## Contracts & Edge Cases

- `HealthProbeServer(zpages, *, host, port)` serves `GET /healthz`, `/readyz`, `/statusz`, `/varz` via asyncio TCP (`405` with `Allow: GET` on non-`GET`, `503` during lame-duck drain, `404` on unknown paths).
- `execute_worker_cli(..., health_probe: tuple[str, int] | None = None)` resolves `QueueService` via `WorkerRunner.resolve_queue_service`, enters app lifespan, starts `HealthProbeServer` when requested, and runs until stopped when `run_until_stopped=True` (`vtb workers run`).
- `vtb run --worker-only` removes `VERTEBRA_WORKER_ONLY`, rejects `--with-workers` and `--oneshot-job` (`CLIError`, exit `2`), and delegates directly to `execute_worker_cli(..., run_until_stopped=True, health_probe=(host, port))`.

## Test Matrix

- Parameterized tests in `test_run_with_workers.py` covering `vtb workers run` job execution + signal stop, `--worker-only` `/healthz` `200` -> `503` lame-duck -> clean exit without `vertebra-server`, `405` on `POST /healthz`, and flag exclusivity errors.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/server/platform.py src/py/vertebra/cli && uv run --no-sync mypy src/py/vertebra/server/platform.py src/py/vertebra/cli && uv run --no-sync pytest src/py/tests/unit/cli/test_run_with_workers.py -q
```

## Acceptance Criteria

- [ ] `vtb workers run` processes queued tasks until signalled.
- [ ] `vtb run --worker-only` serves health probes and drains lanes without launching `vertebra-server`.

[^platform-py]: `src/py/vertebra/server/platform.py::ZPagesRegistry` — Z-page health and readiness renderer.
[^workers-cli-py]: `src/py/vertebra/cli/workers.py::execute_worker_cli` — Worker CLI execution helper.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — vtb run CLI command.
