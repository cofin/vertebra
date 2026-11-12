---
type: Task
id: "server-runtime-correctness:ch05:t03"
chapter_id: ch05-supervisor-exports-and-worker-lanes
title: Implement vtb run --worker-only with HealthProbeServer
description: Add an asyncio HealthProbeServer that serves ZPagesRegistry probes, extend execute_worker_cli with a health_probe option that runs it beside the worker lanes, and make vtb run --worker-only run lanes behind the probe listener without launching vertebra-server, rejecting --with-workers and --oneshot-job combinations.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch05:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch05-supervisor-exports-and-worker-lanes/spec.md
  - src/py/vertebra/server/platform.py
  - src/py/vertebra/cli/run.py
  - src/py/vertebra/cli/workers.py
  - src/py/vertebra/cli/_launch.py
  - src/py/tests/unit/cli/test_run_with_workers.py
files:
  - src/py/vertebra/server/platform.py
  - src/py/vertebra/cli/run.py
  - src/py/vertebra/cli/workers.py
tests:
  - src/py/tests/unit/cli/test_run_with_workers.py
estimated_loc: 220
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
  - id: platform-py
    resource: src/py/vertebra/server/platform.py::ZPagesRegistry
    title: ZPagesRegistry.handle_zpage and LameDuckController.readiness_status
  - id: run-cli-py
    resource: src/py/vertebra/cli/run.py::run
    title: vtb run options and supervisor launch
  - id: workers-cli-py
    resource: src/py/vertebra/cli/workers.py::execute_worker_cli
    title: Worker CLI lifespan and runner loop
tags:
  - workers
  - cli
  - health
---

# Task t03: Implement vtb run --worker-only with HealthProbeServer

## Objective & Scope Boundary

Add `HealthProbeServer` to `src/py/vertebra/server/platform.py`, add a `health_probe: tuple[str, int] | None` option to `execute_worker_cli` that starts the probe server on the same event loop as `WorkerRunner.run` and stops it after the runner stops, and route `vtb run --worker-only` to `execute_worker_cli(resolved_app, queues=..., concurrency=..., run_until_stopped=True, health_probe=(host, port))` before any `build_server_command` call. Remove `env["VERTEBRA_WORKER_ONLY"]`. Out of scope: Litestar `litestar run` CLI plugin flags.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/server/platform.py::{ZPagesRegistry,ZPagesRegistry.handle_zpage,LameDuckController.readiness_status}` [^platform-py].
- `src/py/vertebra/cli/run.py::run` — option list, `worker_only`, `oneshot_job`, `with_workers`, `worker_queues`, `lane_concurrency`, and `VERTEBRA_WORKER_ONLY` assignment [^run-cli-py].
- `src/py/vertebra/cli/workers.py::execute_worker_cli` (after `t02`) [^workers-cli-py].
- `src/py/vertebra/cli/_launch.py::{load_workers_pyproject_config,parse_lane_concurrency_map}`.
- `src/py/tests/unit/cli/test_run_with_workers.py`.

## Symbol & Signature Contracts

- `src/py/vertebra/server/platform.py::HealthProbeServer` (added to `__all__`):
  - `__init__(self, zpages: ZPagesRegistry, *, host: str, port: int) -> None`.
  - `@property def bound_port(self) -> int` (actual port; `0` requests an ephemeral port; raises `RuntimeError("health probe server not started")` before `start`).
  - `async def start(self) -> None` uses `asyncio.start_server`; each connection reads one request line and headers (bounded to `8 KiB`), maps `GET /healthz`, `/readyz`, `/livez` → `/livez` always `200 {"status":"ok"}`, `/healthz` and `/readyz` → `zpages.handle_zpage("/healthz")`, other `GET` paths → `zpages.handle_zpage(path)`; non-`GET` → `405` with `allow: GET`; oversize or malformed → `400`; responses are `HTTP/1.1` with `content-type: application/json`, `content-length`, and `connection: close`.
  - `async def stop(self) -> None` closes the server and waits for it.
- `src/py/vertebra/cli/workers.py::execute_worker_cli(..., health_probe: tuple[str, int] | None = None) -> int`: when set (requires `run_until_stopped=True`, else `ValueError`), builds `ZPagesRegistry(lame_duck=lame_duck)`, sets `worker_state` var, and runs `asyncio.run(self_contained_coroutine)` that starts the probe, runs `runner.run(install_signal_handlers=True)`, then `runner.stop()` and `probe.stop()` in `finally`.
- `src/py/vertebra/cli/run.py::run`: before launch-env assembly, `if worker_only:` → `if oneshot_job or effective_with_workers: raise CLIError("--worker-only cannot be combined with --oneshot-job or --with-workers", exit_code=EXIT_CONFIG_ERROR)`; resolve queues and lane concurrency (`effective_queues`, `effective_lanes`) into the `queues`/`concurrency` strings `execute_worker_cli` expects; `sys.exit(execute_worker_cli(..., run_until_stopped=True, health_probe=(host, port)))`. The `oneshot_job` check runs first as today, so the exclusivity error is raised before either branch.

## Edge Cases & Error Semantics

- Port in use → `OSError` propagates; exit non-zero.
- SIGTERM → `WorkerRunner` lame-duck → `/healthz` returns `503 {"status":"lame_duck",...}` until shutdown completes.
- Missing queue service → `ImproperConfigurationError` from `t02` before the probe binds.
- Lane order: `effective_lanes` keys missing from `effective_queues` are ignored; missing limits default to `10`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/cli/test_run_with_workers.py`, dependency category `unit` (loopback sockets, `pytest.mark.anyio`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Live probe | `HealthProbeServer(ZPagesRegistry(), host="127.0.0.1", port=0)`; `await start()`; `GET /livez` | `HTTP/1.1 200`, body `{"status":"ok"}` |
| Readiness lame-duck | Same, after `zpages.lame_duck.enter_lame_duck()`; `GET /readyz` | `503`, body contains `"lame_duck"` |
| Method not allowed | `POST /healthz` | `405`, header `allow: GET` |
| Unknown path | `GET /nope` | `404`, body `{"error":"not_found"}` |
| Malformed | Send `b"garbage\r\n\r\n"` | `400` |
| Worker-only routing | Monkeypatch `vertebra.cli.run.execute_worker_cli` to record kwargs and return `0`, and `build_server_command` to raise; `CliRunner().invoke(run, ["app:app", "--worker-only", "--port", "9100", "--queues", "default,etl", "--lane-concurrency", "etl=2"])` | Exit `0`; recorded `health_probe == ("127.0.0.1", 9100)`, `queues == "default,etl"`, `concurrency == "10,2"`, `run_until_stopped is True`; `build_server_command` not called |
| Exclusive flags | `["app:app", "--worker-only", "--with-workers"]` and `["app:app", "--worker-only", "--oneshot-job", "x"]` (parametrized) | Exit code `2`; output contains `"--worker-only cannot be combined"` |
| No env flag | Inspect `ServerLaunchEnv`-built env in a normal `vtb run` (patched supervisor) | `"VERTEBRA_WORKER_ONLY" not in env` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/cli/test_run_with_workers.py -q -k "worker_only or health"`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/cli/test_run_with_workers.py src/py/tests/unit/cli/test_cli_commands.py -q
  uv run --no-sync ruff check src/py/vertebra/cli src/py/vertebra/server
  uv run --no-sync mypy src/py/vertebra/cli src/py/vertebra/server
  ```
- Planned local commit: `feat(cli): run worker lanes behind health probes for vtb run --worker-only`

## Acceptance Criteria

- [ ] `HealthProbeServer` serves `/livez`, `/healthz`, `/readyz`, and z-pages with the stated status codes (R13).
- [ ] `vtb run --worker-only` runs lanes with probes and never launches `vertebra-server`; exclusive flags fail with exit code `2` (R13).
- [ ] `VERTEBRA_WORKER_ONLY` is no longer set (R13).

## Notes & Discoveries

[^platform-py]: `src/py/vertebra/server/platform.py::ZPagesRegistry` — `ZPagesRegistry.handle_zpage` and `LameDuckController.readiness_status`.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — `vtb run` options and supervisor launch.
[^workers-cli-py]: `src/py/vertebra/cli/workers.py::execute_worker_cli` — Worker CLI lifespan and runner loop.
