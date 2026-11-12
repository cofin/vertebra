---
type: Task
id: "server-runtime-correctness:ch10:t01"
chapter_id: ch10-docs-and-okf-sync
title: Document streaming, lifespan failure, worker-only, queue binding, and supervisor exports
description: Update docs/usage/server-and-workers.md, docs/reference/cli.md, and docs/reference/build-and-wasm.md to describe incremental mounted streaming, the buffered batch path, WorkerStartupError and exit code 78, QueueService binding for worker lanes, vtb run --worker-only with health probes, and the vertebra.supervisor exports.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch07:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/spec.md
  - docs/usage/server-and-workers.md
  - docs/reference/cli.md
  - docs/reference/build-and-wasm.md
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/server/platform.py
files:
  - docs/usage/server-and-workers.md
  - docs/reference/cli.md
  - docs/reference/build-and-wasm.md
tests: []
estimated_loc: 160
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
  - id: server-docs
    resource: docs/usage/server-and-workers.md
    title: Server and workers usage page
  - id: cli-ref
    resource: docs/reference/cli.md
    title: CLI reference table
  - id: build-ref
    resource: docs/reference/build-and-wasm.md
    title: Build, Wasm, and supervisor reference
tags:
  - docs
---

# Task t01: Document Streaming, Lifespan Failure, Worker-Only, Queue Binding, and Supervisor Exports

## Objective & Scope Boundary

Rewrite the affected sections of three public pages in present tense. Out of scope: packaging and installation pages (`t02`) and OKF chapters (`t03`, `t04`).

## Bounded Read Manifest

- [Root spec](../../../spec.md) — delivered behaviour (Sections 2.1, 2.4).
- `docs/usage/server-and-workers.md` [^server-docs].
- `docs/reference/cli.md` [^cli-ref].
- `docs/reference/build-and-wasm.md` [^build-ref].
- `src/py/vertebra/server/lanes.py::{WorkerRunner.resolve_queue_service,QUEUE_SERVICE_STATE_KEY}`.
- `src/py/vertebra/server/platform.py::HealthProbeServer`.

## Symbol & Signature Contracts

- `docs/usage/server-and-workers.md`:
  - New section "Streaming Responses": mounted ASGI (`http.response.body`, `http.response.zerocopysend`, `http.response.sse` with `more_body=True`) and RSGI `response_stream` flush each chunk as it is sent in embedded and SHM topologies; trailers; `send` raises `ConnectionResetError` after an embedded client disconnects; the buffered batch path note next to `VERTEBRA_ASGI_BATCH_TIMEOUT` states that only the buffered path collects full bodies; short Starlette `StreamingResponse` example mounted with `App.mount`.
  - New section "Startup Failures": lifespan `startup.failed`, exceptions, and `30s` timeouts raise `WorkerStartupError`; embedded servers exit non-zero; SHM workers exit `78` and the server stops instead of respawning.
  - Worker lanes section: `vtb workers run APP` runs until `SIGTERM`/`SIGINT`; lanes need a `QueueService` passed as the target or set at `app.state["vertebra_queue_service"]`; example for Litestar `State` and Vertebra `App.state`; `--with-workers` uses the same lookup.
  - New subsection "Worker-Only Processes": `vtb run APP --worker-only --port 8080` serves `/livez`, `/healthz`, `/readyz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz` and never starts `vertebra-server`; incompatible with `--with-workers` and `--oneshot-job`.
- `docs/reference/cli.md`: `--worker-only` row reads "Run worker lanes behind a health-probe listener on `--host`/`--port` without starting the HTTP server (excludes `--with-workers` and `--oneshot-job`)"; `vtb workers run` row states it runs until signalled and requires a `QueueService`.
- `docs/reference/build-and-wasm.md`: supervisor section lists `run_supervisor`, `build_server_command`, `find_server_binary`, `find_repo_root`, `worker_main` importable from `vertebra.supervisor`, changes the directive to `::: vertebra.supervisor.run_supervisor` (and adds `build_server_command`, `find_server_binary`), and mentions `EmbeddedPythonRuntime`.

## Edge Cases & Error Semantics

- Examples import only public names (`vertebra.supervisor`, `vertebra.framework.queues.QueueService`).
- Code examples are concise: no single-use intermediate variables and no `frozen=True, gc=False` boilerplate.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Build | `make docs-check` | Exit `0`, no broken mkdocstrings references |
| Symbols present | `git grep -n "WorkerStartupError\|vertebra_queue_service\|worker-only\|ConnectionResetError" -- docs/usage/server-and-workers.md` | Each term appears |
| CLI row | `git grep -n "health-probe listener" -- docs/reference/cli.md` | One match |
| Supervisor directive | `git grep -n "::: vertebra.supervisor.run_supervisor" -- docs/reference/build-and-wasm.md` | One match |
| No stale claim | `git grep -n "health probes only" -- docs` | No matches |

## Verification

- Initial baseline: `git grep -n "health probes only" -- docs`
- Final:
  ```bash
  make docs-check
  git grep -n "WorkerStartupError" -- docs/usage/server-and-workers.md
  git grep -n "::: vertebra.supervisor.run_supervisor" -- docs/reference/build-and-wasm.md
  ```
- Planned local commit: `docs(server): document streaming, startup failures, and worker modes`

## Acceptance Criteria

- [ ] `docs/usage/server-and-workers.md` documents streaming, startup failures, queue binding, and worker-only mode (R18).
- [ ] `docs/reference/cli.md` and `docs/reference/build-and-wasm.md` match the delivered CLI and supervisor API (R18).

## Notes & Discoveries

[^server-docs]: `docs/usage/server-and-workers.md` — Server and workers usage page.
[^cli-ref]: `docs/reference/cli.md` — CLI reference table.
[^build-ref]: `docs/reference/build-and-wasm.md` — Build, Wasm, and supervisor reference.
