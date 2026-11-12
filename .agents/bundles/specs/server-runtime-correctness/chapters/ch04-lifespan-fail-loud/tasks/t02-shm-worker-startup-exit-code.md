---
type: Task
id: "server-runtime-correctness:ch04:t02"
chapter_id: ch04-lifespan-fail-loud
title: Exit SHM workers with code 78 on startup failure and verify embedded non-zero exit
description: Make vertebra.supervisor._worker.main exit with EXIT_STARTUP_FAILURE (78) when run_worker raises WorkerStartupError, and add server-binary integration cases proving embedded and SHM servers exit non-zero when a mount's lifespan startup fails.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch04:t01"
  - "server-runtime-correctness:ch03:t04"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch04-lifespan-fail-loud/spec.md
  - src/py/vertebra/supervisor/_worker.py
  - src/py/vertebra/server/worker.py
  - src/py/tests/unit/cli/test_cli_commands.py
  - src/py/tests/integration/server/conftest.py
  - src/py/tests/integration/server/test_vertebra_server.py
files:
  - src/py/vertebra/supervisor/_worker.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
  - src/py/tests/integration/server/test_vertebra_server.py
estimated_loc: 90
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
  - id: supervisor-worker-py
    resource: src/py/vertebra/supervisor/_worker.py::main
    title: SHM worker entry point calling run_worker
  - id: worker-py
    resource: src/py/vertebra/server/worker.py::WorkerStartupError
    title: Typed worker startup failure from ch04:t01
tags:
  - lifespan
  - shm
  - exit-code
---

# Task t02: Exit SHM Workers with Code 78 on Startup Failure and Verify Embedded Non-Zero Exit

## Objective & Scope Boundary

Wrap the `run_worker(...)` call in `vertebra.supervisor._worker.main` so `WorkerStartupError` logs the error with its cause chain and calls `sys.exit(EXIT_STARTUP_FAILURE)`; other exceptions propagate unchanged (exit `1`). Add binary-level cases to the `ch03:t04` integration module for both topologies. Out of scope: the Rust supervisor response (`t03`, which the SHM integration case depends on and is therefore asserted there).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/supervisor/_worker.py::{main,resolve_worker_targets,__all__}` [^supervisor-worker-py].
- `src/py/vertebra/server/worker.py::WorkerStartupError` [^worker-py].
- `src/py/tests/unit/cli/test_cli_commands.py::test_worker_main_asgi_app_avoids_double_mount` — monkeypatching pattern for `supervisor_worker.run_worker` and `import_app`.
- `src/py/tests/integration/server/conftest.py::launch_server` and `test_vertebra_server.py`.

## Symbol & Signature Contracts

- `src/py/vertebra/supervisor/_worker.py`:
  - `EXIT_STARTUP_FAILURE: Final = 78` added to `__all__`.
  - `main()` → `try: run_worker(...) except WorkerStartupError: logger.exception("worker %d startup failed", worker_id); sys.exit(EXIT_STARTUP_FAILURE)`.
  - Module-top imports: `sys`, `Final`, and `WorkerStartupError` from `vertebra.server.worker`.
- `src/py/tests/integration/server/conftest.py::launch_server` already accepts `wait_ready=False` and `ServerProcess` exposes `wait_exit` and `log_text` (defined by `ch03:t04`).

## Edge Cases & Error Semantics

- `WorkerStartupError` raised during `resolve_worker_targets` cannot happen (no lifespan there); only `run_worker` is wrapped.
- `KeyboardInterrupt` and `SystemExit` pass through untouched.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/cli/test_cli_commands.py` (unit), `src/py/tests/integration/server/test_vertebra_server.py` (binary).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Startup failure exit code | Monkeypatch `supervisor_worker.run_worker` to raise `WorkerStartupError("/api", "lifespan")`; call `supervisor_worker.main()` | `SystemExit` with `code == 78` |
| Other errors propagate | `run_worker` raises `RuntimeError("boom")` | `RuntimeError("boom")` escapes `main()` |
| Embedded binary exit | `launch_server(app_source=<ASGI app sending lifespan.startup.failed "db offline">, topology="embedded", wait_ready=False)` | Process exits within `20s` with non-zero code; log contains `"db offline"` and `"lifespan startup failed for mount '/'"` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q -k startup`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q
  make dev
  uv run --no-sync pytest src/py/tests/integration/server/test_vertebra_server.py -q -k startup
  uv run --no-sync ruff check src/py/vertebra/supervisor
  ```
- Planned local commit: `fix(supervisor): exit SHM workers with code 78 on startup failure`

## Acceptance Criteria

- [ ] `vertebra.supervisor._worker.main` exits `78` on `WorkerStartupError` and propagates other errors (R10).
- [ ] The embedded server binary exits non-zero when lifespan startup fails (R9).

## Notes & Discoveries

[^supervisor-worker-py]: `src/py/vertebra/supervisor/_worker.py::main` — SHM worker entry point calling `run_worker`.
[^worker-py]: `src/py/vertebra/server/worker.py::WorkerStartupError` — Typed worker startup failure from `ch04:t01`.
