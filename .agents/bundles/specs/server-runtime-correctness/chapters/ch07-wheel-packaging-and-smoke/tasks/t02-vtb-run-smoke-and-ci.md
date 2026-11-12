---
type: Task
id: "server-runtime-correctness:ch07:t02"
chapter_id: ch07-wheel-packaging-and-smoke
title: Smoke-test vtb run from an installed core wheel locally and in CI
description: Add tools/scripts/smoke_vtb_run.py that writes a temporary project and runs the installed vtb run against native, streaming ASGI, SHM, and worker-only scenarios, a make core-wheel-check target that builds the core wheel with the binary and runs the smoke from a clean uv-managed venv, and a CI job running it on Linux x86_64, Linux aarch64, and macOS.
status: draft
state: pending
priority: P1
verification_strategy: integration
depends_on:
  - "server-runtime-correctness:ch07:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch07-wheel-packaging-and-smoke/spec.md
  - Makefile
  - .github/workflows/ci.yml
  - tools/scripts/build_server_binary.py
  - src/py/tests/integration/server/conftest.py
  - src/py/vertebra/server/lanes.py
files:
  - tools/scripts/smoke_vtb_run.py
  - Makefile
  - .github/workflows/ci.yml
tests:
  - tools/scripts/smoke_vtb_run.py
estimated_loc: 240
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
  - id: makefile
    resource: Makefile
    title: postgres-wheel-check and oracle-wheel-check target patterns
  - id: ci-yaml
    resource: .github/workflows/ci.yml
    title: changes, lint, test-python, and test-rust-features jobs
  - id: server-conftest
    resource: src/py/tests/integration/server/conftest.py::launch_server
    title: Raw-socket streaming probe pattern from ch03:t04
tags:
  - smoke
  - ci
  - wheel
---

# Task t02: Smoke-Test vtb run from an Installed Core Wheel Locally and in CI

## Objective & Scope Boundary

Create `tools/scripts/smoke_vtb_run.py`, add `make core-wheel-check`, and add a `core-wheel-smoke` job to `.github/workflows/ci.yml`. The smoke runs only the installed console script (`<venv>/bin/vtb` or `Scripts\vtb.exe`) from a directory outside the checkout so it never imports `src/py`. Out of scope: release workflow changes (`t03`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `Makefile` — `postgres-wheel-check`, `oracle-wheel-check`, `.PHONY` and `##` help conventions [^makefile].
- `.github/workflows/ci.yml` — job layout, `changes` filter, and Rust/uv/bun setup steps [^ci-yaml].
- `tools/scripts/build_server_binary.py` — `build` subcommand.
- `src/py/tests/integration/server/conftest.py::launch_server` — free-port and raw-socket read pattern [^server-conftest].
- `src/py/vertebra/server/lanes.py::QUEUE_SERVICE_STATE_KEY` — worker-only scenario wiring.

## Symbol & Signature Contracts

- `tools/scripts/smoke_vtb_run.py` (module docstring, `__all__`, no inline comments, `sys.stdout.write`):
  - `SmokeScenario` fields as in the chapter spec.
  - `VtbRunSmoke.write_project()` writes `smokeapp.py` defining `app = App()` with `@app.get("/ping")` returning `"pong"`, an ASGI streaming callable mounted at `/stream` that sends `b"first\n"` with `more_body=True`, waits for `release` file in the workdir, then sends `b"second\n"`, and `app.state["vertebra_queue_service"] = QueueService(QueueConfig(backend_url="memory://"))`.
  - `VtbRunSmoke.run(scenario)` picks a free port, launches `[vtb, "run", "smokeapp:app", "--host", "127.0.0.1", "--port", str(port), "--no-banner", *scenario.args]` with `cwd=workdir`, waits up to `30s` for the port, sends `GET <probe_path>`, asserts the body (streaming scenarios assert `b"first\n"` arrives before creating `release`), sends `SIGTERM` (`CTRL_BREAK_EVENT` on Windows with `CREATE_NEW_PROCESS_GROUP`), and asserts exit code `0` within `15s`; failures write the captured output and raise `SystemExit(1)`.
  - `default_scenarios()` → `embedded-native` (`()`, `/ping`, `b"pong"`), `embedded-stream` (`()`, `/stream`, `b"first\nsecond\n"`, `streaming=True`), `shm-native` (`("--topology", "shm", "--workers", "2")`, `/ping`, `b"pong"`, `posix_only=True`), `worker-only` (`("--worker-only",)`, `/livez`, `b'{"status":"ok"}'`).
  - `main(["--vtb", PATH, "--workdir", DIR])` runs every applicable scenario and returns `0`.
- `Makefile`:
  ```make
  .PHONY: core-wheel-check
  core-wheel-check:                                   ## Build the core wheel with vertebra-server and smoke-test vtb run from a clean venv
  	@rm -rf /tmp/vertebra-core-wheel-check
  	@uv run --no-sync python -m tools.scripts.build_server_binary build
  	@uv build --wheel --out-dir /tmp/vertebra-core-wheel-check
  	@uv venv --python 3.12 --managed-python /tmp/vertebra-core-wheel-check/venv
  	@uv pip install --python /tmp/vertebra-core-wheel-check/venv/bin/python /tmp/vertebra-core-wheel-check/vertebra_core-*.whl
  	@mkdir -p /tmp/vertebra-core-wheel-check/project
  	@uv run --no-sync python -m tools.scripts.smoke_vtb_run --vtb /tmp/vertebra-core-wheel-check/venv/bin/vtb --workdir /tmp/vertebra-core-wheel-check/project
  ```
- `.github/workflows/ci.yml` job `core-wheel-smoke`: `needs: changes`, runs when Python or Rust paths change, matrix `os: [ubuntu-latest, ubuntu-24.04-arm, macos-latest]`, steps checkout, rust toolchain, bun + `make ts-build`, `astral-sh/setup-uv`, `make core-wheel-check`.

## Edge Cases & Error Semantics

- `uv venv --managed-python` guarantees a python-build-standalone interpreter with a shared libpython.
- The smoke never falls back to the repository binary: it asserts `vtb` resolves `vertebra/bin/vertebra-server` inside the venv by checking the server log line `vertebra-server starting` and that `VERTEBRA_SERVER_BINARY` is unset in the child environment.
- `worker-only` asserts the port answers `/livez` and no `vertebra-server` process is spawned (child process tree contains only Python).

## Public-Seam Test Blueprint

- **Verification category**: `integration` (the script is the test; it runs from `make core-wheel-check`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| embedded-native | `vtb run smokeapp:app --port P` | `GET /ping` → `200`, body `b"pong"`; SIGTERM exit `0` |
| embedded-stream | Same, `GET /stream` | `b"first\n"` received before `release` exists; full body `b"first\nsecond\n"` after release |
| shm-native | `--topology shm --workers 2` (POSIX) | `GET /ping` → `b"pong"`; exit `0` |
| worker-only | `--worker-only` | `GET /livez` → `{"status":"ok"}`; exit `0`; no `vertebra-server` child |
| Failure surfacing | Corrupt `smokeapp.py` (manual check) | Script exits `1` and prints the captured `vtb` output |

## Verification

- Initial Red baseline: `make core-wheel-check`
- Final Green + lint:
  ```bash
  make core-wheel-check
  uv run --no-sync ruff check tools/scripts/smoke_vtb_run.py
  uv run --no-sync mypy tools/scripts/smoke_vtb_run.py
  uvx --from check-jsonschema check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/ci.yml
  ```
- Planned local commit: `test(wheel): smoke-test vtb run from an installed core wheel`

## Acceptance Criteria

- [ ] `make core-wheel-check` builds the wheel with the binary and passes all four scenarios from a clean venv (R17).
- [ ] CI runs `core-wheel-smoke` on Linux x86_64, Linux aarch64, and macOS (R17).

## Notes & Discoveries

[^makefile]: `Makefile` — `postgres-wheel-check` and `oracle-wheel-check` target patterns.
[^ci-yaml]: `.github/workflows/ci.yml` — `changes`, `lint`, `test-python`, and `test-rust-features` jobs.
[^server-conftest]: `src/py/tests/integration/server/conftest.py::launch_server` — Raw-socket streaming probe pattern from `ch03:t04`.
