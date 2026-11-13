---
type: Task
id: "server-runtime-correctness:ch04:t01"
flow_id: server-runtime-correctness
chapter_id: ch04-wheel-smoke-and-ci
title: Create smoke_vtb_run.py Wheel Smoke Harness and make core-wheel-check
description: Implement SmokeScenario and VtbRunSmoke in tools/scripts/smoke_vtb_run.py and add the core-wheel-check target to Makefile.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - tools/scripts/smoke_vtb_run.py
  - Makefile
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - Makefile
  - pyproject.toml
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 210
tags:
  - smoke
  - wheel
  - packaging
  - cli
sources:
  - id: makefile
    resource: Makefile
    title: Existing wheel-check Make targets
  - id: pyproject-toml
    resource: pyproject.toml
    title: Core package wheel configuration
---

# Task `t01`: Create smoke_vtb_run.py Wheel Smoke Harness and make core-wheel-check

## Objective & Scope Boundary

Implement `tools/scripts/smoke_vtb_run.py` and `make core-wheel-check` to verify wheel-installed `vtb run` outside the repo tree ([#82](https://github.com/cofin/vertebra/issues/82)) per [`../spec.md` §2](../spec.md) [^makefile] [^pyproject-toml].

## Bounded Read Manifest

- `Makefile` — `postgres-wheel-check` and `oracle-wheel-check` isolation target conventions.
- `src/py/tests/unit/build/test_packaging_and_pgo.py` — packaging unit test suite.

## Contracts & Edge Cases

- `SmokeScenario(msgspec.Struct, frozen=True, gc=False)` and `VtbRunSmoke(vtb_executable, workdir)` write a self-contained sample app in `workdir` (outside the repo), pick ephemeral ports, and verify four scenarios: embedded unary route, progressive ASGI SSE stream, POSIX-only SHM `--workers 2`, and `--worker-only` `/healthz`.
- `smoke_vtb_run.py --wheel <path>` also verifies `vertebra/bin/vertebra-server[.exe]` is present inside the wheel zip archive and zero `libpython*` shared libraries are bundled.
- `Makefile` `core-wheel-check` stages `vertebra-server` via `build_server_binary.py build`, builds the wheel into `/tmp/vtb-core-wheel`, installs into a clean `/tmp` `uv venv`, and executes `smoke_vtb_run.py`.

## Test Matrix

- Unit tests in `test_packaging_and_pgo.py` covering `VtbRunSmoke.default_scenarios()` (`posix_only` skip on `win32`), wheel zip member inspection (rejecting missing binary or bundled `libpython`), and early-exit stderr diagnostics.

## Verification

```bash
uv run --no-sync ruff check tools/scripts/smoke_vtb_run.py && uv run --no-sync mypy tools/scripts/smoke_vtb_run.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [ ] `smoke_vtb_run.py` passes `ruff`, `mypy`, and unit tests.
- [ ] `Makefile` defines `core-wheel-check` using isolated `/tmp` build and venv directories.

[^makefile]: `Makefile` — Existing wheel-check Make targets.
[^pyproject-toml]: `pyproject.toml` — Core package wheel configuration.
