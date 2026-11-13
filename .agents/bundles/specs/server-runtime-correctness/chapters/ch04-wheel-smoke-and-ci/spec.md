---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch04-wheel-smoke-and-ci
title: "Chapter 04: Wheel Binary Isolation Smoke Test and CI Matrix"
description: "Verify clean-venv core wheel installation and vtb run execution locally via make core-wheel-check, in PR CI, and across the multi-platform release wheel matrix."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch03-relocatable-server-binary-and-packaging
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - wheel
  - smoke
  - ci
  - packaging
sources:
  - id: makefile
    resource: Makefile
    title: Existing companion wheel isolation check targets
  - id: ci-yaml
    resource: .github/workflows/ci.yml
    title: Pull request CI verification jobs
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Multi-platform cibuildwheel and test_wheels workflow
  - id: release-binaries-yml
    resource: .github/workflows/release-binaries.yml
    title: Standalone server binary release matrix
---

# Chapter 04: Wheel Binary Isolation Smoke Test and CI Matrix

<!-- truth: start -->
- `tools/scripts/smoke_vtb_run.py` (`SmokeScenario`, `VtbRunSmoke`) installs a built `vertebra-core` wheel into an isolated virtual environment and exercises `vtb run` across native HTTP, progressive ASGI streaming, POSIX SHM, and `--worker-only` health probes [^makefile].
- `.github/workflows/ci.yml` runs `core-wheel-smoke` on pull requests so wheel packaging or rpath regressions fail before merge [^ci-yaml].
- `.github/workflows/release.yaml` wires core-only `build_server_binary.py` build/repair hooks and runs `smoke_vtb_run.py` across Linux `x86_64` and `aarch64`, macOS, and Windows, while `release-binaries.yml` adds `linux-arm64` ([#82](https://github.com/cofin/vertebra/issues/82)) [^release-yaml] [^release-binaries-yml].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Even after `ch03` includes `vertebra/bin/vertebra-server` in `[tool.maturin]`, nothing verifies that a clean-venv wheel install outside the source repository can execute `vtb run` across embedded, streaming, SHM, and `--worker-only` modes, and `release-binaries.yml` still lacks a `linux-arm64` target [^makefile] [^ci-yaml] [^release-yaml] [^release-binaries-yml].

## 2. Requirements, Concrete Contracts & Architecture

- **`R6d` (`t01`)**: Implement `SmokeScenario` and `VtbRunSmoke` in `tools/scripts/smoke_vtb_run.py` (verifying `vertebra/bin/vertebra-server[.exe]` archive presence, free-port probing, embedded native route, progressive ASGI SSE chunk timing, POSIX SHM `--workers 2`, and `--worker-only` `/healthz` + SIGTERM drain) and add `make core-wheel-check` to `Makefile`.
- **`R6e` (`t02`)**: Add a `core-wheel-smoke` job to `.github/workflows/ci.yml` building the frontend bundle, staging `vertebra-server` via `build_server_binary.py build`, building the wheel (`uv build --wheel`), installing it into a clean `uv venv` in `/tmp`, and running `smoke_vtb_run.py`.
- **`R6f` (`t03`)**: Update `.github/workflows/release.yaml` with core-package `CIBW_BEFORE_BUILD` and `CIBW_REPAIR_WHEEL_COMMAND_{LINUX,MACOS}` hooks plus matrix `test_wheels` (`ubuntu-latest`, `ubuntu-24.04-arm`, `macos-latest`, `windows-latest`) running `smoke_vtb_run.py`, and add `linux-arm64` (`ubuntu-24.04-arm`) to `.github/workflows/release-binaries.yml`.

## 3. Documentation & OKF Knowledge Plan

`make core-wheel-check` and platform release matrix details are documented in `ch07` (`docs/usage/packaging-and-deployment.md`, `docs/getting-started/installation.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R6d` | `t01` | `tools/scripts/smoke_vtb_run.py`, `Makefile` | `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` |
| `R6e` | `t02` | `.github/workflows/ci.yml` | workflow YAML validation + `test_packaging_and_pgo.py` |
| `R6f` | `t03` | `.github/workflows/release.yaml`, `.github/workflows/release-binaries.yml` | workflow YAML validation + `test_packaging_and_pgo.py` |

## 5. Implementation Plan

- [ ] [t01-vtb-run-wheel-smoke-harness.md](tasks/t01-vtb-run-wheel-smoke-harness.md) — Create `tools/scripts/smoke_vtb_run.py` and wire `make core-wheel-check` in `Makefile`
- [ ] [t02-pr-ci-core-wheel-smoke-job.md](tasks/t02-pr-ci-core-wheel-smoke-job.md) — Add `core-wheel-smoke` clean-venv verification job to `.github/workflows/ci.yml`
- [ ] [t03-release-wheel-matrix-and-arm64-binaries.md](tasks/t03-release-wheel-matrix-and-arm64-binaries.md) — Wire cibuildwheel binary hooks + matrix smoke tests in `release.yaml` and `linux-arm64` in `release-binaries.yml`

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 30s`)**: `uv run --no-sync ruff check tools/scripts/smoke_vtb_run.py && uv run --no-sync mypy tools/scripts/smoke_vtb_run.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q` (full `make core-wheel-check` runs in `/flow:finish`).

[^makefile]: `Makefile` — Existing companion wheel isolation check targets.
[^ci-yaml]: `.github/workflows/ci.yml` — Pull request CI verification jobs.
[^release-yaml]: `.github/workflows/release.yaml` — Multi-platform cibuildwheel and test_wheels workflow.
[^release-binaries-yml]: `.github/workflows/release-binaries.yml` — Standalone server binary release matrix.
