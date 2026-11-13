---
type: Task
id: "server-runtime-correctness:ch04:t03"
flow_id: server-runtime-correctness
chapter_id: ch04-wheel-smoke-and-ci
title: "Wire Release Wheel Binary Hooks, Multi-OS Smoke Matrix, and linux-arm64 Binaries"
description: "Configure core-only cibuildwheel build/repair hooks and multi-OS test_wheels smoke in release.yaml, and add linux-arm64 to release-binaries.yml."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch04:t01"
files:
  - .github/workflows/release.yaml
  - .github/workflows/release-binaries.yml
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - .github/workflows/release.yaml
  - .github/workflows/release-binaries.yml
  - tools/scripts/build_server_binary.py
  - tools/scripts/smoke_vtb_run.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 125
tags:
  - release
  - wheel
  - cibuildwheel
  - arm64
sources:
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Release cibuildwheel and test_wheels workflow
  - id: release-binaries-yml
    resource: .github/workflows/release-binaries.yml
    title: Standalone server binary release matrix
---

# Task `t03`: Wire Release Wheel Binary Hooks, Multi-OS Smoke Matrix, and linux-arm64 Binaries

## Objective & Scope Boundary

Wire core-wheel `vertebra-server` build/repair hooks and cross-platform `test_wheels` smoke execution in `.github/workflows/release.yaml`, and add `linux-arm64` (`ubuntu-24.04-arm`) to `.github/workflows/release-binaries.yml` ([#82](https://github.com/cofin/vertebra/issues/82)) per [`../spec.md` §2](../spec.md) [^release-yaml] [^release-binaries-yml].

## Bounded Read Manifest

- `.github/workflows/release.yaml` — `build_wheels` (`matrix.package.name`) and `test_wheels`.
- `.github/workflows/release-binaries.yml` — standalone binary build matrix.

## Contracts & Edge Cases

- Scope `CIBW_BEFORE_BUILD` (`build_server_binary.py build`) and `CIBW_REPAIR_WHEEL_COMMAND_{LINUX,MACOS}` (`build_server_binary.py repair`) strictly to the core `vertebra` package so companion DB/storage wheels remain unchanged.
- Expand `test_wheels` across `ubuntu-latest` (`x86_64`), `ubuntu-24.04-arm` (`aarch64`), `macos-latest`, and `windows-latest` executing `smoke_vtb_run.py` against built core wheels.
- Add `ubuntu-24.04-arm` (`aarch64-unknown-linux-gnu` / `linux-arm64`) entry to `release-binaries.yml`.

## Test Matrix

- Workflow contract assertions in `test_packaging_and_pgo.py` verifying core-scoped `CIBW_*` hooks, 4-OS `test_wheels` smoke matrix, and `linux-arm64` presence in `release-binaries.yml`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [ ] `release.yaml` builds, repairs (excluding `libpython`), and smoke-tests core wheels across Linux `x86_64`/`aarch64`, macOS, and Windows.
- [ ] `release-binaries.yml` includes `linux-arm64`.

[^release-yaml]: `.github/workflows/release.yaml` — Release cibuildwheel and test_wheels workflow.
[^release-binaries-yml]: `.github/workflows/release-binaries.yml` — Standalone server binary release matrix.
