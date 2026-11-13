---
type: Task
id: "server-runtime-correctness:ch04:t02"
flow_id: server-runtime-correctness
chapter_id: ch04-wheel-smoke-and-ci
title: Add core-wheel-smoke Job to Pull Request CI
description: Wire core-wheel-smoke in .github/workflows/ci.yml to build vertebra-core wheel with vertebra-server and run smoke_vtb_run.py in a clean venv.
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
  - .github/workflows/ci.yml
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - .github/workflows/ci.yml
  - Makefile
  - tools/scripts/smoke_vtb_run.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 65
tags:
  - ci
  - wheel
  - smoke
  - packaging
sources:
  - id: ci-yaml
    resource: .github/workflows/ci.yml
    title: Pull request CI workflow
---

# Task `t02`: Add core-wheel-smoke Job to Pull Request CI

## Objective & Scope Boundary

Add the `core-wheel-smoke` job to `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^ci-yaml].

## Bounded Read Manifest

- `.github/workflows/ci.yml` — existing Rust/Python/wheel-check CI jobs and caching.
- `Makefile` & `tools/scripts/smoke_vtb_run.py` — `make core-wheel-check` invocation.

## Contracts & Edge Cases

- Add `core-wheel-smoke` on `ubuntu-latest` setting up `uv` (shared-libpython CPython `3.12`), Bun (for TS client bundle), Rust toolchain, and running `make core-wheel-check`.
- Assert workflow YAML validity and `core-wheel-smoke` step wiring in `test_packaging_and_pgo.py`.

## Test Matrix

- Structural workflow test in `test_packaging_and_pgo.py` parsing `.github/workflows/ci.yml` and asserting `core-wheel-smoke` runs `core-wheel-check` / `smoke_vtb_run.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [ ] `.github/workflows/ci.yml` parses cleanly and runs `core-wheel-smoke` on PRs.
- [ ] Zero regressions in existing CI jobs.

[^ci-yaml]: `.github/workflows/ci.yml` — Pull request CI workflow.
