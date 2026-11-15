---
type: Task
id: native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t02
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Author _sqlite.pyi Stub, Makefile Targets, and Release Matrix
description: Create src/py/vertebra/_sqlite.pyi and wire Makefile sqlite-test/sqlite-wheel-check and .github/workflows/release.yaml.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 473e1f2e
depends_on:
  - native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t01
files:
  - src/py/vertebra/_sqlite.pyi
  - Makefile
  - .github/workflows/release.yaml
tests: []
read_set:
  - src/py/vertebra/_duckdb.pyi
  - Makefile
  - .github/workflows/release.yaml
estimated_loc: 135
tags:
  - sqlite
  - stubs
  - ci
  - makefile
sources:
  - id: duckdb-pyi
    resource: src/py/vertebra/_duckdb.pyi
    title: Standalone extension stub precedent
  - id: makefile
    resource: Makefile
    title: Polyglot build, lint, stubtest, and wheel-check targets
  - id: release-wf
    resource: .github/workflows/release.yaml
    title: Release wheel and sdist build matrix
verification_strategy: implementation_first
---

# Task `t02`: Author _sqlite.pyi Stub, Makefile Targets, and Release Matrix

## Objective & Scope Boundary

Author initial `src/py/vertebra/_sqlite.pyi` and wire `vertebra-sqlite` across `Makefile` and `.github/workflows/release.yaml` per [`../spec.md` §2](../spec.md) [^duckdb-pyi] [^makefile] [^release-wf].

## Bounded Read Manifest

- `src/py/vertebra/_duckdb.pyi` — Initial stub layout.
- `Makefile` — `dev`, `build`, `lint-all`, `duckdb-test`, and `duckdb-wheel-check` recipes.
- `.github/workflows/release.yaml` — `build_wheels`, `build_sdist`, and `test_wheels` matrix.

## Contracts & Edge Cases

- Add `vertebra._sqlite` to `Makefile` `dev`, `build`, `lint`, `lint-all`, and `py-test` `mypy.stubtest` lists, plus dedicated `sqlite-test` and `sqlite-wheel-check` targets.
- Add `vertebra-sqlite` wheel, sdist, and `vertebra-core[sqlite]` smoke-install steps to `.github/workflows/release.yaml`.
- **Edge Case**: `sqlite-wheel-check` verifies built wheel archives contain `_sqlite.*` and `vertebra/_sqlite.pyi` while excluding `vertebra/*.py` and sibling `.pyi` files.

## Test Matrix

- `make dev` installs `vertebra._sqlite` and `mypy.stubtest vertebra._sqlite` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._sqlite --concise
```

## Acceptance Criteria

- [ ] `Makefile` and `release.yaml` build, stubtest, and verify `vertebra-sqlite` identically to sibling wheels.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-pyi]: `src/py/vertebra/_duckdb.pyi` — Standalone extension stub precedent.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
