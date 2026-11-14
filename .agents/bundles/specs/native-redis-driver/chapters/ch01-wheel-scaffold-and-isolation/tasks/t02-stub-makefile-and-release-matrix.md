---
type: Task
id: native-redis-driver:ch01-wheel-scaffold-and-isolation:t02
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Author _redis.pyi Stub, Makefile Targets, and Release Matrix
description: Create src/py/vertebra/_redis.pyi and wire Makefile redis-test/redis-wheel-check and .github/workflows/release.yaml.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 104fd6fa
depends_on:
- native-redis-driver:ch01-wheel-scaffold-and-isolation:t01
files:
- src/py/vertebra/_redis.pyi
- Makefile
- .github/workflows/release.yaml
tests: []
read_set:
- src/py/vertebra/_mysql.pyi
- Makefile
- .github/workflows/release.yaml
estimated_loc: 145
tags:
- redis
- stubs
- makefile
- ci
sources:
- id: mysql-pyi
  resource: src/py/vertebra/_mysql.pyi
  title: Standalone extension stub precedent
- id: makefile
  resource: Makefile
  title: Polyglot build, lint, stubtest, and wheel-check targets
- id: release-wf
  resource: .github/workflows/release.yaml
  title: Release wheel and sdist build matrix
---

# Task `t02`: Author _redis.pyi Stub, Makefile Targets, and Release Matrix

## Objective & Scope Boundary

Create `src/py/vertebra/_redis.pyi` and wire `vertebra-redis` into `Makefile` and `.github/workflows/release.yaml` per [`../spec.md` §2](../spec.md) [^mysql-pyi] [^makefile] [^release-wf].

## Bounded Read Manifest

- `src/py/vertebra/_mysql.pyi` — Stub conventions.
- `Makefile` — `dev`, `build`, `lint-all`, `mysql-test`, and `mysql-wheel-check` recipes.
- `.github/workflows/release.yaml` — Wheel, sdist, and `test_wheels` matrix.

## Contracts & Edge Cases

- Add `vertebra._redis` to `Makefile` `dev`, `build`, `lint`, `lint-all`, and `py-test` `stubtest`, plus `redis-test` and `redis-wheel-check`.
- Verify both `vertebra-core[redis]` and `vertebra-core[valkey]` wheel installs in `.github/workflows/release.yaml`.
- **Edge Case**: `redis-wheel-check` asserts `_redis.*` and `vertebra/_redis.pyi` are present and `vertebra/*.py` + sibling `.pyi` stubs are absent.

## Test Matrix

- `make dev` installs `vertebra._redis` and `mypy.stubtest vertebra._redis` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._redis --concise
```

## Acceptance Criteria

- [ ] `Makefile` and `release.yaml` build and verify `vertebra-redis` cleanly.
- [ ] Zero regressions in the fast verification gate.

[^mysql-pyi]: `src/py/vertebra/_mysql.pyi` — Standalone extension stub precedent.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
