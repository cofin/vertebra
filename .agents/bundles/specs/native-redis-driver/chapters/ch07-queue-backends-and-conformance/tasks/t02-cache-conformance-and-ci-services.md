---
type: Task
id: "native-redis-driver:ch07:t02"
flow_id: native-redis-driver
chapter_id: ch07-queue-backends-and-conformance
title: "Redis and Valkey Conformance Checks and CI Service Matrix"
description: "Add redis_service + valkey_service and make redis-test/redis-wheel-check to ci.yml and update test_driver_conformance.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch07:t01"
files:
  - .github/workflows/ci.yml
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
read_set:
  - src/py/tests/integration/db/test_driver_conformance.py
  - .github/workflows/ci.yml
estimated_loc: 125
tags:
  - redis
  - valkey
  - conformance
  - ci
sources:
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: "Database and cache driver conformance suite"
  - id: ci-wf
    resource: .github/workflows/ci.yml
    title: "CI service containers and driver test gates"
---

# Task `t02`: Redis and Valkey Conformance Checks and CI Service Matrix

## Objective & Scope Boundary

Wire `vertebra._redis` fail-loud checks on `CACHE_ENGINE_SPECS` (`redis` and `valkey`) in `test_driver_conformance.py` and add `redis_service` + `valkey_service`, `make redis-test`, and `make redis-wheel-check` to `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^conformance-suite] [^ci-wf].

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py` — `CACHE_ENGINE_SPECS` and missing-wheel tests.
- `.github/workflows/ci.yml` — Database/cache service containers and driver test steps.

## Contracts & Edge Cases

- Verify missing `vertebra._redis` raises `MissingDependencyError` naming `vertebra-core[redis]` and `vertebra-core[valkey]` respectively.
- Configure `redis_service` (Redis `8.0` / `7.2+`) and `valkey_service` (Valkey `8.0+`) in `.github/workflows/ci.yml`.
- **Edge Case**: Offline conformance subset (`not docker`) executes cleanly inside sandboxed environments without live containers.

## Test Matrix

- Run `pytest src/py/tests/integration/db/test_driver_conformance.py -k 'redis or valkey'`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k 'redis or valkey' -q
```

## Acceptance Criteria

- [ ] `CACHE_ENGINE_SPECS` and `.github/workflows/ci.yml` enforce both Redis and Valkey native conformance.
- [ ] Zero regressions in the chapter fast verification gate.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database and cache driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — CI service containers and driver test gates.
