---
type: Task
id: native-redis-driver:ch04-queues-conformance-and-docs:t02
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: Wire Redis 8.0 Live Conformance and CI Service
description: Enforce fail-loud vertebra._redis checks on CACHE_ENGINE_SPECS (redis) in test_driver_conformance.py and add Redis 8.0 CI job.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch04-queues-conformance-and-docs:t01
files:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/conformance_gate.py
  - src/py/tests/unit/db/test_conformance_gate.py
  - Makefile
  - .github/workflows/ci.yml
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_conformance_gate.py
read_set:
  - src/py/tests/integration/db/test_driver_conformance.py
  - .github/workflows/ci.yml
estimated_loc: 110
tags:
  - redis
  - conformance
  - ci
sources:
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Database and cache driver conformance suite
  - id: ci-wf
    resource: .github/workflows/ci.yml
    title: CI service containers and driver test gates
verification_strategy: regression_tdd
---

# Task `t02`: Wire Redis 8.0 Live Conformance and CI Service

## Objective & Scope Boundary

Wire `vertebra._redis` fail-loud checks on `CACHE_ENGINE_SPECS` (`redis`) in `test_driver_conformance.py` add `redis` to `ConformanceGate.SUPPORTED_ENGINES`, and add a `redis` `test-db-conformance` matrix entry plus `make redis-test` and `make redis-wheel-check` to `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^conformance-suite] [^ci-wf].

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py` — `CACHE_ENGINE_SPECS` and missing-wheel tests; `CacheConformanceSpec` has no `native_extension_module` field and annotates `config_cls: type[RedisConfig]` / `sync_driver_cls: type[SyncRedisDriver]`, so add the field and widen the annotations (Valkey classes stop subclassing Redis in `ch03:t01`).
- `.github/workflows/ci.yml` — Database/cache service containers and driver test steps.

## Contracts & Edge Cases

- Verify missing `vertebra._redis` raises `MissingDependencyError` naming `vertebra-core[redis]`.
- Add a `redis` entry (Redis `8.0` / `7.2+`) to the `test-db-conformance` matrix in `.github/workflows/ci.yml` (`make db-conformance ENGINE=redis VERSION=…`); the lane uses the existing `pytest-databases` `redis_service` fixture via `src/py/tests/integration/conftest.py`, and `ci.yml` has no `services:` blocks.
- **Edge Case**: Offline conformance subset (`not docker`) executes cleanly inside sandboxed environments without live containers.

## Test Matrix

- Run `pytest src/py/tests/integration/db/test_driver_conformance.py -k 'redis'`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k 'redis' -q
```

## Acceptance Criteria

- [ ] `CACHE_ENGINE_SPECS` (`redis`) and `.github/workflows/ci.yml` enforce Redis native conformance.
- [ ] Zero regressions in the fast verification gate.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database and cache driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — CI service containers and driver test gates.
