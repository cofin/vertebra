---
type: Task
id: native-valkey-driver:ch04-conformance-and-docs:t02
flow_id: native-valkey-driver
chapter_id: ch04-conformance-and-docs
title: Wire Valkey 9.0 Live Conformance and CI Service
description: Enforce fail-loud vertebra._valkey checks on CACHE_ENGINE_SPECS (valkey) in test_driver_conformance.py and add Valkey 9.0 CI job.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch04-conformance-and-docs:t01
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
  - valkey
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

# Task `t02`: Wire Valkey 9.0 Live Conformance and CI Service

## Objective & Scope Boundary

Wire `vertebra._valkey` fail-loud checks on `CACHE_ENGINE_SPECS` (`valkey`) in `test_driver_conformance.py` add `valkey` to `ConformanceGate.SUPPORTED_ENGINES`, and add a `valkey` `test-db-conformance` matrix entry (Valkey `8.0+`/`9.0`) plus `make valkey-test` and `make valkey-wheel-check` to `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^conformance-suite] [^ci-wf].

## Bounded Read Manifest

- `src/py/tests/integration/db/test_driver_conformance.py` — `CACHE_ENGINE_SPECS` and missing-wheel tests.
- `.github/workflows/ci.yml` — `test-db-conformance` matrix (`make db-conformance ENGINE=… VERSION=…`); the file has no `services:` blocks, and the lane uses the existing `pytest-databases` `valkey_service` fixture via `src/py/tests/integration/conftest.py`.

## Contracts & Edge Cases

- Verify missing `vertebra._valkey` raises `MissingDependencyError` naming `vertebra-core[valkey]`.
- Add a `valkey` entry (Valkey `8.0+`/`9.0`) to the `test-db-conformance` matrix and `valkey` to `ConformanceGate.SUPPORTED_ENGINES` (`conformance_gate.py` + `test_conformance_gate.py`).
- **Edge Case**: Offline conformance subset (`not docker`) executes cleanly inside sandboxed environments without live containers.

## Test Matrix

- Run `pytest src/py/tests/integration/db/test_driver_conformance.py -k 'valkey'`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k 'valkey' -q
```

## Acceptance Criteria

- [ ] `CACHE_ENGINE_SPECS` (`valkey`) and `.github/workflows/ci.yml` enforce Valkey native conformance.
- [ ] Zero regressions in the fast verification gate.

[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database and cache driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — CI service containers and driver test gates.
