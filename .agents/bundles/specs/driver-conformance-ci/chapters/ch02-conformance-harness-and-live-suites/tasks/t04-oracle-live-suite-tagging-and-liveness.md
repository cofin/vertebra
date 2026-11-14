---
type: Task
id: "driver-conformance-ci:ch02:t04"
flow_id: driver-conformance-ci
chapter_id: ch02-conformance-harness-and-live-suites
title: "Tag test_oracle_native.py for lane selection and verify AQ wait_timeout_seconds liveness"
description: "Attach driver_engine('oracle') to test_oracle_native.py, resolve config via vtb_oracle_config, and add a live AQ idle wait_timeout_seconds liveness test."
status: stable
state: completed
commit: 38e02b16
last_verified_commit: 38e02b16
last_verified_at: "2026-11-14T17:42:00Z"
verification_evidence: "pytest test_conformance_gate.py test_oracle_native.py + ruff + mypy clean"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_revision: 4
plan_commit: 37972f2a
depends_on:
  - "driver-conformance-ci:ch02:t01"
  - "driver-conformance-ci:ch01:t03"
files:
  - src/py/tests/integration/db/test_oracle_native.py
tests:
  - src/py/tests/unit/db/test_conformance_gate.py
read_set:
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/conformance_gate.py
  - src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 155
tags:
  - oracle
  - notifications
  - conformance
  - integration
sources:
  - id: oracle-native
    resource: src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config
    title: "Existing live Oracle native suite and DSN/Docker resolution"
  - id: oracle-notify-py
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: "OracleNotificationSource and OracleNotifyConfig(wait_timeout_seconds=...)"
---

# Task `t04`: Tag `test_oracle_native.py` for Lane Selection and Verify AQ `wait_timeout_seconds` Liveness

## Objective & Scope Boundary

Add `pytestmark = [pytest.mark.driver_engine("oracle")]` to `src/py/tests/integration/db/test_oracle_native.py`, route container fallback through `vtb_oracle_config` (`ServiceVersionMatrix` `oracle_23ai_service`), and add a live `OracleNotificationSource` test exercising `OracleNotifyConfig(wait_timeout_seconds=2)` idle liveness followed by notification delivery and sub-second `aclose()` per [`../spec.md` §2](../spec.md) [^oracle-native] [^oracle-notify-py].

## Bounded Read Manifest

- `src/py/tests/integration/db/test_oracle_native.py` — Existing Oracle live tests and fixture resolution.
- `src/py/tests/integration/{conftest,conformance_gate}.py` — `vtb_oracle_config` and `driver_engine("oracle")` selection.
- `src/py/vertebra/db/adapters/oracle/notify.py` — `OracleNotifyConfig` and `OracleNotificationSource`.

## Contracts & Edge Cases

- When `VERTEBRA_CONFORMANCE_ORACLE_DSN` is unset and `VERTEBRA_CONFORMANCE_DOCKER != "1"`, skip offline before requesting `vtb_oracle_config`; when `VERTEBRA_CONFORMANCE_DOCKER == "1"`, delegate to `request.getfixturevalue("vtb_oracle_config")`.
- Add a live test configuring `OracleNotifyConfig(wait_timeout_seconds=2)`, idling past one full `2s` liveness probe window on an open listener, publishing a message to prove the session stays live and delivers without duplicate/dropped frames, and asserting `aclose()` completes in `< 2.0s`.

## Test Matrix

- Unit assertion in `test_conformance_gate.py` verifying collected `test_oracle_native.py` items carry `item_engines == frozenset({"oracle"})`, plus clean offline skip execution of `test_oracle_native.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py src/py/tests/integration/db/test_oracle_native.py -q && uv run --no-sync ruff check src/py/tests/integration/db/test_oracle_native.py src/py/tests/unit/db/test_conformance_gate.py && uv run --no-sync mypy src/py/tests/integration/db/test_oracle_native.py
```

## Acceptance Criteria

- [x] Every test in `test_oracle_native.py` is selected under `VERTEBRA_CONFORMANCE_ENGINES=oracle` and skips cleanly offline without Docker calls.
- [x] Live AQ `wait_timeout_seconds=2` idle liveness and post-idle delivery are covered with clean queue teardown.

[^oracle-native]: `src/py/tests/integration/db/test_oracle_native.py::_resolve_oracle_config` — Existing live Oracle native suite and DSN/Docker resolution.
[^oracle-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — `OracleNotificationSource` and `OracleNotifyConfig(wait_timeout_seconds=...)`.
