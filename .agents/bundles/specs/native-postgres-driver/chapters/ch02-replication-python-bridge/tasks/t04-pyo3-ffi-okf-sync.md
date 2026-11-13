---
type: Task
id: "native-postgres-driver:ch02-replication-python-bridge:t04"
flow_id: native-postgres-driver
chapter_id: ch02-replication-python-bridge
title: "Synchronize PyO3 FFI pattern, runtime gotcha, and worker dispatch OKF chapters"
description: "Update the 3 PyO3 FFI OKF chapters (each <= 200 lines) for vertebra._postgres, ShmCapsule, and PendingDeliveryGuard."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch02:t03"
files: 
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
tests: 
  - src/py/tests/unit/test_review_setup.py
read_set:
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
estimated_loc: 145
tags:
  - docs
  - okf
  - pyo3
  - postgres
sources:
  - id: ffi-okf
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: "PyO3 0.30.1 FFI and GIL discipline"
---

# Task `t04`: Synchronize PyO3 FFI pattern, runtime gotcha, and worker dispatch OKF chapters

## Objective & Scope Boundary

Synchronize the 3 PyO3 FFI OKF chapters and trim `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`) [^ffi-okf].

## Bounded Read Manifest

- 3 target OKF chapters + `py_async.rs` + `_postgres.pyi`.

## Contracts & Edge Cases

- Document `spawn_rust_db_task_with_attr_and_future`, `PendingDeliveryGuard`, and `_postgres` (`gil_used = false`).
- Every modified OKF file must be `<= 200` lines with `<= 40` truth-block lines and multi-line YAML `sources:`.

## Test Matrix

- `make review-check` and `pytest src/py/tests/unit/test_review_setup.py`.

## Verification

```bash
make review-check && uv run --no-sync pytest src/py/tests/unit/test_review_setup.py -q
```

## Acceptance Criteria

- [ ] All 3 OKF files are `<= 200` lines and pass `make review-check` with `0` issues.
- [ ] Zero regressions in `ch02-replication-python-bridge` fast verification gate.

[^ffi-okf]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline.
