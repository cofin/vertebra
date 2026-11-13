---
type: Task
id: "server-runtime-correctness:ch06:t03"
flow_id: server-runtime-correctness
chapter_id: ch06-runtime-native-cleanup
title: Bind _zero_copy Helpers Directly and Remove Spanner batch_write max_commit_delay_ms
description: Import the five native helpers directly from vertebra._vertebra in _zero_copy.py and drop the inert max_commit_delay_ms parameter from Spanner batch_write.
status: stable
state: completed
commit: db02a0b6
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/utils/serializers/_zero_copy.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
tests:
  - src/py/tests/unit/db/test_vector_wire_codecs.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
read_set:
  - src/py/vertebra/utils/serializers/_zero_copy.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
estimated_loc: 95
tags:
  - ffi
  - spanner
  - sqlspec
  - cleanup
sources:
  - id: zero-copy-py
    resource: src/py/vertebra/utils/serializers/_zero_copy.py
    title: "Zero-copy serializer helpers with _py_* fallbacks"
  - id: spanner-driver-py
    resource: "src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write"
    title: Spanner driver batch_write
---

# Task `t03`: Bind _zero_copy Helpers Directly and Remove Spanner batch_write max_commit_delay_ms

## Objective & Scope Boundary

Bind `_zero_copy.py` functions directly from mandatory `vertebra._vertebra` and remove inert `max_commit_delay_ms` from sync/async Spanner `batch_write` per [`../spec.md` §2](../spec.md) [^zero-copy-py] [^spanner-driver-py].

## Bounded Read Manifest

- `src/py/vertebra/utils/serializers/_zero_copy.py` & `_vertebra.pyi` — native helper bindings.
- `src/py/vertebra/db/adapters/spanner/driver.py` & `extensions/sqlspec/spanner.py` — `batch_write` signatures.

## Contracts & Edge Cases

- Import `hydrate_one_vectorcall`, `hydrate_rows_vectorcall`, `instantiate_struct_vectorcall`, `orm_to_struct_vectorcall`, and `pack_grpc_lpm_frame` directly from `vertebra._vertebra` and delete all `_py_*` fallback functions in `_zero_copy.py`.
- Remove `max_commit_delay_ms` from `SyncSpannerDriver.batch_write`, `AsyncSpannerDriver.batch_write`, and SQLSpec sync/async Spanner `batch_write` (retaining `max_commit_delay_ms` on `apply_mutations`, `run_in_transaction`, and `SpannerConfig`).

## Test Matrix

- Tests in `test_vector_wire_codecs.py`, `test_spanner.py`, and `test_sqlspec_spanner.py` verifying native function identity on `_zero_copy` and `TypeError` when passing `max_commit_delay_ms` to sync/async `batch_write`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/utils/serializers/_zero_copy.py src/py/vertebra/db/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/spanner.py && uv run --no-sync mypy src/py/vertebra/utils/serializers/_zero_copy.py src/py/vertebra/db/adapters/spanner/driver.py src/py/vertebra/extensions/sqlspec/spanner.py && uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q
```

## Acceptance Criteria

- [ ] Zero `_py_*` fallbacks remain in `_zero_copy.py`.
- [ ] Sync and async Spanner `batch_write` reject `max_commit_delay_ms` with `TypeError`.

[^zero-copy-py]: `src/py/vertebra/utils/serializers/_zero_copy.py` — Zero-copy serializer helpers with _py_* fallbacks.
[^spanner-driver-py]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver.batch_write` — Spanner driver batch_write.
