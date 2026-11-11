---
type: Task
id: "native-postgres-driver:t65"
title: "Preserve typed arrays composites and vectors in Arrow"
description: "Preserve typed arrays composites and vectors in Arrow for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t64"
files:
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
plan_revision: 2
plan_commit: 9af810f5
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - native
  - data-events
---

# Preserve typed arrays composites and vectors in Arrow

## Objective

Preserve typed arrays composites and vectors in Arrow, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 410 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Assert child values/validity for zero-dimensional arrays, NULL versus empty arrays/NULL elements, multidimensional negative lower bounds, NULL composite versus all-null attrs, nested arrays/composites, enums/domains and ordered attributes. Test typmod/dynamic vector dimensions, exact half values, bit lengths0/1/9, schema mismatch/depth/byte cap.
2. Extend recursive t64 builder. Arrays keep flat row-major values and dimensions/lower bounds, validate checked product=flat length and do not normalize lower bounds. Composite positions/OIDs follow catalog non-dropped attributes; append child nulls under null parent. Bound depth32 and aggregate bytes under existing cap.
3. FixedSizeList only with known dimension, otherwise List; reject inconsistent fixed dimension. halfvec Float32 values exactly represent half precision and metadata records source type. Bits retain bytes+length, validate unused low bits and byte count.
4. Complete multiple batches under one stable schema; arrow-ipc roundtrip must retain exact child data/validity. No opaque binary fallback for known nested catalog types. Commit `feat(postgres): build typed nested Arrow arrays`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests nested_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests vector_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R20: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
