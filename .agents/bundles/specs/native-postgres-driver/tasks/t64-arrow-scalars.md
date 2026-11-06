---
type: Task
id: native-postgres-driver:t64
title: Build bounded native Arrow scalar batches
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t63
files:
- src/rs/vertebra-postgres/src/arrow.rs
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs::scalar_values_are_lossless
- src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs::batch_limits_and_empty_schema
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R20
---

# Build bounded native Arrow scalar batches

## Objective

Build bounded native Arrow scalar batches, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 390 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Assert real Arrow values: signed boundaries, float NaN, Unicode, NULL/empty bytea, UUID, huge/scaled/special numeric, JSON null versus SQL NULL, temporal infinity/out-of-Python range, time24:00 and mixed-sign/month-bearing intervals. Compare temporal kind/raw fields and exact decimal display scale.
2. Implement `PostgresArrowBatchBuilder` consuming raw DataRow fields through shared decode_value directly in Rust, appending columns immediately. Flush at requested rows(default65536) or aggregate value-buffer cap(default8MiB). A single value above cap raises typed DataError before reserve. Bound offsets/checked sizes; retain one incomplete row and one batch, never all QueryResult.rows.
3. Match t63 native builders/Struct validity. SQL NULL yields null parent; finite kind0 keeps exact PG epoch raw value; infinity stores raw0+sentinel kind. Exact wire fallback uses kind2/valueNULL plus captured wire_text/date_style/time_zone; tests verify all context and distinguish parent SQL NULL. Empty result publishes explicit zero-row batch with schema. Preserve ordered duplicate names without maps.
4. Tests observe row/byte boundaries across multiple produced batches; no source-shape assertions. No Python refs/callbacks/rows/dicts on Tokio workers. Commit `feat(postgres): build bounded native Arrow scalar batches`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
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
