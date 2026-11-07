---
type: Task
id: native-postgres-driver:t67
title: Convert Arrow batches into exact COPY field values
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t66
files:
- src/rs/vertebra-postgres/src/arrow.rs
- src/rs/vertebra-postgres/src/copy.rs
- src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs::arrow_import_roundtrip
- src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs::arrow_import_rejects_loss
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
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
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- native
- data-events
requirements:
- R20
---

# Convert Arrow batches into exact COPY field values

## Objective

Convert Arrow batches into exact COPY field values, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 420 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Roundtrip every t63 type: Arrow IPC -> COPY bytes -> shared codec, comparing PgValue dimensions/lower bounds, validity, numeric scale/specials, temporals/composites/vectors/bits. Import ordinary Arrow primitives/decimal/date/timestamp/list/struct against target OIDs. Overflow, precision/timezone loss and missing/duplicate/unknown columns fail during import-plan/first-batch validation before COPY starts; later-batch value failures abort its transaction.
2. `PostgresArrowImportPlan::from_schema(schema,target_columns,catalog)` validates mapping; encode_batch uses BinaryCopyEncoder and shared encode_value(Binary). Resolve target table metadata once through bound pg_catalog lookups; quote identifiers using double-quote escaping, reject NUL/unsupported multipart targets, never interpolate values.
3. Metadata version1 inverts all binary-representable t63 mappings exactly and requires compatible type identity; unresolved temporal kind2/wire_text cannot become binary COPY, so DataError occurs before sending that batch and aborts COPY (first-batch validation precedes COPY startup); dedicated numeric parser handles strings. Ordinary unsigned/signed narrowing is checked; timestamp unit conversion requires zero submicrosecond remainder, timestamptz UTC normalization, date epoch checks; reject timezone-aware->timestamp loss. Ordinary lists imply1D lower-bound1 arrays; Struct matches target attrs without missing/extra fields. Dictionary values resolve directly in Rust.
4. Validate each bounded batch before writing it; later failure aborts COPY-owned transaction. Commit `feat(postgres): encode typed Arrow batches for COPY`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
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
