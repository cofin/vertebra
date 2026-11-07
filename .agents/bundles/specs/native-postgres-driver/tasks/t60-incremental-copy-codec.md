---
type: Task
id: "native-postgres-driver:t60"
title: "Frame binary COPY rows incrementally"
description: "Frame binary COPY rows incrementally for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t59"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs
plan_revision: 2
plan_commit: cdb978d3
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

# Frame binary COPY rows incrementally

## Objective

Frame binary COPY rows incrementally, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 390 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Golden bytes cover zero rows/fields, NULL versus empty bytes, exact int2/int4/int8 widths, text/bytea/numeric/arrays and rows crossing CopyData boundaries. Feed every split position and bytewise. Reject critical/OID flags, extension overflow, invalid field counts/lengths, mismatched column count, oversize fields/rows, truncated header/body/trailer and bytes after trailer.
2. Implement private `copy.rs::{BinaryCopyDecoder,BinaryCopyEncoder}`. Decoder Header/Extension/FieldCount/FieldLength/FieldBytes/Done retains only one incomplete row. `push(&[u8])`, `next_row()` and `finish()` enforce one -1i16 trailer and checked arithmetic. Frame/row/field caps default16MiB, extension cap64KiB; validate before reserve. Reject critical bits16..31 including OID bit16; ignore reserved low flags per COPY format.
3. Encoder writes signature/flags0/extension0 once, checked i16 field counts and i32 nullable lengths, one trailer. Shared `encode_value(...,Format::Binary)` supplies exact PostgreSQL send widths. Never use DataRow framing for COPY or whole-buffer shared CPU helper for production streaming.
4. Commit `feat(postgres): frame incremental binary COPY`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_codec_tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 and record actual command, exit and observable assertion evidence. These are planned commands, not passing evidence. Native rebuild precedes Python tests. No external writes, tags, push or hosted dispatch are authorized.

## Acceptance Criteria

- [ ] R19: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every Steps contract and named negative/lifetime assertion passes.
- [ ] Buffer/queue/lease limits hold; no Python row/dict roundtrip or duplicated standard backend parser enters the native data path.
- [ ] Only owned work enters the atomic local commit; task state and parent checklist synchronize after verification.

## Notes & Discoveries

None.
