---
type: Task
id: "native-postgres-driver:t63"
title: "Define lossless PostgreSQL Arrow schemas"
description: "Define lossless PostgreSQL Arrow schemas for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t62"
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
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

# Define lossless PostgreSQL Arrow schemas

## Objective

Define lossless PostgreSQL Arrow schemas, with observable value, wire and ownership assertions through the named tests.

## Context

Read `../spec.md`, dependencies, protocol research, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md`, database driver/CDC knowledge and ADR-004. One worksheet produces one signed conventional local commit on the complete driver's shared branch and PR. No implementation starts with `plan_commit: null`. Preserve other changes; listed files are owned only after predecessors complete.

Estimated handwritten changes including tests: 360 lines. If the work cannot fit roughly400 lines, use `/flow:revise native-postgres-driver` to split its boundary while preserving complete one-PR scope. Do not move implementation into acceptance tasks.

## Steps

1. Assert exact Arrow DataType/metadata for empty/all-NULL results, duplicate column names, every scalar/nested type, typmods and unknown-OID text fallback. Add direct pure arrow-array/schema/buffer/ipc workspace dependencies at already locked60.1.0; no second major or Python FFI crate.
2. Implement `PostgresArrowSchema::from_columns(columns,catalog)`. Fields carry postgres.oid/typmod/type_name/encoding; schema postgres.arrow.version=1. Map bool/int16/32/64/float32/64/UTF8/Binary/UUID FixedSizeBinary16; JSON/JSONB validated UTF8 with type metadata. Numeric uses dedicated canonical decimal UTF8 retaining scale and specials, encoding=numeric-decimal; never generic str(value).
3. Full-range date is Struct(kind:Int8,value:Int32 PG days), timestamp/timestamptz Struct(kind:Int8,value:Int64 PG micros), kind -1/0/1 for negative infinity/finite/positive infinity. Temporal Struct also has nullable wire_text/date_style/time_zone UTF8 children; kind2 and null raw value preserve unresolved legacy DateStyle/zone-abbreviation source exactly, never an inferred epoch. time Int64 PG micros preserves24:00; interval Struct(months:Int32,days:Int32,microseconds:Int64). These are deliberately PostgreSQL full-range schemas, not Arrow standard temporal arrays.
4. Arrays Struct(dimensions:List<Int32>,lower_bounds:List<Int32>,values:List<elementtype>); composites ordered catalog Struct attrs; vector/halfvec FixedSizeList<Float32> with typmod dimension else List<Float32>; bits Struct(bytes:Binary,bit_length:Int32). Domain keeps base schema+domain OID; enum UTF8. Unknown OIDs require requested text format, never binary UTF8 guessing. Bound recursive catalog depth32/cycles with typed DataError before execution.
5. Commit `feat(postgres): define lossless Arrow schemas`.

## Verification

Write the named failing tests first. Initial RED is the absent named public method/module or wrong value, bytes, state or cleanup explicitly described above; record the actual failing assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests schema_
cargo tree --manifest-path src/rs/Cargo.toml -p vertebra-postgres -i arrow-array
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
