---
type: Task
id: native-postgres-driver:t75
title: Decode pgoutput relation metadata and lossless tuples
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t74
files:
- src/rs/vertebra-postgres/src/replication/pgoutput.rs
- src/rs/vertebra-postgres/src/replication/mod.rs
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs
- src/py/vertebra/db/adapters/postgres/types.py
tests:
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs::relation_and_tuple_values
- src/rs/vertebra-postgres/tests/postgres_pgoutput_tests.rs::malformed_pgoutput_is_bounded
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
- R22
---

# Decode pgoutput relation metadata and lossless tuples

## Objective

Decode pgoutput relation metadata and lossless tuples; verify returned events, wire boundaries and owner cleanup through the named tests.

## Context

Read `../spec.md`, predecessor worksheets, protocol research, database drivers/CDC knowledge, ADR-004 and local runtime-ipc/pyo3-bridge skills. One worksheet makes one signed conventional local commit on the complete driver's shared branch and PR. No implementation with `plan_commit: null`; preserve others' edits. Listed files are owned after dependencies complete.

Estimated handwritten changes including tests: 390 lines. If this exceeds roughly400 lines during execution, `/flow:revise native-postgres-driver` splits the boundary while retaining the full one-PR scope. Planned acceptance is never passing evidence.

## Steps

1. Golden proto-v1 Relation(R)/Type(Y) and TupleData fixtures cover namespace/name/replident, attrs key flag/name/OID/typmod, UTF8, n(NULL)/u(unchanged TOAST)/t(text)/b(binary). Assert distinction between missing attr, SQL NULL and unchanged TOAST. Reject unknown relation, width mismatch, malformed cstrings/lengths/tags, oversize/depth and incomplete terminal message on clean CopyDone. Transport EOF discards partial state for t78 replay, rather than misclassifying a reconnectable drop as permanent protocol failure. Feed every split position and multiple coalesced pgoutput messages.
2. Implement deep pgoutput.rs::PgOutputDecoder with bounded RelationCache keyed relation OID and type metadata; Relation updates replace schema atomically, never mutate a queued event's captured schema. Type messages retain namespace/name and invalidate stale catalog entry; configured relation-cache cap4096 fails explicitly on overflow. Incremental `push(payload)`/`next_record()` parses network-endian fields with checked cursor helpers, retains at most one bounded incomplete record, and consumes exact message prefixes while preserving coalesced successors; XLogData boundaries need not be treated as logical-record boundaries. no PostgreSQL SQL parser or duplicate row codec.
3. Tuple values decode through shared codecs using relation OIDs/typmods; binary=false requests text output, but decoder validates binary form using shared codec when present. Unknown OID text remains tagged Text+OID; known arrays/composites use catalog discovery on ordinary metadata session, not replication connection SQL midstream. Preload publication types before START, invalidate/cache-refresh at a safe bounded boundary when new metadata appears. Preserve key/old/new tuple role and dropped/missing attrs.
4. Keep unchanged TOAST as explicit adapter-owned PgUnchangedToast singleton/tag, never None or borrowed old value; define adapter types.py::PgUnchangedToast frozen value and a replication-only Rust PgOutputTupleValue::UnchangedToast variant, separate from SQL PgValue. Its converter in replication/mod.rs imports that singleton; do not make it a bindable SQL value. Commit `feat(postgres): decode pgoutput relations and tuples`.

## Verification

Add the named failing tests before behavior changes; record initial RED at the absent public seam or the wrong event, LSN, state or cleanup assertion. Then run:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_pgoutput_tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Require final exit0 with actual command/exit and asserted behavior recorded. Rebuild before Python tests; no stale-extension evidence. No external write, push, tags or hosted workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R22: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All listed normal, malformed, reconnect, backpressure and lifetime cases pass.
- [ ] Dedicated session ownership and bounded resources hold; feedback never acknowledges unprocessed data.
- [ ] One owned local commit is reviewable; worksheet state and spec checklist synchronize after verification.

## Notes & Discoveries

None.
