---
type: Task
id: "native-postgres-driver:t38"
title: "Typed ordered composite binary and text codecs"
description: "Typed ordered composite binary and text codecs for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t37"
files:
  - src/rs/vertebra-postgres/src/codecs/composite.rs
  - src/rs/vertebra-postgres/src/codecs/text_container.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/tests/composite_codecs.rs
tests:
  - src/rs/vertebra-postgres/tests/composite_codecs.rs
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
  - driver
  - rust
---

# Typed ordered composite binary and text codecs

## Objective

Preserve composite type identity, ordered field metadata and NULL semantics in both formats.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED server fixtures for named composite/record, nested composite/array/JSON/numeric, dropped attributes, empty quoted fields versus NULL, embedded parentheses/commas/backslashes/double quotes, NULL parent, NULL field and anonymous record OID2249. Add inconsistent field count/OID/length, recursion and trailing-byte rejection.

2. Implement composite.rs::{decode_composite_binary,encode_composite_binary,decode_composite_text,encode_composite_text}. Binary records carry each field OID; reconcile with known catalog ordered fields and retain actual field OIDs. Text known named composites use catalog order and the t37 bounded scanner with composite empty-unquoted=NULL rules, not array NULL rules.

3. Use PgComposite(type_oid,field_names,field_oids,values). For anonymous binary records, preserve transmitted OIDs and stable positional field names when no server names exist. Unknown named/anonymous text records without field metadata stay PgValue::Text at the outer Column OID; no guessed CSV decoding. Discovered named records support full binary/text send/receive.

4. Validate bind PgComposite metadata against the requested OID; reject stale/dropped-schema mismatches without implicit query replay. Recurse through the same dispatcher for arrays/composites and update catalog invalidation tests. Commit feat(postgres): encode typed PostgreSQL composites.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test composite_codecs
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_text
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Named composite identity, ordered names/OIDs and parent/field NULL distinctions survive.
- [ ] Quoted text and nested binary records use exact known metadata.
- [ ] Unknown record text and stale schemas fail/fallback explicitly without guessing.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
