---
type: Task
id: native-postgres-driver:t36
title: Dimensioned binary arrays with exact bounds
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t35
files:
- src/rs/vertebra-postgres/src/codecs/array.rs
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/tests/array_codecs.rs
tests:
- src/rs/vertebra-postgres/tests/array_codecs.rs
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
requirements:
- R11
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Dimensioned binary arrays with exact bounds

## Objective

Encode and decode PostgreSQL binary arrays without dropping rank, lower bounds, element OID or nulls.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 360 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED independent server fixtures for dimensions0/1/2/3, empty arrays, non-one/negative lower bounds, NULL/empty elements, NULL parent, arrays of numeric/temporal/JSON/UUID/composite placeholders and domains. Add signed-count, dimension product, has-null flag, element OID, field-length, truncation/trailing and recursion attacks.

2. Implement array.rs::{decode_array_binary,encode_array_binary} using TypeCatalog element metadata and the shared value dispatcher. Preserve flat ordered elements, dimensions and lower_bounds. Validate declared count/product before reserve, number of dimensions against PostgreSQL limits, i32 bounds and consistent element OID. Reject unsupported binary element types rather than converting bytes to text.

3. PgArray is the exact Python carrier for all decoded arrays. Binding ordinary rectangular lists/tuples remains a convenience: infer dimensions, use lower bounds1 and resolve element OID from declared parameter type; reject ragged shapes and all-null/untyped-empty ambiguity unless PgParameter supplies OID. Do not flatten nested composites as arrays.

4. Register all known/discovered array OIDs and prove nested element codec errors preserve the complete field path. Commit feat(postgres): preserve binary array dimensions and bounds.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_codecs
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Dimensions, lower bounds, nulls and element OIDs survive binary round trips.
- [ ] Convenience list input validates rectangular shape and typed ambiguity.
- [ ] Malformed array counts cannot trigger unbounded allocation.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
