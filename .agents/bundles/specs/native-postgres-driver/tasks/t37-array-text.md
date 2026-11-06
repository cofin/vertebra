---
type: Task
id: native-postgres-driver:t37
title: Escaped multidimensional text arrays
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t36
files:
- src/rs/vertebra-postgres/src/codecs/array_text.rs
- src/rs/vertebra-postgres/src/codecs/text_container.rs
- src/rs/vertebra-postgres/src/codecs/array.rs
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/tests/array_text.rs
tests:
- src/rs/vertebra-postgres/tests/array_text.rs
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
requirements:
- R11
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Escaped multidimensional text arrays

## Objective

Decode cold/simple-query arrays with explicit bounds, quoting and exact NULL semantics.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED server fixtures for braces/commas/backslashes/quotes/whitespace, quoted NULL versus unquoted NULL, empty string, array_nulls output behavior, explicit [lower:upper] prefixes, multidimensional bounds, custom delimiter (box), escaped composite/JSON elements, and malformed nesting/ragged arrays.

2. Implement bounded text_container.rs quoted/escaped field scanning with a caller-specified delimiter and container grammar. Keep array/composite NULL rules separate. array_text.rs parses dimension prefixes, rectangular nesting and element text into PgArray via TypeCatalog and decode_value. Preserve zero-dimensional empty arrays and nonstandard lower bounds.

3. Implement canonical text array encoding with PostgreSQL-compatible escaping, dimension prefixes when bounds are nonstandard and element-specific delimiter from pg_type.typdelim. NULL and empty string receive different encodings. Resolve known built-in array element OIDs without extra queries; genuinely unknown custom array OIDs remain typed-column text until discovery.

4. Check text-versus-binary value equality against independent server fixtures, including nested composite element spellings consumed after t38. Expose the bounded quoted-field scanner to t38 without a public Python parsing API. Commit feat(postgres): decode escaped dimensioned text arrays.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_text
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Text arrays preserve bounds/rank and distinguish NULL, quoted NULL and empty string.
- [ ] Element delimiter metadata and escaping agree with PostgreSQL output.
- [ ] Shared text scanning is bounded and rejects malformed nesting.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
