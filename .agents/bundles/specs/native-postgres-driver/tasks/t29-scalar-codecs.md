---
type: Task
id: native-postgres-driver:t29
title: Scalar binary and text value codecs
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t28
files:
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/src/codecs/scalar.rs
- src/rs/vertebra-postgres/src/query.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/scalar_codecs.rs
tests:
- src/rs/vertebra-postgres/tests/scalar_codecs.rs
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

# Scalar binary and text value codecs

## Objective

Install checked encode/decode primitives for built-in scalars and safe per-column text fallback.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 380 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED golden send-format tests for bool,int2/int4/int8,float4/float8,text/varchar/bpchar/name,bytea and UUID. Cover integer bounds, negative zero, IEEE NaN/infinities, invalid fixed lengths/bools, malformed UTF8/UUID/bytea hex, escaped bytea text, NUL text and null-versus-empty fields. Compare independently supplied server fixtures rather than only encoder/decoder round trips.

2. Implement codecs/mod.rs::{CodecContext,decode_value,encode_value,format_for_oid}; scalar.rs uses pinned postgres_protocol::types helpers where their checked behavior matches. Context carries current ParameterStatus values and bounded recursion/field/frame limits. Known supported OIDs choose binary; unknown OIDs request text and return PgValue::Text. Reject unexpected unknown binary data explicitly; never generic binary-to-UTF8.

3. Implement checked text parsing and canonical text encoding for all scalar variants, including bytea escape/hex input forms. Bind format is explicit per value and result format per column. OID zero is an unresolved parameter type, not a license to guess byte encodings. All field lengths/counts use checked conversions before allocation.

4. Integrate decode_value into the simple-query collector for recognized OIDs. Keep format discrimination attached to each Column, not one global result format. Expose exact binary scalar encoders for future COPY reuse. Commit feat(postgres): encode and decode native scalar values.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test scalar_codecs
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test query_protocol
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Golden binary and text results preserve values and reject malformed lengths.
- [ ] Simple-query typed scalar rows use the same dispatcher as later extended/COPY paths.
- [ ] Unknown text OIDs remain owned text; unknown binary OIDs fail explicitly.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
