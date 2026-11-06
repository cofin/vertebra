---
type: Task
id: native-postgres-driver:t39
title: Validated vector halfvec and bit send formats
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- native-postgres-driver:t38
files:
- src/rs/vertebra-db/src/postgres/mod.rs
- src/rs/vertebra-postgres/src/codecs/vector.rs
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/Cargo.toml
- src/rs/Cargo.lock
- src/rs/vertebra-db/Cargo.toml
- src/rs/vertebra-db/tests/postgres_vector_tests.rs
- src/rs/vertebra-postgres/tests/vector_codecs.rs
tests:
- src/rs/vertebra-db/tests/postgres_vector_tests.rs
- src/rs/vertebra-postgres/tests/vector_codecs.rs
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

# Validated vector halfvec and bit send formats

## Objective

Correct reusable pgvector CPU helpers and integrate dynamically discovered vector/halfvec/bit codecs.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

Use regression_tdd for this task because the shared CPU half/dimension behavior is an existing defect; the initial failure must demonstrate the wrong bytes or accepted invalid input, not merely a missing module.

## Steps

1. Write RED server send/recv fixtures exposing current midpoint rounding, invalid dimensions, zero/nonfinite vectors and finite-to-half overflow. Include ties-to-even, signed zero, subnormal boundaries, largest finite half, NaN/infinity rejection, unused header validation and truncated/trailing bytes. Use pgvector0.8.1 fixtures to retain PG14 support. Bits test valid zero-length bit/varbit, non-byte-aligned lengths and exact zero unused low padding; malformed nonzero padding is rejected rather than silently masked. Add codec↔PgBitString/PgVector contract cases through the shared native values, with Python wrapper projection tested in t47.

2. Narrowly repair existing vertebra_db::postgres::{encode_pgvector_f32,decode_pgvector_f32,encode_pgvector_f16,decode_pgvector_f16,encode_pgvector_bit,decode_pgvector_bit}. Enforce dimensions1..16000 inclusive for vector/halfvec and finite data before and after narrowing; do not substitute ANN-index dimension limits. Use half = "=2.7.1", already present in the resolved graph, as an explicit dependency of vertebra-db rather than another handwritten half converter; reject overflow before submission. Preserve public helper signatures. Permit standard bit/varbit length0; vector dimension restrictions do not apply to SQL bitstrings.

3. Implement codec adapters selected through TypeCatalog extension identity, not hard-coded dynamic vector OIDs. Text forms validate bracket/count/finite values and bit strings; binary headers/dimension/tail handling match real server fixtures. The exact Python result/input wrappers are PgVector(values:tuple[float,...],kind:Literal["vector","halfvec"]) and PgBitString(data:bytes,bit_length:int) from t27. Vector results are exact binary32 values widened to Python float; halfvec results are exact quantized binary16 values widened to Python float. Ordinary numeric sequences are only binding conveniences after the target OID is known; decoded results never become plain lists/tuples or unspecified views. Bytes remain bytea, not an implicit bitstring. Validate wrapper kind against actual dynamic target OID and reject mismatch before send.

4. Run existing CPU-helper tests as a regression lane and new native-crate tests against independent fixtures. Do not add sparsevec, which is outside the charter. Commit fix(postgres): match pgvector dimension and half conversion rules.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib postgres::tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --test postgres_vector_tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test vector_codecs
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Half rounding, finite overflow and extension dimension limits match server behavior.
- [ ] Existing public CPU helpers retain signatures and focused behavior coverage.
- [ ] Dynamic extension OIDs and text fallback share the validated dispatcher.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
