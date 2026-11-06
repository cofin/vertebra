---
type: Task
id: native-postgres-driver:t47
title: Owned Python value extraction and typed result projection
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t46
files:
- src/rs/vertebra-postgres/src/python_values.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/python_value_contract.rs
tests:
- src/rs/vertebra-postgres/tests/python_value_contract.rs
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
- R6
- R7
- R11
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Owned Python value extraction and typed result projection

## Objective

Convert the shared lossless value model to/from Python once for query, COPY, pipeline and Arrow inputs.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 440 handwritten changed lines including tests after the explicit vector/bit wrappers. The parent approves this cohesive single-module bridge and parameterized behavior table as one invocation near the approximate400-line heuristic; retain the estimate rather than hide test work or add indirection. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

The new Rust test exercises the production conversion seam directly and initializes Python with the same test embedding configuration established in the scaffold. Query facade methods are introduced only in t48; no method is added early solely to make this task testable.

## Steps

1. Write RED behavior tests in tests/python_value_contract.rs through production rlib python_values conversion functions, with an initialized interpreter and adapter type imports. Exercise every scalar/container/wrapper: arbitrary Decimal scale/specials under a hostile decimal Context, aware/naive datetime, time24/out-of-range/infinities, PgInterval mixed signs, PgArray bounds, typed composites, PgJson(None) versus SQL NULL, UUID, owned byte buffers and typed NULL/OID, PgBitString empty/partial-byte/padding/length invariants, and PgVector vector/halfvec kind/dimension/nonfinite/quantized values. Test extraction and projection for each wrapper both directly and nested in an array/composite. These are actual value-boundary contracts, not scanners or private structure snapshots.

2. Implement python_values.rs::{extract_parameters,extract_pg_value,pg_value_to_python,query_result_to_python}. Extract all owned data while attached, then detach for native work. Preserve integer widths/type hints, Decimal.as_tuple coefficient/scale/specials without rounding, timezone semantics, buffer ownership and recursive container limits. Lists/tuples are arrays only with inferred/explicit array metadata; an already-known vector/halfvec OID permits numeric-sequence convenience input. PgVector kind and PgBitString length/data are otherwise explicit typed wrappers, not ambiguous sequence/view cases. Check target-OID/kind conflicts and preserve exact half/f32-quantized result floats.

3. Project directly into adapter-owned frozen PostgresColumn/PostgresQueryResult/PgInterval/PgArray/PgComposite/PgTemporal/PgVector/PgBitString records through cached class constructors resolved by PyOnceLock; ordinary scalar values use Python primitives. QueryResult rows and metadata are immutable tuples; result conversion must occur after the operation guard releases Rust locks and must not commit an owning transaction. JSON hydration uses msgspec with exact integer semantics. SQL NULL projects to None; a top-level JSON/JSONB null projects to PgJson(None), inner JSON null remains None inside its JSON container, and other JSON values remain ordinary Python values. Test native SQL NULL versus JSON null versus [null], and rebind to preserve their distinct wire values. High-level adapter convenience projection uses the existing _project_public_row helper; this raw result mapper never collapses the marker.

4. Define PgTemporal raw PG-epoch units and special tags as in the full spec. Reject bool-as-int ambiguity deliberately before integer extraction. Unknown text OID remains str with OID in Column; PgParameter rebinds it explicitly. Validate mismatched wrapper metadata before network submission. TemporalText projects to PgTemporal with exact wire_text/context; it is not a plain str and every unresolved typed bind raises DataError before submission, including matching-context text/binary/pipeline/COPY paths. Callers explicitly resolve it or intentionally bind raw text. Keep engine types out of generic db modules and do not pass PyObject into Tokio workers.

5. Export the production Rust bridge module functions for client/COPY/pipeline FFI consumers; do not add Python-visible test-only conversion helpers or reference future query methods. Commit feat(postgres): bridge lossless native values to Python records.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test python_value_contract
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R6, R7, R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every result/bind path uses the same lossless value conversion implementation.
- [ ] Decimal precision, temporal range, interval months and array/composite metadata survive.
- [ ] All Python references are owned outside detached/Tokio work; no GIL reacquisition under session locks.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
