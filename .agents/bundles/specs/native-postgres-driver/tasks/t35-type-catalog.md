---
type: Task
id: native-postgres-driver:t35
title: Session type discovery and format negotiation
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t34
files:
- src/rs/vertebra-postgres/src/types.rs
- src/rs/vertebra-postgres/src/codecs/mod.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/tests/type_catalog.rs
tests:
- src/rs/vertebra-postgres/tests/type_catalog.rs
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
- R15
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Session type discovery and format negotiation

## Objective

Resolve extension, array, composite and domain metadata before binary requests when a safe discovery boundary exists.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 380 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED scripted public TypeCatalog tests for built-ins, user arrays, nested composite fields, domains over built-in/array types, extension vector/halfvec OIDs, dropped attributes, reconnect and DDL invalidation, recursive types and hostile catalog sizes. Test unknown OIDs stay text without an unsafe speculative decoder.

2. Implement public types.rs::{TypeCatalog,TypeInfo,CompositeField}. Seed built-ins, discover missing OIDs using pg_catalog.pg_type/pg_namespace/pg_attribute/pg_class/pg_extension through PostgresConnection::simple_query only when no extended exchange is pending. A simple Query destroys the unnamed prepared statement: mark that local generation invalid on every Query emission, never promise unnamed-state survival across catalog discovery or an idle Sync through a transaction pooler. Catalog SQL interpolates only validated u32 OID literals or uses established typed query APIs when available; never concatenate identifiers/user SQL. Preserve ordered non-dropped composite field name/OID/typmod and array element OID; domains retain base OID and own identity.

3. Resolve transitive element/attribute/base types with bounded work/depth and cycle detection. Cache per physical session/generation, namespace-aware extension identity and relevant search_path/schema generation. Expose discover, type_info, invalidate and snapshot_for_batch. The later t40 ordinary-query helper resolves custom parameter metadata before Parse, or discovers result OIDs after the original Execute has fully completed and its raw text fields are owned; no Parse→catalog Query→Bind sequence is permitted. Do not run metadata queries from the receive loop, during COPY, while a portal owns the session, or inside a single-Sync pipeline batch.

4. Extend format_for_oid and decode/encode_value to use catalog entries with per-column fallback. First-use pipeline columns with undiscovered custom OIDs remain exact text with OID metadata; ordinary query can resolve custom parameter types before Parse or normalize already-owned text results after ReadyForQuery through t40. Catalog discovery after execution must never be followed by Bind to the destroyed unnamed statement. A cold DDL-dependent pipeline is never pre-described outside its transaction. Schema mismatch invalidates only stale metadata and raises without replaying side effects.

5. Export concrete catalog types for Arrow/COPY consumers and commit feat(postgres): discover session types and negotiate formats.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test type_catalog
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11, R15: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Dynamic OIDs, arrays, composites, domains and extension names resolve under bounded discovery.
- [ ] Discovery never inserts an extra exchange into a cold pipeline or an active streaming exchange.
- [ ] Metadata invalidation and unknown-text fallback are explicit and session-local.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
