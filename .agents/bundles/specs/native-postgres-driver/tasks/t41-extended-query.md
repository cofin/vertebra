---
type: Task
id: "native-postgres-driver:t41"
title: "Extended query parameter description and mixed formats"
description: "Extended query parameter description and mixed formats for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t40"
files:
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/tests/extended_protocol.rs
tests:
  - src/rs/vertebra-postgres/tests/extended_protocol.rs
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
  - driver
  - rust
---

# Extended query parameter description and mixed formats

## Objective

Execute parsed/bound statements with exact parameter OIDs, typed NULLs and per-column format selection.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED scripted transcripts for Parse/DescribeStatement/ParameterDescription/RowDescription/Bind/DescribePortal/Execute/Sync, empty parameter lists, typed NULLs, an unresolved NULL, binary scalars, unknown text columns and mixed known/unknown result formats. Verify statement Describe has text format metadata while portal Describe confirms negotiated formats. Include VACUUM or an equivalent real transaction-prohibited command with no emitted BEGIN, a stateful peer where every Query destroys unnamed state and every idle Sync may rotate backends, and cold cache0 custom results whose catalog lookup occurs only after Execute/ReadyForQuery. The complete live PgBouncer acceptance is t96.

2. Implement extended.rs::{PreparedDescription,Parameter,encode_bind,ExtendedQueryCollector} and PostgresConnection::query(sql,parameters,parameter_oids)->Result<QueryResult,PostgresError>. Parameter stores owned PgValue plus optional explicit type_oid; PgParameter(None,oid) supplies a typed NULL. Check count/OID/value compatibility and frontend i16 count/i32 field lengths before send. Reject multi-command Parse according to server rules.

3. Use t40 metadata.rs::{resolve_parameter_metadata,normalize_materialized_result,OwnedTextResult}. Before any Parse, resolve only custom PARAMETER metadata required to serialize a wrapper; built-in or inferable parameters use canonical text directly. The safe cold default emits Parse+DescribeStatement+Bind(TEXT parameters,all TEXT results)+DescribePortal+Execute+Sync in one group, with no preceding implicit BEGIN, no intermediate Sync and no catalog Query between Parse and Bind. Own the raw text fields and their effective CodecContext snapshots, finish/drain through ReadyForQuery, then optionally discover unknown result OIDs and decode those already-owned text fields into typed values. No prepared statement remains to Bind after that catalog Query. Unknown/unresolvable catalog OIDs remain exact text in their Column OID. An unresolved TemporalText/PgTemporal(special="wire_text") parameter raises DataError before submission in every format, even under matching captured context; callers explicitly resolve it or intentionally bind raw text.

4. Warm descriptions/type metadata may select known binary parameter/result formats within the same Parse/Bind/DescribePortal/Execute/Sync group. A capacity0 statement cache still sends a fresh unnamed Parse in every group; known local metadata is not evidence that an old unnamed statement survives. When metadata is missing use the cold all-text route, not a split preflight. Validate the fresh ParameterDescription count/OIDs and portal RowDescription formats before decode. Preserve canonical ambiguous-NULL SQLSTATE42P18. Never replay an executed query after normalization/catalog/schema failure. Pipeline cold submission stays its own all-text single-Sync path and does not invoke ordinary post-batch catalog normalization.

5. Close anonymous portal resources on completion and maintain transaction state through ReadyForQuery. A decode failure drains boundedly before deciding reuse; malformed framing breaks the connection. Commit feat(postgres): execute typed extended queries.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test extended_protocol
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R12: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Parameters, inferred OIDs and result formats match actual statement/portal descriptions.
- [ ] Unknown result OIDs request text; supported columns use independent binary formats.
- [ ] Typed NULL and ambiguous untyped NULL behavior is explicit and canonical.
- [ ] Cold execution has no implicit BEGIN or Parse→catalog→Bind window; VACUUM and cache0 custom first-use survive backend rotation.
- [ ] Parameter metadata precedes Parse; result discovery follows complete ReadyForQuery and uses owned fields/context without execution replay.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
