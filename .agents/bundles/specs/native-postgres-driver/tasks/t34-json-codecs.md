---
type: Task
id: "native-postgres-driver:t34"
title: "JSON and versioned JSONB codecs"
description: "JSON and versioned JSONB codecs for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t33"
files:
  - src/rs/vertebra-postgres/src/codecs/json.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/tests/json_codecs.rs
tests:
  - src/rs/vertebra-postgres/tests/json_codecs.rs
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

# JSON and versioned JSONB codecs

## Objective

Distinguish JSON/JSONB from SQL NULL and validate the binary JSONB version marker.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 260 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED scalar/container/Unicode JSON fixtures covering JSON null versus SQL NULL, integer precision beyond f64, empty object/list, escaped characters, invalid UTF8/JSON, truncated JSONB, unknown version and bytewise-fragmented DataRow delivery.

2. Implement JSON OID114 and JSONB OID3802 encode/decode. JSON binary is UTF8 JSON; JSONB binary begins with exactly version1 followed by UTF8 JSON. Store owned validated JSON bytes in PgValue::Json, distinguishing SQL NULL at the row layer. Reuse existing checked JSONB header CPU helper only after tests prove parity.

3. Use strict JSON validation without converting numbers through f64. Python conversion in t47 uses msgspec JSON decoding into ordinary primitives, preserving large integer values; PgJson(value) is the explicit JSON input carrier, including PgJson(None) versus a typed SQL NULL; ordinary dict/list inputs select JSON only from actual target OID metadata. Explicit Decimal inside JSON is rejected unless the caller supplies an intentional JSON representation. COPY and bind reuse these exact encoders.

4. Select JSONB binary only for known OIDs and retain unknown extension text. Commit feat(postgres): validate JSON and JSONB wire values.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test json_codecs
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R11: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] JSON null and SQL NULL remain distinct until ordinary Python projection.
- [ ] JSONB version and payload validation fail explicitly.
- [ ] Large JSON integers are not rounded by an intermediate float.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
