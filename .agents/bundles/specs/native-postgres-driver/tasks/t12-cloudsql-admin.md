---
type: Task
id: native-postgres-driver:t12
title: Discover Cloud SQL addresses and issue scoped ephemeral certificates
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t11
files:
- src/rs/vertebra-postgres/src/cloud/mod.rs
- src/rs/vertebra-postgres/src/cloud/cloudsql.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::cloudsql_admin_contract
plan_revision: 1
plan_commit: null
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
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R9
---

# Discover Cloud SQL addresses and issue scoped ephemeral certificates

## Objective

Discover Cloud SQL addresses and issue scoped ephemeral certificates, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 370 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add mock Admin RED assertions for exact GET/POST paths, Admin bearer versus login token separation, real public PEM, IAM-only access_token body field, selected IP type, URI region mismatch, non-PostgreSQL engine, missing CA/leaf and effective expiry.
2. GET https://sqladmin.googleapis.com/sql/v1beta4/projects/{project}/instances/{instance}/connectSettings. Decode serverCaCert.cert, ipAddresses[{type:PRIMARY|PRIVATE,ipAddress}], region, databaseVersion and DNS metadata. Region must match parsed URI; databaseVersion must name PostgreSQL. PUBLIC selects PRIMARY, PRIVATE selects PRIVATE; PSC uses advertised DNS metadata and fails if unavailable. Never silently switch requested network type.
3. POST the same instance path with :generateEphemeralCert and body {public_key:real_public_PEM,access_token:login_token} for IAM, omitting access_token in password mode. Authorization uses separate Admin cloud-platform/sqlservice.admin token; login token requires sqlservice.login. Decode ephemeralCert.cert, validate issued key/leaf/signed expiry, and bound lifetime by IAM snapshot expiry. No invented clientCert/private-key response fields.
4. Return prepared CloudSql target default3307, resolved address, server CA, issued client identity and metadata DNS/exact project:instance CN policy for t14. Cache only complete validated material. Commit feat(postgres): issue Cloud SQL connection credentials.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloudsql_admin_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
