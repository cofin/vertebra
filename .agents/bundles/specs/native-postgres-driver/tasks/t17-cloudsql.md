---
type: Task
id: "native-postgres-driver:t17"
title: "Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens"
description: "Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t16"
files:
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
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
  - connection
  - native
---

# Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens

## Objective

Authenticate automatic Cloud SQL mTLS endpoints with IAM tokens, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 360 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.

Automatic control-plane material is supplied by t10, t11, t12, t13, t14, t15, t16. Physical cache instances are shared within this crate linkage only; using the same GcpTokenCache Rust implementation does not authorize extracting another extension module's Arc/PyRef.

## Steps

1. Add Cloud SQL mock mTLS/auth tests through automatic CloudConnector::prepare, not caller-supplied certificate material. Cover valid URI discovery, issued key/certificate, IAM token refresh, correct/incorrect server identity, failed Admin response and total deadline. Capture first PostgreSQL bytes to prove cloud immediate TLS sends no SSLRequest inside the protected stream.
2. cloud/mod.rs::connect_cloudsql(config, absolute_deadline) obtains fresh PreparedCloudMaterial from t10, t11, t12, t13, t14, t15, t16, then passes its typed EndpointTarget::CloudSql to shared TransportStream::connect. Mandatory Cloud SQL TLS1.3/identity verification is t14; default port3307. Normal direct TCP continues to use t05 SSLRequest. Never silently reroute cloud failure to direct TCP.
3. For IAM, use the material's scope-correct sqlservice.login snapshot as PostgreSQL password; password mode uses configured password without fetching a Cloud SQL login token. Admin cache uses owned cloud_auth_options; login cache uses designated iam_auth_options, or derives the same credential source with required login scopes when that map is absent. An opaque Admin static token without designated login credentials fails rather than becoming a login grant. Both caches are constructed inside this optional crate through the completed ScopedTokenProvider. Admin and dynamic login sources use its scope-aware provider path; designated static login sources use t15 StaticLoginCredential source construction, preserving Some(actual_supplied_expiry) and declared scopes, never generic from_options; they are not extracted from core PyO3 classes or passed across cdylib Rust layouts. Retain token generation only in Rust for invalidation and expiry. Failed auth is terminal, with no same-stream token retry.
4. Run the existing startup/auth engine with the same original total deadline and PendingSession publication guard. Add no second parser, runtime or Python callback. Commit feat(postgres): connect automatic Cloud SQL IAM endpoints.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloudsql_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5, R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
