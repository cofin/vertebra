---
type: Task
id: "native-postgres-driver:t21"
title: "Refresh direct-host IAM passwords without changing transport policy"
description: "Refresh direct-host IAM passwords without changing transport policy for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:t20"
files:
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/credentials.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
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
  - iam
---

# Refresh direct-host IAM passwords without changing transport policy

## Objective

Preserve direct-host IAM through the complete native public constructor with fresh scoped login credentials and unchanged direct transport policy.

## Context

Read ../spec.md and ../contracts.md::Connection constructors, especially direct-host IAM. This is one invocation and one signed conventional local commit in the complete-driver PR, estimated300 handwritten lines including tests. Ownership is only listed files after t20/t16; preserve others' work. plan_commit null is not implementation approval. Near400 overflow requires worksheet revision, never removing direct IAM support.

## Steps

1. Add RED direct_iam_refresh using the existing auth scripted listener plus local OAuth/metadata fixture. Expired shared cache refreshes before a new physical connection; valid cache is reused while valid. Observe exact requested sqlservice.login scope by default, authoritative explicit iam_auth_options scopes including alloydb.login, and token used as PostgreSQL password even when config.password is also supplied. Share the same scoped provider instance within this optional crate for deterministic expiry tests; no cross-cdylib Rust cache extraction.
2. With enable_iam_auth and no cloud resource, session connect resolves credentials.rs::ScopedTokenProvider within the original ConnectAttempt deadline before ordinary TLS/startup. Use owned iam_auth_options if supplied; otherwise derive the same source from cloud_auth_options with default https://www.googleapis.com/auth/sqlservice.login. Explicit nonempty iam_auth_options scopes remain authoritative through existing GCP parsing semantics. A static Admin token cannot be reclassified as login unless explicitly designated in iam_auth_options. Lazy source/cache discovery occurs during connect only; constructors remain pure.
3. Feed the returned snapshot.access_token as password to the existing password/MD5/SCRAM engine without mutating caller config or exposing token. IAM supersedes any supplied/DSN-derived password. Direct TCP still uses configured SSLRequest/sslmode/certificate identity; no cloud Admin discovery, proxy certificate issuance, metadata exchange or implicit TLS downgrade occurs. Preserve the same Unix transport semantics when selected: token resolution does not add network TLS to a Unix socket.
4. direct_iam_grant_failure records rejected scope/consent, malformed token response and refresh deadline: connection fails canonically before PostgreSQL listener accepts, and no Admin API endpoint is contacted. direct_iam_authentication_failure returns server28P01/other SQLSTATE after one startup and proves terminal Closed, no token refresh/replay on that stream, and token/old password are redacted. Failed token acquisition also closes the reservation after runtime cleanup.
5. Commit feat(postgres): preserve direct-host IAM token authentication.

## Verification

Write named regression tests first; initial RED is ignored direct IAM, stale token or forbidden password/transport behavior. Then require exit0:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests direct_iam_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes
```

Record actual exits and server/token-request assertions during execution. These commands are planned, not passing evidence. No external repository write or workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R5: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Direct-host IAM uses fresh/default or explicitly scoped designated login credentials and supersedes supplied password.
- [ ] Existing direct TLS/Unix policy and total deadline remain effective, with no cloud Admin/certificate path.
- [ ] Expiry/grant/authentication failures close without same-stream retry or credential leakage.
- [ ] One local verified commit and synchronized worksheet/checklist complete the task.

## Notes & Discoveries

None.
