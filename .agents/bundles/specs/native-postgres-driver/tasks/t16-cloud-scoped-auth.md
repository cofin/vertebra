---
type: Task
id: native-postgres-driver:t16
title: Create scope-correct cloud Admin and login credential caches
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t15
files:
- src/rs/vertebra-postgres/src/auth.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/credentials.rs
- src/rs/vertebra-core/tests/it/gcp_auth.rs
- src/rs/vertebra-storage/src/gcp_auth.rs
- src/rs/vertebra-core/src/gcp_auth.rs
- src/rs/vertebra-postgres/src/cloud/material.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-core/tests/it/gcp_auth.rs::explicit_login_scopes
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::cloud_scoped_token_sources
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::static_login_lifetime_and_scopes
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
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
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R5
- R9
---

# Create scope-correct cloud Admin and login credential caches

## Objective

Create scope-correct cloud Admin and login credential caches, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 390 lines including provider integration and pre-transmission guards; static parsing/source construction and their unit matrix are the completed t15 prerequisite; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.

Primary sources are official google-auth _client.refresh_grant and gcloud auth application-default print-access-token flags. Support AuthorizedUser and ADC CLI scoped paths; failures for unavailable grants/consent remain explicit. Native instance sharing is confined to this optional crate linkage.

## Steps

1. Add source-specific RED tests that inspect actual request payloads and CLI argv: metadata custom scope, service-account JWT scope, impersonation target scope, AuthorizedUser refresh scope, Gcloud ADC --scopes, forbidden unscoped fallback, optional response scope lacking requested grant, Admin/login separation and expiry. Testing cache construction alone cannot prove scope behavior.
2. Implement credentials.rs::ScopedTokenProvider as the shared scope-keyed native credential seam for both cloud and direct IAM; cloud/material.rs delegates to it rather than duplicating refresh logic. Provider construction stores owned primitive options only; call GcpTokenCache::from_options lazily during connect using spawn_blocking for ADC discovery/file reads, with its join awaited under the original deadline; a late result is discarded and never publishes a connection. Never initialize it in Python constructors. Construct separate native caches inside this optional crate from owned cloud_auth_options for Admin and designated iam_auth_options for login: Admin cloud-platform; Cloud SQL login https://www.googleapis.com/auth/sqlservice.login; AlloyDB login https://www.googleapis.com/auth/alloydb.login. Metadata must receive explicit custom scopes, not only constructor default_scopes. Preserve scope-aware service-account/impersonation paths. Existing Sync/AsyncGcpTokenCache Python classes are not extracted across independently linked cdylibs; reuse common Rust implementation and primitive configuration only.
3. Extend GcpAuthSource::AuthorizedUser and GcloudCli to retain requested scopes; update existing source literals in the canonical core integration tests and storage auth tests with empty scopes to preserve unscoped behavior. Extend AuthorizedUser refresh and send space-joined scope in OAuth refresh form, matching official google-auth refresh_grant. Parse optional response scope; a declared scope set missing requested login grants yields canonical authentication error. Google consent/refresh grant rejection propagates; never claim unavailable grant succeeded. Preserve existing unscoped source behavior for callers that request none.
4. For explicit scopes, GcloudCli uses `gcloud auth application-default print-access-token --scopes=comma,separated,list`. Do not fall back to `gcloud auth print-access-token`, which cannot request scopes. Preserve the old fallback only for unscoped callers. CLI inability or missing consent is a clear canonical auth/unsupported error, not scope downgrading. A missing iam_auth_options map derives the same credential source with required login scopes; an opaque static Admin token cannot be rescoped and fails if no designated login source exists. Static opaque login tokens must be supplied explicitly through iam_auth_options; do not infer grants from token bytes. Tests use a fake executable/HTTP fixture, never real accounts.
5. Consume the predecessor t15 StaticLoginCredential helper for every designated static login source; do not repeat its canonical timestamp/scope parsing or route through generic from_options. Its source construction stores Some(actual_supplied_expiry). Dynamic ADC sources continue through the scope-aware path above; unrelated unscoped legacy defaults stay unchanged. Represent resolved credentials as credentials.rs::ScopedLoginSnapshot, carrying the native token snapshot, required/declared scope provenance and effective cache refresh skew. ScopedLoginSnapshot::ensure_usable(now_ms) delegates the strict static policy when applicable and rejects any expired/inside-skew login snapshot. Call it immediately before password/SASL credential responses and token-bearing control-plane requests. The real AlloyDB metadata boundary is inside the current shared TransportStream::connect and is not reachable here: t18 adds the split shared TLS/metadata seam and invokes this same guard immediately after verified TLS, before metadata. Do not claim an earlier guard covers that later write. Equality, expiry or inside-cache-skew values fail terminally without transmission/replay, even if a slow TLS/API stage crosses the boundary after resolution.
6. Keep the canonical static_login_lifetime_and_scopes wire test focused on provider integration: designated static source preserves supplied snapshot expiry; invalid helper result causes zero password/SASL credential writes; a valid source crossing now+skew during held direct PostgreSQL TLS fails before the first credential response; repeated get_or_refresh never manufactures a new static lifetime. The full pure malformed/missing/scope/boundary matrix is t15's unit test and is not duplicated. This t16 test owns direct password/SASL boundaries only: the actual held-AlloyDB-TLS/zero-metadata integration is implemented and run in t18 after its new shared seam exists, never called forward from this task. Map provider/expiry/grant failures through native PostgresError::Auth to exact existing core GoogleCloudAuthError; actual PostgreSQL ErrorResponse SQLSTATE28 remains DatabaseError. Feed validated expiry into cloud material lifetime and redact credentials. Commit fix(postgres): request scope-correct cloud login tokens.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_scoped_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests static_login_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcp_auth
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5, R9: designated static login credentials have explicit matching scopes and actual canonical supplied expiry; expired/inside-skew/unknown provenance refuses before transmission with canonical core auth identity.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
