---
type: Task
id: "native-postgres-driver:t15"
title: "Validate designated static login scope and actual lifetime"
description: "Validate designated static login scope and actual lifetime for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:t14"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/credentials.rs
tests:
  - src/rs/vertebra-postgres/src/credentials.rs
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
  - connection
  - credentials
---

# Validate designated static login scope and actual lifetime

## Objective

Reject static login credentials with unknown scope/lifetime and preserve the supplied expiry in the real native cache source.

## Context

Read ../spec.md, ../contracts.md, t14 cloud-identity prerequisites and ../contracts.md. This is one invocation and one signed conventional local commit in the complete-driver PR, estimated200 handwritten lines including tests. Ownership is only the listed files; t16 extends the same credentials.rs only after this task completes. Preserve all unrelated work. State is `pending` at `plan_revision: 2` and `plan_commit: cdb978d3`.

The deep policy seam is StaticLoginCredential; t16 adds ADC refresh providers and wire integration around this validated helper. Keep the core generic unscoped Static default unchanged; PostgreSQL strict login does not use that unknown-lifetime default.

## Steps

1. Add credentials.rs::tests::static_login_contract before behavior. Parameterize missing/empty token, missing scopes/expiry, malformed/overflow/noncanonical unsigned timestamp, required-scope mismatch, expired/inside-skew/exact-boundary and one millisecond beyond boundary. Include repeated cache refresh of the constructed static source; its actual snapshot expiry must remain the supplied value.
2. Register credentials.rs and implement crate-private StaticLoginCredential::from_options(options:&HashMap<String,String>,required_scopes:&[String],now_ms:u64,refresh_skew_ms:u64)->Result<Self,PostgresError>. It accepts a static token designated only through iam_auth_options using access_token or the existing bearer_token alias; if both are present they must match, otherwise fail canonically without printing either. Require expires_at_unix_ms and scopes. Timestamp is unsigned ASCII decimal fitting u64 with parsed.to_string()==raw: no sign, whitespace, leading zeros, fraction/exponent or overflow. Parse scopes with existing GCP comma/whitespace semantics, require a nonempty declared set containing every requested login scope and retain that provenance. Do not inspect opaque token contents or infer Admin grants.
3. ensure_usable(now_ms) requires now_ms.saturating_add(refresh_skew_ms)<supplied_expiry; equality/refresher-skew/expiry failure is native PostgresError::Auth. Use injected now values in tests; no sleeps. Add the Auth native category for credential-provider failures, redacting tokens and never fabricating SQLSTATE. The later FFI resolver maps it to exact existing core GoogleCloudAuthError; server SQLSTATE28 is a separate DatabaseError path.
4. Provide a source/cache construction method using GcpAuthSource::Static{access_token,expires_at_unix_ms:Some(supplied_expiry),quota_project_id}. Construct NativeGcpTokenCache with that exact source and the same effective refresh skew. Never call generic from_options for strict static login, never synthesize TTL/u64::MAX expiry, and never let get_or_refresh invent renewal. Snapshot tests prove supplied source expiry survives. A helper result carries the exact expiry/scopes and can re-run ensure_usable at transmission time in t16.
5. Commit fix(postgres): validate static login provenance and expiry. t16 consumes this helper and owns real pre-transmission guards/wire tests, without copying this parser or unit matrix.

## Verification

Initial RED is missing policy helper/Auth variant or the old lost-expiry/static-forever behavior. After implementation require exit0:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib static_login_contract
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Record actual assertions and exits during execution; these are planned commands. Do not count source checks as wire acceptance. No external write, dispatch, tag or push is authorized.

## Acceptance Criteria

- [ ] R5/R9: designated login requires explicit matching scope provenance and canonical actual supplied expiry.
- [ ] R5/R9: exact boundary/inside-skew rejects and source/cache preserves Some(actual expiry) without fabricated lifetime.
- [ ] Pure helper remains bounded near200 lines, with shared credentials.rs ownership ordered before t16.

## Notes & Discoveries

None.
