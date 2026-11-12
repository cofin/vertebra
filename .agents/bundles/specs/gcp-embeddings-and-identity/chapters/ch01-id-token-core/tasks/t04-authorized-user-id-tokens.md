---
type: Task
id: "gcp-embeddings-and-identity:ch01:t04"
chapter_id: ch01-id-token-core
title: Mint ID tokens from authorized_user ADC through the OAuth refresh-token grant
description: Implement AuthorizedUser minting in GcpIdTokenCache by posting a refresh_token grant to the credential token_uri and taking the returned id_token, bound to the ADC client_id audience, with an actionable audience-mismatch error that names impersonation and IAP programmaticClients.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t02"
read_set:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch01-id-token-core/spec.md
files:
  - src/rs/vertebra-core/src/gcp_identity.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
estimated_loc: 170
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
created_at: "2026-11-12T01:35:00Z"
updated_at: "2026-11-12T01:35:00Z"
commit: null
sources:
  - id: core-auth-source
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: AuthorizedUser variant (client_id, client_secret, refresh_token, quota_project_id, token_uri)
  - id: core-refresh-grant
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::fetch_from_source
    title: Existing refresh_token form encoding and x-goog-user-project header for access tokens
  - id: google-oauth-refresh
    resource: https://developers.google.com/identity/protocols/oauth2/native-app#offline
    title: Google OAuth 2.0 refresh-token grant at https://oauth2.googleapis.com/token
  - id: iap-programmatic-clients
    resource: https://cloud.google.com/iap/docs/sharing-oauth-clients#programmatic_access
    title: IAP programmaticClients allowlist for OAuth client IDs used as ID token audiences
tags:
  - gcp
  - identity
  - oidc
  - adc
  - rust
---

# Task t04: Mint ID Tokens from authorized_user ADC Through the OAuth Refresh-Token Grant

## Objective & Scope Boundary

Replace the interim `AuthorizedUser` arm of `GcpIdTokenCache::fetch_id_token` in `src/rs/vertebra-core/src/gcp_identity.rs` with real minting for credentials written by `gcloud auth application-default login` [^core-auth-source]:

- `AuthorizedUser { client_id, client_secret, refresh_token, quota_project_id, token_uri }` posts `grant_type=refresh_token`, `client_id`, `client_secret`, `refresh_token` as `application/x-www-form-urlencoded` to `token_uri` (default `https://oauth2.googleapis.com/token`) and takes the `id_token` field of the JSON response [^google-oauth-refresh].
- The token's `aud` is always the credential's `client_id`. Any other requested audience fails before network I/O with an error that names both fixes: `target_service_account` impersonation, or adding the `client_id` to IAP `programmaticClients` and requesting the `client_id` as the audience [^iap-programmatic-clients].
- Snapshots are cached and refreshed by the JWT `exp` claim minus `refresh_skew_ms`, through the `t01` cache path.

Out of scope: ADC order (`t03`), impersonation (`t02`; an `ImpersonatedServiceAccount` whose `base` is `AuthorizedUser` already mints for any audience there), Python bindings (`ch02`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache::fetch_id_token`, `::parse_id_token_claims`, `::GcpIdTokenCache::new` (from `t01`/`t02`).
- `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource`, `::GcpTokenCache::fetch_from_source` (`AuthorizedUser` arm: form fields and `x-goog-user-project`), `::GcpTokenCache::http_client`.
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` (loopback token-server helpers).
- Chapter spec.

## Symbol & Signature Contracts

```rust
#[derive(Deserialize)]
struct RefreshIdTokenResponse { id_token: Option<String> }

fn authorized_user_audience_error(client_id: &str, audience: &str) -> String;
```

`authorized_user_audience_error` returns exactly:

```text
authorized_user credentials mint ID tokens only for audience '<client_id>' (the ADC client_id), not '<audience>'; set target_service_account to impersonate a service account for any audience, or add '<client_id>' to the IAP programmaticClients allowlist and request audience '<client_id>'
```

`fetch_id_token` arm for `AuthorizedUser`:

1. `audience != client_id` -> `Err(authorized_user_audience_error(client_id, audience))`; no request.
2. Form body via `url::form_urlencoded::Serializer` with `client_id`, `client_secret`, `refresh_token`, `grant_type=refresh_token`, and no `scope` pair, so the refresh token's original `openid` grant applies; `x-goog-user-project: <quota_project_id>` when set; sent through `credentials.http_client()`.
3. Transport failure -> `Err("OAuth refresh ID token request failed: <detail>")`; non-2xx -> `Err("OAuth refresh ID token request returned HTTP <status>")`; missing or empty `id_token` -> `Err("OAuth refresh response contained no id_token; run gcloud auth application-default login again so the refresh token carries the openid scope")`.
4. The token passes `parse_id_token_claims(token, client_id, now)`, which yields `expires_at_unix_ms` from `exp`.

There is no gcloud CLI fallback on refresh failure; errors surface unchanged.

## Edge Cases & Error Semantics

- Audience equal to `client_id` mints; every other audience (including a Cloud Run URL) returns the mismatch error and leaves the cache and generation unchanged.
- An `invalid_grant` `400` (revoked refresh token) returns `OAuth refresh ID token request returned HTTP 400 Bad Request`; nothing cached.
- A token whose `aud` differs from `client_id` fails with `ID token audience mismatch` from `parse_id_token_claims`.
- Token text, refresh token, and client secret never appear in `Debug` output or error strings.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/gcp_auth.rs` (tests prefixed `id_token_authorized_user_`). A loopback `TcpListener` stands in for the OAuth token endpoint; sources come from `GcpTokenCache::new(GcpAuthSource::AuthorizedUser { token_uri: "http://127.0.0.1:<p>/token", .. })` passed to `GcpIdTokenCache::new`, and from an ADC file `{"type": "authorized_user", "client_id": "123-abc.apps.googleusercontent.com", "client_secret": "s", "refresh_token": "r", "token_uri": "http://127.0.0.1:<p>/token"}` through `GcpIdTokenCache::from_options`. Test JWTs use HS256 with secret `b"test"`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `id_token_authorized_user_refresh_grant` | `client_id = "123-abc.apps.googleusercontent.com"`; server answers `{"access_token": "at", "expires_in": 3599, "id_token": JWT(aud="123-abc.apps.googleusercontent.com", exp=now+3600)}`; `get_or_refresh("123-abc.apps.googleusercontent.com", false)` | One `POST /token`; form decodes to `{client_id: "123-abc.apps.googleusercontent.com", client_secret: "s", refresh_token: "r", grant_type: "refresh_token"}` with no `scope` key; snapshot `audience == client_id`, `expires_at_unix_ms == exp * 1000`, `generation == 1` |
| `id_token_authorized_user_quota_project_header` | Same with `quota_project_id = Some("qp")` | Request carries `x-goog-user-project: qp` |
| `id_token_authorized_user_cached_until_skew` | Token `exp = now + 3600`; second `get_or_refresh(client_id, false)`; then a token with `exp = now + 299` and a third call | Second call makes no request; the `299 s` token is refetched on the next call (skew `300_000`) |
| `id_token_authorized_user_audience_mismatch` | `get_or_refresh("https://svc-abc-uc.a.run.app", false)` | `Err` equal to `authorized_user credentials mint ID tokens only for audience '123-abc.apps.googleusercontent.com' (the ADC client_id), not 'https://svc-abc-uc.a.run.app'; set target_service_account to impersonate a service account for any audience, or add '123-abc.apps.googleusercontent.com' to the IAP programmaticClients allowlist and request audience '123-abc.apps.googleusercontent.com'`; server records `0` requests; `generation() == 0` |
| `id_token_authorized_user_missing_id_token` | Server answers `{"access_token": "at", "expires_in": 3599}` | `Err` containing `contained no id_token` and `gcloud auth application-default login` |
| `id_token_authorized_user_invalid_grant` | Server answers `400` `{"error": "invalid_grant"}` | `Err` equal to `OAuth refresh ID token request returned HTTP 400 Bad Request`; nothing cached |
| `id_token_authorized_user_from_adc_file` | ADC file above via `GOOGLE_APPLICATION_CREDENTIALS`, no `target_service_account`; `GcpIdTokenCache::from_options(&HashMap::new())` then `get_or_refresh(client_id, false)` | Construction succeeds; snapshot returned from the loopback token endpoint |
| `id_token_authorized_user_impersonated_any_audience` | Same ADC file plus `target_service_account = "svc@p.iam.gserviceaccount.com"`, `iam_endpoint = <loopback>`; `get_or_refresh("https://svc-abc-uc.a.run.app", false)` | Base refresh grant (access token) then `POST /v1/projects/-/serviceAccounts/svc@p.iam.gserviceaccount.com:generateIdToken` with body `{"audience":"https://svc-abc-uc.a.run.app","includeEmail":true}`; snapshot audience is the Cloud Run URL |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it id_token_authorized_user_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
```

Write the tests first (Red: the interim arm returns the later-task error), implement, re-run. Commit as `feat(core): mint ID tokens from authorized_user ADC`.

## Acceptance Criteria

- [ ] `authorized_user` ADC mints ID tokens through the refresh-token grant with the `client_id` audience (R1).
- [ ] Requests for any other audience fail before I/O with the exact two-fix error message (R1).
- [ ] Snapshots cache and refresh by JWT `exp` minus `refresh_skew_ms` (R2).
- [ ] Impersonation on an `authorized_user` base mints any audience (R1).

## Notes & Discoveries

- None yet.

[^core-auth-source]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — `AuthorizedUser` fields.
[^core-refresh-grant]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::fetch_from_source` — Refresh-grant form and quota header pattern.
[^google-oauth-refresh]: `https://developers.google.com/identity/protocols/oauth2/native-app#offline` — Refresh-token grant.
[^iap-programmatic-clients]: `https://cloud.google.com/iap/docs/sharing-oauth-clients#programmatic_access` — IAP `programmaticClients`.
