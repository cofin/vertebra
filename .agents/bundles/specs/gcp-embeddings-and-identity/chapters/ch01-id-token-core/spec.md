---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch01-id-token-core
title: "Chapter 01: ID Token Core"
description: Rust GcpIdTokenCache with service-account, metadata-server, impersonation, gcloud CLI, and authorized-user ADC minting, per-audience single-flight caching, and the metadata-server step at the end of ambient ADC discovery.
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:35:00Z"
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Access-token cache, from_options resolution, fetch_from_source dispatch, and HTTP clients
  - id: core-auth-tests
    resource: src/rs/vertebra-core/tests/it/gcp_auth.rs
    title: Canonical gcp_auth integration tests with loopback HTTP servers and test RSA keys
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: GCS signing token-cache resolution with metadata-server retries
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options
    title: Python metadata-server heuristic duplicated from core ADC order
  - id: rust-idtoken-reference
    resource: https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html
    title: Official google-cloud-auth ID token flows used as the wire reference
tags:
  - gcp
  - identity
  - oidc
  - rust
---

# Chapter 01: ID Token Core

<!-- truth: start -->
- **Scope**: `ch01:t01` adds `src/rs/vertebra-core/src/gcp_identity.rs` (`GcpIdTokenCache`, `GcpIdTokenSnapshot`, `GcpIdTokenOptions`, `MetadataIdTokenFormat`, `validate_id_token_audience`) with service-account and metadata-server minting; `ch01:t02` adds impersonation (`generateIdToken`, any audience) and gcloud CLI minting and rejects static sources; `ch01:t03` ends ambient ADC discovery with the metadata server and deletes duplicated metadata heuristics; `ch01:t04` mints `authorized_user` ADC tokens through the OAuth refresh-token grant, bound to the credential `client_id` audience [^core-gcp-auth].
- **Credential Audiences**: service-account keys, the metadata server, and `target_service_account` impersonation mint any audience; `authorized_user` ADC mints only its `client_id` audience and fails other audiences with an error naming `target_service_account` impersonation and IAP `programmaticClients`.
- **Invariants**: one upstream fetch per audience at a time; cached snapshots always carry the requested `aud`; expiry and refresh come from JWT `exp` minus `refresh_skew_ms`; explicit credential options never fall through to ambient sources [^rust-idtoken-reference].
- **Verification Gate**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vertebra-core::gcp_auth` mints only OAuth access tokens. Calls to Cloud Run, Cloud Functions, and IAP-protected services need audience-bound OIDC ID tokens ([#113](https://github.com/cofin/vertebra/issues/113)). This chapter adds a per-audience ID token cache that reuses the credential source resolved by `GcpTokenCache::from_options`, so access tokens and ID tokens follow one precedence order [^core-gcp-auth].

`GcpTokenCache::from_options` also stops ambient discovery after the ADC file and token variables, which forces callers (`VertexEmbedder`, `OracleEmbeddingPushdown`, `GCSConfig`, GCS signing) to inject `use_metadata_server` heuristics. Ending ambient discovery with the metadata server removes those heuristics [^py-gcs-config] [^storage-signing].

### 1.2 Non-Goals

- Python bindings (`ch02`).
- ID token signature verification or JWKS fetching.
- `external_account` (Workload Identity Federation) sources.

### 1.3 Upstream Baseline & Code Analysis

- `GcpTokenCache` holds a private `source: GcpAuthSource`, `client`, and `metadata_client`; `fetch_from_source` is private and recursive for impersonation. `t01` adds crate-visible accessors without changing existing behavior.
- `ServiceAccountJwtClaims` (private) carries `iss`, `sub`, `scope`, `aud`, `iat`, `exp`; ID token assertions need an extra `target_audience` claim and no `scope`, so `gcp_identity.rs` defines its own claim struct.
- The `AuthorizedUser` arm of `GcpTokenCache::fetch_from_source` already posts the `refresh_token` grant to `token_uri` (default `DEFAULT_OAUTH_TOKEN_URI = https://oauth2.googleapis.com/token`) and falls back to the gcloud CLI on failure; the ID token path reuses the same form fields, takes the response `id_token`, and has no CLI fallback.
- `fetch_token_from_gcloud_cli` runs `auth application-default print-access-token`; ID tokens need `auth print-identity-token --audiences=...` and get a sibling function in `gcp_identity.rs`.
- Canonical tests extend `src/rs/vertebra-core/tests/it/gcp_auth.rs`, which already provides loopback HTTP helpers and test RSA keys [^core-auth-tests].
- `jsonwebtoken = 11.1` provides `jsonwebtoken::dangerous::insecure_decode_claims`, which reads `exp` and `aud` without a new `base64` dependency.

Applicable guidance: [gcp-bigquery-and-spanner-drivers.md](../../../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md), [runtime-ipc SKILL.md](../../../../../skills/runtime-ipc/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```rust
// src/rs/vertebra-core/src/gcp_identity.rs
pub const DEFAULT_ID_TOKEN_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_MAX_ID_TOKEN_AUDIENCES: usize = 64;
pub const MAX_ID_TOKEN_AUDIENCE_BYTES: usize = 2_048;
pub enum MetadataIdTokenFormat { Standard, Full }          // default Full
pub struct GcpIdTokenOptions { pub refresh_skew_ms: u64, pub include_email: bool, pub metadata_format: MetadataIdTokenFormat, pub max_audiences: usize }
pub struct GcpIdTokenSnapshot { pub id_token: String, pub audience: String, pub expires_at_unix_ms: u64, pub generation: u64 }
pub struct GcpIdTokenCache { /* see t01 */ }
impl GcpIdTokenCache {
    pub fn new(credentials: Arc<GcpTokenCache>, options: GcpIdTokenOptions) -> Result<Arc<Self>, String>;
    pub fn from_options(raw_opts: &HashMap<String, String>) -> Result<Arc<Self>, String>;
    pub fn generation(&self) -> u64;
    pub fn refresh_skew_ms(&self) -> u64;
    pub fn get_cached(&self, audience: &str, now_unix_ms: u64) -> Option<Arc<GcpIdTokenSnapshot>>;
    pub fn needs_refresh(&self, audience: &str, now_unix_ms: u64) -> bool;
    pub fn invalidate(&self, audience: &str, failed_generation: u64);
    pub async fn get_or_refresh(&self, audience: &str, force_refresh: bool) -> Result<Arc<GcpIdTokenSnapshot>, String>;
}
pub fn validate_id_token_audience(audience: &str) -> Result<&str, String>;

// src/rs/vertebra-core/src/gcp_auth.rs (impl GcpTokenCache)
pub fn auth_source(&self) -> &GcpAuthSource;
pub(crate) fn http_client(&self) -> &reqwest::Client;
pub(crate) fn metadata_http_client(&self) -> &reqwest::Client;
pub(crate) async fn fetch_source_access_token(&self, source: &GcpAuthSource) -> Result<String, String>;
```

### Caller -> Seam -> Collaborator Map

- `GcpIdTokenCache::get_or_refresh` -> `validate_id_token_audience` -> fast-path `get_cached` -> per-audience `tokio::sync::Mutex` flight -> `fetch_id_token(source)` -> `ServiceAccount` (`t01`), `MetadataServer` (`t01`), `ImpersonatedServiceAccount` (`t02`, via `GcpTokenCache::fetch_source_access_token(base)`), `GcloudCli` (`t02`), `AuthorizedUser` (`t04`, audience check then `POST {token_uri}` refresh grant) -> `parse_id_token_claims` (`aud`, `exp`) -> insert with bounded eviction.
- `GcpTokenCache::from_options` (`t03`) -> explicit options -> ambient ADC file -> ambient token variables -> `GcpAuthSource::MetadataServer` (ambient allowed and no explicit credential keys).

### Edge-Case & Invariant Matrix

| Scenario | Input | Expected | Owner |
| --- | --- | --- | --- |
| Single flight | `8` concurrent requests for one audience | One upstream request | `t01` |
| Distinct audiences | `"https://a"` then `"https://b"` | Two upstream requests, two entries | `t01` |
| Skew window | Token `exp = now + 299s`, skew `300_000` | Cache miss, refresh | `t01` |
| Audience mismatch | JWT `aud = "https://other"` | `Err` containing `audience mismatch` | `t01` |
| Malformed token | Body `"not-a-jwt"` | `Err` containing `invalid ID token` | `t01` |
| Eviction | `3` audiences with `max_audiences = 2` | Earliest-expiring entry evicted | `t01` |
| Stale invalidate | `invalidate(aud, generation - 1)` | Entry kept | `t01` |
| Impersonation | Base metadata token + IAM `generateIdToken` | Body `{"audience":"https://svc","includeEmail":true}` with base bearer | `t02` |
| Static source | `Static` access token | `Err` containing `static access tokens cannot mint ID tokens` | `t02` |
| Impersonation any audience | Impersonated source, two audiences | Two `generateIdToken` calls, two entries | `t02` |
| User ADC client audience | `authorized_user` ADC, audience equal to `client_id` | Refresh grant without `scope`; snapshot from response `id_token` | `t04` |
| User ADC other audience | `authorized_user` ADC, audience `https://svc-abc-uc.a.run.app` | `Err` naming the `client_id`, `target_service_account`, and IAP `programmaticClients`; no request | `t04` |
| User ADC no `id_token` | Refresh response lacks `id_token` | `Err` containing `contained no id_token` | `t04` |
| User ADC impersonated | `authorized_user` base plus `target_service_account` | `generateIdToken` for any audience | `t04` |
| CLI timeout | gcloud stub sleeps past timeout | `Err` containing `timed out` | `t02` |
| Explicit missing key | `service_account_path = "/nonexistent.json"` with ambient ADC present | `from_options` returns `None` | `t03` |
| Ambient end | No options, no ADC, no token env, ambient allowed | `MetadataServer` source | `t03` |
| Ambient disabled | `allow_ambient_adc = "false"`, no options | `None` | `t03` |

### Alternatives Comparison

- A `get_id_token` method on `GcpTokenCache` would change the single-snapshot generation contract consumed by the `GcpTokenCacheCapsule` C-ABI; a separate cache keeps the capsule untouched.
- Probing the metadata server during `from_options` (as `google-auth` Python does) adds a blocking network call to construction; ending discovery with a lazy metadata source follows `google-cloud-auth` and fails on first fetch with a clear message.

## 3. Documentation & OKF Knowledge Plan

- `[0/0 files documented]` in this chapter; ID token and ADC documentation lands in `ch06:t01` (`docs/usage/storage-auth-and-signing.md`) and `ch06:t02`/`ch06:t03` (OKF).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical Test |
| --- | --- | --- | --- |
| R1 (service account, metadata), R2 | `t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_*` |
| R1 (impersonation, CLI, static rejection) | `t02` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_impersonated_*`, `::id_token_gcloud_*`, `::id_token_rejects_static` |
| R1 (authorized-user ADC), R2 | `t04` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_authorized_user_*` |
| R4 | `t03` | `regression_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::adc_*`, `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_gcs_config_ambient_adc_precedence_and_isolation` |

## 5. Implementation Plan

- [ ] t01 — ID token cache with service-account and metadata-server minting (tasks/t01-id-token-cache-and-direct-sources.md)
- [ ] t02 — Impersonated and gcloud CLI ID tokens with fail-loud static-source rejection (tasks/t02-impersonated-and-cli-id-tokens.md)
- [ ] t03 — Metadata-server end of ambient ADC discovery and duplicate removal (tasks/t03-adc-metadata-fallback-and-dedupe.md)
- [ ] t04 — Authorized-user ADC ID tokens through the OAuth refresh-token grant (tasks/t04-authorized-user-id-tokens.md)
- **Chapter Recovery Checkpoint**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage --all-targets -- -D warnings && make dev && uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/storage/test_signing.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/4` tasks completed.
- **Remaining**: `t01`, `t02`, `t03`, `t04` in order (`t02` and `t04` extend the module from `t01`; `t03` edits `gcp_auth.rs` after the `t01` accessors land; `t04` runs its ADC-file case against the `t03` resolution order).

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Access-token cache and credential resolution.
[^core-auth-tests]: `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Canonical integration tests for this chapter.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Duplicated metadata-server retries.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options` — Duplicated metadata-server heuristic.
[^rust-idtoken-reference]: `https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html` — Official ID token flows.
