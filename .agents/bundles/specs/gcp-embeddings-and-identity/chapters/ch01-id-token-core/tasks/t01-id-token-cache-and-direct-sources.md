---
type: Task
id: "gcp-embeddings-and-identity:ch01:t01"
chapter_id: ch01-id-token-core
title: Cache audience-bound ID tokens minted from service-account keys and the metadata server
description: Add gcp_identity.rs with GcpIdTokenCache, GcpIdTokenSnapshot, GcpIdTokenOptions, MetadataIdTokenFormat, and validate_id_token_audience; mint from ServiceAccount and MetadataServer sources with per-audience single flight, JWT exp expiry, aud validation, generation invalidation, and bounded eviction.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - src/rs/vertebra-core/tests/it/main.rs
  - src/rs/vertebra-core/Cargo.toml
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch01-id-token-core/spec.md
files:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/src/lib.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
estimated_loc: 245
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
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Credential source, private HTTP clients, and fetch_from_source dispatch
  - id: core-auth-tests
    resource: src/rs/vertebra-core/tests/it/gcp_auth.rs
    title: Loopback HTTP helpers and TEST_RSA_PRIVATE_KEY_PEM fixtures
  - id: rust-idtoken-reference
    resource: https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html
    title: Service-account target_audience exchange and metadata identity endpoint
tags:
  - gcp
  - identity
  - rust
  - cache
---

# Task t01: Cache Audience-Bound ID Tokens Minted from Service-Account Keys and the Metadata Server

## Objective & Scope Boundary

Create `src/rs/vertebra-core/src/gcp_identity.rs` (behind `#[cfg(feature = "gcp-auth")]` in `lib.rs`) with the per-audience `GcpIdTokenCache` and two minting paths: `GcpAuthSource::ServiceAccount` and `GcpAuthSource::MetadataServer`. Add crate accessors to `GcpTokenCache` in `gcp_auth.rs` [^core-gcp-auth]. Other sources return `Err("ID token minting for <variant> is implemented in a later task")` only until a later task replaces that arm: `t02` owns impersonation, gcloud CLI, and static-source behavior, and `t04` owns authorized-user minting.

Out of scope: impersonation, gcloud CLI, static rejection messaging (`t02`), authorized-user minting (`t04`), ADC order changes (`t03`), Python bindings (`ch02`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` (fields `source`, `client`, `metadata_client`; `from_options`; `fetch_from_source`), `::GcpAuthSource`, `::ServiceAccountKey`, `::current_unix_ms`, `::DEFAULT_GCP_SCOPES`.
- `src/rs/vertebra-core/src/lib.rs` (module list, `#[cfg(feature = "gcp-auth")] pub mod gcp_auth;`).
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` (`read_http_request`, `TEST_RSA_PRIVATE_KEY_PEM`, `TEST_RSA_PUBLIC_KEY_PEM`) [^core-auth-tests].
- `src/rs/vertebra-core/tests/it/main.rs` (`#[cfg(feature = "gcp-auth")] mod gcp_auth;`).
- `src/rs/vertebra-core/Cargo.toml` (`jsonwebtoken`, `gcp-auth` feature).
- Chapter spec contracts.

## Symbol & Signature Contracts

```rust
// gcp_auth.rs additions inside impl GcpTokenCache
pub fn auth_source(&self) -> &GcpAuthSource { &self.source }
pub(crate) fn http_client(&self) -> &reqwest::Client { &self.client }
pub(crate) fn metadata_http_client(&self) -> &reqwest::Client { &self.metadata_client }
pub(crate) async fn fetch_source_access_token(&self, source: &GcpAuthSource) -> Result<String, String> {
    self.fetch_from_source(source, current_unix_ms()).await.map(|(token, _, _)| token)
}

// gcp_identity.rs
pub const DEFAULT_ID_TOKEN_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_MAX_ID_TOKEN_AUDIENCES: usize = 64;
pub const MAX_ID_TOKEN_AUDIENCE_BYTES: usize = 2_048;
const ID_TOKEN_ASSERTION_TTL_SECS: u64 = 3_600;
const JWT_BEARER_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetadataIdTokenFormat { Standard, #[default] Full }
impl MetadataIdTokenFormat { fn as_query_value(self) -> &'static str }   // "standard" | "full"

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenOptions { pub refresh_skew_ms: u64, pub include_email: bool, pub metadata_format: MetadataIdTokenFormat, pub max_audiences: usize }
impl Default for GcpIdTokenOptions  // 300_000, true, Full, 64

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenSnapshot { pub id_token: String, pub audience: String, pub expires_at_unix_ms: u64, pub generation: u64 }

pub struct GcpIdTokenCache {
    credentials: Arc<GcpTokenCache>,
    options: GcpIdTokenOptions,
    entries: std::sync::RwLock<HashMap<String, Arc<GcpIdTokenSnapshot>>>,
    flights: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    generation: AtomicU64,
}
impl std::fmt::Debug for GcpIdTokenCache   // prints refresh_skew_ms, generation, audience count; never tokens

impl GcpIdTokenCache {
    pub fn new(credentials: Arc<GcpTokenCache>, options: GcpIdTokenOptions) -> Result<Arc<Self>, String>; // rejects max_audiences == 0
    pub fn from_options(raw_opts: &HashMap<String, String>) -> Result<Arc<Self>, String>;
    pub fn generation(&self) -> u64;
    pub fn refresh_skew_ms(&self) -> u64;
    pub fn get_cached(&self, audience: &str, now_unix_ms: u64) -> Option<Arc<GcpIdTokenSnapshot>>; // hit iff now + skew < expires_at
    pub fn needs_refresh(&self, audience: &str, now_unix_ms: u64) -> bool;
    pub fn invalidate(&self, audience: &str, failed_generation: u64);   // removes only when generations match
    pub async fn get_or_refresh(&self, audience: &str, force_refresh: bool) -> Result<Arc<GcpIdTokenSnapshot>, String>;
    async fn fetch_id_token(&self, audience: &str) -> Result<String, String>;
    fn store(&self, snapshot: Arc<GcpIdTokenSnapshot>, now_unix_ms: u64);   // eviction
}
pub fn validate_id_token_audience(audience: &str) -> Result<&str, String>;  // trims; rejects empty, > 2048 bytes, control chars
fn parse_id_token_claims(token: &str, audience: &str, now_unix_ms: u64) -> Result<u64, String>; // returns expires_at_unix_ms

#[derive(Serialize)] struct IdTokenAssertionClaims<'a> { iss: &'a str, sub: &'a str, aud: &'a str, target_audience: &'a str, iat: u64, exp: u64 }
#[derive(Deserialize)] struct IdTokenExchangeResponse { id_token: String }
#[derive(Deserialize)] struct IdTokenClaims { aud: serde_json::Value, exp: u64 }
```

`from_options` parses `id_token_refresh_skew_ms` (u64), `include_email` (`true`/`false`/`1`/`0`), `metadata_id_token_format` (`full`/`standard`), `max_id_token_audiences` (usize > 0), returning `Err("invalid <key>: <value>")` on bad values, then builds credentials with `GcpTokenCache::from_options(raw_opts, DEFAULT_GCP_SCOPES)` and maps `None` to `Err("unable to discover GCP credentials for ID token minting (checked options, GOOGLE_APPLICATION_CREDENTIALS, ambient ADC, and the metadata server)")`.

Flow: `get_or_refresh` validates the audience, returns `get_cached` unless `force_refresh`, takes the per-audience flight mutex (created under `flights` lock), re-checks the cache, calls `fetch_id_token`, runs `parse_id_token_claims`, increments `generation`, stores, and returns. After the flight completes, the flight entry is removed when `Arc::strong_count == 1`.

Minting (wire shapes follow the official `google-cloud-auth` ID token flows [^rust-idtoken-reference]):
- `ServiceAccount { key, .. }`: RS256 assertion with `IdTokenAssertionClaims { iss: client_email, sub: client_email, aud: key.token_uri, target_audience: audience, iat: now_secs, exp: now_secs + 3600 }`; `POST key.token_uri` form `grant_type=JWT_BEARER_GRANT_TYPE&assertion=<jwt>` via `credentials.http_client()`; non-2xx -> `Err("service account ID token request returned HTTP <status>")`; JSON `id_token` field.
- `MetadataServer { service_account, endpoint, .. }`: `GET {endpoint}/computeMetadata/v1/instance/service-accounts/{service_account}/identity` with query `audience=<audience>` and `format=<full|standard>` built with `url::Url::query_pairs_mut`, header `Metadata-Flavor: Google`, via `credentials.metadata_http_client()`; non-2xx -> `Err("metadata server identity request returned HTTP <status>")`; body text trimmed.

## Edge Cases & Error Semantics

- `parse_id_token_claims`: `jsonwebtoken::dangerous::insecure_decode_claims::<IdTokenClaims>` failure -> `Err("invalid ID token: <detail>")`; `aud` string must equal the audience, or an array must contain it, else `Err("ID token audience mismatch: requested '<a>', token carries '<b>'")`; `exp * 1000 <= now` -> `Err("ID token already expired")`.
- Eviction in `store`: when inserting a new key and `entries.len() >= max_audiences`, drop entries with `now + skew >= expires_at`; if still full, drop the entry with the smallest `expires_at_unix_ms`.
- Failed fetches never insert or remove entries and never bump `generation`.
- `Debug` and error strings never include token text or private key material.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/gcp_auth.rs` (new tests prefixed `id_token_`). Dependency category: remote-owned endpoints replaced by loopback `TcpListener` servers. Test JWTs are built with `jsonwebtoken::encode` (HS256, secret `b"test"`) carrying `aud` and `exp`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `id_token_service_account_exchange_and_single_flight` | SA key JSON with `token_uri = http://127.0.0.1:<p>/token`; server answers `{"id_token": JWT(aud="https://svc.run.app", exp=now+3600)}`; `8` concurrent `get_or_refresh("https://svc.run.app", false)` | Server sees `1` POST whose form has `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`; decoded assertion (RS256 with `TEST_RSA_PUBLIC_KEY_PEM`) has `target_audience == "https://svc.run.app"`, `aud == token_uri`, no `scope`; all `8` snapshots share `generation == 1` and `expires_at_unix_ms == exp * 1000` |
| `id_token_metadata_identity_query` | `{"use_metadata_server":"true","metadata_host":"http://127.0.0.1:<p>"}`; audience `"https://svc a"` | Request line `GET /computeMetadata/v1/instance/service-accounts/default/identity?audience=https%3A%2F%2Fsvc+a&format=full`; header `metadata-flavor: Google`; snapshot `audience == "https://svc a"` |
| `id_token_metadata_standard_format` | Same plus `metadata_id_token_format = "standard"` | Query contains `format=standard` |
| `id_token_cache_distinct_audiences_and_skew` | Audiences `"https://a"` and `"https://b"`; then token for `"https://a"` with `exp = now + 299` | Two fetches for distinct audiences; a third `get_or_refresh("https://a")` with the `299 s` token refetches (skew `300_000`) |
| `id_token_cache_rejects_audience_mismatch_and_garbage` | Server returns JWT `aud="https://other"`, then body `"not-a-jwt"` | `Err` containing `audience mismatch`; then `Err` containing `invalid ID token`; `get_cached` stays `None` and `generation() == 0` |
| `id_token_cache_invalidate_and_eviction` | `max_id_token_audiences = "2"`; audiences `a` (exp +1000 s), `b` (exp +2000 s), `c` (exp +3000 s); `invalidate("b", 0)` then `invalidate("b", gen_b)` | After `c`, `get_cached("a")` is `None`; stale invalidate keeps `b`; matching invalidate removes `b` |
| `id_token_options_validation` | `include_email = "maybe"`; `max_id_token_audiences = "0"`; audience `""`; audience of `2049` bytes; audience `"https://a\n"` with embedded newline before suffix | `Err("invalid include_email: maybe")`; `Err` containing `max_id_token_audiences`; three `validate_id_token_audience` errors |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it id_token_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
cargo fmt --manifest-path src/rs/Cargo.toml --all -- --check
```

Write the `id_token_*` tests first and confirm they fail to compile (Red), implement, then run the commands above. Commit as `feat(core): cache audience-bound GCP ID tokens`.

## Acceptance Criteria

- [ ] `GcpIdTokenCache` mints from service-account keys and the metadata server with correct wire shapes (R1).
- [ ] Per-audience single flight, `exp`-based expiry with skew, `aud` validation, generation invalidation, and bounded eviction hold (R2).
- [ ] `GcpTokenCache` gains `auth_source`, `http_client`, `metadata_http_client`, and `fetch_source_access_token` without behavior changes to existing tests.
- [ ] No token or key material appears in `Debug` output or error strings.

## Notes & Discoveries

- [2026-11-12 00:35] `jsonwebtoken 11.1.0` exposes `dangerous::insecure_decode_claims`, so claim parsing needs no new dependency.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Source, clients, and fetch dispatch reused by the ID token cache.
[^core-auth-tests]: `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Canonical loopback test helpers.
[^rust-idtoken-reference]: `https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html` — Wire reference for both minting paths.
