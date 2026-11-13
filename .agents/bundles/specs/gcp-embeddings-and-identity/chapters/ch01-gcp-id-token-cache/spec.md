---
type: Spec
flow_id: gcp-embeddings-and-identity
chapter_id: ch01-gcp-id-token-cache
title: "Chapter 01: Audience-Bound GCP ID Token Cache and Python Surface"
description: "Implements per-audience single-flight OIDC ID token minting across all GCP ADC credential sources in vertebra-core, standardizes ambient ADC metadata fallback, and exposes SyncGcpIdTokenCache and AsyncGcpIdTokenCache in vertebra._vertebra and vertebra.storage."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - gcp
  - identity
  - oidc
  - pyo3
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: "Access-token cache, GcpAuthSource variants, and HTTP clients"
  - id: core-auth-tests
    resource: src/rs/vertebra-core/tests/it/gcp_auth.rs
    title: "Canonical gcp_auth integration suite with loopback HTTP servers and RSA keys"
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: "GCS signing token-cache resolution with redundant metadata retries"
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache
    title: "PyO3 access-token cache bindings pattern"
  - id: py-stub
    resource: src/py/vertebra/_vertebra.pyi
    title: "Native stub surface for GCP token caches"
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCSConfig token-cache options and vertebra.storage re-exports"
---

# Chapter 01: Audience-Bound GCP ID Token Cache and Python Surface

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `vertebra-core::gcp_auth` mints only OAuth2 access tokens and omits `MetadataServer` at the end of ambient ADC discovery [^core-gcp-auth]; callers duplicate `use_metadata_server` heuristics [^storage-signing] [^py-gcs-config].
- **Target Architecture**: `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache` ([#113](https://github.com/cofin/vertebra/issues/113)) mints and caches audience-bound OIDC ID tokens across `ServiceAccount`, `MetadataServer`, `ImpersonatedServiceAccount` (`generateIdToken`), `GcloudCli`, and `AuthorizedUser` ADC (refresh-token grant for `client_id` audience, never rejecting user ADC), while `src/rs/vertebra-py/src/gcp_identity.rs` exposes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` on `vertebra._vertebra` and `vertebra.storage` [^py-gcp-auth] [^py-stub].
- **Scope & Boundaries**: Covers `R1`–`R4` across `vertebra-core`, `vertebra-storage`, `vertebra-py`, `_vertebra.pyi`, and `vertebra.storage` (`4` compact tasks). Embedder wire and pushdown callers land in `ch02`–`ch03`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Service-to-service requests to Cloud Run, Cloud Functions, and IAP-protected endpoints require an audience-bound OIDC ID token (`Authorization: Bearer <id_token>`) rather than a scope-bound OAuth2 access token ([#113](https://github.com/cofin/vertebra/issues/113)). Reusing `GcpTokenCache::from_options` inside `vertebra-core` keeps credential precedence unified across Rust and Python while ending ambient ADC discovery at `GcpAuthSource::MetadataServer` so manual metadata fallbacks in `vertebra-storage/src/signing.rs` and `GCSConfig.to_token_cache_options` can be deleted [^core-gcp-auth] [^storage-signing] [^py-gcs-config]. Guidance: [gcp-bigquery-and-spanner-drivers.md](../../../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md) and [pyo3-ffi-and-batch-dispatch.md](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md).

## 2. Requirements, Concrete Contracts & Architecture

```rust
// src/rs/vertebra-core/src/gcp_identity.rs (feature = "gcp-auth")
pub const DEFAULT_ID_TOKEN_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_MAX_ID_TOKEN_AUDIENCES: usize = 64;
pub const MAX_ID_TOKEN_AUDIENCE_BYTES: usize = 2_048;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetadataIdTokenFormat { Standard, #[default] Full }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenOptions {
    pub refresh_skew_ms: u64,
    pub include_email: bool,
    pub metadata_format: MetadataIdTokenFormat,
    pub max_audiences: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenSnapshot {
    pub id_token: String,
    pub audience: String,
    pub expires_at_unix_ms: u64,
    pub generation: u64,
}

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

// src/rs/vertebra-core/src/gcp_auth.rs (added on impl GcpTokenCache)
pub fn auth_source(&self) -> &GcpAuthSource;
pub(crate) fn http_client(&self) -> &reqwest::Client;
pub(crate) fn metadata_http_client(&self) -> &reqwest::Client;
pub(crate) async fn fetch_source_access_token(&self, source: &GcpAuthSource) -> Result<String, String>;
```

```python
# src/py/vertebra/_vertebra.pyi & re-exported in vertebra.storage.__all__
@final
class GcpIdTokenSnapshot:
    def __new__(cls, *, id_token: str, audience: str, expires_at_unix_ms: int, generation: int) -> GcpIdTokenSnapshot: ...
    @property
    def id_token(self) -> str: ...
    @property
    def audience(self) -> str: ...
    @property
    def expires_at_unix_ms(self) -> int: ...
    @property
    def generation(self) -> int: ...

@final
class SyncGcpIdTokenCache:
    def __new__(cls, options: dict[str, str] | None = None, *, allow_ambient_adc: bool = True) -> SyncGcpIdTokenCache: ...
    def to_async(self) -> AsyncGcpIdTokenCache: ...
    @property
    def generation(self) -> int: ...
    @property
    def refresh_skew_ms(self) -> int: ...
    def needs_refresh(self, audience: str, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, audience: str, failed_generation: int) -> None: ...
    def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot: ...

@final
class AsyncGcpIdTokenCache:
    def __new__(cls, options: dict[str, str] | None = None, *, allow_ambient_adc: bool = True) -> AsyncGcpIdTokenCache: ...
    def to_sync(self) -> SyncGcpIdTokenCache: ...
    @property
    def generation(self) -> int: ...
    @property
    def refresh_skew_ms(self) -> int: ...
    def needs_refresh(self, audience: str, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, audience: str, failed_generation: int) -> None: ...
    def get_token(self, audience: str, *, force_refresh: bool = False) -> RustDbTaskAwaitable[GcpIdTokenSnapshot]: ...
```

- **Source & Audience Rules**:
  - `ServiceAccount`: signs RS256 JWT assertion (`iss`, `sub`, `aud = token_uri`, `target_audience = audience`, `iat`, `exp`, no `scope`) and exchanges `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer` for `{"id_token": ...}`.
  - `MetadataServer`: `GET {endpoint}/computeMetadata/v1/instance/service-accounts/{account}/identity?audience=<urlenc>&format={full|standard}` with `Metadata-Flavor: Google`.
  - `ImpersonatedServiceAccount`: fetches base access token via `fetch_source_access_token(base)` and posts `{"audience", "includeEmail"}` to `{iam_endpoint}/v1/projects/-/serviceAccounts/{sa}:generateIdToken` -> `{"token": ...}` (mints any audience, including when `base` is `AuthorizedUser`).
  - `GcloudCli`: runs `gcloud auth print-identity-token --audiences=<audience> --quiet` with timeout.
  - `AuthorizedUser`: never rejected at construction; when `audience == client_id`, posts `grant_type=refresh_token` (`client_id`, `client_secret`, `refresh_token`, no `scope`) to `token_uri` and extracts `id_token` (failing loud mentioning `contained no id_token` and `gcloud auth application-default login` if absent). Any other `audience` fails before network I/O with: `authorized_user credentials mint ID tokens only for audience '<client_id>' (the ADC client_id), not '<audience>'; set target_service_account to impersonate a service account for any audience, or add '<client_id>' to the IAP programmaticClients allowlist and request audience '<client_id>'`.
  - `Static`: rejected at `GcpIdTokenCache::new` / `from_options` (`static access tokens cannot mint ID tokens`).
  - Claims & Eviction: `jsonwebtoken::dangerous::insecure_decode` decodes unverified JWT `exp` and `aud` (`String` or `[String]`), rejecting expired tokens or `aud` mismatches; `max_audiences` evicts expired entries first, then earliest `expires_at_unix_ms`.

## 3. Documentation & OKF Knowledge Plan

User-facing credentials documentation and OKF updates for `GcpIdTokenCache` and ADC resolution land in `ch04` (`t01`–`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` (SA, metadata), `R2` (cache, single-flight, claims, eviction) | `t01` | `src/rs/vertebra-core/src/{gcp_identity,gcp_auth,lib}.rs` | `cargo test -p vertebra-core --test it gcp_auth::` |
| `R1` (impersonation, CLI, `authorized_user` ADC, static rejection), `R4` (ADC fallback & signing dedupe) | `t02` | `src/rs/vertebra-core/src/{gcp_identity,gcp_auth}.rs`, `src/rs/vertebra-storage/src/signing.rs` | `cargo test -p vertebra-core --test it gcp_auth::` |
| `R3` (PyO3 `gil_used = false` bindings) | `t03` | `src/rs/vertebra-py/src/{gcp_identity,lib}.rs` | `pytest test_gcs_token_refresh.py -k id_token` |
| `R3`, `R4` (`_vertebra.pyi` stubs, `vertebra.storage` exports, `GCSConfig` cleanup) | `t04` | `src/py/vertebra/{_vertebra.pyi,storage/_store.py,storage/__init__.py}` | `mypy.stubtest vertebra._vertebra` & `pytest test_gcs_token_refresh.py` |

## 5. Implementation Plan

- [ ] [t01-rust-id-token-cache-and-direct-sources.md](tasks/t01-rust-id-token-cache-and-direct-sources.md) — Add `gcp_identity.rs` with per-audience single-flight `GcpIdTokenCache`, JWT claim validation, and service-account + metadata-server minting.
- [ ] [t02-impersonation-cli-user-adc-and-fallback.md](tasks/t02-impersonation-cli-user-adc-and-fallback.md) — Add IAM `generateIdToken`, `gcloud`, and `authorized_user` ADC refresh-token ID minting, reject static sources, and finalize ambient ADC metadata fallback.
- [ ] [t03-pyo3-id-token-bindings.md](tasks/t03-pyo3-id-token-bindings.md) — Expose `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` on `vertebra._vertebra` with detached-GIL refreshes.
- [ ] [t04-id-token-stubs-and-storage-exports.md](tasks/t04-id-token-stubs-and-storage-exports.md) — Add `_vertebra.pyi` stubs, re-export ID token classes from `vertebra.storage`, and remove manual metadata-server heuristics from `GCSConfig`.

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 40s`)**:
  `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage -p vertebra-py --features vertebra-core/gcp-auth -- -D warnings && uv run --no-sync maturin develop --uv && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync ruff check src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q`

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Access-token cache, `GcpAuthSource` variants, and HTTP clients.
[^core-auth-tests]: `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Canonical `gcp_auth` integration suite with loopback HTTP servers and RSA keys.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — GCS signing token-cache resolution with redundant metadata retries.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PyAsyncGcpTokenCache` — PyO3 access-token cache bindings pattern.
[^py-stub]: `src/py/vertebra/_vertebra.pyi` — Native stub surface for GCP token caches.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — `GCSConfig` token-cache options and `vertebra.storage` re-exports.
