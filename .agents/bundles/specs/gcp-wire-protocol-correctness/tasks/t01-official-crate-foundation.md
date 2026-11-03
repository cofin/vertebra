---
type: Task
id: gcp-wire-protocol-correctness:t01
title: Official crate dependencies and GCP credential bridge
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/gcp.rs
tests:
  - src/rs/vertebra-db/tests/gcp_credentials_tests.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T18:15:39Z
last_verified_commit: 5494583
verification_evidence: "RED: compile error E0433/E0432 on google_cloud_auth and vertebra_db::gcp; GREEN: 4 passed in gcp_credentials_tests.rs, cargo clippy -p vertebra-db --all-targets -- -D warnings, cargo fmt --check, and all cargo tree/grep feature gates passed"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T18:16:22Z
commit: fadb99e
tags: [rust, gcp, dependencies, auth]
---

# t01 — Official crate dependencies and GCP credential bridge

## Objective

Add the official Google Cloud Rust crates to `vertebra-db` with every default feature off, configure `tonic` for rustls+ring with OS and bundled roots, and add `vertebra_db::gcp` with `GcpTokenCredentials`, `official_credentials`, and `install_rustls_ring_provider`.

## Context

- Spec R1, R2 (`../spec.md`); research sections "google-cloud-gax-internal (transport)" and "Decision".
- `behavior_tdd` for the bridge; R1 is checked with `cargo tree` gates that exit 0 on success.
- `opentelemetry`, `opentelemetry_sdk`, and `tracing-opentelemetry` are unconditional dependencies of `google-cloud-gax-internal` (`_internal-common`); the gates only check that `google-cloud-monitoring-v3` is absent and that `builtin-metrics` / `default-rustls-provider` / `tls-aws-lc` are not enabled.
- The transport builds TLS with `ClientTlsConfig::new().with_enabled_roots()`; enabling `tonic/tls-webpki-roots` adds bundled Mozilla roots to the OS roots (`tls-native-roots`, enabled by the transport), so images without a CA bundle still verify Google endpoints.
- `aws-lc-rs` is already in `src/rs/Cargo.lock` through `reqwest`'s `rustls` feature, so rustls cannot choose a provider by itself; `install_rustls_ring_provider` installs ring. The only other `install_default` calls in the workspace (`vertebra-http/tests/it/{tls.rs,http3_quic.rs}`) also install ring and ignore the `Err` returned when a provider exists.
- `GcpTokenCache::get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String>`; `GcpTokenSnapshot { access_token, quota_project_id, .. }`; `GcpAuthSource::Static { access_token, expires_at_unix_ms, quota_project_id }` needs no network.
- `google_cloud_auth::credentials::{Credentials, CredentialsProvider, CacheableResource, EntityTag, anonymous}`; `CredentialsProvider::headers(&self, http::Extensions) -> impl Future<Output = std::result::Result<CacheableResource<http::HeaderMap>, google_cloud_auth::errors::CredentialsError>> + Send` (the crate's `Result` alias is private); `google_cloud_auth::errors::CredentialsError::{from_msg(is_transient, msg), from_source(is_transient, err)}`.
- New test file `src/rs/vertebra-db/tests/gcp_credentials_tests.rs` is the canonical module for `vertebra_db::gcp`.

## Steps

1. **Write failing tests (RED).** Create `src/rs/vertebra-db/tests/gcp_credentials_tests.rs`:

   ```rust
   use std::sync::Arc;

   use google_cloud_auth::credentials::{CacheableResource, Credentials};
   use http::{Extensions, HeaderMap};
   use vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache};
   use vertebra_db::gcp::{GcpTokenCredentials, install_rustls_ring_provider, official_credentials};

   fn static_cache(token: &str, quota_project: Option<&str>) -> Arc<GcpTokenCache> {
       Arc::new(GcpTokenCache::new(
           GcpAuthSource::Static {
               access_token: token.to_string(),
               expires_at_unix_ms: None,
               quota_project_id: quota_project.map(str::to_string),
           },
           60_000,
       ))
   }

   async fn header_map(credentials: Credentials) -> HeaderMap {
       match credentials.headers(Extensions::new()).await.expect("headers") {
           CacheableResource::New { data, .. } => data,
           CacheableResource::NotModified => panic!("expected new headers"),
       }
   }

   #[tokio::test]
   async fn gcp_token_credentials_emit_bearer_and_quota_headers() {
       let headers = header_map(GcpTokenCredentials::new(static_cache("tok-123", Some("billing-proj"))).into()).await;
       assert_eq!(headers.get("authorization").unwrap(), "Bearer tok-123");
       assert_eq!(headers.get("x-goog-user-project").unwrap(), "billing-proj");
   }

   #[tokio::test]
   async fn gcp_token_credentials_omit_quota_header_without_project() {
       let headers = header_map(GcpTokenCredentials::new(static_cache("tok-456", None)).into()).await;
       assert_eq!(headers.get("authorization").unwrap(), "Bearer tok-456");
       assert!(headers.get("x-goog-user-project").is_none());
   }

   #[tokio::test]
   async fn official_credentials_without_cache_are_anonymous() {
       let headers = header_map(official_credentials(None)).await;
       assert!(headers.get("authorization").is_none());
   }

   #[test]
   fn install_rustls_ring_provider_is_idempotent() {
       install_rustls_ring_provider();
       install_rustls_ring_provider();
       assert!(rustls::crypto::CryptoProvider::get_default().is_some());
   }
   ```

   Run `cd src/rs && cargo test -p vertebra-db --test gcp_credentials_tests` → compile error (`vertebra_db::gcp` and `google_cloud_auth` missing).

2. **Add dependencies (GREEN).**
   - `src/rs/Cargo.toml` `[workspace.dependencies]`, alphabetical:

     ```toml
     google-cloud-auth = { version = "1.18", default-features = false }
     google-cloud-bigquery = { version = "=0.19.0", default-features = false }
     google-cloud-gax = { version = "1.15", default-features = false }
     google-cloud-longrunning = { version = "1.14", default-features = false }
     google-cloud-spanner = { version = "=0.34.8-preview", default-features = false }
     tonic = { version = "0.14", default-features = false, features = ["tls-ring", "tls-webpki-roots"] }
     ```

     (`futures = "0.3"` already exists in `[workspace.dependencies]`.)
   - `src/rs/vertebra-db/Cargo.toml` `[dependencies]`, alphabetical: `futures`, `google-cloud-auth`, `google-cloud-bigquery`, `google-cloud-gax`, `google-cloud-longrunning`, `google-cloud-spanner`, `tonic`, each `{ workspace = true }`.
   - Run `cd src/rs && cargo fetch`. If crates.io is unreachable, set this task `blocked` with `blocked_reason: "crates.io unreachable for cargo fetch"` and `unblock_condition: "cargo fetch succeeds"`.

3. **Add `src/rs/vertebra-db/src/gcp.rs` (GREEN).**

   ```rust
   //! Wiring between Vertebra's `GcpTokenCache` and the official Google Cloud Rust clients.

   use std::sync::Arc;

   use google_cloud_auth::credentials::{CacheableResource, Credentials, CredentialsProvider, EntityTag, anonymous};
   use google_cloud_auth::errors::CredentialsError;
   use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
   use http::{Extensions, HeaderMap};
   use vertebra_core::gcp_auth::GcpTokenCache;

   const QUOTA_PROJECT_HEADER: HeaderName = HeaderName::from_static("x-goog-user-project");

   /// Install rustls' ring `CryptoProvider` as the process default; later calls leave the installed provider in place.
   pub fn install_rustls_ring_provider() {
       let _ = rustls::crypto::ring::default_provider().install_default();
   }

   /// `google-cloud-auth` credentials backed by one shared [`GcpTokenCache`].
   #[derive(Clone, Debug)]
   pub struct GcpTokenCredentials {
       cache: Arc<GcpTokenCache>,
   }

   impl GcpTokenCredentials {
       /// Wrap `cache` so official clients reuse its tokens and refresh policy.
       pub fn new(cache: Arc<GcpTokenCache>) -> Self {
           Self { cache }
       }
   }

   impl CredentialsProvider for GcpTokenCredentials {
       async fn headers(&self, _extensions: Extensions) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
           let snapshot = self
               .cache
               .get_or_refresh()
               .await
               .map_err(|message| CredentialsError::from_msg(true, message))?;
           let mut headers = HeaderMap::with_capacity(2);
           let bearer = HeaderValue::from_str(&format!("Bearer {}", snapshot.access_token))
               .map_err(|err| CredentialsError::from_source(false, err))?;
           headers.insert(AUTHORIZATION, bearer);
           if let Some(project) = snapshot.quota_project_id.as_deref().filter(|p| !p.is_empty()) {
               let value = HeaderValue::from_str(project).map_err(|err| CredentialsError::from_source(false, err))?;
               headers.insert(QUOTA_PROJECT_HEADER, value);
           }
           Ok(CacheableResource::New { entity_tag: EntityTag::new(), data: headers })
       }

       async fn universe_domain(&self) -> Option<String> {
           None
       }
   }

   /// Return credentials for an official client: the shared cache when present, anonymous otherwise.
   pub fn official_credentials(cache: Option<Arc<GcpTokenCache>>) -> Credentials {
       match cache {
           Some(cache) => GcpTokenCredentials::new(cache).into(),
           None => anonymous::Builder::new().build(),
       }
   }
   ```

4. **Register the module.** In `src/rs/vertebra-db/src/lib.rs` add `pub mod gcp;` after `pub mod duckdb;` and the module-doc bullet `//! - Shared credential and TLS wiring for the official Google Cloud Rust clients (`gcp`).`.

5. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-db --test gcp_credentials_tests` → compile error.
- GREEN (each command exits 0):
  - `cd src/rs && cargo test -p vertebra-db --test gcp_credentials_tests` (4 passed).
  - `cd src/rs && cargo clippy -p vertebra-db --all-targets -- -D warnings && cargo fmt --check`.
  - `cd src/rs && ! cargo tree -p vertebra-db --prefix none | grep -q '^google-cloud-monitoring-v3 '`
  - `cd src/rs && ! cargo tree -p vertebra-db -e features --prefix none | grep -qE '"(builtin-metrics|default-rustls-provider|default-idtoken-backend|tls-aws-lc)"'`
  - `cd src/rs && cargo tree -p vertebra-db -e features --prefix none -i tonic | grep -q '"tls-webpki-roots"'`
  - `grep -n 'max_decoding_message_size(usize::MAX)' "${CARGO_HOME:-$HOME/.cargo}"/registry/src/*/google-cloud-gax-internal-0.7.21/src/grpc.rs` (confirms unlimited gRPC decode size).
  - `! grep -rn 'install_default' src/rs --include=*.rs | grep -v 'ring::default_provider'` (every provider install uses ring).
- Commit: `git commit -S -m "feat(rs-db): add official Google Cloud clients and GcpTokenCache credential bridge"`.

## Acceptance Criteria

- [x] R1: official crates added with default features off; no monitoring exporter; `tonic` has `tls-ring` and `tls-webpki-roots`; gates exit 0.
- [x] R2: bearer and quota headers; anonymous without a cache; idempotent ring provider install.

## Notes & Discoveries
