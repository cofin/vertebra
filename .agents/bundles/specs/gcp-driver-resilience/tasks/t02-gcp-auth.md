---
type: Task
id: gcp-driver-resilience:t02
title: Async gcloud fetch, proxy policy, explicit OAuth scopes
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-db/src/spanner.rs
  - src/rs/vertebra-db/src/bigquery.rs
  - src/rs/vertebra-storage/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-py/src/gcp_auth.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
plan_revision: 2
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
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:10:00Z
commit: null
tags: [rust, gcp, auth, bugfix]
---

# t02 — Async gcloud fetch, proxy policy, explicit OAuth scopes

## Objective

`gcloud` token fetches stop blocking the async runtime and honor a per-source binary and timeout; token/IAM requests follow a `ProxyPolicy` while metadata-server calls bypass proxies; `GcpTokenCache::from_options` receives default scopes from the caller (no GCS fallback, `from_gcs_options` removed). Tests inject everything through constructors and never mutate environment variables.

## Context

- Spec R3, R4, R5 (`../spec.md`).
- `regression_tdd` in `src/rs/vertebra-core/tests/it/gcp_auth.rs` (binary `it`, feature `gcp-auth`).
- `src/rs/vertebra-core/src/gcp_auth.rs` today: `fetch_token_from_gcloud_cli(impersonate_sa: Option<&str>) -> Result<String, String>` uses `std::process::Command::output()` and reads `GCLOUD_BIN`; it is called from the async `fetch_from_source` (AuthorizedUser refresh fallback and `GcpAuthSource::GcloudCli { quota_project_id, impersonate_service_account }`). `GcpTokenCache::new(source, refresh_skew_ms)` builds one `reqwest::Client` with `.no_proxy()`. `from_options` uses `DEFAULT_GCP_SCOPES` for service accounts and `["devstorage.full_control", "cloud-platform"]` for impersonation when `scopes` is absent; `from_gcs_options` forwards to it. `reqwest 0.13` honors proxy environment variables unless `no_proxy()` is called; `ClientBuilder::proxy(reqwest::Proxy)` adds an explicit proxy.
- `vertebra-core/Cargo.toml`: `tokio = { workspace = true, default-features = false, features = ["sync"] }`, `gcp-auth = ["dep:reqwest", "dep:url", "dep:chrono"]`.
- Callers of `from_options` / `from_gcs_options` / `GcpTokenCache::new`: `vertebra-db/src/spanner.rs::SpannerClient::new` (inserts a `scopes` option), `vertebra-db/src/bigquery.rs::BigQueryClient::new` (inserts a `scopes` option), `vertebra-storage/src/store.rs` and `vertebra-storage/src/lib.rs` (`from_gcs_options`), `vertebra-storage/src/signing.rs` (four `from_options` calls), `vertebra-storage/src/gcp_auth.rs` (two `GcpTokenCache::new`), `vertebra-py/src/gcp_auth.rs::PyGcpTokenCache::new`, and `vertebra-core/tests/it/gcp_auth.rs`.

## Steps

1. **Write failing tests (RED)** in `src/rs/vertebra-core/tests/it/gcp_auth.rs`:
   - Update every `from_options(&opts)` call to `from_options(&opts, DEFAULT_GCP_SCOPES)` and add the option `"proxy_policy": "direct"` to each of those option maps; replace every `GcpTokenCache::new(source, skew)` in the test file with `GcpTokenCache::with_proxy_policy(source, skew, ProxyPolicy::Direct)`. Developer proxy variables then cannot affect the loopback mocks.
   - `impersonation_defaults_to_caller_scopes`: copy the mock from `metadata_server_and_impersonation_custom_scopes`, drop the `scopes` option, add `"proxy_policy": "direct"`, call `GcpTokenCache::from_options(&opts, &["https://www.googleapis.com/auth/spanner.data"])`, assert the `:generateAccessToken` request contains `spanner.data` and not `devstorage`.
   - `gcloud_cli_token_fetch_is_async_and_times_out` (`#[cfg(unix)] #[tokio::test(flavor = "current_thread")]`): write an executable script `#!/bin/sh\nsleep 60\n` into `tempfile::tempdir()` (mode `0o755` via `std::os::unix::fs::PermissionsExt`); build `GcpTokenCache::new(GcpAuthSource::GcloudCli { binary: script_path, timeout: Duration::from_millis(200), quota_project_id: None, impersonate_service_account: None }, 60_000)`; spawn a ticker task that sleeps 50 ms and sets an `Arc<AtomicBool>`; `let outcome = cache.get_or_refresh().await;` assert `outcome.is_err()`, the error contains `"timed out after 200 ms"`, and the ticker flag is `true` (the runtime kept running). Add `tempfile = "3"` to `vertebra-core` `[dev-dependencies]`.
   - `explicit_proxy_routes_token_requests_and_metadata_bypasses_it`: start a TCP listener acting as an HTTP proxy that records the request line and replies `200` with `{"access_token":"proxied","expires_in":3600,"token_type":"Bearer"}`; build `GcpTokenCache::with_proxy_policy(GcpAuthSource::AuthorizedUser { client_id: "c".into(), client_secret: "s".into(), refresh_token: "r".into(), quota_project_id: None, token_uri: "http://oauth.invalid/token".into() }, 60_000, ProxyPolicy::Explicit(reqwest::Proxy::http(format!("http://127.0.0.1:{proxy_port}")).unwrap()))`; assert the token is `proxied` and the proxy saw `POST http://oauth.invalid/token`. Then build a metadata-server source (`endpoint = http://127.0.0.1:{metadata_port}`) with the same explicit proxy and assert the metadata listener received the request and the proxy count did not change. Add `reqwest = { workspace = true }` to `vertebra-core` `[dev-dependencies]` if the test cannot reach it through the `gcp-auth` feature.
   - RED: `cd src/rs && cargo test -p vertebra-core --features gcp-auth --test it gcp_auth` → compile errors (`ProxyPolicy`, `with_proxy_policy`, `GcloudCli { binary, timeout }`, `from_options` arity).

2. **Implement (GREEN) in `src/rs/vertebra-core/src/gcp_auth.rs`.**
   - `Cargo.toml`: `gcp-auth = ["dep:reqwest", "dep:url", "dep:chrono", "tokio/process", "tokio/time"]`.
   - `pub const GCLOUD_CLI_TIMEOUT: Duration = Duration::from_secs(30);` and `pub fn default_gcloud_binary() -> PathBuf` (`GCLOUD_BIN` when set and non-empty, else `"gcloud"`).
   - `GcpAuthSource::GcloudCli { binary: PathBuf, timeout: Duration, quota_project_id: Option<String>, impersonate_service_account: Option<String> }`; `from_options` fills `binary: default_gcloud_binary(), timeout: GCLOUD_CLI_TIMEOUT`.
   - `async fn fetch_token_from_gcloud_cli(binary: &Path, timeout: Duration, impersonate_sa: Option<&str>) -> Result<String, String>`: for each subcommand pair (`auth application-default print-access-token`, `auth print-access-token`) run `tokio::process::Command::new(binary).args(args).arg("--quiet")` (plus `--impersonate-service-account=…` when set) `.kill_on_drop(true).output()` inside `tokio::time::timeout(timeout, ..)`; elapsed → `Err(format!("`{}` print-access-token timed out after {} ms", binary.display(), timeout.as_millis()))`; spawn error → `Err(format!("failed to execute `{}` CLI: {e}", binary.display()))`; keep the existing success/stderr handling. Call sites add `.await`; the AuthorizedUser fallback uses `default_gcloud_binary()` and `GCLOUD_CLI_TIMEOUT`.
   - `#[derive(Clone, Debug, Default)] pub enum ProxyPolicy { #[default] Environment, Direct, Explicit(reqwest::Proxy) }` (doc: how token and IAM requests reach the network; metadata requests never use a proxy).
   - `GcpTokenCache`: fields `client` (token/IAM) and `metadata_client` (built with `.no_proxy().timeout(5 s)`). `pub fn new(source, refresh_skew_ms) -> Self` = `with_proxy_policy(source, refresh_skew_ms, ProxyPolicy::Environment)`. `pub fn with_proxy_policy(source, refresh_skew_ms, policy) -> Self`: `Environment` → `reqwest::Client::builder().timeout(5 s)`; `Direct` → `.no_proxy()`; `Explicit(p)` → `.no_proxy().proxy(p)`. Every `MetadataServer` request (direct source or impersonation base) uses `metadata_client`.
   - `pub fn from_options(raw_opts: &HashMap<String, String>, default_scopes: &[&str]) -> Option<Arc<Self>>`: the `scopes` option overrides; otherwise service-account and impersonation sources use `default_scopes`; metadata-server sources keep `custom_scopes.unwrap_or_default()`; option `proxy_policy` = `"direct"` → `ProxyPolicy::Direct`, absent or `"environment"` → `Environment` (any other value → `Environment`). Delete the GCS impersonation default and `from_gcs_options`.
   - Scope constants and callers: `vertebra-db/src/spanner.rs` `pub const SPANNER_OAUTH_SCOPES: &[&str] = &["https://www.googleapis.com/auth/spanner.data", "https://www.googleapis.com/auth/cloud-platform"];` and `SpannerClient::new` calls `GcpTokenCache::from_options(&config.auth_options, SPANNER_OAUTH_SCOPES)` (delete the `scopes` insertion). `vertebra-db/src/bigquery.rs` `pub const BIGQUERY_OAUTH_SCOPES: &[&str] = &["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform"];`, delete the `scopes` insertion, keep the cleartext `allow_ambient_adc` insertion, call `from_options(&auth_opts, BIGQUERY_OAUTH_SCOPES)`. `vertebra-storage/src/gcp_auth.rs` `pub const GCS_OAUTH_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.full_control", "https://www.googleapis.com/auth/cloud-platform"];`; `store.rs`, `lib.rs`, and the four `signing.rs` calls pass `GCS_OAUTH_SCOPES`. `vertebra-py/src/gcp_auth.rs::PyGcpTokenCache::new` passes `vertebra_core::gcp_auth::DEFAULT_GCP_SCOPES`. Update any `GcpAuthSource::GcloudCli { .. }` construction in `vertebra-storage` to the new fields.

3. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-core --features gcp-auth --test it gcp_auth` → compile errors.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-core --features gcp-auth --test it gcp_auth`
  - `cd src/rs && cargo test -p vertebra-storage && cargo test -p vertebra-db`
  - `cd src/rs && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
  - `! grep -rnE "from_gcs_options|std::process::Command" src/rs/vertebra-core/src/gcp_auth.rs src/rs/vertebra-storage/src`
  - `! grep -rnE "set_var|remove_var" src/rs/vertebra-core/tests/it/gcp_auth.rs`
  - `make dev && uv run pytest src/py/tests/unit/storage -q`
- Commit: `git commit -S -m "fix(rs-core): fetch gcloud tokens asynchronously, add a proxy policy, and require explicit OAuth scopes"`.

## Acceptance Criteria

- [ ] R3: async gcloud fetch with per-source binary and timeout; the runtime keeps running during the fetch.
- [ ] R4: `ProxyPolicy` routes token/IAM requests; metadata requests bypass proxies.
- [ ] R5: explicit default scopes; no GCS default; `from_gcs_options` gone; tests mutate no environment variables.

## Notes & Discoveries
