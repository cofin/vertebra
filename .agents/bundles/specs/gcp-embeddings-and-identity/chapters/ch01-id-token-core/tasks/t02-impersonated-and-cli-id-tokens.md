---
type: Task
id: "gcp-embeddings-and-identity:ch01:t02"
chapter_id: ch01-id-token-core
title: Mint ID tokens through impersonation and the gcloud CLI and reject static sources
description: Implement ImpersonatedServiceAccount minting via IAM Credentials generateIdToken for any audience and GcloudCli minting via print-identity-token, and reject Static sources at GcpIdTokenCache construction.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t01"
read_set:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch01-id-token-core/spec.md
files:
  - src/rs/vertebra-core/src/gcp_identity.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
estimated_loc: 190
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
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: ImpersonatedServiceAccount, GcloudCli, AuthorizedUser, and Static variants
  - id: core-gcloud-cli
    resource: src/rs/vertebra-core/src/gcp_auth.rs::fetch_token_from_gcloud_cli
    title: Existing async gcloud CLI runner with kill_on_drop and timeout
  - id: rust-idtoken-reference
    resource: https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/impersonated/index.html
    title: IAM Credentials generateIdToken request body (audience, includeEmail, delegates)
tags:
  - gcp
  - identity
  - impersonation
  - rust
---

# Task t02: Mint ID Tokens Through Impersonation and the gcloud CLI and Reject Static Sources

## Objective & Scope Boundary

Complete `GcpIdTokenCache` source coverage in `src/rs/vertebra-core/src/gcp_identity.rs`:

- `ImpersonatedServiceAccount { base, target_service_account, iam_endpoint, .. }` obtains the base access token through `GcpTokenCache::fetch_source_access_token(base)` and calls `POST {iam_endpoint}/v1/projects/-/serviceAccounts/{target_service_account}:generateIdToken` (default endpoint `https://iamcredentials.googleapis.com`) with `includeEmail` from `include_email` (default `true`); any audience is allowed, including when `base` is `AuthorizedUser` [^rust-idtoken-reference].
- `GcloudCli { binary, timeout, impersonate_service_account, .. }` runs `<binary> auth print-identity-token --audiences=<audience> --quiet [--impersonate-service-account=<sa>]` [^core-gcloud-cli].
- `GcpIdTokenCache::new` rejects `Static` sources [^core-gcp-auth].

Out of scope: ADC order (`t03`), `AuthorizedUser` minting (`t04`; the `t01` interim arm stays for that variant until then), delegation chains (`delegates` is omitted), Python bindings.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache::fetch_id_token`, `::GcpIdTokenCache::new` (from `t01`).
- `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource`, `::fetch_token_from_gcloud_cli`, `::GcpTokenCache::fetch_source_access_token`, `::GcpTokenCache::http_client`, `::GCLOUD_CLI_TIMEOUT`.
- `src/rs/vertebra-core/tests/it/gcp_auth.rs::gcloud_cli_token_fetch_is_async_and_times_out` (stub-binary pattern), `::impersonation_defaults_to_caller_scopes` (loopback metadata + IAM pattern).
- Chapter spec.

## Symbol & Signature Contracts

```rust
#[derive(Serialize)]
struct GenerateIdTokenRequest<'a> {
    audience: &'a str,
    #[serde(rename = "includeEmail")]
    include_email: bool,
}
#[derive(Deserialize)]
struct GenerateIdTokenResponse { token: String }

async fn fetch_id_token_from_gcloud_cli(
    binary: &Path,
    timeout: Duration,
    impersonate_service_account: Option<&str>,
    audience: &str,
) -> Result<String, String>;
```

`GcpIdTokenCache::new` matches `credentials.auth_source()`:
- `AuthorizedUser { .. }` is accepted; `t04` owns its minting and audience rule.
- `Static { .. }` -> `Err("static access tokens cannot mint ID tokens; configure a service account key, the metadata server, target_service_account impersonation, or use_gcloud_cli")`.
- `ImpersonatedServiceAccount` whose `base` is `Static` is accepted (the static token authorizes the IAM call).

`fetch_id_token` arms:
- Impersonation: `source_token = credentials.fetch_source_access_token(base).await?`; `credentials.http_client().post(url).bearer_auth(source_token).json(&GenerateIdTokenRequest { audience, include_email: options.include_email })`; non-2xx -> `Err("IAM generateIdToken returned HTTP <status>")`; JSON `token`.
- gcloud: `tokio::process::Command` with `kill_on_drop(true)`, args `["auth", "print-identity-token", "--audiences=<audience>", "--quiet"]` plus `--impersonate-service-account=<sa>` when set; wrapped in `tokio::time::timeout(timeout, cmd.output())`; non-zero exit -> `Err("`<binary>` print-identity-token failed: <stderr trimmed>")`; timeout -> `Err("`<binary>` print-identity-token timed out after <ms> ms")`; empty stdout -> `Err("`<binary>` print-identity-token returned no token")`.

## Edge Cases & Error Semantics

- Base token failure (metadata `500`) surfaces the base error unchanged (`metadata server returned HTTP 500 Internal Server Error`) and makes no IAM request.
- IAM `403` surfaces as `Err("IAM generateIdToken returned HTTP 403 Forbidden")`; nothing cached.
- The returned token still passes `parse_id_token_claims`, so an impersonated token with a different `aud` fails with `audience mismatch`.
- gcloud for user accounts rejects `--audiences`; its stderr reaches the caller verbatim (fail loud, no retry without `--audiences`).
- The audience reaches gcloud as a single argv element (`--audiences=<audience>`), never through a shell.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/gcp_auth.rs`. Loopback `TcpListener` stands in for metadata and IAM; a temporary executable shell script (written with `tempfile`, `chmod 0o755`, `#[cfg(unix)]`) stands in for gcloud and selected through `GCLOUD_BIN`-independent `GcpAuthSource::GcloudCli { binary, .. }` built via `GcpTokenCache::new` and `GcpIdTokenCache::new`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `id_token_impersonated_generate_id_token` | Options `use_metadata_server=true`, `metadata_host=<loopback>`, `target_service_account=svc@p.iam.gserviceaccount.com`, `iam_endpoint=<loopback>`, `proxy_policy=direct`; metadata token `meta-base-token`; IAM answers `{"token": JWT(aud="https://svc.run.app")}` | IAM request line `POST /v1/projects/-/serviceAccounts/svc@p.iam.gserviceaccount.com:generateIdToken`; `authorization: Bearer meta-base-token`; body `{"audience":"https://svc.run.app","includeEmail":true}`; snapshot audience matches |
| `id_token_impersonated_include_email_false` | Same with `include_email = "false"` | Body `{"audience":"https://svc.run.app","includeEmail":false}` |
| `id_token_impersonated_iam_forbidden` | IAM returns `403` | `Err` containing `generateIdToken returned HTTP 403`; `generation() == 0` |
| `id_token_gcloud_cli_audience_argv` | Script echoes `$@` to a temp file then prints JWT(aud="https://svc") | Recorded argv `auth print-identity-token --audiences=https://svc --quiet`; snapshot returned |
| `id_token_gcloud_cli_timeout` | Script `sleep 5`; `timeout = 200ms` | `Err` containing `timed out after 200 ms` within `1 s` |
| `id_token_impersonated_any_audience` | Impersonated source as above; `get_or_refresh("https://svc.run.app")` then `get_or_refresh("https://iap.example.com")` | Two IAM requests whose bodies carry each audience; two cached entries |
| `id_token_rejects_static` | `GcpTokenCache::new(GcpAuthSource::Static{..})` passed to `GcpIdTokenCache::new` | `Err` contains `static access tokens cannot mint ID tokens` |
| `id_token_accepts_authorized_user_source` | `GcpTokenCache::new(GcpAuthSource::AuthorizedUser{..})` passed to `GcpIdTokenCache::new` | `Ok`; no network call at construction |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it id_token_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --all-targets -- -D warnings
```

Write the new tests first (Red: impersonation and CLI arms return errors from `t01`), implement, re-run. Commit as `feat(core): mint ID tokens via impersonation and gcloud`.

## Acceptance Criteria

- [ ] Impersonated sources mint through `generateIdToken` with the base bearer and `includeEmail` option (R1).
- [ ] gcloud sources mint through `print-identity-token --audiences` with timeout and verbatim stderr errors (R1).
- [ ] Static sources fail at construction with an actionable message; authorized-user sources construct (R1).
- [ ] Every source path validates `aud` and `exp` through `parse_id_token_claims` (R2).

## Notes & Discoveries

- [2026-11-12 00:35] `GcpTokenCache::from_options` never builds `GcloudCli` with `impersonate_service_account: Some(..)`; impersonation with gcloud goes through `ImpersonatedServiceAccount { base: GcloudCli, .. }`, which this task already covers via the base token path.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — Source variants dispatched here.
[^core-gcloud-cli]: `src/rs/vertebra-core/src/gcp_auth.rs::fetch_token_from_gcloud_cli` — Process runner pattern.
[^rust-idtoken-reference]: `https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/impersonated/index.html` — `generateIdToken` request shape.
