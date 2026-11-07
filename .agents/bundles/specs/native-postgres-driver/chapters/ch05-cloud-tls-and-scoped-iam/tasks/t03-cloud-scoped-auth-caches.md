---
type: Task
id: "native-postgres-driver:ch05:t03"
chapter_id: ch05-cloud-tls-and-scoped-iam
title: Create scope-correct cloud Admin and login credential caches
description: Propagate explicit requested OAuth scopes across vertebra-core GcpTokenCache sources without breaking vertebra-storage, implement ScopedTokenProvider and ScopedLoginSnapshot, and bound cloud material lifetime by login expiry.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch05:t02"
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - src/rs/vertebra-postgres/src/credentials.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-postgres/src/credentials.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 245
plan_revision: 5
plan_commit: 399e068c
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Pure-Rust GCP ADC discovery and OAuth refresh implementation
  - id: core-gcp-tests
    resource: src/rs/vertebra-core/tests/it/gcp_auth.rs
    title: Canonical vertebra-core GCP auth integration tests
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Official google-auth refresh_grant and gcloud --scopes behavior
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Admin vs login cache separation and opaque static Admin token rejection
tags:
  - postgres
  - gcp
  - iam
  - credentials
---

# Task t03: Create Scope-Correct Cloud Admin and Login Credential Caches

## Objective & Scope Boundary

Extend `src/rs/vertebra-core/src/gcp_auth.rs` so `GcpTokenCache` propagates explicit requested OAuth scopes across `MetadataServer`, `ServiceAccount`, `ImpersonatedServiceAccount`, `AuthorizedUser` (`scope` form parameter + response `scope` grant validation + no unscoped CLI fallback), and `GcloudCli` (`gcloud auth application-default print-access-token --scopes=comma,separated,list` with no `gcloud auth print-access-token` fallback), while keeping `GcpAuthSource` variant fields backwards-compatible so `src/rs/vertebra-storage/src/gcp_auth.rs` requires zero edits [^core-gcp-auth] [^cloud-research]. Implement `ScopedTokenProvider` and `ScopedLoginSnapshot` in `src/rs/vertebra-postgres/src/credentials.rs`, and wire `src/rs/vertebra-postgres/src/cloud/material.rs` to delegate Admin (`cloud-platform`) and login (`sqlservice.login` / `alloydb.login`) token resolution to `ScopedTokenProvider` and bound `PreparedCloudMaterial.effective_expiry_ms = min(signed_cert_not_after_ms, login_snapshot.snapshot.expires_at_unix_ms)` [^pg-contracts]. Session-level wire startup guards (`auth.rs` / `session.rs` / `cloud/mod.rs` and `static_login_` wire tests) belong to `ch05:t04` and `ch06:t01`.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCache`, `GcpAuthSource`, `OAuthTokenResponse`, `AuthorizedUser` refresh form, `GcloudCli` command builder, and `MetadataServer` `?scopes=` query formatting [^core-gcp-auth].
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Canonical `vertebra-core` OAuth/CLI/metadata integration test harness [^core-gcp-tests].
- `src/rs/vertebra-postgres/src/credentials.rs` — `StaticLoginCredential` from `ch05:t02`.
- `src/rs/vertebra-postgres/src/cloud/material.rs` — `CloudMaterialCache`, `CloudConnectorConfig`, and `PreparedCloudMaterial` from `ch04:t01` and `ch05:t01`.
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — `google-auth` `_client.refresh_grant` and `gcloud auth application-default print-access-token --scopes` rules [^cloud-research].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) Admin vs login credential rules [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-core/src/gcp_auth.rs` (preserving existing `GcpAuthSource` struct variant fields so `vertebra-storage` compiles untouched):
  - Store `requested_scopes: Vec<String>` on `GcpTokenCache` (defaulting to `Vec::new()` in `new`, `with_proxy_policy`, and unscoped `from_options` so legacy unscoped callers keep identical behavior).
  - Add `pub fn with_requested_scopes(mut self, requested_scopes: Vec<String>) -> Self` and `pub fn from_scoped_options(options: Option<&HashMap<String, String>>, default_scopes: &[String]) -> Result<Self, String>`:
    - Resolve effective scopes from explicit nonempty `options["scopes"]` (comma/whitespace split) or `default_scopes`.
    - Populate `scopes` on `ServiceAccount`, `MetadataServer` (emitting `?scopes=...` URL query parameter for custom scopes), and `ImpersonatedServiceAccount`, and attach `requested_scopes` onto `GcpTokenCache`.
    - Reject opaque `Static` tokens inside `from_scoped_options` when `requested_scopes` is nonempty unless callers construct a validated `Static` cache explicitly (preventing opaque Admin tokens from masquerading as scoped login tokens).
  - When `GcpTokenCache.requested_scopes` is nonempty:
    - `AuthorizedUser`: include `"scope" => requested_scopes.join(" ")` in the OAuth refresh form body (`grant_type=refresh_token`), matching `google-auth` `refresh_grant`; parse optional `#[serde(default)] scope: Option<String>` on `OAuthTokenResponse` and, when present, verify every entry in `requested_scopes` is present in the space/comma-separated response `scope` set (returning an explicit auth error on missing grant); never fall back to `gcloud` CLI if `AuthorizedUser` refresh fails.
    - `GcloudCli`: invoke `gcloud auth application-default print-access-token --scopes=<comma,separated,list>` (plus `--impersonate-service-account=...` when configured) and **never** fall back to `gcloud auth print-access-token` (which cannot request custom scopes; preserve that fallback only when `requested_scopes.is_empty()`).
- `src/rs/vertebra-postgres/src/credentials.rs`:
  - `pub const ADMIN_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";`
  - `pub const CLOUDSQL_LOGIN_SCOPE: &str = "https://www.googleapis.com/auth/sqlservice.login";`
  - `pub const ALLOYDB_LOGIN_SCOPE: &str = "https://www.googleapis.com/auth/alloydb.login";`
  - `pub(crate) struct ScopedLoginSnapshot { pub snapshot: GcpTokenSnapshot, pub required_scopes: Vec<String>, pub declared_scopes: Option<Vec<String>>, pub refresh_skew_ms: u64, is_static: bool }` with `pub(crate) fn ensure_usable(&self, now_ms: u64) -> Result<(), PostgresError>` enforcing `now_ms.saturating_add(self.refresh_skew_ms) < self.snapshot.expires_at_unix_ms`.
  - `pub(crate) struct ScopedTokenProvider`:
    - Pure constructor storing owned `cloud_auth_options: Option<HashMap<String, String>>`, `iam_auth_options: Option<HashMap<String, String>>`, and `Arc<dyn Clock>` (zero file I/O or ADC discovery in constructor).
    - Lazily initializes separate internal Admin and scope-keyed login `Arc<GcpTokenCache>` instances during `connect` via `tokio::task::spawn_blocking` awaited under `tokio::time::timeout_at(absolute_deadline, ...)` (discarding late results if the deadline elapses).
    - When `iam_auth_options` contains `access_token` or `bearer_token`, routes strictly through `ch05:t02` `StaticLoginCredential::from_options` -> `into_token_cache` (never `from_scoped_options` or `from_options`).
    - When `iam_auth_options` is `None`, derives the login source from `cloud_auth_options` with the required login scope, **except** if `cloud_auth_options` carries a static `access_token`/`bearer_token`, which fails immediately with `PostgresError::Auth` rather than assuming an opaque Admin token holds login scopes.
- `src/rs/vertebra-postgres/src/cloud/material.rs`:
  - Replace interim `ch04:t01` `GcpTokenCache::from_options` calls with `ScopedTokenProvider` (`ADMIN_CLOUD_PLATFORM_SCOPE` for Admin requests, `CLOUDSQL_LOGIN_SCOPE` for Cloud SQL IAM `:generateEphemeralCert`, `ALLOYDB_LOGIN_SCOPE` for AlloyDB), check `login_snapshot.ensure_usable(clock.now_ms())` before token-bearing `:generateEphemeralCert` POSTs, and preserve the exact field name `effective_expiry_ms: u64` on `PreparedCloudMaterial`, computing `effective_expiry_ms = min(signed_cert_not_after_ms, login_snapshot.snapshot.expires_at_unix_ms)` when a login token is bound.

## Edge Cases & Error Semantics

- `AuthorizedUser` token endpoint returning HTTP 200 with `{"access_token":"t","expires_in":3600,"scope":"https://www.googleapis.com/auth/cloud-platform"}` when `sqlservice.login` was requested fails with `PostgresError::Auth` (mapped to `GoogleCloudAuthError`).
- `AuthorizedUser` HTTP 400 `invalid_scope` or `GcloudCli` non-zero exit when requesting `--scopes=https://www.googleapis.com/auth/sqlservice.login` propagates `PostgresError::Auth` without retrying unscoped `gcloud auth print-access-token`.
- Unscoped callers (`requested_scopes.is_empty()`, including `vertebra-core` defaults and `vertebra-storage`) retain exact existing refresh and CLI fallback behavior.
- All credential errors redact bearer/refresh tokens and map through `PostgresError::Auth` to `vertebra._vertebra.GoogleCloudAuthError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-core/tests/it/gcp_auth.rs` (`explicit_login_scopes_*`) and `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (`cloud_scoped_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Metadata, SA JWT, and Impersonation custom scope | `from_scoped_options` requesting `["https://www.googleapis.com/auth/sqlservice.login"]` against mock HTTP servers | Metadata GET includes `?scopes=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fsqlservice.login` (or raw URL query); SA JWT `scope` claim and IAM `:generateAccessToken` JSON `scope` array equal `sqlservice.login`. |
| `AuthorizedUser` refresh form & under-scoped response | `AuthorizedUser` requesting `alloydb.login`; mock OAuth server inspects POST body, first returns `scope: "cloud-platform"`, second returns `scope: "https://www.googleapis.com/auth/alloydb.login"` | POST body contains `scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Falloydb.login`; first response fails with auth error; second succeeds. |
| `GcloudCli` `--scopes` & forbidden unscoped fallback | Fake `gcloud` script recording `argv`: exits `1` on `application-default print-access-token --scopes=a,b` and `0` on `auth print-access-token` | Scoped cache invokes `auth application-default print-access-token --scopes=a,b`, never invokes `auth print-access-token`, and returns `Err(_)`. |
| Admin vs login cache separation & opaque static Admin rejection | `cloud_auth_options` with `access_token = "admin-tok"` and `iam_auth_options = None` on Cloud SQL IAM `prepare` | Fails before `:generateEphemeralCert` with `PostgresError::Auth` (`0` login uses of `"admin-tok"`). |
| Material lifetime bounded by static login expiry | Signed ephemeral cert `NotAfter = now + 3_600_000 ms`, static `iam_auth_options` `expires_at_unix_ms = now + 600_000 ms` | `PreparedCloudMaterial.effective_expiry_ms == now + 600_000`; advancing injected clock to `now + 360_001` (`240s` lead) triggers renewal instead of reusing stale cert. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_scoped_`
- Final Green + Storage regression gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it explicit_login_scopes`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_scoped_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcp_auth`

## Acceptance Criteria

- [ ] `vertebra_core::gcp_auth::GcpTokenCache` sends explicit requested scopes across Metadata, ServiceAccount, ImpersonatedServiceAccount, AuthorizedUser (with response `scope` grant check), and GcloudCli (`--scopes=...` without unscoped fallback) while keeping `vertebra-storage` green untouched (R5, R9).
- [ ] `ScopedTokenProvider` lazily constructs separate Admin and login caches under `absolute_deadline`, delegates static login options to `StaticLoginCredential`, and rejects opaque static Admin tokens when `iam_auth_options` is absent (R5, R9).
- [ ] `CloudMaterialCache` bounds `PreparedCloudMaterial.effective_expiry_ms` by login snapshot expiry and commit `fix(postgres): request scope-correct cloud login tokens` passes all four test commands (R5, R9).

## Notes & Discoveries

- `src/rs/vertebra-storage/src/gcp_auth.rs` is already deduplicated onto `vertebra_core::gcp_auth::GcpTokenCache` and must not be modified in this task.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Shared GCP ADC discovery and OAuth refresh implementation.
[^core-gcp-tests]: `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Core GCP auth integration tests.
[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Official `google-auth` and `gcloud` scope propagation rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Separate Admin/login options and static provenance contracts.
