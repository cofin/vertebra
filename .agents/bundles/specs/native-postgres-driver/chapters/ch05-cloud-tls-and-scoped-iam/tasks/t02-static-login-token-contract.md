---
type: Task
id: "native-postgres-driver:ch05:t02"
chapter_id: ch05-cloud-tls-and-scoped-iam
title: Validate designated static login scope, actual lifetime, and core auth error mapping
description: Implement StaticLoginCredential strict scope and unsigned decimal expiry validation, preserve actual supplied expiry in GcpAuthSource::Static, and add PostgresError::Auth with PyOnceLock mapping to vertebra._vertebra exceptions.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch05:t01"
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/credentials.rs
tests:
  - src/rs/vertebra-postgres/src/credentials.rs
estimated_loc: 215
plan_revision: 4
plan_commit: e8901de4
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: GcpAuthSource::Static and GcpTokenCache refresh skew semantics
  - id: core-py-errors
    resource: src/rs/vertebra-py/src/errors.rs::db_exception_types
    title: Canonical vertebra._vertebra database and GoogleCloudAuthError exception classes
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Static login provenance, actual expiry, and GoogleCloudAuthError vs SQLSTATE 28 contract
tags:
  - postgres
  - credentials
  - iam
  - errors
---

# Task t02: Validate Designated Static Login Scope, Actual Lifetime, and Core Auth Error Mapping

## Objective & Scope Boundary

Implement `StaticLoginCredential` in `src/rs/vertebra-postgres/src/credentials.rs` to reject designated static login credentials with missing/mismatched scopes or missing/non-canonical/expired/inside-skew `expires_at_unix_ms`, preserving `Some(supplied_expiry)` in `GcpAuthSource::Static` without fabricating TTLs or `u64::MAX` lifetimes [^core-gcp-auth] [^pg-contracts]. Simultaneously add `PostgresError::Auth(String)` and `postgres_error_to_py` (`PyOnceLock` cache over `vertebra._vertebra` exception classes) in `src/rs/vertebra-postgres/src/errors.rs` so credential-provider failures map to exact `GoogleCloudAuthError` while server `SQLSTATE` `28xxx` remains `DatabaseError` [^core-py-errors] [^pg-contracts]. Leave core unscoped `GcpTokenCache::from_options` defaults unchanged; dynamic ADC scoped caches and wire guards belong to `ch05:t03` and `ch05:t04`.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs::{GcpAuthSource,GcpTokenCache,DEFAULT_REFRESH_SKEW_MS}` — Existing `Static` source behavior (`expires_at_unix_ms: Option<u64>`) and scope delimiter rules [^core-gcp-auth].
- `src/rs/vertebra-py/src/errors.rs` — Canonical `vertebra._vertebra` `DatabaseError`, `InterfaceError`, `NotSupportedError`, `ConnectionFailure`, and `GoogleCloudAuthError` hierarchy [^core-py-errors].
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError` enum created in `ch01:t04`.
- `src/rs/vertebra-postgres/src/lib.rs` — Module registry for `vertebra-postgres`.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Authentication and lexical defaults` [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Register `pub(crate) mod credentials;`.
- `src/rs/vertebra-postgres/src/errors.rs`:
  - Extend `PostgresError` with `Auth(String)` for credential-provider, scope-provenance, and token-lifetime failures (redacting raw token values and never fabricating a 5-char `sqlstate`).
  - Implement `pub(crate) fn postgres_error_to_py(py: Python<'_>, err: PostgresError) -> PyErr` caching imported `vertebra._vertebra` classes via `PyOnceLock<Py<PyAny>>` (no stand-in duplicate classes and no cross-`cdylib` Rust struct extraction):
    - `PostgresError::Config(_)` -> `PyValueError` (type-level map errors at FFI raise `PyTypeError`).
    - `PostgresError::Interface(_)` -> `vertebra._vertebra.InterfaceError` (`sqlstate = None`).
    - `PostgresError::Data(_)` -> `vertebra._vertebra.DataError` (`sqlstate = None`).
    - `PostgresError::UnsupportedAuth(_)` | `PostgresError::UnsupportedFeature(_)` -> `vertebra._vertebra.NotSupportedError` (`sqlstate = None`).
    - `PostgresError::Transport(_)` | `PostgresError::Protocol(_)` -> `vertebra._vertebra.ConnectionFailure` (`sqlstate = None`).
    - `PostgresError::Server(err)` -> canonical SQLSTATE subclass with `.sqlstate = Some(err.sqlstate)` and sanitized primary `err.message`: `08*` -> `ConnectionFailure`, `22*` -> `DataError`, `23*` -> `IntegrityError`, `42*` -> `ProgrammingError`, `"40001" | "40P01"` -> `SerializationFailure`, `"57014"` -> `QueryCancelledError`, and `"28000" | "28P01" | _` -> `DatabaseError`.
    - `PostgresError::Auth(msg)` -> `vertebra._vertebra.GoogleCloudAuthError` with sanitized `msg` and no fabricated `sqlstate`.
- `src/rs/vertebra-postgres/src/credentials.rs`:
  - `#[derive(Clone, Debug)] pub(crate) struct StaticLoginCredential` holding `access_token: String` (redacted in `Debug`), `expires_at_unix_ms: u64`, `declared_scopes: Vec<String>`, `quota_project_id: Option<String>`, and `refresh_skew_ms: u64`.
  - `pub(crate) fn from_options(options: &HashMap<String, String>, required_scopes: &[String], now_ms: u64, refresh_skew_ms: u64) -> Result<Self, PostgresError>`:
    - Accept `access_token` or `bearer_token`; if both keys are present and non-equal, fail with `PostgresError::Auth` without printing either secret; if missing or empty/whitespace, fail with `PostgresError::Auth`.
    - Require `expires_at_unix_ms`: parse as canonical unsigned ASCII decimal `u64` satisfying `!raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit())` and `parsed.to_string() == raw` (rejecting signs `+`/`-`, whitespace, leading zeros like `"01700000000000"`, fractions, exponents, and `u64` overflow).
    - Require `scopes`: split on ASCII commas and whitespace (`c == ',' || c.is_ascii_whitespace()`), require a nonempty declared set containing every scope in `required_scopes`, and retain `declared_scopes`. Never inspect opaque token bytes or infer Admin grants.
    - Call `self.ensure_usable(now_ms)?` before returning.
  - `pub(crate) fn ensure_usable(&self, now_ms: u64) -> Result<(), PostgresError>`:
    - Require `now_ms.saturating_add(self.refresh_skew_ms) < self.expires_at_unix_ms`; equality, inside-skew, or expired values return `Err(PostgresError::Auth(_))`.
  - `pub(crate) fn into_token_cache(&self, proxy_policy: ProxyPolicy) -> GcpTokenCache`:
    - Construct `GcpTokenCache::with_proxy_policy(GcpAuthSource::Static { access_token: self.access_token.clone(), expires_at_unix_ms: Some(self.expires_at_unix_ms), quota_project_id: self.quota_project_id.clone() }, self.refresh_skew_ms, proxy_policy)`. Never call generic `GcpTokenCache::from_options`, never synthesize a default TTL or `u64::MAX`, and never allow `get_or_refresh` to invent a renewed expiry.

## Edge Cases & Error Semantics

- Both `access_token` and `bearer_token` supplied with different values (`"tok-a"` vs `"tok-b"`) fail with `PostgresError::Auth`, and neither `"tok-a"` nor `"tok-b"` appears in `Display` or `Debug` output.
- Boundary math uses `now_ms.saturating_add(refresh_skew_ms) < expires_at_unix_ms`: when `now_ms = 1_000_000` and `refresh_skew_ms = 300_000`, `expires_at_unix_ms = 1_300_000` fails (`PostgresError::Auth`), while `expires_at_unix_ms = 1_300_001` succeeds. `now_ms = u64::MAX` saturates cleanly without overflow panic and fails.
- Repeated `cache.get_or_refresh().await` calls on `into_token_cache` return `snapshot.expires_at_unix_ms == supplied_expiry` while valid, and return `Err(...)` once expired rather than manufacturing a fresh 3600s TTL.
- All unit tests inject deterministic `now_ms: u64` values with zero `sleep` calls.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/src/credentials.rs` (`credentials::tests::static_login_contract`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Missing or empty static token | `options = {"expires_at_unix_ms": "2000000", "scopes": "https://www.googleapis.com/auth/sqlservice.login"}` (or `access_token = ""`) | `Err(PostgresError::Auth(_))`. |
| Conflicting `access_token` and `bearer_token` | `access_token = "secret-alpha"`, `bearer_token = "secret-beta"` | `Err(PostgresError::Auth(msg))` where `!msg.contains("secret-alpha") && !msg.contains("secret-beta")`. |
| Missing or non-canonical `expires_at_unix_ms` | `expires_at_unix_ms` missing, `"01700000000000"`, `"+1700000000000"`, `" 1700000000000"`, `"1700.5"`, `"1e12"`, or `"18446744073709551616"` | Every input fails with `Err(PostgresError::Auth(_))`. |
| Missing or insufficient `scopes` | `required = ["https://www.googleapis.com/auth/sqlservice.login"]`, `scopes = "https://www.googleapis.com/auth/cloud-platform"` (or `""`) | `Err(PostgresError::Auth(_))`. |
| Exact skew boundary vs +1 ms | `now_ms = 1_000_000`, `skew_ms = 300_000`: `expires_at_unix_ms = "1300000"` vs `"1300001"` with comma/space `scopes` | `"1300000"` returns `Err(PostgresError::Auth(_))`; `"1300001"` succeeds and `ensure_usable(1_000_000)` is `Ok(())` while `ensure_usable(1_000_001)` is `Err(PostgresError::Auth(_))`. |
| Cache source preserves actual expiry | Valid `StaticLoginCredential` (`expires_at_unix_ms = 4_102_444_800_000`) -> `into_token_cache(ProxyPolicy::Direct)`, call `get_or_refresh()` twice | Both snapshots report `expires_at_unix_ms == 4_102_444_800_000` (never `u64::MAX` or `now + 3_600_000`). |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib static_login_contract`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib static_login_contract`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Designated static login credentials require matching `access_token`/`bearer_token`, canonical unsigned decimal `expires_at_unix_ms`, and explicit `scopes` covering every required login scope (R5, R9).
- [ ] `ensure_usable(now_ms)` rejects expired, inside-skew, and exact-boundary timestamps via `PostgresError::Auth`, and `into_token_cache` preserves `Some(supplied_expiry)` without fabricating lifetime (R5, R9).
- [ ] `errors.rs::postgres_error_to_py` resolves `vertebra._vertebra` exception classes via `PyOnceLock` (`PostgresError::Auth` -> `GoogleCloudAuthError`, `SQLSTATE` `28xxx` -> `DatabaseError`), and commit `fix(postgres): validate static login provenance and expiry` passes tests and Clippy (R5, R9).

## Notes & Discoveries

- None.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — Shared GCP token source variants and refresh skew constants.
[^core-py-errors]: `src/rs/vertebra-py/src/errors.rs::db_exception_types` — Core `vertebra._vertebra` database and GCP auth Python exception classes.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Static login provenance, actual expiry, and error mapping contracts.
