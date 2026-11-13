---
type: Task
id: "gcp-embeddings-and-identity:ch01:t01"
flow_id: gcp-embeddings-and-identity
chapter_id: ch01-gcp-id-token-cache
title: "Add Rust GcpIdTokenCache with Service-Account and Metadata-Server Minting"
description: "Create vertebra-core::gcp_identity with per-audience single-flight caching, JWT aud/exp validation, bounded eviction, and service-account plus metadata-server ID token exchange."
status: stable
state: completed
commit: a601dee4
last_verified_commit: a601dee4
last_verified_at: "2026-11-13T04:49:30Z"
verification_evidence: "cargo test gcp_auth:: (13 passed) + clippy + stubtest + pytest (19 passed, 2 skipped)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/src/lib.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
estimated_loc: 240
tags:
  - gcp
  - identity
  - oidc
  - rust
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: "Credential resolution, RSA signing helpers, and reqwest HTTP clients"
  - id: core-auth-tests
    resource: src/rs/vertebra-core/tests/it/gcp_auth.rs
    title: "Loopback token/metadata server test harness"
---

# Task `t01`: Add Rust `GcpIdTokenCache` with Service-Account and Metadata-Server Minting

## Objective & Scope Boundary

Implement `src/rs/vertebra-core/src/gcp_identity.rs` (`#[cfg(feature = "gcp-auth")]` re-exported from `lib.rs`) and crate-visible `GcpTokenCache` accessors per [`../spec.md` §2](../spec.md) [^core-gcp-auth]. Out of scope: impersonation, `gcloud`, `authorized_user` ADC, and ambient metadata fallback (`t02`), plus PyO3 bindings (`t03`–`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCache`, `GcpAuthSource`, JWT signing, and HTTP clients [^core-gcp-auth].
- `src/rs/vertebra-core/src/lib.rs` — `gcp_auth` feature-gated module export.
- `src/rs/vertebra-core/Cargo.toml` — `jsonwebtoken`, `reqwest`, `url` workspace dependencies.
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` — loopback HTTP server fixtures and test RSA keys [^core-auth-tests].

## Contracts & Edge Cases

- Expose `GcpIdTokenOptions` (`id_token_refresh_skew_ms`, `include_email`, `metadata_id_token_format`, `max_id_token_audiences` option keys; invalid values return `Err` naming the key), `MetadataIdTokenFormat`, `GcpIdTokenSnapshot` (redacting `id_token` in `Debug`), `GcpIdTokenCache`, and `validate_id_token_audience` (`1..=2048` bytes, trimmed non-empty, zero ASCII control chars).
- Add `auth_source`, `http_client`, `metadata_http_client`, and `fetch_source_access_token` on `GcpTokenCache` in `gcp_auth.rs`.
- Mint `ServiceAccount` ID tokens via `target_audience` RS256 JWT assertion (`no scope`) at `token_uri`, and `MetadataServer` ID tokens via `/computeMetadata/v1/instance/service-accounts/{account}/identity?audience=<encoded>&format={full|standard}` with `Metadata-Flavor: Google`.
- Decode JWT `exp` and `aud` claims: reject malformed JWTs, `exp <= now`, or `aud != audience`; refresh single-flight per audience via per-key `Arc<tokio::sync::Mutex<()>>`; fence `invalidate(audience, failed_generation)` against stale generations; evict expired entries then earliest `expires_at_unix_ms` when exceeding `max_audiences`.

## Test Matrix

- Extend `src/rs/vertebra-core/tests/it/gcp_auth.rs`: service-account `target_audience` exchange + cached second call (`1` upstream POST); metadata `/identity` URL/header verification (`full` vs `standard`); `8`-task concurrent single-flight refresh; skew-window refresh (`exp < now + skew`); JWT `aud` mismatch, expired-on-arrival, and malformed-JWT errors; stale-generation `invalidate` no-op; `max_audiences = 2` eviction order; invalid audience rejection before network I/O.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core --features gcp-auth --all-targets -- -D warnings
```

## Acceptance Criteria

- [x] `GcpIdTokenCache` mints, validates, and caches per-audience tokens for `ServiceAccount` and `MetadataServer` sources with single-flight concurrency and bounded eviction (`R1`, `R2`).
- [x] Zero regressions in `gcp_auth` integration tests and Clippy.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Credential resolution, RSA signing helpers, and `reqwest` HTTP clients.
[^core-auth-tests]: `src/rs/vertebra-core/tests/it/gcp_auth.rs` — Loopback token/metadata server test harness.
