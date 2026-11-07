---
type: Task
id: storage-completeness:ch01:t03
chapter_id: ch01-native-engine-and-auth
title: Deduplicate Rust GCS auth wrapper and complete multi-cloud presigning including OCI PAR
description: Trim vertebra-storage gcp_auth.rs to RefreshingGcpCredentialProvider over vertebra_core::gcp_auth::GcpTokenCache
  and complete GCS V4, IAM signBlob, S3 SigV4, Azure SAS, and OCI PAR URL presigning.
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch01:t01
read_set:
- src/rs/vertebra-core/src/gcp_auth.rs
- src/rs/vertebra-storage/src/gcp_auth.rs
- src/rs/vertebra-storage/src/signing.rs
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_signing.py
files:
- src/rs/vertebra-storage/src/gcp_auth.rs
- src/rs/vertebra-storage/src/signing.rs
- src/rs/vertebra-storage/src/oci.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_signing.py
estimated_loc: 190
plan_revision: 4
plan_commit: null
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T17:32:04Z'
last_verified_commit: 494ad92b
verification_evidence: 'uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage (22 passed: 3 unit
  + 19 integration), cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings (0 warnings),
  uv run --no-sync pytest src/py/tests/unit/storage/test_signing.py -q (4 passed)'
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-07T17:32:04Z'
commit: 494ad92b
sources:
- id: core-gcp-cache
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
  title: Shared single-flight GCP token cache in vertebra-core
- id: storage-gcp-provider
  resource: src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider
  title: object_store CredentialProvider adapter in vertebra-storage
- id: storage-signing
  resource: src/rs/vertebra-storage/src/signing.rs
  title: GCS V4, S3 SigV4, and Azure SAS presigned URL builders
- id: storage-oci
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Oracle Cloud Infrastructure Object Storage and PAR URL support
tags:
- storage
- gcp
- signing
- oci
- rust
---

# Task t03: Deduplicate Rust GCS Auth Wrapper and Complete Multi-Cloud Presigning Including OCI PAR

## Objective & Scope Boundary

Eliminate duplicated `gcp_auth` re-exports and redundant token-refresh tests in `src/rs/vertebra-storage/src/gcp_auth.rs` so `vertebra-storage` keeps only `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider` over `vertebra_core::gcp_auth::GcpTokenCache` [^core-gcp-cache] [^storage-gcp-provider], and complete deterministic presigned URL generation across GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob` digest formatting), S3 SigV4, Azure SAS, and OCI Pre-Authenticated Request (PAR) URLs in `signing.rs` and `oci.rs` [^storage-signing] [^storage-oci]. Removing the duplicate PyO3 token classes from `lib.rs` and updating Python callers is owned by `storage-completeness:ch01:t04`.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Canonical `GcpTokenCache`, `GcpAuthOptions`, and `GcpAuthError` [^core-gcp-cache].
- `src/rs/vertebra-storage/src/gcp_auth.rs` — `RefreshingGcpCredentialProvider` and duplicate re-exports/tests to prune [^storage-gcp-provider].
- `src/rs/vertebra-storage/src/signing.rs` — Existing GCS V4, S3 SigV4, and Azure SAS signing functions [^storage-signing].
- `src/rs/vertebra-storage/src/oci.rs` — OCI S3-compat / native PAR URL formatting and RSA request signing helpers [^storage-oci].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests for storage and signing.

- `src/py/tests/unit/storage/test_signing.py` — Canonical Python signing regressions.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/src/gcp_auth.rs`:
  - Keep `pub const GCS_OAUTH_SCOPES: &[&str]` and `pub struct RefreshingGcpCredentialProvider` implementing `object_store::CredentialProvider` via `Arc<vertebra_core::gcp_auth::GcpTokenCache>`; replace duplicated `GcpTokenCache` unit tests with focused unit tests on `RefreshingGcpCredentialProvider::get_credential`.
- `src/rs/vertebra-storage/src/signing.rs` & `src/rs/vertebra-storage/src/oci.rs`:
  - Ensure `sign_gcs_v4_url` (local RSA PEM `GOOG4-RSA-SHA256`), `build_gcs_v4_canonical_request` + `assemble_gcs_iam_v4_url` (for IAM `signBlob` hex signature assembly without local private key), `sign_s3_v4_url` (supporting AWS S3 and generic custom S3 endpoints with explicitly configured region), `sign_azure_sas_url` (`sv`, `sr=b`, `sp`, `se`, `sig` HMAC-SHA256), and `build_oci_par_url` / `sign_oci_request_headers` (`oci` namespace/region/bucket/object PAR URL construction and request-target RSA-SHA256 signature) have clean, deterministic pure-Rust APIs with `expires_in_secs` bounds validation (`1..=604800`).

## Edge Cases & Error Semantics

- `expires_in_secs == 0` or `expires_in_secs > 604_800` (7 days for GCS V4 / S3 SigV4) returns a descriptive `SigningError` (`InvalidExpiry`) mapped to `ValueError` in Python.
- Object keys containing spaces, `+`, `/`, and UTF-8 percent-encoded segments (`"reports/2026 Q4/summary+final.parquet"`) encode path segments per RFC 3986 without double-escaping `/` in GCS/S3 canonical URIs.
- Invalid RSA PEM or malformed base64 Azure account key returns `SigningError::InvalidKey` (never `unwrap()`/`expect()`).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` (and pure-Rust unit tests in `gcp_auth.rs` / `signing.rs` / `oci.rs`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `RefreshingGcpCredentialProvider` credential fetch | Mock OAuth token endpoint returning `access_token = "tok_1"`, call `provider.get_credential().await` | Returns `GcpCredential { bearer: "tok_1" }` via `vertebra_core::gcp_auth::GcpTokenCache`. |
| GCS V4 local RSA & IAM `signBlob` parity | Fixed timestamp `20261107T000000Z`, RSA test key, object `"dir/a b+c.txt"`, `expires=900` | Local `sign_gcs_v4_url` and IAM `assemble_gcs_iam_v4_url` with same RSA signature produce identical `X-Goog-Signature` URLs with `/dir/a%20b%2Bc.txt`. |
| S3 SigV4 custom endpoint signing | Bucket `"assets"`, key `"img/logo.png"`, region `"auto"`, endpoint `"https://s3.example.test"`, `expires=3600` | Produces `X-Amz-Algorithm=AWS4-HMAC-SHA256`, `X-Amz-Credential=...%2Fauto%2Fs3%2Faws4_request`, deterministic 64-hex `X-Amz-Signature`. |
| Azure Blob SAS & OCI PAR URLs | Azure base64 key + `"container"`, `"blob/file.csv"`, `method="PUT"`; OCI region `"us-ashburn-1"`, namespace `"ns1"`, bucket `"bkt"`, par token `"p/tok/"`, key `"k/1.bin"` | Azure URL carries `sp=cw` (or `w`), `sr=b`, valid HMAC `sig`; OCI URL equals `"https://objectstorage.us-ashburn-1.oraclecloud.com/p/tok/n/ns1/b/bkt/o/k%2F1.bin"` (properly escaped). |
| Out-of-bounds expiry | `expires_in_secs = 0` and `expires_in_secs = 604_801` | Both return `Err(SigningError::InvalidExpiry(_))`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `src/rs/vertebra-storage/src/gcp_auth.rs` delegates exclusively to `vertebra_core::gcp_auth::GcpTokenCache` without duplicate token-cache re-exports or duplicated core tests (R2).
- [x] GCS V4 (RSA + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR URL builders handle RFC 3986 path encoding and expiry bounds without panics (R2).

## Notes & Discoveries

- [2026-11-07 04:15] `vertebra-core` already owns single-flight proactive refresh and `gcloud` fallback tests in `src/rs/vertebra-core/src/gcp_auth.rs`.
- [2026-11-07 17:32] Kept crate-private `pub(crate) use` in `gcp_auth.rs` so `store.rs` and `lib.rs` compile cleanly before `ch01:t04` deletes the duplicate PyO3 token classes; added typed `SigningError` (`InvalidExpiry`, `InvalidKey`, `InvalidUrl`, `SignBlob`) with `1..=604_800`s bounds and `build_oci_par_url` / `sign_oci_request_headers`.

[^core-gcp-cache]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Canonical GCP OAuth2 token cache.
[^storage-gcp-provider]: `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` — GCS `CredentialProvider` wrapper.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Multi-cloud URL presigning module.
[^storage-oci]: `src/rs/vertebra-storage/src/oci.rs` — OCI Object Storage and PAR helper module.
