---
type: Task
id: "storage-remediation:ch01:t02"
chapter_id: ch01-native-gcs-plane-and-signing
title: Wire SharedGcpTokenCacheHandle into vertebra-storage and harden GCS if_match, skip_signature, ambient ADC, quota project, and HTTP pooling
description: Adopt SharedGcpTokenCacheHandle in RefreshingGcpCredentialProvider and SyncObjectStore/AsyncObjectStore, fix GCS PutMode::Update version generation handling, attach StaticCredentialProvider on skip_signature=true, isolate GoogleCloudStorageBuilder::new() on allow_ambient_adc=false, forward x-goog-user-project, and apply pooled ClientOptions across HTTP backends.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch01:t01"
read_set:
  - src/rs/vertebra-storage/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
  - src/rs/vertebra-storage/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/store.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 235
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-08T22:32:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T22:41:28Z"
last_verified_commit: 585e5dda
verification_evidence:
  red:
    exit_code: 101
    summary: "error[E0599]: no associated function or constant named `from_url_with_cache` found for struct `ObjectStoreEngine`"
  green:
    cargo_test_exit_code: 0
    cargo_test_summary: "3 unit + 23 integration passed"
    clippy_exit_code: 0
created_at: "2026-11-08T20:30:00Z"
updated_at: "2026-11-08T22:42:00Z"
commit: 585e5dda
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:30:00Z"
verified: []
sources:
  - id: storage-gcp-auth
    resource: src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider
    title: RefreshingGcpCredentialProvider in vertebra-storage
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: ObjectStoreEngine builder and conditional put logic
tags:
  - storage
  - gcs
  - pooling
  - tdd
---

# Task t02: Harden Native GCS Data Plane, Token Capsule Adoption, and HTTP Pooling

## Objective & Scope Boundary

Wire `SharedGcpTokenCacheHandle` through `RefreshingGcpCredentialProvider` and `ObjectStoreEngine` [^storage-gcp-auth] [^storage-store-rs], and remediate all native GCS data-plane correctness and HTTP pooling defects in `store.rs`:
1. GCS `PutMode::Update` (`if_match`): map numeric ASCII-digit generation strings directly to `UpdateVersion { e_tag: None, version: Some(val) }`; for non-numeric ETags, fetch `self.inner.head(&location).await`, verify `existing.e_tag.as_deref() == Some(val.as_str())` (returning `StorageError::Precondition` on `NotFound` or mismatch), and pass `UpdateVersion { e_tag: None, version: existing.version }` (returning `StorageError::Precondition` if `existing.version` is `None`).
2. GCS `skip_signature=true` / `STORAGE_EMULATOR_HOST`: when `skip_signature` is `true` and neither `credential_cache` nor `access_token` is configured, attach `Arc::new(StaticCredentialProvider::new(GcpCredential { bearer: String::new() }))` alongside `.with_skip_signature(true)` so `put`, `multipart`, and `delete` never contact `169.254.169.254`.
3. GCS builder isolation & `x-goog-user-project`: construct `GoogleCloudStorageBuilder::new()` (instead of `from_env()`) whenever `allow_ambient_adc == false`, `skip_signature == true`, `credential_cache.is_some()`, or explicit credentials are supplied; forward `quota_project_id` (from `options` or `credential_cache.quota_project_id()`) via `HeaderMap` (`x-goog-user-project`) on `ClientOptions::with_default_headers`.
4. Shared pooled `build_http_client_options`: configure `pool_max_idle_per_host(64)`, `pool_idle_timeout(90s)`, `connect_timeout(10s)`, `timeout(300s)`, `http2_keep_alive_interval(30s)`, `with_allow_http(allow_http)`, and `with_proxy_excludes("127.0.0.1,localhost,::1,metadata.google.internal")` (when `allow_http` or loopback endpoint) across `Gcs`, `S3`, `R2`, `Azure`, and `Http`.

Out of scope: PyO3 `lib.rs` / `_storage.pyi` / Python `_store.py` wiring (`t03` updates `lib.rs`, `_storage.pyi`, and `_store.py` atomically).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcp_auth.rs` — `RefreshingGcpCredentialProvider` [^storage-gcp-auth].
- `src/rs/vertebra-storage/src/store.rs` — `BackendConfig`, `ObjectStoreEngine::from_url`, `ObjectStoreEngine::put_opts` [^storage-store-rs].
- `src/rs/vertebra-storage/src/signing.rs` — `sign_gcs_v4_with_options_and_cache_checked` caller compatibility if `SharedGcpTokenCacheHandle` is referenced.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Loopback HTTP GCS integration tests.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/src/gcp_auth.rs`:
  - `RefreshingGcpCredentialProvider::new(cache: SharedGcpTokenCacheHandle) -> Self`
  - `RefreshingGcpCredentialProvider::from_arc(cache: Arc<GcpTokenCache>) -> Self`
- `src/rs/vertebra-storage/src/store.rs`:
  - `ObjectStoreEngine::from_url_with_cache(url: &str, options: Option<HashMap<String, String>>, external_token_cache: Option<SharedGcpTokenCacheHandle>) -> Result<Self, StorageError>` (with `from_url` delegating with `None`).
  - `fn build_http_client_options(allow_http: bool, endpoint: Option<&str>, default_headers: Option<http::HeaderMap>) -> ClientOptions`.

## Edge Cases & Error Semantics

- Invalid `quota_project_id` containing control characters / invalid HTTP header bytes returns `StorageError::InvalidInput`.
- `gs://` `if_match` when object does not exist on non-numeric ETag lookup maps `object_store::Error::NotFound` to `StorageError::Precondition { path, message }`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| GCS `if_match` numeric generation | Loopback GCS server expecting `x-goog-if-generation-match: 42`; `engine.put_opts("obj.txt", b"v2", if_match="42")` vs `if_match="41"` (HTTP `412`) | `"42"` sends `1` PUT request with `x-goog-if-generation-match: 42` and succeeds; `"41"` returns `Err(StorageError::Precondition { .. })` / `ObjectStore(Precondition)`. |
| GCS `if_match` ETag string | Loopback GCS HEAD returning `ETag: "abc"`, `x-goog-generation: 77`; `put_opts(..., if_match="\"abc\"")` vs `if_match="\"wrong\""` | Matching ETag sends PUT with `x-goog-if-generation-match: 77`; mismatched ETag fails with `StorageError::Precondition { .. }` before PUT. |
| GCS `skip_signature=true` write/multipart/delete | `ObjectStoreEngine::from_url("gs://emu-bucket", Some({"endpoint": loop_url, "allow_http": "true", "skip_signature": "true"}))` with `GCE_METADATA_HOST=127.0.0.1:1` | `put`, `create_multipart` + `write_part` + `complete`, and `delete` succeed with zero metadata requests and empty/omitted `Authorization`. |
| `x-goog-user-project` & `allow_ambient_adc=false` | `GOOGLE_OAUTH_ACCESS_TOKEN="leaked"` in env, `allow_ambient_adc="false"`, `SharedGcpTokenCacheHandle` with `quota_project_id="billing-proj"` | Requests carry `Bearer <cache-token>` (never `"leaked"`) and `x-goog-user-project: billing-proj`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
```

## Acceptance Criteria

- [x] `ObjectStoreEngine::from_url_with_cache` and `RefreshingGcpCredentialProvider` adopt `SharedGcpTokenCacheHandle` (R1.2).
- [x] GCS `if_match` succeeds for numeric generations (`1` RTT) and ETag strings, and returns `StorageError::Precondition` on mismatch (R1.2).
- [x] `skip_signature=true` completes `put`, `multipart`, and `delete` without dialing GCE metadata, `allow_ambient_adc=false` isolates env vars, `x-goog-user-project` is sent, and pooled `ClientOptions` are configured across HTTP builders (R1.2).

## Notes & Discoveries

- [2026-11-08 20:30] Keep `sign_gcs_v4_with_options_and_cache` compatible with both `&Arc<GcpTokenCache>` and `SharedGcpTokenCacheHandle` so existing Rust tests in `storage_engine_tests.rs` continue to compile cleanly.

[^storage-gcp-auth]: `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` — `RefreshingGcpCredentialProvider` in `vertebra-storage`.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` builder and conditional put logic.
