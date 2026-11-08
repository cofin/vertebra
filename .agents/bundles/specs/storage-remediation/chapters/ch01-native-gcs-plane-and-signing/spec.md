---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch01-native-gcs-plane-and-signing
title: "Chapter 01: Native GCS Data Plane, Token Capsule, and Signing"
description: Share SyncGcpTokenCache and AsyncGcpTokenCache across vertebra._vertebra and vertebra._storage via a C-ABI PyCapsule, harden native GCS conditional generation updates, skip_signature emulator static-bearer bypass, ambient ADC isolation, x-goog-user-project headers, and pooled HTTP/IAM clients, support custom-endpoint GCS V4 presigning, and remove blocking Python GCSConfig setup.
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "storage-remediation:ch01:t03"
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
last_verified_checkpoint: 585e5dda
last_reviewed_commit: null
created_at: "2026-11-08T20:30:00Z"
updated_at: "2026-11-08T22:42:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:30:00Z"
verified: []
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Core GCP OAuth2 token cache shared across PyO3 extensions
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache
    title: PyO3 SyncGcpTokenCache and AsyncGcpTokenCache bindings on vertebra._vertebra
  - id: storage-gcp-auth
    resource: src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider
    title: Storage CredentialProvider adapter over GCP token caches
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Native GCS and multicloud ObjectStoreEngine builders and conditional operations
  - id: storage-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked
    title: GCS V4 local RSA and IAM signBlob presigning implementation
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: Python GCSConfig, SyncObjectStore, and AsyncObjectStore facade
tags:
  - storage
  - gcs
  - auth
  - ffi
  - pooling
---

# Chapter 01: Native GCS Data Plane, Token Capsule, and Signing

<!-- truth: start -->
- **C-ABI Capsule Handle (`FFI-04`)**: `vertebra-core::gcp_auth` defines `GCP_TOKEN_CACHE_CAPSULE_NAME` (`c"vertebra.GcpTokenCacheCapsule"`), `#[repr(C)] pub struct GcpTokenCacheCapsule`, and safe `SharedGcpTokenCacheHandle` so `_storage.so` consumes `SyncGcpTokenCache._capsule()` / `AsyncGcpTokenCache._capsule()` from `_vertebra.so` via C-ABI vtable function pointers without casting `#[repr(Rust)]` layouts across `cdylib` boundaries [^core-gcp-auth] [^py-gcp-auth].
- **Native GCS Plane & Connection Pooling**: `ObjectStoreEngine` backs `RefreshingGcpCredentialProvider` with `SharedGcpTokenCacheHandle`, maps numeric `if_match` generations directly to `UpdateVersion { e_tag: None, version: Some(gen) }` (`1` RTT) and verifies non-numeric ETags via `head` before passing `existing.version`, supplies `StaticCredentialProvider::new(GcpCredential { bearer: String::new() })` when `skip_signature=true` without credentials, isolates `GoogleCloudStorageBuilder::new()` when `allow_ambient_adc=false`, forwards `x-goog-user-project`, and applies pooled `ClientOptions` across HTTP backends [^storage-gcp-auth] [^storage-store-rs].
- **Pooled IAM `signBlob`, Custom GCS Endpoint & Non-Blocking `GCSConfig`**: `signing.rs` reuses `LazyLock<reqwest::Client>` pools, honors custom `endpoint` in GCS V4 canonical request and URL assembly, and `GCSConfig.to_url_and_options()` removes blocking `resolve_adc_token()`, `eager_impersonation`, and global `os.environ["NO_PROXY"]` mutation [^storage-signing-rs] [^py-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vertebra._vertebra` and `vertebra._storage` compile as separate `cdylib` shared libraries. `SyncObjectStore` and `AsyncObjectStore` currently retain `token_cache=` only on Python `self._gcp_token_cache` and never forward it to `vertebra._storage`, splitting refresh state and breaking GCS data-plane authentication whenever credentials live on `token_cache` [^py-store] [^storage-store-rs]. In addition, `object_store`'s GCS client fails on `PutMode::Update` unless `UpdateVersion.version` is populated, `skip_signature=true` without credentials attempts GCE metadata discovery on `put`/`multipart`/`delete`, `GoogleCloudStorageBuilder::from_env()` ignores `allow_ambient_adc=false`, `quota_project_id` is omitted from GCS/IAM headers, `signing.rs` builds an unpooled `reqwest::Client` per IAM `signBlob` call and hardcodes `storage.googleapis.com`, and `GCSConfig.to_url_and_options()` performs blocking OAuth I/O and mutates `os.environ["NO_PROXY"]` [^storage-store-rs] [^storage-signing-rs] [^py-store].

**Non-Goals**:
- `PyBackedBytes` FFI buffer extraction, `PreconditionError` mapping, `StorePath::parse`, S3/R2/OCI backend changes (Chapter 02).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `fd571e3a`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Core & PyO3 Capsule | `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` [^core-gcp-auth], `src/rs/vertebra-py/src/gcp_auth.rs::SyncGcpTokenCache` [^py-gcp-auth] | In-process C-ABI `PyCapsule` vtable + `SharedGcpTokenCacheHandle`. |
| Native GCS Engine | `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` [^storage-gcp-auth], `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` [^storage-store-rs] | Remote-owned HTTP boundary verified with loopback GCS/OAuth2 HTTP servers. |
| GCS Presigning & Python Config | `src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked` [^storage-signing-rs], `src/py/vertebra/storage/_store.py::GCSConfig` [^py-store] | Remote-owned IAM `signBlob` + in-process `GOOG4-RSA-SHA256` URL builder and Python store construction. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`object-storage.md`](../../../../knowledge/domains/storage/object-storage.md), [`ffi-and-gil-discipline.md`](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [`pyo3-bridge`](../../../../../skills/pyo3-bridge/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1.1 (`t01`)**: Implement `GCP_TOKEN_CACHE_CAPSULE_NAME`, `#[repr(C)] pub struct GcpTokenCacheCapsule` (with producer-runtime non-blocking async `refresh_async_fn` + `oneshot` completion callback so consumer Tokio worker threads never block), `SharedGcpTokenCacheHandle`, and `GcpTokenCache::quota_project_id(&self) -> Option<&str>` in `vertebra-core/src/gcp_auth.rs`, and expose `_capsule(self) -> object` on `PySyncGcpTokenCache` (`SyncGcpTokenCache`) and `PyAsyncGcpTokenCache` (`AsyncGcpTokenCache`) in `vertebra-py/src/gcp_auth.rs` and `src/py/vertebra/_vertebra.pyi`.
- **R1.2 (`t02`)**: Back `RefreshingGcpCredentialProvider` with `SharedGcpTokenCacheHandle`, fix GCS `PutMode::Update` (`version` generation + ETag `head` fallback returning `StorageError::Precondition { path, message }`), attach `StaticCredentialProvider` on `skip_signature=true`, honor `allow_ambient_adc=false` via `GoogleCloudStorageBuilder::new()`, forward `x-goog-user-project`, and apply shared pooled `ClientOptions` (`pool_max_idle_per_host(64)`, `pool_idle_timeout(90s)`, `connect_timeout(10s)`, `timeout(300s)`, `http2_keep_alive_interval(30s)`, loopback proxy excludes).
- **R1.3 (`t03`)**: Pool `reqwest::Client` in `signing.rs` via `LazyLock`, forward `x-goog-user-project` on IAM `signBlob`, add `endpoint: Option<String>` to `GcsSignConfig`, `build_gcs_v4_canonical_request`, `py_sign_gcs_v4_iam_url`, and `py_sign_gcs_v4_iam_url_async` (+ `token_cache_capsule: object | None = None` on `py_sign_gcs_v4_iam_url(_async)` and `PySyncObjectStore::new` / `PyAsyncObjectStore::new`) and synchronize `_storage.pyi`.
- **R1.4 (`t04`)**: Remove `eager_impersonation`, blocking `resolve_adc_token()`, and `os.environ["NO_PROXY"]` mutation from `GCSConfig.to_url_and_options()`, preserve `quota_project_id` and `target_service_account` on `_build_signing_adc_helper`, auto-share one `GcpTokenCache` across data-plane requests and IAM `signBlob` presigning when dynamic OAuth2 credentials are configured, and wire `token_cache._capsule()` + signing `endpoint` in `_store.py`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- `vertebra_core::gcp_auth::GcpTokenCacheCapsule` (`#[repr(C)]` vtable with `abi_version: u32`, `ctx: *const c_void`, `clone_ctx_fn`, `drop_ctx_fn`, `generation_fn`, `invalidate_fn`, `try_cached_fn`, `refresh_async_fn`, `service_account_key_fn`, `quota_project_id_fn`).
- `vertebra_core::gcp_auth::SharedGcpTokenCacheHandle` (`from_arc`, `unsafe fn from_capsule_ptr`, `get_cached(&self, now_unix_ms: u64) -> Option<Arc<GcpTokenSnapshot>>`, `async fn get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String>`, `async fn get_or_refresh_with_force(&self, force_refresh: bool) -> Result<Arc<GcpTokenSnapshot>, String>`, `generation`, `invalidate`, `service_account_key`, `quota_project_id`).
- `SyncObjectStore.__new__(cls, url: str, options: dict[str, str] | None = None, token_cache_capsule: object | None = None) -> SyncObjectStore` (and `AsyncObjectStore.__new__`).
- `GcsSignConfig { pub bucket: String, pub service_account_email: String, pub private_key_pem: Option<String>, pub access_token: Option<String>, pub endpoint: Option<String> }`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncObjectStore.__init__` / `AsyncObjectStore.__init__` -> `resolved_cache._capsule()` -> `native_mod.SyncObjectStore(url, options, capsule)` -> `SharedGcpTokenCacheHandle::from_capsule_ptr` -> `ObjectStoreEngine::from_url_with_cache` -> `RefreshingGcpCredentialProvider`.
- `_BaseObjectStore._sign_gcs_v4_iam` / `_sign_gcs_v4_iam_async` -> `py_sign_gcs_v4_iam_url` / `py_sign_gcs_v4_iam_url_async(..., endpoint=self._signing.endpoint, token_cache_capsule=capsule)` -> `build_gcs_v4_canonical_request` + pooled `IAM_HTTP_CLIENT` / `IAM_LOOPBACK_CLIENT`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Invalid `PyCapsule` object or name | Non-capsule or wrong capsule name passed to `SyncObjectStore` | Raises `TypeError` / `ValueError` (`ImproperConfigurationError` in Python facade) without dereferencing pointer | Memory safety across FFI (`FFI-04`). |
| Cached token fast path | Valid cached token with `> 300s` TTL remaining | `SharedGcpTokenCacheHandle::get_or_refresh` returns immediately from `try_cached_fn` without touching Tokio | Zero thread-hop overhead on warm GCS requests. |
| GCS `if_match` mismatch or missing object | `put("k", b"v", if_match="999")` or mismatched `"\"bad-etag\""` | Returns `StorageError::Precondition { path, message }` | Conditional write atomicity on GCS. |
| `skip_signature=true` without credentials | `put`, `create_multipart`, `delete` against loopback GCS | Uses empty `StaticCredentialProvider`; zero outbound calls to `169.254.169.254` | Hermetic emulator execution. |
| Custom GCS `endpoint` presigning | `endpoint="http://127.0.0.1:4443/storage/v1"` | Canonical `host:127.0.0.1:4443`, canonical URI `/storage/v1/<bucket>/<key>`, signed URL rooted at `http://127.0.0.1:4443/storage/v1/...` | Valid `GOOG4-RSA-SHA256` signatures for fake-gcs-server / regional endpoints. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** `SyncGcpTokenCache` / `AsyncGcpTokenCache` passed via `token_cache=` to `SyncObjectStore` / `AsyncObjectStore("gs://bucket", ...)`, **When** `put`, `get`, and `sign` execute against loopback OAuth2 + GCS + IAM servers, **Then** OAuth2 token exchange runs once and `invalidate()` propagates across both extensions.
2. **Given** `GCSConfig(bucket="b", target_service_account="sa@p.iam.gserviceaccount.com")`, **When** `to_url_and_options()` is called inside an active event loop with `HTTP_PROXY` set, **Then** zero network calls occur during `to_url_and_options()` and `os.environ` is untouched.

### 2.6 Alternatives Considered

- **Chosen**: C-ABI `GcpTokenCacheCapsule` vtable + `SharedGcpTokenCacheHandle` in `vertebra-core::gcp_auth`.
- **Rejected**: Passing raw `Arc<GcpTokenCache>` pointer across `_vertebra.so` and `_storage.so` (unsound `#[repr(Rust)]` cross-`cdylib` ABI and split Tokio runtime globals).

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch01]` (consolidated in Chapter 06 `ch06:t01` and `ch06:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test |
| --- | --- | --- | --- |
| R1.1 | `ch01 / t01` | `behavior_tdd` | `src/rs/vertebra-core/src/gcp_auth.rs`, `src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| R1.2 | `ch01 / t02` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R1.3 | `ch01 / t03` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_signing.py` |
| R1.4 | `ch01 / t04` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py` |

## 5. Implementation Plan

- [x] [f38d895b] storage-remediation:ch01:t01 — Add C-ABI `GcpTokenCacheCapsule` and `SharedGcpTokenCacheHandle` in `vertebra-core` and `_capsule()` on `_vertebra` token caches (tasks/t01-gcp-token-cache-capsule.md)
- [x] [585e5dda] storage-remediation:ch01:t02 — Wire `SharedGcpTokenCacheHandle` into `vertebra-storage` and harden GCS `if_match`, `skip_signature`, `allow_ambient_adc`, `x-goog-user-project`, and HTTP pooling (tasks/t02-native-gcs-data-plane-and-pooling.md)
- [ ] storage-remediation:ch01:t03 — Pool IAM `signBlob` HTTP clients, add custom GCS `endpoint` presigning, and wire PyO3 `_storage` bindings and `_storage.pyi` (tasks/t03-gcs-presigning-and-python-config-wiring.md)
- [ ] storage-remediation:ch01:t04 — Wire Python `GCSConfig`, `SyncObjectStore`, and `AsyncObjectStore` to `token_cache._capsule()` and custom GCS signing `endpoint` without blocking I/O (tasks/t04-python-gcs-config-and-store-wiring.md)
- **Chapter / Phase Recovery Checkpoint**: `PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage && uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/storage/test_signing.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `585e5dda` — `2/4` tasks completed in `ch01`.
- **Remaining Tasks in Chapter**: `t03`, `t04`.
- **Remaining Chapters**: `ch01`–`ch06`.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Core GCP OAuth2 token cache shared across PyO3 extensions.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` — PyO3 `SyncGcpTokenCache` and `AsyncGcpTokenCache` bindings on `vertebra._vertebra`.
[^storage-gcp-auth]: `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` — Storage `CredentialProvider` adapter over GCP token caches.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Native GCS and multicloud `ObjectStoreEngine` builders and conditional operations.
[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked` — GCS V4 local RSA and IAM `signBlob` presigning implementation.
[^py-store]: `src/py/vertebra/storage/_store.py::GCSConfig` — Python `GCSConfig`, `SyncObjectStore`, and `AsyncObjectStore` facade.
