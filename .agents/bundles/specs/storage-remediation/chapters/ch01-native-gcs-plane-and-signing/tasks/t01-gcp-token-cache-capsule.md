---
type: Task
id: "storage-remediation:ch01:t01"
chapter_id: ch01-native-gcs-plane-and-signing
title: Add C-ABI GcpTokenCacheCapsule and SharedGcpTokenCacheHandle and expose _capsule() on _vertebra caches
description: Define GCP_TOKEN_CACHE_CAPSULE_NAME, #[repr(C)] GcpTokenCacheCapsule, SharedGcpTokenCacheHandle, and GcpTokenCache::quota_project_id in vertebra-core, and expose _capsule() on SyncGcpTokenCache and AsyncGcpTokenCache in vertebra-py and _vertebra.pyi.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-py/src/gcp_auth.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
files:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-py/src/gcp_auth.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 210
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-08T22:25:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T22:30:02Z"
last_verified_commit: f38d895b
verification_evidence:
  red:
    exit_code: 1
    summary: "AttributeError: 'builtins.SyncGcpTokenCache' object has no attribute '_capsule'"
  green:
    cargo_test_exit_code: 0
    stubtest_exit_code: 0
    pytest_exit_code: 0
    pytest_summary: "14 passed in 6.34s"
    clippy_exit_code: 0
created_at: "2026-11-08T20:30:00Z"
updated_at: "2026-11-08T22:30:30Z"
commit: f38d895b
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:30:00Z"
verified: []
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Core GcpTokenCache implementation
  - id: core-shm
    resource: src/rs/vertebra-core/src/shm.rs::ShmCapsule
    title: Reference C-ABI PyCapsule pattern across Vertebra extensions
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache
    title: PyO3 token cache bindings on vertebra._vertebra
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: Type stubs for vertebra._vertebra
tags:
  - storage
  - gcs
  - ffi
  - tdd
---

# Task t01: Add C-ABI `GcpTokenCacheCapsule` and `SharedGcpTokenCacheHandle`

## Objective & Scope Boundary

Implement a sound `#[repr(C)]` C-ABI capsule contract (`GCP_TOKEN_CACHE_CAPSULE_NAME = c"vertebra.GcpTokenCacheCapsule"`, `GcpTokenCacheCapsule`, and `SharedGcpTokenCacheHandle`) plus `GcpTokenCache::quota_project_id(&self) -> Option<&str>` in `vertebra-core/src/gcp_auth.rs` [^core-gcp-auth] [^core-shm], and expose `def _capsule(self) -> object` on `PySyncGcpTokenCache` (`SyncGcpTokenCache`) and `PyAsyncGcpTokenCache` (`AsyncGcpTokenCache`) in `vertebra-py/src/gcp_auth.rs` [^py-gcp-auth] and `src/py/vertebra/_vertebra.pyi` [^vertebra-pyi].

Out of scope: `vertebra-storage` adoption (`t02` and `t03`) and Python `_store.py` wiring (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCache`, `GcpTokenSnapshot`, `GcpAuthSource`, `ServiceAccountKey` [^core-gcp-auth].
- `src/rs/vertebra-core/src/shm.rs` — `ShmCapsule` C-ABI capsule reference pattern [^core-shm].
- `src/rs/vertebra-py/src/gcp_auth.rs` — `PySyncGcpTokenCache`, `PyAsyncGcpTokenCache`, `crate::async_bridge::db_runtime_handle` [^py-gcp-auth].
- `src/py/vertebra/_vertebra.pyi` — `SyncGcpTokenCache` and `AsyncGcpTokenCache` stub declarations [^vertebra-pyi].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — Canonical token-cache unit test module.

## Symbol & Signature Contracts

- `src/rs/vertebra-core/src/gcp_auth.rs`:
  - `pub const GCP_TOKEN_CACHE_CAPSULE_NAME: &CStr = c"vertebra.GcpTokenCacheCapsule";`
  - `pub const GCP_TOKEN_CACHE_ABI_VERSION: u32 = 1;`
  - `GcpTokenCache::pub fn quota_project_id(&self) -> Option<&str>` inspecting `&self.source` (`GcpAuthSource` variants, recursing into `base` on `ImpersonatedServiceAccount` when `quota_project_id.is_none()`).
  - `#[repr(C)] pub struct GcpTokenCacheCapsule`:
    - `pub abi_version: u32`,
    - `pub ctx: *const c_void` (owned refcounted pointer created by `Arc::into_raw`),
    - `pub clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void`,
    - `pub drop_ctx_fn: unsafe extern "C" fn(*const c_void)`,
    - `pub generation_fn: unsafe extern "C" fn(*const c_void) -> u64`,
    - `pub invalidate_fn: unsafe extern "C" fn(*const c_void, u64)`,
    - `pub try_cached_fn: unsafe extern "C" fn(*const c_void, u64, *mut c_void, unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, u64, *const u8, usize)) -> bool` (lock-free `std::sync::RwLock` fast path when cached `GcpTokenSnapshot` is valid outside refresh skew),
    - `pub refresh_async_fn: unsafe extern "C" fn(*const c_void, bool, *mut c_void, unsafe extern "C" fn(*mut c_void, bool, *const u8, usize, u64, u64, *const u8, usize))` (spawns `cache.get_or_refresh_with_force(force_refresh)` onto the producing `cdylib`'s Tokio runtime handle and invokes `complete_cb` once with either `(true, token, expires_at_unix_ms, generation, quota_project_id)` or `(false, err_utf8, 0, 0, null, 0)`),
    - `pub service_account_key_fn: unsafe extern "C" fn(*const c_void, *mut c_void, unsafe extern "C" fn(*mut c_void, *const u8, usize, *const u8, usize, *const u8, usize, *const u8, usize, *const u8, usize)) -> bool`,
    - `pub quota_project_id_fn: unsafe extern "C" fn(*const c_void, *mut c_void, unsafe extern "C" fn(*mut c_void, *const u8, usize)) -> bool`.
  - `impl GcpTokenCacheCapsule`: `pub fn new(cache: Arc<GcpTokenCache>, refresh_async_fn: ...) -> Self` + `Drop` calling `drop_ctx_fn(self.ctx)`.
  - `#[derive(Clone)] pub struct SharedGcpTokenCacheHandle` wrapping either a local `Arc<GcpTokenCache>` or a foreign refcounted `GcpTokenCacheCapsule` vtable copy (`Send + Sync`, `Drop` calling `drop_ctx_fn`):
    - `pub fn from_arc(cache: Arc<GcpTokenCache>) -> Self`
    - `pub unsafe fn from_capsule_ptr(capsule_ptr: *const GcpTokenCacheCapsule) -> Result<Self, String>`
    - `pub fn get_cached(&self, now_unix_ms: u64) -> Option<Arc<GcpTokenSnapshot>>`
    - `pub async fn get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String>` (checks `get_cached(current_unix_ms())` first; on miss/expiry awaits a `tokio::sync::oneshot` channel completed by `refresh_async_fn` on foreign capsule or `cache.get_or_refresh().await` on local `Arc`)
    - `pub async fn get_or_refresh_with_force(&self, force_refresh: bool) -> Result<Arc<GcpTokenSnapshot>, String>`
    - `pub fn generation(&self) -> u64`
    - `pub fn invalidate(&self, failed_generation: u64)`
    - `pub fn service_account_key(&self) -> Option<ServiceAccountKey>`
    - `pub fn quota_project_id(&self) -> Option<String>`
- `src/rs/vertebra-py/src/gcp_auth.rs` & `src/py/vertebra/_vertebra.pyi`:
  - Add `fn _capsule(&self, py: Python<'_>) -> PyResult<Py<PyCapsule>>` on `PySyncGcpTokenCache` and `PyAsyncGcpTokenCache` using `GCP_TOKEN_CACHE_CAPSULE_NAME` and spawning `refresh_async_fn` onto `crate::async_bridge::db_runtime_handle()`, and declare `def _capsule(self) -> object: ...` on both `SyncGcpTokenCache` and `AsyncGcpTokenCache` in `_vertebra.pyi`.

## Edge Cases & Error Semantics

- Null `capsule_ptr`, null `ctx`, or mismatched `abi_version` in `from_capsule_ptr` returns `Err("invalid or incompatible GcpTokenCacheCapsule".to_string())`.
- Dropping Python `PyCapsule` while `SharedGcpTokenCacheHandle` clones remain alive keeps the underlying `Arc<GcpTokenCache>` valid via `clone_ctx_fn` (`Arc::increment_strong_count`) until the last handle drops (`// SAFETY:` documented on every `unsafe` block).
- Error strings from `GcpTokenCache::get_or_refresh_with_force` (`Result<Arc<GcpTokenSnapshot>, String>`) round-trip losslessly across `refresh_async_fn`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-core/src/gcp_auth.rs` (`#[cfg(test)]`) and `src/py/tests/unit/storage/test_gcs_token_refresh.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Foreign capsule warm + refresh + invalidate | `GcpTokenCacheCapsule` over loopback token server (`tok-1`, `tok-2`), cloned into `SharedGcpTokenCacheHandle`, original capsule dropped | First `get_or_refresh()` fetches `tok-1` (`gen=1`), second hits `get_cached()` (`1` HTTP call total), `invalidate(1)` + `get_or_refresh()` fetches `tok-2` (`gen=2`). |
| Service account & quota project metadata | `GcpTokenCache` with `quota_project_id = "qp-123"` and SA key `sa@proj.iam.gserviceaccount.com` | `handle.quota_project_id() == Some("qp-123".into())` and `handle.service_account_key().unwrap().client_email == "sa@proj.iam.gserviceaccount.com"`. |
| Python `_capsule()` parity across `to_async` / `to_sync` | `SyncGcpTokenCache({"access_token": "static-tok", "quota_project_id": "qp-1"})._capsule()` | Returns `PyCapsule` named `"vertebra.GcpTokenCacheCapsule"`; `mypy.stubtest vertebra._vertebra` passes. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core gcp_auth
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target uv run --offline --no-sync maturin develop --uv --offline
uv run --no-sync python -m mypy.stubtest vertebra._vertebra
uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q
```

## Acceptance Criteria

- [x] `GcpTokenCacheCapsule` (`#[repr(C)]`) and `SharedGcpTokenCacheHandle` pass warm-cache, single-flight refresh, `invalidate()`, `service_account_key()`, `quota_project_id()`, and post-capsule-drop lifetime tests (R1.1).
- [x] `SyncGcpTokenCache._capsule()` and `AsyncGcpTokenCache._capsule()` are exposed and pass `mypy.stubtest vertebra._vertebra` (R1.1).

## Notes & Discoveries

- [2026-11-08 20:30] Every `unsafe` block in `gcp_auth.rs` must carry an explicit `// SAFETY:` comment explaining pointer ownership and UTF-8 / lifetime invariants.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Core `GcpTokenCache` implementation.
[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmCapsule` — Reference C-ABI `PyCapsule` pattern across Vertebra extensions.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` — PyO3 token cache bindings on `vertebra._vertebra`.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Type stubs for `vertebra._vertebra`.
