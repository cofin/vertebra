---
type: Task
id: "storage-remediation:ch01:t03"
chapter_id: ch01-native-gcs-plane-and-signing
title: Pool IAM signBlob clients, add custom GCS endpoint presigning, and wire PyO3 _storage bindings and _storage.pyi
description: Pool reqwest::Client instances in signing.rs, forward x-goog-user-project on IAM signBlob, add endpoint to GcsSignConfig and py_sign_gcs_v4_iam_url(_async), accept token_cache_capsule on PySyncObjectStore::new and PyAsyncObjectStore::new, and synchronize _storage.pyi.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch01:t02"
read_set:
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - src/py/tests/unit/storage/test_signing.py
files:
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - src/py/tests/unit/storage/test_signing.py
estimated_loc: 185
plan_revision: 1
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:30:00Z"
updated_at: "2026-11-08T20:30:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:30:00Z"
verified: []
sources:
  - id: storage-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_url
    title: GCS V4 canonical request, URL assembly, and IAM signBlob client
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::py_sign_gcs_v4_iam_url
    title: PyO3 GCS V4 signing and store constructor bindings
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::py_sign_gcs_v4_iam_url
    title: Type stubs for vertebra._storage
tags:
  - storage
  - gcs
  - signing
  - ffi
  - tdd
---

# Task t03: Pool IAM `signBlob`, Add Custom GCS Presigning `endpoint`, and Wire `vertebra._storage` Bindings & Stubs

## Objective & Scope Boundary

Update `src/rs/vertebra-storage/src/signing.rs` [^storage-signing-rs], `src/rs/vertebra-storage/src/lib.rs` [^storage-lib-rs], and `src/py/vertebra/_storage.pyi` [^storage-pyi] (`3` files):
1. Replace per-call `reqwest::Client::builder().no_proxy().build()` in `signing.rs` with two `LazyLock<reqwest::Client>` pools (`IAM_LOOPBACK_CLIENT` with `.no_proxy()` when `iam_base_url` host is loopback, and `IAM_HTTP_CLIENT` with `pool_max_idle_per_host(32)`, `pool_idle_timeout(90s)`, `connect_timeout(10s)`, `timeout(30s)` otherwise), and attach `x-goog-user-project` when `snap.quota_project_id` is set.
2. Add `pub endpoint: Option<String>` to `GcsSignConfig` (defaulting to `None` -> `https://storage.googleapis.com` and `host:storage.googleapis.com`), supporting custom scheme, host:port, and path prefix in `build_gcs_v4_canonical_request` and `assemble_gcs_iam_v4_url`, and add `endpoint: Option<String> = None` to `build_gcs_v4_canonical_request`, `py_sign_gcs_v4_iam_url`, and `py_sign_gcs_v4_iam_url_async` plus `token_cache_capsule: Option<&Bound<'_, PyAny>> = None` to `py_sign_gcs_v4_iam_url` and `py_sign_gcs_v4_iam_url_async` (forwarding `SharedGcpTokenCacheHandle` to `sign_gcs_v4_with_iam_endpoint_and_cache` so Rust IAM `signBlob` shares single-flight refresh, 401 invalidation, and `snap.quota_project_id`).
3. Add `token_cache_capsule: Option<&Bound<'_, PyAny>> = None` on `PySyncObjectStore::new` and `PyAsyncObjectStore::new` in `lib.rs` (extracting `PyCapsule` with `GCP_TOKEN_CACHE_CAPSULE_NAME` into `SharedGcpTokenCacheHandle`) and update `src/py/vertebra/_storage.pyi`.

Out of scope: Python `src/py/vertebra/storage/_store.py` wiring (`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/signing.rs` — `GcsSignConfig`, `build_gcs_v4_canonical_request`, `assemble_gcs_iam_v4_url`, `sign_gcs_v4_with_options_and_cache_checked` [^storage-signing-rs].
- `src/rs/vertebra-storage/src/lib.rs` — `PySyncObjectStore::new`, `PyAsyncObjectStore::new`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async` [^storage-lib-rs].
- `src/py/vertebra/_storage.pyi` — `SyncObjectStore.__new__`, `AsyncObjectStore.__new__`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async` [^storage-pyi].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust `GcsSignConfig` call sites and presigning tests.
- `src/py/tests/unit/storage/test_signing.py` — Python native signing binding tests.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/src/signing.rs`:
  - `pub struct GcsSignConfig { pub bucket: String, pub service_account_email: String, pub private_key_pem: Option<String>, pub access_token: Option<String>, pub endpoint: Option<String> }`
- `src/rs/vertebra-storage/src/lib.rs` & `src/py/vertebra/_storage.pyi`:
  - `SyncObjectStore.__new__(cls, url: str, options: dict[str, str] | None = None, token_cache_capsule: object | None = None) -> SyncObjectStore`
  - `AsyncObjectStore.__new__(cls, url: str, options: dict[str, str] | None = None, token_cache_capsule: object | None = None) -> AsyncObjectStore`
  - `py_sign_gcs_v4_iam_url(..., private_key_pem: str | None = None, endpoint: str | None = None, token_cache_capsule: object | None = None) -> str`
  - `py_sign_gcs_v4_iam_url_async(..., private_key_pem: str | None = None, endpoint: str | None = None, token_cache_capsule: object | None = None) -> RustTaskAwaitable`
  - `build_gcs_v4_canonical_request(bucket: str, service_account_email: str, method: str, object_path: str, expires_in_secs: int = 3600, timestamp: int | None = None, endpoint: str | None = None) -> tuple[str, str, str, str]`

## Edge Cases & Error Semantics

- Invalid `token_cache_capsule` (non-`PyCapsule` or mismatched capsule name) raises `PyValueError`.
- Malformed custom `endpoint` (missing `http://` / `https://` scheme or empty host) returns `SigningError::InvalidInput` (`ValueError` in Python).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` and `src/py/tests/unit/storage/test_signing.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Custom GCS `endpoint` V4 URL & canonical host | `py_sign_gcs_v4_iam_url("my-bkt", sa_email, "GET", "dir/file.txt", 900, 1700000000, private_key_pem=pem, endpoint="http://127.0.0.1:4443/storage/v1")` | Signed URL starts with `http://127.0.0.1:4443/storage/v1/my-bkt/dir/file.txt?` and RSA signature verifies against canonical `host:127.0.0.1:4443` and URI `/storage/v1/my-bkt/dir/file.txt`. |
| Invalid `token_cache_capsule` rejected | `vertebra._storage.SyncObjectStore("memory://test", None, "not-a-capsule")` | Raises `ValueError`. |
| IAM `signBlob` `x-goog-user-project` header | `GcpTokenCache` with `quota_project_id = "qp-sign"` signing via loopback IAM `signBlob` | IAM request carries `x-goog-user-project: qp-sign`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target uv run --offline --no-sync maturin develop --uv --offline --manifest-path src/rs/vertebra-storage/Cargo.toml
uv run --no-sync python -m mypy.stubtest vertebra._storage --allowlist tools/stubtest/allowlist.txt
uv run --no-sync pytest src/py/tests/unit/storage/test_signing.py -q
```

## Acceptance Criteria

- [ ] `signing.rs` reuses `LazyLock<reqwest::Client>` pools and attaches `x-goog-user-project` on IAM `signBlob` (R1.3).
- [ ] `GcsSignConfig`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`, and `PySyncObjectStore::new` / `PyAsyncObjectStore::new` match `_storage.pyi` (`mypy.stubtest vertebra._storage` exits `0`) (R1.2, R1.3).

## Notes & Discoveries

- [2026-11-08 20:30] Update existing `GcsSignConfig { ... }` struct literals in `storage_engine_tests.rs` with `endpoint: None`.

[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_url` — GCS V4 canonical request, URL assembly, and IAM `signBlob` client.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::py_sign_gcs_v4_iam_url` — PyO3 GCS V4 signing and store constructor bindings.
[^storage-pyi]: `src/py/vertebra/_storage.pyi::py_sign_gcs_v4_iam_url` — Type stubs for `vertebra._storage`.
