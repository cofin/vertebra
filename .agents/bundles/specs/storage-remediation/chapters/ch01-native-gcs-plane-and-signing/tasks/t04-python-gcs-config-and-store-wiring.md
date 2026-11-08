---
type: Task
id: "storage-remediation:ch01:t04"
chapter_id: ch01-native-gcs-plane-and-signing
title: Wire Python GCSConfig, SyncObjectStore, and AsyncObjectStore to token_cache._capsule() and custom GCS signing endpoint without blocking I/O
description: Remove blocking resolve_adc_token(), eager_impersonation, and NO_PROXY env mutation from GCSConfig.to_url_and_options, pass resolved_cache._capsule() into native SyncObjectStore/AsyncObjectStore, and forward SigningConfig.endpoint to GCS V4 presigning.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch01:t03"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/storage/test_signing.py
files:
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/storage/test_signing.py
estimated_loc: 120
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
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: Python GCSConfig, SyncObjectStore, and AsyncObjectStore implementation
tags:
  - storage
  - gcs
  - python
  - tdd
---

# Task t04: Wire Python `GCSConfig` and `SyncObjectStore` / `AsyncObjectStore` Token Capsule & Signing Endpoint

## Objective & Scope Boundary

Update `src/py/vertebra/storage/_store.py` [^py-store] (`1` production file):
1. Remove `eager_impersonation` and the blocking `self.resolve_adc_token()` call from `GCSConfig.to_url_and_options(self) -> tuple[str, dict[str, str]]`, and delete global `os.environ["NO_PROXY"]` / `os.environ["no_proxy"]` mutation.
2. When constructing `SyncObjectStore` or `AsyncObjectStore` over a `gs://` / `gcs://` store with dynamic OAuth2 credentials (`token_cache is None`, `skip_signature != "true"`, and no static `access_token`), auto-construct a shared `SyncGcpTokenCache` / `AsyncGcpTokenCache` when `GcpTokenCache` can be configured from options, and pass `resolved_cache._capsule() if resolved_cache is not None else None` to `native_mod.SyncObjectStore` and `native_mod.AsyncObjectStore`.
3. Preserve `quota_project_id` and `target_service_account` on `_build_signing_adc_helper`, and forward `self._signing.endpoint` plus `token_cache_capsule` (`cache._capsule()`) into `py_sign_gcs_v4_iam_url` and `py_sign_gcs_v4_iam_url_async` so Rust IAM `signBlob` shares single-flight refresh, `401` invalidation, and `x-goog-user-project` directly with the native GCS data plane.

Out of scope: `_store.py` `open_readable_file` and `_get_native_module` removal (`ch05:t02`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — `GCSConfig.to_url_and_options`, `SigningConfig.from_storage_config`, `_execute_gcs_v4_iam_sign`, `_sign_gcs_v4_iam_async`, `SyncObjectStore.__init__`, `AsyncObjectStore.__init__` [^py-store].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — End-to-end GCS data-plane + signing `token_cache=` sharing tests.
- `src/py/tests/unit/storage/test_signing.py` — Python `GCSConfig` custom endpoint signing tests.

## Symbol & Signature Contracts

- `GCSConfig.to_url_and_options(self) -> tuple[str, dict[str, str]]`
- `SyncObjectStore.__init__` -> `native_mod.SyncObjectStore(self._url, self._options or None, capsule)`
- `AsyncObjectStore.__init__` -> `native_mod.AsyncObjectStore(self._url, self._options or None, capsule)`

## Edge Cases & Error Semantics

- Constructing `SyncObjectStore` or `AsyncObjectStore` with `GCSConfig(bucket="b", target_service_account="imp@p.iam.gserviceaccount.com")` inside an active `anyio` loop performs zero blocking network calls during `__init__`; token exchange happens lazily through native `GcpTokenCache` (`GcpAuthSource::ImpersonatedServiceAccount`).
- Process `os.environ` is never mutated when `GCSConfig(endpoint="http://127.0.0.1:4443")` is constructed.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_gcs_token_refresh.py` and `src/py/tests/unit/storage/test_signing.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| End-to-end `SyncObjectStore` & `AsyncObjectStore` `token_cache=` sharing | Loopback OAuth2 + GCS + IAM servers; `cache = AsyncGcpTokenCache({...})`; `store = AsyncObjectStore("gs://b", options={"endpoint": gcs_url, "allow_http": "true"}, signing=SigningConfig(...), token_cache=cache)` | `await store.put("k", b"hi")`, `await store.get("k")`, and `await store.sign("k")` share `1` OAuth2 fetch; `401` on IAM bumps `cache.generation` to `2` for subsequent data-plane calls. |
| Custom `GCSConfig(endpoint=...)` `store.sign(...)` | `SyncObjectStore(GCSConfig(bucket="b", endpoint="http://127.0.0.1:4443", service_account_email=..., private_key_pem=...)).sign("a/b.txt")` | Emits presigned URL rooted at `http://127.0.0.1:4443/b/a/b.txt?...` without mutating `os.environ["NO_PROXY"]`. |
| Non-blocking `target_service_account` config | `GCSConfig(bucket="b", target_service_account="imp@p.iam.gserviceaccount.com").to_url_and_options()` | Returns `("gs://b", {"target_service_account": "imp@p.iam.gserviceaccount.com"})` immediately with zero HTTP calls. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/storage/test_signing.py
uv run --no-sync mypy src/py/vertebra/storage/_store.py
uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/storage/test_signing.py -q
```

## Acceptance Criteria

- [ ] `SyncObjectStore` and `AsyncObjectStore` share `token_cache=` across native GCS data-plane operations and IAM `signBlob` (R1.1, R1.3).
- [ ] `GCSConfig.to_url_and_options()` is non-blocking and leaves `os.environ` untouched, and `SigningConfig.endpoint` is honored by `store.sign` (R1.3).

## Notes & Discoveries

- [2026-11-08 20:30] Ensure `_store.py:694` (`SigningConfig.from_storage_config`) calls `config.to_url_and_options()` with zero arguments after removing `eager_impersonation`.

[^py-store]: `src/py/vertebra/storage/_store.py::GCSConfig` — Python `GCSConfig`, `SyncObjectStore`, and `AsyncObjectStore` implementation.
