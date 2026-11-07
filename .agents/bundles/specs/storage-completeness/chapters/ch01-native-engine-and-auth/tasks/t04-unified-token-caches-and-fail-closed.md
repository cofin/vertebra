---
type: Task
id: "storage-completeness:ch01:t04"
chapter_id: ch01-native-engine-and-auth
title: Unify Python GCP token caches on vertebra._vertebra and enforce fail-closed stores
description: Remove duplicate GcpTokenCache PyO3 classes from vertebra._storage, delete pure-Python OAuth fetchers and self._memory fallbacks from storage/_store.py, wire OCI PAR signing, and enforce fail-closed SyncObjectStore and AsyncObjectStore construction.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-completeness:ch01:t02"
  - "storage-completeness:ch01:t03"
read_set:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/storage/test_signing.py
files:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/storage/_store.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/storage/test_signing.py
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 220
plan_revision: 2
plan_commit: cdb978d3
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T18:04:00Z"
last_verified_commit: 7ad5269a
verification_evidence: "cargo clippy -p vertebra-storage --all-targets -- -D warnings (0 warnings), cargo test -p vertebra-storage (22 passed), mypy.stubtest vertebra._storage (0 issues), ruff check (0 issues), mypy src/py/vertebra/storage (0 issues), pytest src/py/tests/unit/storage -q (48 passed)"
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T18:04:00Z"
commit: 7ad5269a
sources:
  - id: core-pyi-gcp
    resource: src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache
    title: Canonical SyncGcpTokenCache, AsyncGcpTokenCache, and GcpTokenSnapshot on vertebra._vertebra
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs
    title: vertebra._storage PyO3 module registration and signing functions
  - id: storage-store-py
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: Python GCSConfig, SigningConfig, SyncObjectStore, and AsyncObjectStore
tags:
  - storage
  - gcp
  - signing
  - pyo3
  - security
---

# Task t04: Unify Python GCP Token Caches on `vertebra._vertebra` and Enforce Fail-Closed Stores

## Objective & Scope Boundary

Remove the duplicate `GcpTokenSnapshot`, `SyncGcpTokenCache`, and `AsyncGcpTokenCache` PyO3 classes from `vertebra._storage` (`src/rs/vertebra-storage/src/lib.rs` and `src/py/vertebra/_storage.pyi`), wire `sign_oci_par_url` / IAM `signBlob` presigning on `vertebra._storage`, delete pure-Python `urllib` OAuth token fetchers and `self._memory` dict fallbacks from `src/py/vertebra/storage/_store.py`, re-export `vertebra._vertebra.{GcpTokenSnapshot, SyncGcpTokenCache, AsyncGcpTokenCache}`, and make `SyncObjectStore` and `AsyncObjectStore` fail closed when `vertebra._storage` or a target config/URI is missing [^core-pyi-gcp] [^storage-lib-rs] [^storage-store-py].

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/lib.rs` — PyO3 module exports (`m.add_class` / `m.add_function`) [^storage-lib-rs].
- `src/py/vertebra/_vertebra.pyi::{GcpTokenSnapshot,SyncGcpTokenCache,AsyncGcpTokenCache}` — Canonical core token cache API (`from_options`, `from_service_account_json`, `get_token`, `invalidate`) [^core-pyi-gcp].
- `src/py/vertebra/_storage.pyi` — Stubs for `vertebra._storage`.
- `src/py/vertebra/storage/_store.py` — `GCSConfig`, `OCIConfig`, `SigningConfig`, `_BaseObjectStore`, `SyncObjectStore`, and `AsyncObjectStore` [^storage-store-py].
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — GCS token cache refresh and impersonation tests.
- `src/py/tests/unit/storage/test_signing.py` — Presigned URL tests across GCS, S3, Azure, and OCI.

## Symbol & Signature Contracts

- `src/rs/vertebra-storage/src/lib.rs` & `src/py/vertebra/_storage.pyi`:
  - Remove `PyGcpTokenSnapshot`, `PySyncGcpTokenCache`, and `PyAsyncGcpTokenCache` from `vertebra._storage`.
  - Register `sign_oci_par_url` and `assemble_gcs_iam_v4_url` (or `sign_gcs_iam_v4_url`) on `vertebra._storage` and declare exact signatures in `_storage.pyi`.
- `src/py/vertebra/storage/_store.py`:
  - Import `GcpTokenSnapshot`, `SyncGcpTokenCache`, and `AsyncGcpTokenCache` from `vertebra._vertebra` and route `GCSConfig` sync/async token acquisition exclusively through `SyncGcpTokenCache.from_options(...)` / `AsyncGcpTokenCache.from_options(...)`.
  - Delete `GCSConfig._fetch_adc_token_with_expiry`, `_exchange_authorized_user_token_with_expiry`, `_impersonate_service_account_token_with_expiry`, `_open_http_json`, and Python-dict token caches.
  - Delete `self._memory` dict fallback and default `target: str | StorageConfig = "memory://"` from `_BaseObjectStore`, `SyncObjectStore`, and `AsyncObjectStore`:
    - `SyncObjectStore(target: str | StorageConfig)` and `AsyncObjectStore(target: str | StorageConfig)`: raise `ImproperConfigurationError` if `target` is `None`, empty `""`, or lacks a valid scheme, and raise `MissingDependencyError("vertebra._storage", install_package="vertebra-core[storage]", extra="storage")` when `import_optional("vertebra._storage")` returns `None`.
    - Explicit `MemoryConfig(...)` and `"memory://..."` URIs remain supported via `vertebra._storage`'s Rust `InMemory` backend.
  - Wire `sign` on `SyncObjectStore` (sync) and `AsyncObjectStore` (async) across `gs` (local RSA PEM and IAM `signBlob` via `SyncGcpTokenCache` / `AsyncGcpTokenCache`), `s3` / `r2`, `az`, and `oci` (PAR base token / native PAR).

## Edge Cases & Error Semantics

- `hasattr(vertebra._storage, "SyncGcpTokenCache")` is `False`, while `from vertebra.storage import SyncGcpTokenCache, AsyncGcpTokenCache, GcpTokenSnapshot` resolves the exact classes from `vertebra._vertebra` (`SyncGcpTokenCache is vertebra._vertebra.SyncGcpTokenCache`).
- Passing `target=""` or `target=None` (via `Any`) to `SyncObjectStore` or `AsyncObjectStore` raises `ImproperConfigurationError`.
- Simulating `vertebra._storage` uninstalled (`monkeypatch.setitem(sys.modules, "vertebra._storage", None)` + clearing `import_optional` cache) causes `SyncObjectStore("memory://test")` to raise `MissingDependencyError` mentioning `vertebra-core[storage]`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py`, and `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Token cache identity & single-flight refresh | `from vertebra.storage import SyncGcpTokenCache, AsyncGcpTokenCache` and `import vertebra._vertebra, vertebra._storage` | `SyncGcpTokenCache is vertebra._vertebra.SyncGcpTokenCache`; `not hasattr(vertebra._storage, "SyncGcpTokenCache")`; `GCSConfig` refreshes tokens via `vertebra._vertebra`. |
| Fail-closed empty/missing config | `SyncObjectStore("")` and `AsyncObjectStore(None)` | Both raise `ImproperConfigurationError`. |
| Fail-closed missing `_storage` wheel | Mask `vertebra._storage` unavailable and call `SyncObjectStore("file:///tmp")` | Raises `MissingDependencyError` with `extra="storage"`, never falling back to a Python dict. |
| Explicit `MemoryConfig()` still works | `store = SyncObjectStore(MemoryConfig(namespace="t1")); store.put("k", b"v")` | Delegates to Rust `InMemory` store and `store.get("k") == b"v"`. |
| Multi-cloud `sign` (`gs` RSA + IAM, `s3`, `az`, `oci`) | Call `store.sign("reports/q4.parquet", expires_in=600, method="GET")` on each configured backend | Returns valid provider-signed HTTPS URL matching expected signature/PAR parameters. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/unit/storage -q`
- Final Green + stubtest + lint gate: `make dev && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync ruff check src/py/vertebra/storage src/py/tests/unit/storage && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/storage -q`

## Acceptance Criteria

- [x] Duplicate `GcpTokenSnapshot`, `SyncGcpTokenCache`, and `AsyncGcpTokenCache` classes are removed from `vertebra._storage` and unified on `vertebra._vertebra` (R2).
- [x] Pure-Python `urllib` OAuth token fetchers and `self._memory` fallback dicts are deleted from `src/py/vertebra/storage/_store.py` (R2, R3).
- [x] `SyncObjectStore` and `AsyncObjectStore` fail closed with `ImproperConfigurationError` / `MissingDependencyError` and support GCS V4 (RSA + IAM), S3 SigV4, Azure SAS, and OCI PAR presigning (R2, R3).

## Notes & Discoveries

- [2026-11-07 04:15] Existing unit tests in `src/py/tests/unit/storage/` or `extensions/` that instantiate `SyncObjectStore()` with zero arguments must pass explicit `MemoryConfig()` or `"memory://test"` per the clean-break fail-closed contract.
- [2026-11-07 18:04] `SyncObjectStore.to_async()` and `AsyncObjectStore.to_sync()` clone the underlying Rust `ObjectStoreEngine` handle directly in `vertebra._storage`, sharing `InMemory` namespaces and connection pools without Python `_memory` dicts.

[^core-pyi-gcp]: `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache` — Core GCP token cache type stubs.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — `vertebra._storage` PyO3 module definition.
[^storage-store-py]: `src/py/vertebra/storage/_store.py::GCSConfig` — Python storage configuration and facade classes.
