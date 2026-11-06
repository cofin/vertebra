---
type: Task
id: storage-completeness:t02
title: Unify GCP token caches on vertebra._vertebra, complete provider presigning, and enforce fail-closed stores
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t01
files:
- src/rs/vertebra-storage/src/gcp_auth.rs
- src/rs/vertebra-storage/src/signing.rs
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/__init__.py
tests:
- src/py/tests/unit/storage/test_gcs_token_refresh.py
- src/py/tests/unit/storage/test_signing.py
- src/py/tests/unit/storage/test_storage_store.py
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:38:00Z'
updated_at: '2026-11-06T06:38:00Z'
commit: null
tags:
- storage
- gcp-auth
- signing
requirements:
- R2
- R3
---

# Unify GCP token caches on vertebra._vertebra, complete provider presigning, and enforce fail-closed stores

## Objective

Unify Python GCP token caching on `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`, remove duplicate PyO3 token cache classes from `vertebra._storage` and pure-Python OAuth token fetchers from `storage/_store.py`, complete GCS V4 / S3 SigV4 / Azure SAS / OCI PAR presigning, and enforce fail-closed `SyncObjectStore` / `AsyncObjectStore` construction without `self._memory` fallbacks.

## Context

Read `../spec.md`, `t01-rust-storage-streaming-and-ops.md`, and `.agents/bundles/knowledge/domains/storage/object-storage.md`. Remember never to use live Google OAuth bearer token prefixes in tests (use `"mock-oauth-token"`).

## Steps

1. Write failing tests in `test_gcs_token_refresh.py`, `test_signing.py`, and `test_storage_store.py` asserting that `SyncGcpTokenCache` and `AsyncGcpTokenCache` imported from `vertebra.storage` are identical (`is`) to `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`, presigned URLs work across GCS V4 (key + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR, and `SyncObjectStore` / `AsyncObjectStore` raise `ImproperConfigurationError` or `MissingDependencyError` rather than falling back to `self._memory`.
2. Trim `src/rs/vertebra-storage/src/gcp_auth.rs` strictly to `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider` over `vertebra_core::gcp_auth::GcpTokenCache`, remove duplicate `SyncGcpTokenCache` / `AsyncGcpTokenCache` / `GcpTokenSnapshot` PyO3 classes from `src/rs/vertebra-storage/src/lib.rs` and `_storage.pyi`, and complete OCI PAR / provider presigners in `signing.rs` and `oci.rs`.
3. Delete `GCSConfig._fetch_adc_token_with_expiry`, `_open_http_json`, Python token caches, and `self._memory` fallbacks from `src/py/vertebra/storage/_store.py`, re-exporting `SyncGcpTokenCache` and `AsyncGcpTokenCache` from `vertebra._vertebra`.
4. Run verification and commit `refactor(storage): unify GCP token caches, complete presigning, and fail closed on missing stores`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
make dev
uv run --no-sync python -m mypy.stubtest vertebra._storage
uv run --no-sync pytest src/py/tests/unit/storage -q
```

## Acceptance Criteria

- [ ] R2, R3: Duplicate `_storage` PyO3 token caches, pure-Python OAuth fetchers, and `self._memory` / implicit `memory://` fallbacks are removed; presigning passes across GCS, S3, Azure, and OCI.

## Notes & Discoveries

None.
