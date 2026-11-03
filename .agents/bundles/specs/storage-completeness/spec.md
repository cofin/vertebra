---
type: Spec
flow_id: storage-completeness
title: Feature-Complete Storage
description: Make vertebra-storage ObjectStore the single, complete implementation behind SQLSpec, Advanced Alchemy, Django, and Litestar storage contracts, with zero-copy streaming, ranges, signing, and one shared GCP token cache.
state: draft
parent_prd: native-data-platform
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-03T04:05:00Z
tags: [storage, object-store, sqlspec, advanced-alchemy, django, litestar]
---

# Flow Charter: Feature-Complete Storage

## Scope

1. **ObjectStore.**
   - `src/rs/vertebra-storage` and `src/py/vertebra/storage/_store.py::ObjectStore` support gs, s3, r2, oci, az, file, and memory (tests only).
   - Operations: get, put, multipart, list with pagination, head, copy, rename, delete, and range reads.
   - Sync and async forms.
   - Streaming without full copies.
   - Signed URLs: GCS V4 locally and through IAM, S3 SigV4, Azure SAS, OCI PAR.
2. **Token cache.** One `GcpTokenCache` shared by `_vertebra` and `_storage`, replacing the duplicate in `vertebra-storage/src/lib.rs`.
3. **Framework contracts.**
   - `extensions/sqlspec/storage.py::ObjectStoreBackend` fully implements sqlspec `ObjectStoreProtocol`, including Arrow/Parquet read/write. `build_duckdb_gcs_secret_sql` moves to the DuckDB adapter.
   - `extensions/alchemy/storage.py` implements AA `StorageBackend`/`FileObject` and registers through AA's API, without the `storages.get` monkeypatch.
   - `extensions/django/storage.py` implements the Django `Storage` API (open, save, url, exists, size, listdir, delete).
   - Litestar file/static responses stream from ObjectStore with Range/416 support. `storage/_streaming.py::to_fastpath_tuple` streams without a full copy and without blocking the loop.
4. **Arrow data plane.** The ObjectStore ↔ `ArrowCursor` VTBARROW export/import paths (`select_to_arrow` → Parquet, `load_from_arrow` ← Parquet) are shared by every driver.
5. **Audit.** Remove production fakes and fallbacks found by the gap analysis in storage-adjacent components (recorded in `research/`).

## Public Seams

`ObjectStore`, `ObjectStoreBackend`, the AA storage backend, the Django storage backend, and `StreamingStorageResponse`.

## Dependencies

`db-layout-and-contract`.

## Acceptance Boundary

- Integration tests against MinIO/RustFS (S3), the GCS emulator (with gaps marked `xfail`), Azurite, and local file.
- Contract tests for the sqlspec, AA, and Django storage APIs.
- Range/416 tests.
- `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 ObjectStore operations + async parity.
- t02 Signing across providers + shared token cache.
- t03 SQLSpec `ObjectStoreProtocol`.
- t04 AA `StorageBackend`.
- t05 Django `Storage`.
- t06 Litestar streaming + ranges.
- t07 Arrow ↔ ObjectStore data plane.
