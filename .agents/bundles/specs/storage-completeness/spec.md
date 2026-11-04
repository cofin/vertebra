---
type: Spec
flow_id: storage-completeness
title: Feature-Complete Storage
description: Make vertebra-storage SyncObjectStore and AsyncObjectStore the complete implementation behind SQLSpec, Advanced Alchemy, Django, and Litestar storage contracts, with zero-copy streaming, ranges, signing, strict SHM Arrow allocation, and unified SyncGcpTokenCache / AsyncGcpTokenCache bindings.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/gap-analysis_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-04T01:17:00Z
tags: [storage, object-store, sqlspec, advanced-alchemy, django, litestar]
---

# Flow Charter: Feature-Complete Storage

## Scope

1. **Rust `vertebra-storage` & `SyncObjectStore` / `AsyncObjectStore` operations.**
   - Support `gs`, `s3`, `r2`, `oci`, `az`, `file`, and explicit test-only `memory` across `src/rs/vertebra-storage` (`SyncObjectStore`, `AsyncObjectStore`) and `src/py/vertebra/storage/_store.py` (`SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`) with full sync and async parity.
   - Implement `get`, `get_stream` (chunk iterator over the buffer protocol without full-payload copies), `put`, conditional `put`, streaming multipart upload (eliminating `Vec<Bytes>` buffering and R2 re-concatenation), paginated `list` / `list_with_delimiter`, `head`, `copy`, `rename`, `delete`, content-type/attributes, and byte-range reads.
   - Bring `vertebra-storage`'s async completion drainer to panic/cancel parity (`TaskPanicError` and `AbortHandle` cancellation) with `vertebra_db::py_async` / `vertebra-py`.
2. **Unified Rust `SyncGcpTokenCache` / `AsyncGcpTokenCache` & URL signing.**
   - Signed URLs across providers: GCS V4 (local `GOOG4-RSA-SHA256` and IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR.
   - Unify Python callers on `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}` (`vertebra_core::gcp_auth::GcpTokenCache`), removing the duplicate `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` PyO3 wrappers in `src/rs/vertebra-storage/src/lib.rs` (`_storage.pyi`), trimming `src/rs/vertebra-storage/src/gcp_auth.rs` strictly to `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider`, deleting Python `GCSConfig._fetch_adc_token_with_expiry`, `_open_http_json`, and Python fallback `SyncGcpTokenCache` / `AsyncGcpTokenCache` in `src/py/vertebra/storage/_store.py`, and updating `.agents/bundles/knowledge/domains/storage/object-storage.md` (`sources:` and `src/rs/vertebra-core/src/notify.rs` link).
3. **Fail-closed construction (no `memory://` fallbacks).**
   - `storage/_store.py::{SyncObjectStore,AsyncObjectStore}` raise on native construction failure instead of falling back to `self._memory`.
   - Remove implicit `memory://` defaults from `SyncObjectStore` / `AsyncObjectStore`, `extensions/sqlspec/storage.py::ObjectStoreBackend`, `extensions/alchemy/storage.py::ObjectStoreBackend` / `register_storage_backend`, `extensions/django/storage.py::DjangoStorage`, and Spanner/BigQuery artifact stores.
4. **Framework storage contracts.**
   - **SQLSpec (`extensions/sqlspec/storage.py::ObjectStoreBackend`)**: Complete `ObjectStoreProtocol` conformance (over `SyncObjectStore` and `AsyncObjectStore`) including streaming Arrow/Parquet read and write.
   - **Advanced Alchemy (`extensions/alchemy/storage.py::ObjectStoreBackend`)**: Complete `StorageBackend` / `FileObject` implementation registered through Advanced Alchemy's public registration API, removing the `storages.get` monkeypatch.
   - **Django (`extensions/django/storage.py::DjangoStorage`)**: Subclass `django.core.files.storage.Storage` (`@deconstructible`), return a Django `File` from `open()`, sign URLs, and implement `save`, `exists`, `size`, `delete`, `listdir`, `path`, `get_available_name`, `generate_filename`, `get_valid_name`, `get_*_time`, and `get_alternative_name`.
   - **Litestar**: Implement a `litestar.stores.base.Store` backed by `AsyncObjectStore`, a static-files filesystem adapter, and zero-copy `StreamingStorageResponse` / `storage/_streaming.py::to_fastpath_tuple` with HTTP `Range` / `416 Range Not Satisfiable` support without blocking the event loop.
5. **Arrow <-> `SyncObjectStore` / `AsyncObjectStore` data plane & strict SHM regions.**
   - Shared `SyncObjectStore` / `AsyncObjectStore` <-> `ArrowCursor` `VTBARROW` Parquet export/import helpers (`select_to_arrow` -> Parquet, `load_from_arrow` <- Parquet) with true batch-by-batch `_iter_arrow_async` streaming.
   - `vertebra/db/arrow.py::ArrowCursor.from_rows` always writes valid Arrow IPC (never JSON pseudo-Arrow).
   - `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` returns an explicit error on SHM allocation failure instead of silently falling back to anonymous process-local memory.

## Out of Scope

- Relocating `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` onto `vertebra/db/adapters/duckdb/core.py::DuckDBCore` (owned by `native-embedded-and-cache-drivers`) and removing its `extensions/sqlspec/storage.py` wrapper or replacing `extensions/sqlspec/provider.py::{SharedArrowBuffer,SharedArrowTable}` with broker `VTBARROW` frames (owned by `integration-remediation/sqlspec-adapter-parity`).

## Public Seams

`SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`, `ObjectStoreBackend` (SQLSpec and Advanced Alchemy), `DjangoStorage`, `ObjectStoreLitestarStore`, and `StreamingStorageResponse`.

## Dependencies

`db-layout-and-contract` (`main @ f741701`) and `integration-remediation/db-engine-primitives`.

## Acceptance Boundary

- Integration tests against MinIO/RustFS (S3), fake-gcs-server (with emulator-only gaps marked `xfail(strict=True)`), Azurite, and local filesystem.
- Contract tests for SQLSpec `ObjectStoreProtocol`, Advanced Alchemy `StorageBackend`, Django `Storage`, and HTTP `Range`/`416` responses.
- `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Rust `vertebra-storage` (`SyncObjectStore`, `AsyncObjectStore`) streaming, pagination, conditional put, and completion panic/cancel parity.
- t02 Provider presigning + unified `SyncGcpTokenCache` / `AsyncGcpTokenCache` (delete duplicate PyO3 wrappers in `vertebra-storage` and Python `storage/_store.py` token fetchers) + fail-closed `SyncObjectStore` / `AsyncObjectStore`.
- t03 SQLSpec `ObjectStoreProtocol` parity.
- t04 Advanced Alchemy `StorageBackend` registration (`storages.get` monkeypatch removal).
- t05 Django `Storage` subclass and contract methods.
- t06 Litestar `Store`, static-files adapter, and non-blocking `Range`/`416` streaming.
- t07 Arrow <-> `SyncObjectStore` / `AsyncObjectStore` streaming data plane + strict `allocate_shm_region` & `ArrowCursor.from_rows`.
