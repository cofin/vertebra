---
type: Spec
flow_id: storage-completeness
title: Feature-Complete Storage
description: Make vertebra-storage SyncObjectStore and AsyncObjectStore the complete implementation behind SQLSpec, Advanced Alchemy, Django, and Litestar storage contracts, with zero-copy streaming, ranges, signing, strict SHM Arrow allocation, and unified SyncGcpTokenCache / AsyncGcpTokenCache bindings.
state: planned
parent_prd: native-data-platform
research:
- ../native-data-platform/research/gap-analysis_20261103.md
- ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: '2026-11-03T04:05:00Z'
updated_at: '2026-11-06T06:38:00Z'
tags:
- storage
- object-store
- sqlspec
- advanced-alchemy
- django
- litestar
---

# Feature-Complete Storage

## 1. Specification & Context

### 1.1 Upstream Execution Baseline

This flow executes on `feat/storage-completeness`, stacked on `feat/mcp-webmcp-core` above `fix/post-merge-review-and-plan-sync` (`7657e8b7c2c96bdda6821aec70a2b603f438a127`) and `main @ 80ff8a65e43a2bcef9e48a7763e89a7352118232`. Standalone optional wheel isolation (`vertebra-storage`) and `TaskPanicError` / `AbortHandle` async completion cleanup in `src/rs/vertebra-storage/src/lib.rs` were committed in `e2b282b7` and `f7740207`.

### 1.2 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/rs/vertebra-storage/src/{lib.rs,store.rs,r2.rs}` | Buffers multipart uploads in `Vec<Bytes>` and lacks true chunk-streaming `get_stream`, conditional `put` (`if_not_exists`, `if_match`, `if_none_match`), cursor-paginated `list` / `list_with_delimiter`, and atomic/server-side `rename`. |
| `src/rs/vertebra-storage/src/{lib.rs,gcp_auth.rs}` & `src/py/vertebra/storage/_store.py` | Duplicate `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` PyO3 classes in `vertebra._storage` alongside pure-Python `GCSConfig._fetch_adc_token_with_expiry`, `_exchange_authorized_user_token_with_expiry`, `_impersonate_service_account_token_with_expiry`, and Python token caches instead of delegating to `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`. |
| `src/py/vertebra/storage/_store.py::{SyncObjectStore,AsyncObjectStore}` | Retain `self._memory` fallback dicts and implicit `memory://` defaults instead of failing closed when `vertebra._storage` is uninstalled or misconfigured. |
| `src/py/vertebra/extensions/{sqlspec,alchemy,django,litestar}/storage.py` & `store.py` | `extensions/alchemy/storage.py` monkeypatches `storages.get = storages.get_backend`; `extensions/django/storage.py::DjangoStorage` does not subclass `django.core.files.storage.Storage` (`@deconstructible`) or return Django `File` handles; Litestar lacks an `AsyncObjectStore`-backed `Store`, static-files adapter, and non-blocking HTTP `Range` / `416` streaming. |
| `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows` & `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` | `ArrowCursor.from_rows` must always emit valid Arrow IPC frames (never JSON pseudo-Arrow), and `allocate_shm_region` must fail explicitly on SHM exhaustion rather than silently allocating anonymous process-local memory. |

### 1.3 Relevant Patterns and Glossary

Read [object storage](../../knowledge/domains/storage/object-storage.md), [SQLSpec, Advanced Alchemy, and dialects](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Litestar extension](../../knowledge/domains/extensions/litestar.md), [optional wheels and SHM ownership](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [FFI and GIL discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), and the `pyo3-bridge`, `runtime-ipc`, `python-framework`, and `advanced-alchemy` skills.

`vertebra._storage` is an optional wheel (`pip install vertebra[storage]`): never import `vertebra._storage` eagerly at module top-level in packages imported by `vertebra.cli` or documentation tooling (`import_optional("vertebra._storage")` at construction time), and never introduce `asyncio.to_thread` in `src/py/vertebra/storage/_store.py` (enforced by `test_storage_and_alchemy_zero_to_thread`).

## 2. Requirements

### 2.1 Functional Requirements and Architecture

- **R1.** Implement full sync/async parity across `gs`, `s3`, `r2`, `oci`, `az`, `file`, and explicit test-only `memory` in `src/rs/vertebra-storage` and `src/py/vertebra/storage/_store.py` (`SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, `AsyncReadableFile`): `get`, `get_stream` (zero-copy `StorageBuffer` chunk iterator), byte-range reads, `put`, conditional `put` (`if_not_exists`, `if_match`, `if_none_match`), streaming multipart upload without `Vec<Bytes>` whole-payload buffering, paginated `list` / `list_with_delimiter`, `head`, `copy`, `rename`, `delete`, and object content-type/attributes.
- **R2.** Support presigned URLs across GCS V4 (local `GOOG4-RSA-SHA256` and IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR; unify Python callers on `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`, removing duplicate `SyncGcpTokenCache` / `AsyncGcpTokenCache` / `GcpTokenSnapshot` PyO3 classes from `vertebra._storage` (`_storage.pyi`) and deleting pure-Python OAuth token fetchers from `src/py/vertebra/storage/_store.py`.
- **R3.** Enforce fail-closed construction: `SyncObjectStore` and `AsyncObjectStore` raise `MissingDependencyError` / configuration errors when `vertebra._storage` or target URI/config is missing, removing `self._memory` fallbacks and implicit `memory://` defaults across `storage/_store.py`, `extensions/sqlspec/storage.py`, `extensions/alchemy/storage.py`, and `extensions/django/storage.py`.
- **R4.** Complete SQLSpec `ObjectStoreProtocol` conformance in `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` (preserving `build_duckdb_gcs_secret_sql` until `native-duckdb-driver` relocates it).
- **R5.** Register `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` through Advanced Alchemy's public `StorageRegistry` API without monkeypatching `storages.get`.
- **R6.** Make `src/py/vertebra/extensions/django/storage.py::DjangoStorage` a `@deconstructible` subclass of `django.core.files.storage.Storage` returning Django `File` objects from `open()` and implementing `save`, `exists`, `size`, `delete`, `listdir`, `url` (signed when configured), `path`, `get_available_name`, `generate_filename`, `get_valid_name`, `get_accessed_time`, `get_created_time`, `get_modified_time`, and `get_alternative_name`.
- **R7.** Implement `ObjectStoreLitestarStore` (`litestar.stores.base.Store` over `AsyncObjectStore`), a Litestar static-files filesystem adapter, and non-blocking `StreamingStorageResponse` / `storage/_streaming.py::to_fastpath_tuple` supporting single-range `206 Partial Content` and `416 Range Not Satisfiable`.
- **R8.** Implement batch-streaming Arrow/Parquet <-> `SyncObjectStore` / `AsyncObjectStore` helpers, ensure `ArrowCursor.from_rows` always writes valid Arrow IPC streams, and make `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` return an explicit `DbError` on SHM allocation failure instead of anonymous memory fallback.

### 2.2 Non-Functional and Safety Constraints

- Zero `asyncio.to_thread` calls in `src/py/vertebra/storage/_store.py`; all async storage operations flow through `vertebra._storage` `AsyncCompletionQueue`.
- Never import `vertebra._storage` unconditionally at module top-level; resolve via `import_optional("vertebra._storage")` and raise `MissingDependencyError(..., extra="storage")` on instantiation when absent.
- Explicit `MemoryConfig()` or `"memory://..."` remains valid for unit tests, whereas omitted `store=None` / `config=None` arguments fail closed with `ImproperConfigurationError`.

### 2.3 Acceptance Scenarios

| Given | When | Then |
| --- | --- | --- |
| Large multi-megabyte payload streamed into `AsyncObjectStore.put_stream` and read via `get_stream` / `get_range` | Executed against S3/R2, GCS, Azure, or local filesystem | Chunks upload via streaming multipart without buffering the full payload in `Vec<Bytes>` and yield `StorageBuffer` views; conditional `put(if_not_exists=True)` raises on conflict. |
| `GCSConfig` and `vertebra._storage` | Acquiring tokens and presigning URLs | Tokens come exclusively from `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}` / `vertebra_core::gcp_auth`; duplicate `_storage` token PyO3 classes and Python urllib OAuth fetchers are gone. |
| `ObjectStoreBackend()`, `DjangoStorage()`, or `SyncObjectStore` without a configured store/URI | Instantiated in production or test code | Raises `ImproperConfigurationError` (or `MissingDependencyError` when `_storage` is missing) without falling back to `memory://`. |
| Advanced Alchemy `FileObject`, SQLSpec `ObjectStoreBackend`, Django `Storage`, and Litestar `Range: bytes=0-63` / `bytes=999999-` | Exercised through framework contract suites | All four framework storage contracts pass without monkeypatches or event-loop blocking, returning `206` on valid ranges and `416` on unsatisfiable ranges. |
| `ArrowCursor.from_rows` and `allocate_shm_region` under SHM failure | Called from Python and Rust | `ArrowCursor.from_rows` produces valid Arrow IPC readable by `pyarrow.ipc`, and `allocate_shm_region` surfaces an explicit SHM error without anonymous heap fallback. |

### 2.4 Alternatives Considered

| Decision | Chosen seam and alternative |
| --- | --- |
| GCP auth unification | Sharing `vertebra_core::gcp_auth::GcpTokenCache` and `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}` eliminates ~500 lines of duplicated Rust/Python OAuth code across storage and core. |
| Fail-closed storage | Rejecting missing store configs at construction time prevents silent data loss into process-ephemeral `memory://` buckets in misconfigured deployments. |

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning tasks | Verification strategy | Canonical public seam / gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-rust-storage-streaming-and-ops.md) | behavior_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_multicloud_storage.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R2, R3 | [t02](tasks/t02-unified-gcp-auth-signing-and-fail-closed.md) | behavior_tdd | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R4 | [t03](tasks/t03-sqlspec-object-store-protocol.md) | behavior_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R5 | [t04](tasks/t04-advanced-alchemy-storage-backend.md) | behavior_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R6 | [t05](tasks/t05-django-storage-contract.md) | behavior_tdd | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R7 | [t06](tasks/t06-litestar-store-and-range-streaming.md) | behavior_tdd | `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/server/test_zero_copy_buffer.py` |
| R8 | [t07](tasks/t07-arrow-storage-plane-and-strict-shm.md) | behavior_tdd | `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/storage/test_storage_store.py`, `src/rs/vertebra-db/src/arrow_shm.rs` |

## 4. Implementation Plan

### Phase 1: Native Rust Storage Engine, Unified GCP Auth, Presigning, and Fail-Closed Stores

- [ ] t01 — Implement streaming reads, multipart uploads, conditional puts, pagination, and rename in vertebra-storage (tasks/t01-rust-storage-streaming-and-ops.md)
- [ ] t02 — Unify GCP token caches on vertebra._vertebra, complete provider presigning, and enforce fail-closed stores (tasks/t02-unified-gcp-auth-signing-and-fail-closed.md)

**Phase 1 Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage`, `make dev`, `mypy.stubtest vertebra._storage`, and `uv run --no-sync pytest src/py/tests/unit/storage -q` exit 0.

### Phase 2: Framework Storage Contracts and Arrow SHM Data Plane

- [ ] t03 — Complete SQLSpec ObjectStoreProtocol conformance over SyncObjectStore and AsyncObjectStore (tasks/t03-sqlspec-object-store-protocol.md)
- [ ] t04 — Register Advanced Alchemy ObjectStoreBackend without monkeypatching storages.get (tasks/t04-advanced-alchemy-storage-backend.md)
- [ ] t05 — Subclass Django Storage (@deconstructible) and implement full file/metadata contract (tasks/t05-django-storage-contract.md)
- [ ] t06 — Add Litestar AsyncObjectStore Store, static-files adapter, and non-blocking Range/416 streaming (tasks/t06-litestar-store-and-range-streaming.md)
- [ ] t07 — Stream Arrow/Parquet across ObjectStore, emit valid IPC in ArrowCursor.from_rows, and enforce strict SHM allocation (tasks/t07-arrow-storage-plane-and-strict-shm.md)

**Phase 2 Recovery Checkpoint:** `make lint-all`, `make rs-test`, `make py-test`, and `make docs` exit 0, followed by independent `@code-reviewer` and `@quality-reviewer` passes before archiving on `feat/storage-completeness`.

## 5. Continuity Snapshot

- State is `planned`; all 7 worksheets (`t01`–`t07`) are `pending` at `plan_revision: 1`, anchored at `plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"`.
- Relocating `build_duckdb_gcs_secret_sql` onto `DuckDBCore` remains scoped to `native-duckdb-driver`, and broker `VTBARROW` SQLSpec frame migration remains scoped to `sqlspec-adapter-parity`.
- Next action on `feat/storage-completeness`: `/flow:implement storage-completeness` starting at `t01`.
