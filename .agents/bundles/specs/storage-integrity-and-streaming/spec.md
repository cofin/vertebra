---
type: Spec
flow_id: storage-integrity-and-streaming
title: "Object Storage Content-Integrity Receipts, Verified Reads, and Companion Wheel Isolation"
description: "Adds CRC32C, MD5, and SHA-256 content-integrity receipts and verified streaming reads to vertebra.storage, streams Arrow/Parquet multipart writes across SQLSpec and Advanced Alchemy adapters, and enforces standalone wheel isolation across storage, bigquery, spanner, and bundler."
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-integrity-receipts-and-verified-reads
  - ch02-streaming-arrow-writes-and-integrations
  - ch03-wheel-isolation-and-okf-sync
current_chapter: null
current_task: null
parent_prd: native-data-platform
plan_revision: 3
plan_commit: 18baadcd
last_verified_checkpoint: 66e6913f
last_reviewed_chapter: ch03-wheel-isolation-and-okf-sync
last_reviewed_commit: 541d89b5
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-13T04:59:52Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T04:59:52Z"
    commit: 541d89b5
tags:
  - storage
  - integrity
  - arrow
  - sqlspec
  - packaging
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: "ObjectStoreEngine conditional S3/R2 copy, multipart uploads, and file/stream reads"
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: "Incremental multipart session with UniformPartAccumulator"
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: "PyO3 storage bindings and StorageError mapping"
  - id: py-typing
    resource: src/py/vertebra/typing.py
    title: "Public typing and exception facade (PreconditionError)"
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: "SyncObjectStore and AsyncObjectStore facades, ObjectMeta, and PutResult"
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: "Incremental Arrow/Parquet codec uploads through put_stream"
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: "SQLSpec ObjectStoreBackend with explicit URI resolution and write_arrow_sync/async"
  - id: aa-backend
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: "Advanced Alchemy ObjectStoreBackend"
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: "Django storage backend"
  - id: verify-ora-wheel
    resource: tools/scripts/verify_oracle_wheel.py
    title: "Reference wheel payload isolation and outside-checkout fail-loud verifier"
  - id: makefile
    resource: Makefile
    title: "Companion wheel isolation check targets"
  - id: release-yml
    resource: .github/workflows/release.yaml
    title: "Release matrix cibuildwheel and test_wheels jobs"
  - id: limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: "Object storage implementation limitations OKF chapter"
---

# Specification: Object Storage Content-Integrity Receipts, Verified Reads, and Companion Wheel Isolation

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Merged chapter `ch01-create-only-copy-and-quality` (`18baadcd`) already delivers S3/B2 `S3CopyIfNotExists::Multipart`, R2 `HeaderWithStatus("cf-copy-destination-if-none-match", "*", 412)`, streamed `put_multipart` delegation to `put_stream`, and sentinel-free SQLSpec `ObjectStoreBackend(uri: str | None = None)` resolution [^store-rs] [^py-store] [^sqlspec-backend]. No content-digest receipts or verified reads exist ([#99](https://github.com/cofin/vertebra/issues/99)), SQLSpec/Alchemy adapters lack direct Arrow capability flags and digest forwarding ([#100](https://github.com/cofin/vertebra/issues/100)), and `vertebra-{storage,bigquery,spanner,bundler}` lack local `*-wheel-check` gates [^makefile].
- **Target Architecture**: `ObjectStoreEngine`, `ObjectMeta`, and `PutResult` expose CRC32C, MD5, and SHA-256 content-integrity receipts with abort-before-publish expected-digest checks and verified streaming reads, raising `vertebra.typing.IntegrityError(VertebraError, OSError)` (explicitly re-exported in `src/py/vertebra/typing.py` and `vertebra.storage.__all__`, cleanly distinct from `vertebra.db.IntegrityError(DatabaseError)`) [^lib-rs] [^py-typing]. Streaming Arrow/Parquet multipart writes and digest options wire across `vertebra.storage`, SQLSpec, Advanced Alchemy, and Django [^py-arrow] [^aa-backend] [^django-storage], and `verify_storage_wheel.py` gates `storage`, `bigquery`, `spanner`, and `bundler` wheel isolation [^verify-ora-wheel] [^release-yml].
- **Scope & Boundaries**: `state: active` with `3` remaining consolidated chapters (`ch01`–`ch03`, `9` compact tasks). Atomic cloud rename and upstream `sqlspec` repository edits remain out of scope and are recorded in [object-storage-limitations.md](../../knowledge/domains/storage/object-storage-limitations.md) [^limitations].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Merged Baseline (`18baadcd`) & Remaining Problem Statement

Commit `18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)) landed the create-only copy and streaming quality baseline: `BackendConfig::S3` uses `S3CopyIfNotExists::Multipart`, `BackendConfig::R2` uses `S3CopyIfNotExists::HeaderWithStatus("cf-copy-destination-if-none-match", "*", 412)` (mapping destination `412` / `AlreadyExists` to `FileExistsError` while preserving existing bytes), `SyncObjectStore.put_multipart` and `AsyncObjectStore.put_multipart` stream iterables incrementally through `put_stream`, and SQLSpec `ObjectStoreBackend.__init__(uri: str | None = None, ...)` honors explicit URIs including `"memory://"` [^store-rs] [^py-store] [^sqlspec-backend].

Three gaps remain open under parent PRD [native-data-platform](../native-data-platform/spec.md):
1. `vertebra-storage` computes no content digests over uploaded or downloaded bytes ([#99](https://github.com/cofin/vertebra/issues/99)), and `src/py/vertebra/typing.py` explicitly enumerates `_typing` exports without a storage `IntegrityError` [^py-typing].
2. Streaming Arrow/Parquet multipart writes (`write_batches_sync` / `write_batches_async`) lack digest pass-through, SQLSpec `ObjectStoreBackend` lacks `supports_direct_arrow_write: ClassVar[bool] = True` ([#100](https://github.com/cofin/vertebra/issues/100)), and Advanced Alchemy / Django adapters do not forward integrity options or stream Arrow tables [^py-arrow] [^aa-backend] [^django-storage].
3. `Makefile` verifies standalone wheel isolation for `duckdb`, `mysql`, `mssql`, `oracle`, and `postgres`, leaving `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-bundler` without local `*-wheel-check` targets [^makefile] [^release-yml]. Relevant OKF: [object-storage.md](../../knowledge/domains/storage/object-storage.md) and [object-storage-limitations.md](../../knowledge/domains/storage/object-storage-limitations.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (CRC32C, MD5, & SHA-256 Receipts and Abort-Before-Publish, [#99](https://github.com/cofin/vertebra/issues/99))**: `PutConditionalOptions` adds `checksum: bool`, `expected_sha256: Option<String>`, `expected_crc32c: Option<String>`, and `expected_md5: Option<String>`. `StoredObjectMeta`, `PyObjectMeta`, `ObjectMeta`, and `PutResult` carry `sha256: str | None = None` (64 lowercase hex), `crc32c: str | None = None` (8 lowercase hex Castagnoli CRC32C), and `md5: str | None = None` (32 lowercase hex), computed in Rust over sent bytes when `checksum=True` or an `expected_*` digest is set (`head` and `list` leave digest fields `None`) [^store-rs] [^session-rs]. Mismatches return `StorageError::Integrity` before single-shot publish or after `MultipartUploadSession::abort()` on multipart/stream/file uploads, preserving prior objects.
- **R2 (Verified Reads & Zero-Cost Default Benchmark)**: `ObjectStoreEngine::{get_verified, get_file_verified, get_stream_verified}` validate `expected_sha256` (unlinking `.part` before rename on `get_file_verified` and yielding `Err(Integrity)` on the final poll of `VerifyingStream` without buffering whole streams). Criterion bench `put_checksum` (`put_memory_default` vs `put_memory_checksum` at `1 MiB` and `64 MiB`) proves zero hashing overhead on the default path.
- **R3 (Storage `IntegrityError` Reconciliation & Python Facade)**: `src/py/vertebra/_typing.py` defines `class IntegrityError(VertebraError, OSError)` for storage/payload digest mismatches, explicitly re-exported in `src/py/vertebra/typing.py` (`__all__`) and `src/py/vertebra/storage/{_store,__init__}.py` (`__all__`), while database constraint violations remain `vertebra.db.IntegrityError(DatabaseError)` in `vertebra.db` / `vertebra._vertebra` [^py-typing] [^py-store]. `lib.rs::make_integrity_err` imports `vertebra.typing.IntegrityError`.
- **R4 (Streaming Arrow/Parquet Multipart Writes & Adapter Forwarding, [#100](https://github.com/cofin/vertebra/issues/100))**: `write_batches_sync` / `write_batches_async` forward `checksum` and `expected_*` options into `put_stream`. SQLSpec `ObjectStoreBackend` declares `supports_direct_arrow_write: ClassVar[bool] = True`, normalizes `compression`/`format_hint`/`write_options`, proves mid-stream codec failure aborts multipart uploads and bounds buffered memory by part size, and forwards digest options on `write_bytes_*`. Advanced Alchemy `ObjectStoreBackend` adds `save_arrow` / `save_arrow_async` over `write_batches_*` plus `expected_sha256` forwarding on `save_object` / `save_object_async`, and `DjangoStorage._save` forwards `getattr(content, "sha256", None)` (`crc32c`, `md5`) [^sqlspec-backend] [^aa-backend] [^django-storage].
- **R5 (Companion Wheel Payload Isolation, `G-12`)**: `tools/scripts/verify_storage_wheel.py` (`--package {storage,bigquery,spanner,bundler}`) verifies wheel archive isolation (single `vertebra/_<pkg>*.{so,pyd,dylib}` + `vertebra/_<pkg>.pyi`, zero `vertebra/*.py`), sdist completeness, and outside-checkout isolated-venv fail-loud `MissingDependencyError` contracts; `Makefile` exposes `storage-wheel-check`, `bigquery-wheel-check`, `spanner-wheel-check`, and `bundler-wheel-check`, and `.github/workflows/release.yaml` asserts `_vertebra` excludes `SyncObjectStore` and `Bundler` [^verify-ora-wheel] [^makefile] [^release-yml].
- **R6 (User Docs, OKF Sync `<= 200L`, & Upstream SQLSpec Note)**: `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, `docs/changelog/index.md`, `object-storage.md` (`<= 200L`), `object-storage-limitations.md` (`<= 200L`), and `research/upstream-sqlspec-direct-arrow-write.md` (promoted to `.agents/bundles/research/` prior to `/flow:archive`) record all contracts and caveats [^limitations].

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, `docs/changelog/index.md` (`ch03:t02`).
- `.agents/bundles/knowledge/domains/storage/object-storage.md` (`<= 200L`), `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` (`<= 200L`), and `.agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md` (`ch03:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`, `R2` (read half), `R3` (receipts, verified reads, `vertebra.typing.IntegrityError`) | `ch01` (`t01`–`t03`) | `src/rs/vertebra-storage/src/{store,lib}.rs`, `src/py/vertebra/{_typing,typing,_storage.pyi}`, `src/py/vertebra/storage/{_store,__init__}.py` | `cargo test -p vertebra-storage` & `stubtest vertebra._storage` & `pytest test_storage_store.py` |
| `R4`, `R2` (bench half) (Arrow streaming across storage/SQLSpec/Alchemy/Django + `put_checksum` bench) | `ch02` (`t01`–`t03`) | `src/py/vertebra/storage/_arrow.py`, `src/py/vertebra/extensions/{sqlspec,alchemy,django}/storage.py`, `src/rs/vertebra-storage/benches/put_checksum.rs` | `cargo test -p vertebra-storage --benches` & `pytest test_data_storage_integrations.py` |
| `R5` (`storage`/`bigquery`/`spanner`/`bundler` wheel isolation checks) | `ch03 / t01` | `tools/scripts/verify_storage_wheel.py`, `Makefile`, `.github/workflows/release.yaml` | `pytest test_optional_driver_wheels.py` & `make storage-wheel-check` |
| `R6` (User docs, changelog, `object-storage*.md <= 200L`, upstream SQLSpec note) | `ch03` (`t02`–`t03`) | `docs/usage/storage*.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/storage/*.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [b8b25207] [ch01-integrity-receipts-and-verified-reads](chapters/ch01-integrity-receipts-and-verified-reads/spec.md) — Rust & Python CRC32C/MD5/SHA-256 receipts, abort-before-publish digest checks, verified streaming reads, and `vertebra.typing.IntegrityError` + `vertebra.storage` re-exports.
- [x] [099753b4] [ch02-streaming-arrow-writes-and-integrations](chapters/ch02-streaming-arrow-writes-and-integrations/spec.md) — Streaming Arrow/Parquet multipart writes and digest forwarding across `vertebra.storage`, SQLSpec, Advanced Alchemy, and Django, plus Criterion `put_checksum` benchmark.
- [x] [541d89b5] [ch03-wheel-isolation-and-okf-sync](chapters/ch03-wheel-isolation-and-okf-sync/spec.md) — Companion wheel payload isolation (`storage`, `bigquery`, `spanner`, `bundler`) in `verify_storage_wheel.py` / `Makefile` / `release.yaml`, user docs sync, and `<= 200L` storage OKF + upstream SQLSpec note.

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: All `3` chapters (`ch01`–`ch03`, `9/9` tasks) are completed (`66e6913f`), ready for `/flow:finish` and `/flow:archive`.
- **Release Gate (`/flow:finish`)**: `make lint-all && make rs-test && make py-test && make storage-wheel-check && make bigquery-wheel-check && make spanner-wheel-check && make bundler-wheel-check && make docs && make review-check` (promoting `research/upstream-sqlspec-direct-arrow-write.md` to `.agents/bundles/research/` before `/flow:archive`).

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` conditional S3/R2 copy, multipart uploads, and file/stream reads.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — Incremental multipart session with `UniformPartAccumulator`.
[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — PyO3 storage bindings and `StorageError` mapping.
[^py-typing]: `src/py/vertebra/typing.py` — Public typing and exception facade (`PreconditionError`).
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — `SyncObjectStore` and `AsyncObjectStore` facades, `ObjectMeta`, and `PutResult`.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Incremental Arrow/Parquet codec uploads through `put_stream`.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` with explicit URI resolution and `write_arrow_sync`/`async`.
[^aa-backend]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend`.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage backend.
[^verify-ora-wheel]: `tools/scripts/verify_oracle_wheel.py` — Reference wheel payload isolation and outside-checkout fail-loud verifier.
[^makefile]: `Makefile` — Companion wheel isolation check targets.
[^release-yml]: `.github/workflows/release.yaml` — Release matrix `cibuildwheel` and `test_wheels` jobs.
[^limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Object storage implementation limitations OKF chapter.
