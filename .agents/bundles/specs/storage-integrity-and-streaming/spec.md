---
type: Spec
flow_id: storage-integrity-and-streaming
title: Object Storage Integrity, Create-Only Copy, and Streamed Arrow Writes
description: Create-only copy and rename on S3-compatible and R2 stores, opt-in SHA-256 write receipts and checksum-verified reads, the SQLSpec direct Arrow write capability with its upstream hook dependency, storage quality fixes, a standalone vertebra-storage wheel isolation check, and complete storage limitations records.
status: stable
state: active
has_chapters: true
chapters:
  - ch01-create-only-copy-and-quality
  - ch02-write-receipts-and-verified-reads
  - ch03-arrow-streaming-and-integrations
  - ch04-wheel-check-docs-and-okf
current_chapter: ch02-write-receipts-and-verified-reads
current_task: "storage-integrity-and-streaming:ch02:t01"
parent_prd: native-data-platform
depends_on_flows: []
research:
  - research/upstream-sqlspec-direct-arrow-write.md
plan_revision: 2
plan_commit: c368cb00
state_revision: 1
last_verified_checkpoint: 9c1865b7
last_reviewed_chapter: ch01-create-only-copy-and-quality
last_reviewed_commit: 9c1865b7
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T03:49:28Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: ObjectStoreEngine copy, rename, put_opts, put_multipart, get, and the S3/R2 AmazonS3Builder construction
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: Incremental multipart session with UniformPartAccumulator
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: PyO3 storage bindings and StorageError to Python exception mapping
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: SyncObjectStore and AsyncObjectStore facades, PutResult, ObjectMeta
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: Incremental Arrow codec uploads through put_stream
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend with write_arrow_sync/async
  - id: engine-tests
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible
    title: Create-only copy and rename conflicts and success on memory, local, S3, B2, and R2
  - id: limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Object storage limitations chapter
  - id: makefile
    resource: Makefile
    title: Per-wheel isolation checks (duckdb, mysql, mssql, oracle, postgres)
  - id: issue-99
    resource: https://github.com/cofin/vertebra/issues/99
    title: SHA-256 write receipts and checksum-verified reads
  - id: issue-100
    resource: https://github.com/cofin/vertebra/issues/100
    title: Stream Arrow writes from SQLSpec pipelines without buffering the whole file
tags:
  - storage
  - s3
  - r2
  - integrity
  - arrow
  - sqlspec
---

# Flow: Object Storage Integrity, Create-Only Copy, and Streamed Arrow Writes

<!-- truth: start -->
- S3/B2 create-only copy uses `S3CopyIfNotExists::Multipart`; R2 uses `HeaderWithStatus("cf-copy-destination-if-none-match", "*", 412)`. Destination conflicts preserve existing bytes and map to `FileExistsError`; unsupported provider operations fail loud [^store-rs] [^engine-tests].
- Cloud rename copies before deleting the source and is not atomic. A destination conflict preserves the source [^store-rs] [^limitations].
- Sync and async `put_multipart` accept synchronous buffer iterables and stream through `put_stream`; generator failure aborts before publish [^py-store].
- Explicit nonempty SQLSpec backend URIs, including `"memory://"`, override the URI inferred from a prebuilt store [^sqlspec-backend].
- SHA-256 receipts, checksum-verified reads, the direct Arrow-write capability flag, and `storage-wheel-check` remain planned in chapters `ch02`–`ch04` [^issue-99] [^issue-100] [^makefile].
- SQLSpec pipelines still require an upstream direct Arrow-write hook to avoid encoding whole-file payloads [^sqlspec-backend].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

S3/B2 and R2 use provider-specific create-only copy strategies, with conflict and success tests for memory, local, S3, B2, and R2 [^store-rs] [^engine-tests]. Multipart facades consume inputs incrementally, and explicit SQLSpec backend URIs determine URI resolution [^py-store] [^sqlspec-backend]. The remaining scope adds content digests, verified reads, direct Arrow-write capability and upstream hook tracking, wheel isolation, and complete user documentation [^issue-99] [^issue-100] [^makefile].

### 1.2 Non-Goals

- Atomic rename on cloud object stores (no provider primitive exists); it is documented instead.
- Changing upstream SQLSpec code from this repository; the upstream hook is tracked by a draft issue/PR text and a limitations entry.

### 1.3 Upstream Execution Baseline

HEAD `c8722c76`. Streaming backend writes landed in `067fa901` ([#101](https://github.com/cofin/vertebra/pull/101)).

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam |
| --- | --- | --- |
| Engine | `store.rs::ObjectStoreEngine::new`, `::copy`, `::rename`, `::put_opts`, `::put_multipart`, `::get`, `::get_file`, `::MultipartUploadSession`, `::StoredObjectMeta`, `::PutConditionalOptions`, `::StorageError` | Rust engine API used by PyO3 |
| Bindings | `lib.rs::to_py_err`, `PySyncObjectStore::put`, `::put_multipart`, `::copy`, `::rename`, `::get` | `vertebra._storage` |
| Facade | `_store.py::SyncObjectStore`, `::AsyncObjectStore`, `::PutResult`, `::ObjectMeta` | `vertebra.storage` |
| SQLSpec | `extensions/sqlspec/storage.py::ObjectStoreBackend`, `_arrow.py::write_batches_sync`, `::write_batches_async` | SQLSpec `ObjectStoreProtocol` |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Object Storage](../../knowledge/domains/storage/object-storage.md), [Object Storage Limitations](../../knowledge/domains/storage/object-storage-limitations.md), [Project Workflow](../../knowledge/workflow.md).
- Skills: `runtime-ipc`, `pyo3-bridge`, `python-framework`, `sqlspec`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: Create-only `copy` and `rename` raise `FileExistsError` on conflict and succeed otherwise on `s3`/`b2` and `r2`, proven against a loopback mock S3 server.
- **R2**: `put_multipart` streams parts through the multipart session without building a full Python list; `ObjectStoreBackend` takes `uri: str | None` with no literal sentinel.
- **R3**: Rust engine computes SHA-256 receipts for single, multipart, stream, and file uploads and enforces `expected_sha256` before publish.
- **R4**: Verified reads for `get`, `get_file`, and streams; a Criterion benchmark shows the default path does no hashing.
- **R5**: Python and stub surfaces expose `checksum`, `expected_sha256`, `PutResult.sha256`, and `vertebra.typing.IntegrityError`; SQLSpec, Advanced Alchemy, and Django storage integrations forward `expected_sha256` where their contracts carry write options.
- **R6**: `ObjectStoreBackend` declares `supports_direct_arrow_write = True`; an injected codec failure during `write_arrow_sync` aborts the multipart upload and the prior object survives; peak buffered memory stays bounded by part size.
- **R7**: `make storage-wheel-check` verifies wheel isolation; `AGENTS.md` and `workflow.md` document it.
- **R8**: User docs, changelog, `object-storage.md`, and the limitations chapter describe every behavior and caveat, including presign verification `NotImplementedError` for GCS/Azure/OCI, Django created/accessed time, non-atomic cloud rename, and the pending upstream SQLSpec hook.
- **R9**: Draft upstream SQLSpec issue/PR text for the direct Arrow write hook is committed under this flow's `research/` folder.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
pub struct PutConditionalOptions {
    pub if_not_exists: bool,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub content_type: Option<String>,
    pub attributes: HashMap<String, String>,
    pub checksum: bool,
    pub expected_sha256: Option<String>,
}
pub struct StoredObjectMeta { /* existing fields */ pub sha256: Option<String> }
pub enum StorageError { /* existing variants */ Integrity { path: String, expected: String, actual: String } }
impl ObjectStoreEngine {
    pub async fn get_verified(&self, path: &str, expected_sha256: &str) -> Result<Bytes, StorageError>;
}
```

```python
class PutResult(ObjectMeta, frozen=True, gc=False, kw_only=True):
    sha256: str | None = None

class IntegrityError(OSError): ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncObjectStore.put(..., expected_sha256=d)` → `PySyncObjectStore.put` → `ObjectStoreEngine::put_opts` (hash before `PutPayload` send) → `object_store`.
- `SyncObjectStore.put_stream` → `create_multipart(checksum=True)` → `MultipartUploadSession::write_chunk` (hasher update) → `complete_with_meta` (compare, abort on mismatch).
- SQLSpec pipeline (after upstream hook) → `ObjectStoreBackend.write_arrow_sync` → `write_batches_sync` → `put_stream`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Create-only copy conflict on S3 | `copy("a","b",overwrite=False)` with `b` present | `FileExistsError`; `b` unchanged | No silent overwrite |
| Provider rejects create-only copy | S3-compatible server without conditional complete | Backend error surfaces unchanged | Fail loud |
| Digest mismatch, single put | `put(data, expected_sha256=wrong)` | `IntegrityError` before any request | Prior object intact |
| Digest mismatch, multipart | `put_stream(..., expected_sha256=wrong)` | Upload aborted; `IntegrityError` | Prior object intact |
| Malformed digest | `expected_sha256="xyz"` | `ValueError` (64 lowercase hex required) | Validate early |
| Corrupted read | `get(path, expected_sha256=wrong)` | `IntegrityError`; no bytes returned | No unverified data |
| Default path | No checksum options | No hasher allocated | Zero cost |
| Upstream SQLSpec lacks hook | `sqlspec <= 0.71.1` pipeline `write_arrow` | Pipeline still buffers; limitations entry | Documented gap |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a mock S3 server holding `b`, when `copy("a","b",overwrite=False)` runs, then `FileExistsError` is raised and `b` is unchanged.
- Given `put_stream` of 3 parts with `checksum=True`, when it completes, then `PutResult.sha256 == hashlib.sha256(b"".join(parts)).hexdigest()`.
- Given a prior object and a mismatched `expected_sha256`, when `put_stream` completes, then `IntegrityError` is raised and `get` returns the prior bytes.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| S3 create-only copy | `S3CopyIfNotExists::Multipart` (conditional `CompleteMultipartUpload` with `If-None-Match`) | Header strategies are provider-specific; read-then-put fallback is racy. |
| R2 create-only copy | `HeaderWithStatus("cf-copy-destination-if-none-match","*",412)` | Multipart conditional complete is not documented for R2. |
| Integrity error type | New `IntegrityError(OSError)` in `vertebra.typing` | Reusing `PreconditionError` conflates digest and ETag failures. |
| Hashing location | Rust engine over sent bytes | Python-side hashing duplicates buffers and holds the GIL. |
| Stream verification | Raise on the final read after full hash | Buffering whole streams defeats streaming. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[3/9 files documented]`.

| File | Owning Task |
| --- | --- |
| `Makefile`, `AGENTS.md`, `.agents/bundles/knowledge/workflow.md` | ch04/t01 |
| `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md`, `.agents/bundles/knowledge/domains/storage/object-storage.md`, `docs/usage/storage.md` | ch04/t02 |
| `research/upstream-sqlspec-direct-arrow-write.md`, `docs/changelog/index.md`, `docs/usage/storage-parquet-and-orm.md` | ch04/t03 |

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1 | ch01/t01 | `regression_tdd` | `cargo test -p vertebra-storage copy_if_not_exists` |
| R2 | ch01/t02 | `regression_tdd` | `pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R3 | ch02/t01 | `tdd` | `cargo test -p vertebra-storage sha256` |
| R4 | ch02/t02, ch03/t03 | `tdd`, `benchmark` | `cargo test -p vertebra-storage verified`; `cargo bench -p vertebra-storage --bench put_checksum -- --quick` |
| R5 | ch02/t03, ch02/t04, ch03/t02 | `tdd` | stubtest; `pytest src/py/tests/unit/storage/test_storage_store.py -k "sha256 or verified"`; `pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sha256` |
| R6 | ch03/t01 | `tdd` | `pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k direct_arrow` |
| R7 | ch04/t01 | `static_validation` | `make storage-wheel-check` |
| R8 | ch04/t02, ch04/t03 | `docs` | `make docs` |
| R9 | ch04/t03 | `docs` | `make docs`; draft file present |

## 5. Implementation Plan

- [x] [9c1865b7] ch01-create-only-copy-and-quality ([spec](chapters/ch01-create-only-copy-and-quality/spec.md))
- [ ] ch02-write-receipts-and-verified-reads ([spec](chapters/ch02-write-receipts-and-verified-reads/spec.md))
- [ ] ch03-arrow-streaming-and-integrations ([spec](chapters/ch03-arrow-streaming-and-integrations/spec.md))
- [ ] ch04-wheel-check-docs-and-okf ([spec](chapters/ch04-wheel-check-docs-and-okf/spec.md))

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `da0d764e`; `2/12` tasks completed and `1/4` chapters reviewed.
- **Active Chapter**: `ch02-write-receipts-and-verified-reads`, task `storage-integrity-and-streaming:ch02:t01`; ch01 is complete after correctness and fresh quality reviews at `9c1865b7`.
- **Remaining Plan**: `ch02` adds SHA-256 receipts and verified reads; `ch03` adds direct Arrow-write capability, integration forwarding, and benchmarks; `ch04` adds wheel checks and completes documentation and upstream draft tracking.
- **Documentation & OKF Sync**: `[3/9 files documented]`; the storage guide and two knowledge chapters reflect create-only copy, streamed multipart, and provider caveats. Remaining chapter contracts require further documentation updates.
- **External Dependency**: upstream SQLSpec direct Arrow write hook in `SyncStoragePipeline.write_arrow` / `AsyncStoragePipeline.write_arrow`.
- **Active Invariants**: unsupported conditional operations fail loud; existing objects survive create-only conflicts and failed multipart input. Checksum work remains unimplemented.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Engine operations and S3/R2 builders.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — Multipart session.
[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — Error mapping.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python facades.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Incremental Arrow uploads.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
[^engine-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible` — Existing create-only test.
[^limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations chapter.
[^makefile]: `Makefile` — Wheel isolation checks.
[^issue-99]: [#99](https://github.com/cofin/vertebra/issues/99) — SHA-256 receipts and verified reads.
[^issue-100]: [#100](https://github.com/cofin/vertebra/issues/100) — Streamed Arrow writes from SQLSpec pipelines.
