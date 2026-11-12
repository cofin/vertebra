---
type: Spec
parent_flow: storage-integrity-and-streaming
chapter_id: ch01-create-only-copy-and-quality
title: "Chapter 1: Create-Only Copy and Quality"
description: Copy-if-not-exists strategies for S3/B2 and R2 stores with mock-S3 conflict and success tests, streamed put_multipart, and a sentinel-free ObjectStoreBackend constructor.
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: c368cb00
state_revision: 1
last_verified_checkpoint: da0d764e
last_reviewed_commit: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T03:33:00Z"
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
    title: S3/R2 AmazonS3Builder construction, copy, rename
  - id: engine-tests
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible
    title: Existing create-only copy test
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: put_multipart and put_stream
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: ObjectStoreBackend constructor
tags:
  - storage
  - s3
  - r2
---

# Chapter 1: Create-Only Copy and Quality

<!-- truth: start -->
- `BackendConfig::S3` stores build with `.with_copy_if_not_exists(S3CopyIfNotExists::Multipart)`; `BackendConfig::R2` stores build with `.with_copy_if_not_exists(S3CopyIfNotExists::HeaderWithStatus("cf-copy-destination-if-none-match".into(), "*".into(), StatusCode::PRECONDITION_FAILED))` [^store-rs].
- `ObjectStoreEngine::copy(.., true)` and `::rename(.., true)` map `object_store::Error::AlreadyExists` and `Precondition` on the destination to `StorageError::AlreadyExists`, which Python surfaces as `FileExistsError` [^store-rs].
- `SyncObjectStore.put_multipart` / `AsyncObjectStore.put_multipart` delegate to `put_stream` with the same parts iterable [^py-store].
- `ObjectStoreBackend.__init__(uri: str | None = None, ...)`: when `store` is supplied, `store_uri` is `uri` if given else `store.url` [^sqlspec-backend].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Create-only copy and rename preserve conflicting destinations on memory, local, S3/B2, and R2 [^engine-tests]. Multipart facades consume buffer iterables incrementally, and explicit SQLSpec backend URIs determine URI resolution [^py-store] [^sqlspec-backend].

### 1.2 Non-Goals

- Atomic cloud rename (documented in ch04/t02).
- Azure create-only copy changes (`object_store` Azure already supports it natively).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam |
| --- | --- | --- |
| Engine | `store.rs::ObjectStoreEngine::new` (`BackendConfig::S3`, `BackendConfig::R2` arms), `::copy`, `::rename` [^store-rs] | `ObjectStoreEngine` |
| Facade | `_store.py::SyncObjectStore.put_multipart`, `::put_stream`, `::AsyncObjectStore.put_multipart` [^py-store] | `vertebra.storage` |
| SQLSpec | `extensions/sqlspec/storage.py::ObjectStoreBackend.__init__` [^sqlspec-backend] | SQLSpec backend |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Object Storage](../../../../knowledge/domains/storage/object-storage.md); skills `runtime-ipc`, `python-framework`, `sqlspec`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1**: create-only copy/rename on `s3`/`b2`/`r2` (`t01`).
- **R2**: streamed `put_multipart` and sentinel-free backend (`t02`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
use object_store::aws::S3CopyIfNotExists;
```

```python
def put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult: ...
class ObjectStoreBackend:
    def __init__(self, uri: str | None = None, *, store: StorageTarget | None = None, ...) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncObjectStore.copy(overwrite=False)` → `PySyncObjectStore.copy(if_not_exists=True)` → `ObjectStoreEngine::copy` → `AmazonS3::copy_if_not_exists` (multipart conditional complete or R2 header).

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected | Invariant |
| --- | --- | --- | --- |
| S3 conflict | mock returns `412` on `CompleteMultipartUpload` | `StorageError::AlreadyExists` | No overwrite |
| R2 conflict | mock returns `412` on `PUT` with `cf-copy-destination-if-none-match: *` | `StorageError::AlreadyExists` | No overwrite |
| Success | destination absent | copy succeeds; rename deletes source | Same as memory/local |
| Empty parts | `put_multipart(path, [])` | zero-byte object, same as `put_stream` | Parity |
| `uri=None`, `store=None` | backend ctor | `ImproperConfigurationError` | Fail loud |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a mock S3 server with `b`, when `engine.copy("a","b",true)` runs, then the result is `StorageError::AlreadyExists` and `b` keeps its bytes.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| `put_multipart` | Delegate to `put_stream` | Removing it breaks the public API without a migration need. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[3/3 files documented in this chapter]`.

- `docs/usage/storage.md` describes streamed multipart and conditional cloud copy.
- `.agents/bundles/knowledge/domains/storage/object-storage.md` records the implemented provider strategies and facade behavior.
- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` records non-atomic cloud rename and unsupported provider refusal.

Chapter `ch04/t02` owns the remaining integrity and streaming documentation.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-s3-r2-create-only-copy.md) | `regression_tdd` | `cargo test -p vertebra-storage copy_if_not_exists` |
| R2 | [t02](tasks/t02-streamed-put-multipart-and-backend-uri.md) | `regression_tdd` | `pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [x] [0658fd1f] storage-integrity-and-streaming:ch01:t01 — Configure S3/B2 and R2 create-only copy with mock-S3 tests (tasks/t01-s3-r2-create-only-copy.md)
- [x] [da0d764e] storage-integrity-and-streaming:ch01:t02 — Stream put_multipart and remove the backend URI sentinel (tasks/t02-streamed-put-multipart-and-backend-uri.md)

**Chapter Recovery Checkpoint:** `cd src/rs && cargo test -p vertebra-storage copy_if_not_exists && cd ../.. && uv run pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `da0d764e`; `2/2` task commits are complete.
- **Recovery Checkpoint**: 43 Rust tests, 434 Python tests, focused Clippy, Ruff, and mypy pass; the storage extension is rebuilt from current source.
- **Chapter Review**: correctness and fresh quality reviews remain open; `last_reviewed_commit` stays null.
- **Remaining Plan**: `ch02` receipts and verified reads, `ch03` Arrow capability and integration forwarding, and `ch04` wheel isolation and final docs.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Builders, copy, rename.
[^engine-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs::test_copy_if_not_exists_s3_compatible` — Existing test.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Facade.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
