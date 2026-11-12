---
type: Spec
parent_flow: storage-integrity-and-streaming
chapter_id: ch03-arrow-streaming-and-integrations
title: "Chapter 3: Arrow Streaming and Integrations"
description: SQLSpec direct Arrow write capability with abort-safety and bounded-memory tests, SHA-256 options forwarded through the SQLSpec, Advanced Alchemy, and Django storage integrations, and the storage checksum benchmark.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-write-receipts-and-verified-reads
replan_trigger: null
current_task: "storage-integrity-and-streaming:ch03:t01"
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: write_arrow_sync/async, write_bytes_sync/async
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: Incremental Arrow codec uploads through put_stream
  - id: aa-backend
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: Advanced Alchemy save_object and save_object_async
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: Django storage _save
  - id: issue-100
    resource: https://github.com/cofin/vertebra/issues/100
    title: Streamed Arrow writes from SQLSpec pipelines
tags:
  - storage
  - arrow
  - sqlspec
  - benchmark
---

# Chapter 3: Arrow Streaming and Integrations

<!-- truth: start -->
- SQLSpec `ObjectStoreBackend` declares the class attribute `supports_direct_arrow_write: ClassVar[bool] = True`; `write_arrow_sync/async` stream through `write_batches_sync/async` and an escaping codec error aborts the multipart upload [^sqlspec-backend] [^py-arrow].
- SQLSpec `write_bytes_sync/async`, Advanced Alchemy `save_object/save_object_async`, and Django `DjangoStorage._save` forward `checksum` / `expected_sha256` where supplied and surface `IntegrityError` unchanged [^aa-backend] [^django-storage].
- `cargo bench -p vertebra-storage --bench put_checksum` reports default and checksum write throughput separately.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The Vertebra SQLSpec backend streams Arrow writes since [#101](https://github.com/cofin/vertebra/pull/101), but SQLSpec 0.71.1 pipelines call `encode_arrow_payload` and pass full bytes to `write_bytes_*`, never reaching it [^issue-100]. The backend needs a capability flag the upstream hook can detect, plus proof that failures abort and memory stays bounded. Integrations do not carry digest options.

### 1.2 Non-Goals

- Editing installed SQLSpec; ch04/t03 drafts the upstream change.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam |
| --- | --- | --- |
| SQLSpec | `extensions/sqlspec/storage.py::ObjectStoreBackend.write_arrow_sync`, `::write_arrow_async`, `::write_bytes_sync` | SQLSpec `ObjectStoreProtocol` |
| Arrow | `_arrow.py::write_batches_sync`, `::write_batches_async` | `put_stream` |
| AA | `extensions/alchemy/storage.py::ObjectStoreBackend.save_object`, `::save_object_async` | Advanced Alchemy `StorageBackend` |
| Django | `extensions/django/storage.py::DjangoStorage._save` | Django `Storage` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Object Storage](../../../../knowledge/domains/storage/object-storage.md); skills `sqlspec`, `advanced-alchemy`, `python-framework`, `performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R6** (`t01`), **R5** integration forwarding (`t02`), **R4** benchmark half (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class ObjectStoreBackend:
    supports_direct_arrow_write: ClassVar[bool] = True
    def write_bytes_sync(self, path: str | Path, data: bytes, **kwargs: Any) -> None: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Upstream pipeline (hook present) → `backend.write_arrow_sync(path, table, file_format=..., compression=...)` → `write_batches_sync` → `SyncObjectStore.put_stream`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected | Invariant |
| --- | --- | --- | --- |
| Codec failure mid-write | injected exception after first batch | upload aborted; prior object intact | No partial publish |
| Large table | rows ≫ 10× part size | peak buffered bytes ≤ part size × in-flight parts | Bounded memory |
| Unknown kwarg to write_bytes | `foo=1` | existing behavior unchanged | No silent semantics |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a prior object at `t.parquet`, when `write_arrow_sync` fails mid-stream, then `read_bytes_sync("t.parquet")` returns the prior bytes.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Capability signal | Class attribute `supports_direct_arrow_write` | `hasattr(write_arrow_sync)` duck typing: every protocol backend has the method. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in ch04/t02 and ch04/t03.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6 | [t01](tasks/t01-sqlspec-direct-arrow-write.md) | `tdd` | `pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k direct_arrow` |
| R5 | [t02](tasks/t02-integration-digest-forwarding.md) | `tdd` | `pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k sha256` |
| R4 | [t03](tasks/t03-storage-checksum-benchmark.md) | `benchmark` | `cargo bench -p vertebra-storage --bench put_checksum -- --quick` |

## 5. Implementation Plan

- [ ] storage-integrity-and-streaming:ch03:t01 — SQLSpec direct Arrow write capability and abort safety (tasks/t01-sqlspec-direct-arrow-write.md)
- [ ] storage-integrity-and-streaming:ch03:t02 — Forward digest options through SQLSpec, Advanced Alchemy, and Django storage (tasks/t02-integration-digest-forwarding.md)
- [ ] storage-integrity-and-streaming:ch03:t03 — Storage checksum Criterion benchmark (tasks/t03-storage-checksum-benchmark.md)

**Chapter Recovery Checkpoint:** `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q && cd src/rs && cargo bench -p vertebra-storage --bench put_checksum -- --quick` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, HEAD `c8722c76`; depends on ch02.
- **Remaining Tasks in Chapter**: `t01`–`t03` (`0/3`).

[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Arrow uploads.
[^aa-backend]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy backend.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage.
[^issue-100]: [#100](https://github.com/cofin/vertebra/issues/100) — Streamed Arrow writes.
