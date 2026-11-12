---
type: Spec
parent_flow: storage-integrity-and-streaming
chapter_id: ch04-wheel-check-docs-and-okf
title: "Chapter 4: Wheel Check, Docs, and OKF"
description: make storage-wheel-check, the complete storage limitations record and object-storage chapter, user docs and changelog, and the drafted upstream SQLSpec direct Arrow write hook.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch03-arrow-streaming-and-integrations
replan_trigger: null
current_task: "storage-integrity-and-streaming:ch04:t01"
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
  - id: makefile
    resource: Makefile
    title: postgres-wheel-check and sibling wheel checks
  - id: limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Storage limitations chapter
  - id: presign-verify
    resource: src/py/vertebra/storage/_store.py::SigningConfig
    title: Offline presign verification raising NotImplementedError for GCS, Azure, and OCI
  - id: django-times
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: get_created_time and get_accessed_time
  - id: upstream-pipeline
    resource: .venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py
    title: SQLSpec 0.71.1 SyncStoragePipeline.write_arrow and AsyncStoragePipeline.write_arrow
tags:
  - docs
  - okf
  - makefile
---

# Chapter 4: Wheel Check, Docs, and OKF

<!-- truth: start -->
- `make storage-wheel-check` builds `src/rs/vertebra-storage` into `/tmp/vertebra-storage-wheel-check` and asserts the only `vertebra/` entries are one `vertebra/_storage*.so` and `vertebra/_storage.pyi` [^makefile].
- The limitations chapter lists offline presign verification `NotImplementedError` for GCS/Azure/OCI, Django created/accessed time `NotImplementedError`, non-atomic cloud rename, stream verification at end of stream, and the pending upstream SQLSpec direct Arrow write hook [^limitations].
- `research/upstream-sqlspec-direct-arrow-write.md` holds the upstream issue and PR text [^upstream-pipeline].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

No wheel check exists for `vertebra-storage` [^makefile]. The limitations chapter omits presign verification, Django time, and cloud rename caveats [^limitations]. New behavior from ch01–ch03 needs user docs, OKF, and changelog entries.

### 1.2 Non-Goals

- Release workflow changes.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam |
| --- | --- | --- |
| Make | `Makefile::postgres-wheel-check` | Developer gate |
| Caveats | `_store.py::SigningConfig` verification, `django/storage.py::DjangoStorage.get_created_time`, `::get_accessed_time`, `store.rs::ObjectStoreEngine::rename` | Documentation |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Object Storage](../../../../knowledge/domains/storage/object-storage.md), [Object Storage Limitations](../../../../knowledge/domains/storage/object-storage-limitations.md), [Project Workflow](../../../../knowledge/workflow.md); skills `flow-docs`, `okf`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R7** (`t01`), **R8** (`t02`, `t03`), **R9** (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```make
.PHONY: storage-wheel-check
storage-wheel-check:                                ## Build vertebra-storage wheel and verify payload isolation
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `make storage-wheel-check` → `uv build src/rs/vertebra-storage --wheel` → `zipfile` assertion.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected | Invariant |
| --- | --- | --- | --- |
| Extra Python file in wheel | packaging drift | assertion fails listing entries | Isolation |
| Zero or multiple wheels | stale dir | removed first; assertion on count `1` | Deterministic |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a clean tree, when `make storage-wheel-check` runs, then it prints `vertebra-storage wheel verified`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Upstream tracking | Draft text in this flow's `research/` | Filing upstream from this repo: delivery is user-driven. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/8 files documented]`.

| File | Task |
| --- | --- |
| `Makefile`, `AGENTS.md`, `.agents/bundles/knowledge/workflow.md` | `t01` |
| `object-storage-limitations.md`, `object-storage.md`, `docs/usage/storage.md` | `t02` |
| `research/upstream-sqlspec-direct-arrow-write.md`, `docs/changelog/index.md`, `docs/usage/storage-parquet-and-orm.md` | `t03` |

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R7 | [t01](tasks/t01-storage-wheel-check.md) | `static_validation` | `make storage-wheel-check` |
| R8 | [t02](tasks/t02-storage-limitations-and-okf.md) | `docs` | `make docs` |
| R8, R9 | [t03](tasks/t03-upstream-sqlspec-hook-and-changelog.md) | `docs` | `make docs` |

## 5. Implementation Plan

- [ ] storage-integrity-and-streaming:ch04:t01 — Add make storage-wheel-check and document it (tasks/t01-storage-wheel-check.md)
- [ ] storage-integrity-and-streaming:ch04:t02 — Complete storage limitations, OKF chapter, and usage docs (tasks/t02-storage-limitations-and-okf.md)
- [ ] storage-integrity-and-streaming:ch04:t03 — Draft the upstream SQLSpec hook and update changelog and Arrow docs (tasks/t03-upstream-sqlspec-hook-and-changelog.md)

**Chapter Recovery Checkpoint:** `make storage-wheel-check && make docs` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, HEAD `c8722c76`; depends on ch03.
- **Remaining Tasks in Chapter**: `t01`–`t03` (`0/3`).

[^makefile]: `Makefile` — Wheel checks.
[^limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations.
[^presign-verify]: `src/py/vertebra/storage/_store.py::SigningConfig` — Presign verification.
[^django-times]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django times.
[^upstream-pipeline]: `.venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py` — Upstream pipeline.
