---
type: Spec
parent_flow: storage-completeness
chapter_id: ch07-remove-r2-python
title: Withdraw First-Class R2 Python APIs
description: Withdraw First-Class R2 Python APIs
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 69a77955
state_revision: 0
last_verified_checkpoint: 5fe477fa
last_reviewed_commit: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified: []
sources:
- id: source
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Grounded implementation seam
tags:
- storage
- repair
---

# Withdraw First-Class R2 Python APIs

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Withdraw First-Class R2 Python APIs addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Remove R2 from framework backend classifiers

Remove R2Config imports and entries in `_STORAGE_CONFIGS` from Advanced Alchemy, SQLSpec and Starlette. Remove r2:// advertised URI support in their docstrings. Keep S3Config with arbitrary explicit custom endpoints accepted; no provider hostname blacklist, alias or Cloudflare compatibility shim. This task precedes core Python API withdrawal so its imports stay valid at every commit.

### R2: Delete public R2Config and from_r2 convenience APIs

Delete R2Config, its `chunk_uniform_parts` helper, from_r2 constructor, config union/validation members, signing configuration branch, supported-scheme membership, and public __all__/package exports. Reject `r2://` in facade URI construction as unsupported with ValueError before native construction. Convert relevant canonical signing/config examples to generic S3 custom endpoints or OCI 8 MiB coverage; delete R2-only fixture and its consumers without skipping unrelated provider acceptance. Generic S3 custom endpoints and S3 SigV4 remain fully supported. No alias or deprecation shim remains. Native helper is retained until ch08 withdrawal, keeping stubtest parity between commits.

### R3: Reject removed R2 URIs before staging local-path resolution

`ObjectStoreStaging.validate_staging_target`, `_parse_implicit_uri`, `_resolve_backend_path` and `_resolve_store_key` reject URL scheme r2 with a clear ValueError identifying unsupported storage scheme. Remove r2 from `_REMOTE_SCHEMES` only together with explicit unknown/removed-URI rejection so `r2://bucket/key` never becomes a local filename or directory. Preserve plain paths, file:// and supported remote schemes, and explicit passed-store compatibility checks. No R2 alias or special endpoint normalization.

Caller → seam → collaborator: public storage/framework calls → owned deep module boundary → native SDK, transport or upstream framework. Public signatures remain stable except the deliberate removal of first-class R2 and addition of typed PreconditionError.

| Failure or edge | Required invariant |
| --- | --- |
| Competing writers | The winning object remains intact; stale CAS fails with typed PreconditionError or create-only FileExistsError. |
| Provider/transport error | Preserve the provider failure and cleanup ownership; no silent fallback or full-object buffering. |
| Optional dependency absent | Imports remain lazy and configured construction fails explicitly. |

| Alternative | Decision |
| --- | --- |
| Deep owned boundary with exact provider semantics | Chosen: hides credentials, staging and concurrency while preserving configured SDK behavior. |
| Generic head-then-write, Python lock, copy-create publication or blanket exception conversion | Rejected: loses atomicity, update results, metadata or error identity. |

For an explicitly configured store/backend, validate every fully qualified URI against its actual `store_uri` (SQLSpec backend) or `url` (native store) before stripping qualification. Normalize only the existing protocol aliases (`gcs`→`gs`, `azure`→`az`) for comparison and require matching remote authority/bucket. Never turn a mismatched scheme or bucket into a relative path by discarding its prefix. Preserve the matching fully qualified backend path so SQLSpec performs its own raw-key/root-containment validation. `validate_staging_target` performs the same qualification rejection before upstream query or producer consumption. No metadata request or object mutation is needed for this validation; local/file and supported matching remote behavior remain accepted.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch07:t01](tasks/t01-framework-classifiers.md) | regression_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R2 | [ch07:t02](tasks/t02-withdraw-config-and-exports.md) | regression_tdd | `src/py/tests/unit/storage/test_multicloud_storage.py` |
| R3 | [ch07:t03](tasks/t03-staging-reject-r2.md) | regression_tdd | `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |

## 5. Implementation Plan

- [x] [4486f6fe] storage-completeness:ch07:t01 — Remove R2 from framework backend classifiers (tasks/t01-framework-classifiers.md)
- [x] [75f18e3b] storage-completeness:ch07:t02 — Delete public R2Config and from_r2 convenience APIs (tasks/t02-withdraw-config-and-exports.md)
- [x] [38757809] storage-completeness:ch07:t03 — Reject removed R2 URIs before staging local-path resolution (tasks/t03-staging-reject-r2.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch07-remove-r2-python` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Classifier and API withdrawal are signed at `4486f6fe` and `75f18e3b`; staging withdrawal and qualification validation are signed at `38757809` and verified by 139 canonical tests.
- All three Python withdrawal tasks are implemented and verified. Chapter documentation sync and independent reviews remain; native dispatch/module withdrawal remains assigned to ch08.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/extensions/alchemy/storage.py` — Verified repository implementation seam.
