---
type: Spec
parent_flow: storage-completeness
chapter_id: ch06-adapter-correctness
title: Django Collision Length and SQLSpec Signing Errors
description: Django Collision Length and SQLSpec Signing Errors
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 4
plan_commit: 88d5068031ba82078f2f7c4999cdf3cfd77c66be
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
  resource: src/py/vertebra/extensions/django/storage.py
  title: Grounded implementation seam
tags:
- storage
- repair
---

# Django Collision Length and SQLSpec Signing Errors

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Django Collision Length and SQLSpec Signing Errors addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Retain max_length across real Django create collisions

`DjangoStorage._save(name, content)` performs one create-only native put_stream attempt and propagates FileExistsError. `DjangoStorage.save(name, content, max_length)` owns the collision retry loop around `super().save(..., max_length=max_length)` so each retry retains Django validation, length truncation and available-name selection. Rewind file contents before retries so an upload consumed before conflict does not publish empty bytes. No instance mutable per-call max_length or process lock. `asave` retains equivalent length and rewind behavior. file_overwrite=True preserves overwrite behavior. Work against current committed Django API, not a fabricated alternative storage hierarchy.

### R2: Propagate configured signing validation failures

`ObjectStoreBackend._sign_single` and `_sign_single_async` check the configured store's public supports_signing capability and raise NotImplementedError only when signing is unavailable. Remove broad RuntimeError/ValueError conversion. Delegate supported presign_get/presign_put validation unchanged so invalid expiry, credentials or provider operation errors retain their exact type. Keep scalar/list return shape and for_upload HTTP method. No error-message substring classification.

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

The collision regression covers consumed seekable files on both sync and async paths, explicit rewind, impossible maximum lengths, and bounded same-instance interleavings. Signing tests cover both native HMAC validation `ValueError` and a real local IAM HTTP 503 `RuntimeError` with the existing native status message through GET and upload methods; exact exception types distinguish provider failures from unsupported signing.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch06:t01](tasks/t01-django-max-length-retry.md) | regression_tdd | `src/py/tests/unit/extensions/test_django_plugin.py` |
| R2 | [ch06:t02](tasks/t02-sqlspec-signing-errors.md) | regression_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [x] [cd7e5563] storage-completeness:ch06:t01 — Retain max_length across real Django create collisions (tasks/t01-django-max-length-retry.md)
- [x] [45b068e2] storage-completeness:ch06:t02 — Propagate configured signing validation failures (tasks/t02-sqlspec-signing-errors.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch06-adapter-correctness` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Plan revision 4 is independently approved; completed task receipts match signed source commits.
- Both adapter repairs are implemented and verified; documentation sync, correctness review and fresh quality review remain.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/extensions/django/storage.py` — Verified repository implementation seam.
