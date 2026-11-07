---
type: Spec
parent_flow: storage-completeness
chapter_id: ch06-adapter-correctness
title: Django Collision Length and SQLSpec Signing Errors
description: Preserve Django per-call filename limits and replayable content after collision errors, and retain configured SQLSpec signing failures.
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
updated_at: '2026-11-07T22:33:17Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified: []
sources:
- id: source
  resource: src/py/vertebra/extensions/django/storage.py
  title: Grounded implementation seam
- id: sqlspec-signing
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
  title: Configured signing capability and error propagation
- id: django-regressions
  resource: src/py/tests/unit/extensions/test_django_plugin.py
  title: Consumed-source collision recovery and independent limits
- id: signing-regressions
  resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
  title: Native signing validation and local IAM failure acceptance
tags:
- storage
- repair
---

# Django Collision Length and SQLSpec Signing Errors

<!-- truth: start -->
- Django retries preserve per-call `max_length` and rewind seekable input; non-replayable input retains the original `FileExistsError`. [^source]
- SQLSpec checks signing capability before delegation and preserves configured validation and provider exceptions. [^sqlspec-signing]
- Both task commits are verified; chapter correctness review and fresh quality review remain pending.
- Native atomic multipart publication remains a separate repair and acceptance obligation.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Django Collision Length and SQLSpec Signing Errors addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Retain max_length across real Django create collisions

`DjangoStorage._save(name, content)` performs one native `put_stream` attempt with `if_not_exists=not file_overwrite` and propagates `FileExistsError`. `save(name, content, max_length)` retries through `super().save(..., max_length=max_length)`, retaining Django validation, truncation and available-name selection on each attempt. `asave` applies the same per-call limit while choosing alternative names. Both paths seek input to zero after a collision; if seeking raises `AttributeError`, `OSError` or `ValueError`, the original collision propagates without an alternative upload. An impossible filename limit raises Django `SuspiciousFileOperation`. Overwrite mode propagates a reported `FileExistsError` without retrying. Limits and source positions remain local to each invocation. [^source]

### R2: Propagate configured signing validation failures

`ObjectStoreBackend._sign_single` and `_sign_single_async` qualify the key, check the corresponding store's public `supports_signing` capability and raise `NotImplementedError` with backend context when signing is unavailable. Supported calls delegate to `presign_get` or `presign_put` without exception conversion or message classification. Configured validation and provider failures retain their type and message; `for_upload` selects PUT and scalar/list return shapes remain intact. [^sqlspec-signing]

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

The collision regressions consume the input, publish a racer through a native create-only `put`, then issue a second native create-only `put` that raises `FileExistsError` inside the producer. They verify sync/async rewind, impossible limits, non-replayable input and bounded same-instance interleavings. They do not exercise a conflict at multipart completion; that acceptance remains in the native publication repair chapters. [^django-regressions]

Signing tests exercise native HMAC expiry `ValueError` and a real local IAM HTTP 503 `RuntimeError` through sync/async GET and upload calls. The provider message remains exactly `signing error: IAM signBlob returned HTTP 503 Service Unavailable`. Exact type assertions distinguish provider failures from unsupported signing. [^signing-regressions]

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented]` for this chapter's adapter contracts in [object-storage.md](../../../../knowledge/domains/storage/object-storage.md). Final native guarantees and provider coverage remain with ch09:t03; user guides remain with ch04:t03. The canonical regression tests record the verified adapter behavior and its native publication boundary.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch06:t01](tasks/t01-django-max-length-retry.md) | regression_tdd | `src/py/tests/unit/extensions/test_django_plugin.py` |
| R2 | [ch06:t02](tasks/t02-sqlspec-signing-errors.md) | regression_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [x] [cd7e5563] storage-completeness:ch06:t01 — Retain max_length across real Django create collisions (tasks/t01-django-max-length-retry.md)
- [x] [45b068e2] storage-completeness:ch06:t02 — Propagate configured signing validation failures (tasks/t02-sqlspec-signing-errors.md)

**Chapter Recovery Checkpoint:** both worksheets are completed and the current-source storage/Django/data integration checkpoint passes 88 tests. `/flow:review storage-completeness ch06-adapter-correctness` performs correctness review, followed by fresh quality review before chapter closure.

## 6. Current State & Remaining Plan Snapshot

- Plan revision 4 is independently approved; completed task receipts match signed source commits.
- Both adapter repairs are implemented and verified; correctness review and fresh quality review remain.
- Adapter knowledge is synchronized (`[1/1 files documented]`); final native/provider and user-guide consolidation remains in its owning tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/extensions/django/storage.py` — Verified repository implementation seam.
[^sqlspec-signing]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Signing capability and delegation.
[^django-regressions]: `src/py/tests/unit/extensions/test_django_plugin.py` — Consumed-source producer collision recovery.
[^signing-regressions]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Native validation, provider errors and signing shapes.
