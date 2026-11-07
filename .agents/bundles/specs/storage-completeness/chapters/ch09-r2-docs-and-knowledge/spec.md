---
type: Spec
parent_flow: storage-completeness
chapter_id: ch09-r2-docs-and-knowledge
title: Synchronize Supported Providers and Native Guarantees
description: Synchronize Supported Providers and Native Guarantees
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: bdc7799ab18b9a961dacf99f5eee9af46a557aa3
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
  resource: docs/index.md
  title: Grounded implementation seam
tags:
- storage
- repair
---

# Synchronize Supported Providers and Native Guarantees

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Synchronize Supported Providers and Native Guarantees addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Remove R2 claims from public navigation and reference

Remove first-class R2 entries from object-storage provider tables and reference configuration list. Preserve S3 custom endpoint wording and every remaining provider. The user guide's detailed behavior is owned by ch04:t03, not duplicated here. Keep API reference symbols matching installed exported classes.

### R2: Remove R2 from architecture and product capability claims

Remove first-class R2 from native extension capability summaries, product supported-backend declarations and domain knowledge catalog. Keep HMAC, S3, Azure, GCS and OCI signing claims intact where implemented. Do not rewrite unrelated changelog history, email model labels or unrelated r2 variable substrings.

### R3: Record provider atomicity, key identity, cleanup and R2 withdrawal

Rewrite storage knowledge source provenance and truth blocks around actual supported backends and OCI-owned uniform chunking. Remove r2.rs source/footnote/table entries, R2 signing and configuration claims. Document raw path identity, typed PreconditionError, SDK transport/refresh reuse, provider-specific conditional publication, local cooperating Vertebra process guarantee with uncooperative direct OS writers excluded, cancellation cleanup ownership, and evidence-backed supported platform behavior. No 5 TiB guarantee or universal local metadata guarantee is invented. Evidence is native/public tests, not plan assertions.

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

### R4: Remove R2 from repository and embedding overviews

Remove first-class R2 from `README.md`, `docs/reference/index.md`, and `docs/usage/embeddings-and-vectors.md`. Preserve the remaining supported providers and historical changelog entries.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/11 files documented]`. t01 owns three public provider/reference pages; t02 owns three architecture/product/catalog pages; t03 owns the storage and data-driver knowledge chapters; t04 owns the README, reference index and embedding guide. Detailed storage usage belongs to ch04:t03. t02/t03/t04 depend on t01 because the full documentation build requires its removed-symbol reference repair. t03 additionally waits on completed ch16:t01 native acceptance and reads its recorded evidence. Documentation-only validation uses the actual build and API surface, without artificial public-call regression tests.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch09:t01](tasks/t01-public-provider-indexes.md) | documentation_validation | `src/py/tests/unit/tools/test_docs_tooling.py` |
| R2 | [ch09:t02](tasks/t02-architecture-and-product.md) | documentation_validation | `src/py/tests/unit/tools/test_docs_tooling.py` |
| R3 | [ch09:t03](tasks/t03-storage-knowledge.md) | documentation_validation | Completed ch16:t01 evidence, bundle validation and documentation build |
| R4 | [ch09:t04](tasks/t04-overview-and-embedding-claims.md) | documentation_validation | Documentation build and supported-provider review |

## 5. Implementation Plan

- [ ] storage-completeness:ch09:t01 — Remove R2 claims from public navigation and reference (tasks/t01-public-provider-indexes.md)
- [ ] storage-completeness:ch09:t02 — Remove R2 from architecture and product capability claims (tasks/t02-architecture-and-product.md)
- [ ] storage-completeness:ch09:t03 — Record provider atomicity, key identity, cleanup and R2 withdrawal (tasks/t03-storage-knowledge.md)
- [ ] storage-completeness:ch09:t04 — Remove R2 from repository and embedding overviews (tasks/t04-overview-and-embedding-claims.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch09-r2-docs-and-knowledge` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 4 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/11 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `docs/index.md` — Verified repository implementation seam.
