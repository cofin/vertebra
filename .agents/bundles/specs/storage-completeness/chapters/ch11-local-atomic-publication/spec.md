---
type: Spec
parent_flow: storage-completeness
chapter_id: ch11-local-atomic-publication
title: Local Cross-Process Conditional Publication
description: Local Cross-Process Conditional Publication
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: storage-completeness:ch11:t02
plan_revision: 5
plan_commit: bdc7799a
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
  resource: src/rs/vertebra-storage/src/local_publication.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Local Cross-Process Conditional Publication

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Local Cross-Process Conditional Publication addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Add persistent OS locks per physical parent directory

Private `local_publication.rs` introduces LocalPublication and RAII DirectoryGuard. Every physical destination parent directory uses the persistent SDK-illegal filename `.vtb-cas-lock#0` inside that directory. Use native OS cross-process File::lock/unlock semantics, not a Python/process mutex or OS-temp hash/registry. A single parent-directory lock serializes all leaf spellings including case/Unicode aliases without reimplementing filesystem name equivalence. Canonicalize/open the physical parent; two-directory mutations deduplicate and sort stable physical directory identities (Unix device/inode; Windows volume/file ID) before acquisition, with deterministic ordering on macOS/Linux/Windows. Overlapping LocalConfig roots and symlink aliases therefore converge on the same physical lockfile. Acquire lockfiles using SDK-normal create/umask access modes and existing-file read-lock compatibility; do not require extra user-managed infrastructure or chmod policy. Real permission/unsupported-filesystem errors propagate clearly. Never unlink/recreate persistent lockfiles: that introduces lock ABA. Stage payload outside the lock; acquire only for destination predicate, atomic publication and actual resulting metadata. t03 routes every cooperating Local mutation through this seam; direct uncooperative OS writers remain outside the guarantee.

### R2: Manage SDK-private same-parent staging artifacts

`LocalPublication::stage(destination)` creates an exclusive same-parent file named `.vertebra-stage-<uuid>#<counter>` whose base has no `#` and whose FIRST `#` suffix is nonempty ASCII digits. This exactly matches pinned LocalFileSystem's already unsupported/ignored filename grammar; it cannot collide with an SDK-legal object key and both list and list_with_delimiter hide it through every overlapping root. Never append a suffix to the caller's basename because legal `name#suffix` would make the SDK expose the resulting staging file. Use create_new and retry generated-name collision; retain file handle/path ownership. No OS-temp staging registry, global reserved prefix or new legal-key filtering.

Write the payload incrementally outside the persistent parent lock; same-parent staging guarantees same filesystem. Abort/cancellation/drop ownership removes only this session's actual created stage. Successful link/rename consumes or removes stage ownership. Persistent `.vtb-cas-lock#0` is never unlinked. Do not invent broad crash scavenging of similarly named files; any orphan after process death remains in the SDK's excluded keyspace and cleanup guarantees are bounded honestly. Keep normal SDK umask/access behavior. No mmap, whole-file readback, payload Vec or single-PUT size cap.

### R3: Guard every local mutation and publish with native atomic operations

Route ordinary put/put_opts/put_file/delete/copy/rename and multipart final publication through per-physical-parent DirectoryGuards. Stage payload outside locks. Acquire source/destination physical parent directory guards in t01's deduplicated deterministic order for two-directory mutations; same-directory operations acquire only once. Under the held guard evaluate Create/Match/NotMatch against actual current target metadata and perform native atomic publication. Create uses native no-replace hard-link/create semantics; Match/NotMatch/Overwrite use same-filesystem atomic rename under the guard. Obtain actual resulting metadata/ETag before releasing the guard so returned results cannot describe a competing later object. Delete/rename/copy publication also participates; no ordinary write bypasses the guard. Never invoke unsupported LocalFileSystem PutMode::Update then fall back to unconditional overwrite. Preserve original target until successful publication and retain abort/stage ownership on error. Local SDK's actual content_type/attribute capability is documented honestly rather than fabricating persistence. Direct noncooperating OS writers are outside the guarantee. Lock acquisition executes off async runtime worker threads when it can block; no Python/global process lock or process mutex fallback.

### R4: Prove local CAS across processes, roots and cleanup boundaries

Add canonical native subprocess regressions with independently launched child processes, distinct LocalConfig roots and different TMPDIR environments. Use barriers for stage readiness, competing write and final publication, not sleeps. `/root` key a/b and `/root/a` key b share the physical parent lock. Case/Unicode leaf aliases on supported case-insensitive filesystems share that same parent lock without a custom casefold. Opposing two-directory copy/rename uses physical-directory-ID dedupe/order and cannot deadlock. During upload, list/list_page/list_with_delimiter through narrow and broader overlapping roots must not expose `.vtb-cas-lock#0` or `.vertebra-stage-<uuid>#<counter>`; legal names such as keep#suffix remain visible and readable, while name#123 retains existing SDK unsupported semantics. Cancellation/abort removes the owned stage while the persistent lock remains. Assert actual postpublication metadata is collected under guard, permission errors remain real, and no world-writable temp root/registry or extra infrastructure is required. Record Linux/macOS/Windows evidence separately; direct OS writers remain excluded.

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

t01/t02 implement and characterize the actual private OS/filesystem primitives in a test-only local_publication module; t03 owns its lib.rs activation and all production routing with normal non-test compilation and all-target Clippy. Public cross-process engine acceptance occurs only in t03/t04 after that routing exists. No mocked OS lock or prematurely claimed public publication guarantee substitutes for those gates.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch11:t01](tasks/t01-physical-target-locks.md) | characterization | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch11:t02](tasks/t02-same-fs-stage.md) | characterization | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3 | [ch11:t03](tasks/t03-local-mutations-and-publish.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R4 | [ch11:t04](tasks/t04-local-subprocess-acceptance.md) | integration_acceptance | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [x] [8c808beb] storage-completeness:ch11:t01 — Add persistent OS locks per physical parent directory (tasks/t01-physical-target-locks.md)
- [~] storage-completeness:ch11:t02 — Manage SDK-private same-parent staging artifacts (tasks/t02-same-fs-stage.md)
- [ ] storage-completeness:ch11:t03 — Guard every local mutation and publish with native atomic operations (tasks/t03-local-mutations-and-publish.md)
- [ ] storage-completeness:ch11:t04 — Prove local CAS across processes, roots and cleanup boundaries (tasks/t04-local-subprocess-acceptance.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch11-local-atomic-publication` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 4 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/local_publication.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
