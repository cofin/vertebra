---
type: Spec
parent_flow: storage-completeness
chapter_id: ch08-remove-r2-native
title: Move OCI Chunk Ownership and Remove Native R2
description: Move OCI Chunk Ownership and Remove Native R2
status: stable
state: completed
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 4
plan_commit: ddfb11f2
state_revision: 0
last_verified_checkpoint: c7868b02
last_reviewed_commit: 43654f61
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified:
- correctness_review: 43654f61
- quality_review: 43654f61
sources:
- id: source
  resource: src/rs/vertebra-storage/src/store.rs
  title: Grounded implementation seam
tags:
- storage
- repair
---

# Move OCI Chunk Ownership and Remove Native R2

<!-- truth: start -->
OCI owns uniform multipart accumulation; native constructors reject R2 URLs, and the native R2 module/helper/stub surface is absent. [^source]
All three task implementations are signed through `c7868b02`; native tests and extension parity pass.
This chapter stays active pending correctness review followed by fresh quality review; no review stamp is claimed.
SQLSpec logical-URI and configured native-store rejection remains a ch07 repair. Native conditional publication and Arrow acceptance remain separate.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Move OCI Chunk Ownership and Remove Native R2 addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

### R1: Transfer uniform multipart accumulation into OCI ownership

Move the incremental `UniformPartAccumulator` and fixed 8 MiB constant to `oci.rs` with OCI-specific naming (`OCI_UNIFORM_PART_SIZE`). `store.rs::create_multipart` uses uniform re-chunking only for scheme oci. Preserve existing helper exports still called by lib.rs until the next task; no Python behavior depends on moving them. Do not retain a whole-payload Vec or a 1 GiB buffer. Renaming canonical tests to OCI must retain observable part sizes and exact reconstructed payload assertions. This task stays green while r2.rs still exists; remove only its production accumulator caller, then remove the file in t03.

### R2: Delete native R2 builder, module, helper and stub surface

Remove BackendConfig::R2, its constructor arm and from_url r2 handling; r2 URLs fail through the existing unsupported-scheme branch. Remove `pub mod r2`, helper imports, `py_chunk_r2_uniform_parts` registration/body and stub declaration/__all__ entry. Keep the now-unreferenced r2.rs file until the explicit dead-file task t03; do not delete a fourth production file here. Rust S3 signing tests use generic S3 endpoints, not R2-specific identity. Preserve OCI-owned 8 MiB accumulation and generic S3 endpoint behavior.

### R3: Delete the unreferenced native R2 module

Delete `src/rs/vertebra-storage/src/r2.rs` only after native module/import/helper withdrawal. Verify no source imports, registration or storage plan provenance points to r2.rs, and keep OCI accumulator in oci.rs. Do not remove generic S3 endpoint or signing support. The production file count includes this deletion.

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

Native removal tasks keep intermediate builds coherent: t01 removes every OCI-to-R2 reexport and converts helper tests; t02 withdraws the last lib.rs helper/module registration with its stub; t03 deletes the now-unreferenced source and updates durable storage knowledge links to OCI ownership.

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented]`. Native constructor rejection, removed exports and OCI multipart ownership are synchronized in `.agents/bundles/knowledge/domains/storage/object-storage.md`. Final provider consolidation remains in ch09:t03 and user guides in ch04:t03; this scoped sync does not close their acceptance.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch08:t01](tasks/t01-oci-accumulator-ownership.md) | characterization | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch08:t02](tasks/t02-remove-native-backend.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3 | [ch08:t03](tasks/t03-delete-dead-r2-module.md) | characterization | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [x] [2e343bc3] storage-completeness:ch08:t01 — Transfer uniform multipart accumulation into OCI ownership (tasks/t01-oci-accumulator-ownership.md)
- [x] [4614213f] storage-completeness:ch08:t02 — Delete native R2 builder, module, helper and stub surface (tasks/t02-remove-native-backend.md)
- [x] [c7868b02] storage-completeness:ch08:t03 — Delete the unreferenced native R2 module (tasks/t03-delete-dead-r2-module.md)

**Chapter Recovery Checkpoint:** all task receipts are completed. `/flow:review storage-completeness ch08-remove-r2-native` performs correctness review; a fresh quality review follows before chapter closure.

## 6. Current State & Remaining Plan Snapshot

- Revision 4 is independently approved at `ddfb11f2`; task receipts bind OCI ownership to `2e343bc3`, native API withdrawal to `4614213f`, and dead-module/provenance deletion to `c7868b02`.
- The native suite passes all 25 cases before and after dead-module deletion; formatting and all-target Clippy exit 0. All five extension builds and stubtest pass at the native API withdrawal checkpoint; deleting the unregistered file changes no compiled source.
- Scoped documentation is synchronized: `[1/1 files documented]`. Correctness and fresh quality reviews remain pending; state stays active and last_reviewed_commit stays null.
- SQLSpec logical-R2 URI and configured native-store rejection remains a ch07 repair. Native CAS, original chapter reclosure and Arrow acceptance remain open and do not block this native removal review.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.
