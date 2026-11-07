---
type: Spec
parent_flow: storage-completeness
chapter_id: ch20-ordinary-negative-cas
title: Ordinary Byte-Put Negative Predicate CAS
description: Fence arbitrary negative ETag ordinary writes across native providers
status: stable
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 6
plan_commit: e3e344b5
state_revision: 0
last_verified_checkpoint: bdc7799a
last_reviewed_commit: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified: []
sources:
- id: source
  resource: src/rs/vertebra-storage/src/store.rs
  title: Ordinary conditional put implementation
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Source-grounded provider conditions
tags:
- storage
- conditional
- repair
---

# Ordinary Byte-Put Negative Predicate CAS

<!-- truth: start -->
An ordinary negative ETag predicate must be fenced at the actual byte-put mutation. [^source]
Eight bounded native CAS attempts preserve caller-owned Bytes, metadata and actual provider receipts; no HEAD-to-Overwrite fallback is valid. [^research]
Local uses the native physical-parent guard; GCS carries generation and OCI honors real Create/Update headers. [^research]
<!-- truth: end -->

## 1. Specification, Context & Provenance

The shared ordinary byte-put path currently accepts a negative predicate after HEAD and can then overwrite a competing forbidden version. Multipart provider publication tasks do not own this entry point. This bounded chapter owns that existing R1/R9 contract and depends only on the normalized private boundary, native R2 withdrawal and Local guard artifacts, avoiding reopened-chapter completion cycles. [^source]

## 2. Requirements, Concrete Contracts & Architecture

### R1: Fence ordinary negative predicates through actual native CAS

`ObjectStoreEngine::put_opts` normalizes selectors through the ch10 `PublishCondition` boundary before effects. For ordinary `NotMatch(forbidden)` on supported native stores, call the private `put_not_match` helper with caller-owned `Bytes`, resolved path and complete `PutOptions`. The helper runs at most eight compare-and-write attempts: HEAD the current object; if its raw ETag equals `forbidden`, return typed `StorageError::Precondition` without a PUT; otherwise use `PutMode::Update(UpdateVersion { e_tag: existing.e_tag.clone(), version: existing.version.clone() })`. On HEAD NotFound use `PutMode::Create`. Never select Overwrite after a negative predicate, including missing-object and retry paths. If a provider snapshot has no usable ETag/version for its actual CAS primitive, fail visibly rather than inventing a guard.

Retry only an Update precondition failure or a Create already-exists/precondition race, then HEAD and re-evaluate the original logical predicate. Other permission, credential, transport, missing-update, decode and unsupported-capability errors propagate without fallback. On eight CAS failures return typed precondition conflict with an explicit retry-budget message, not a claim that the forbidden ETag matched. Each attempt clones only the existing `Bytes`/PutPayload view and required metadata; no producer replay, stream buffering or new public API. Preserve attributes/content_type on every attempt and return the exact successful provider PutResult ETag/version in StoredObjectMeta, without an unfenced HEAD.

Dispatch Local through the ch11 physical-parent OS-lock publication path: predicate and final ordinary publish share that guard, and the generic loop must never bypass it or revive the old NotImplemented→HEAD→unconditional-write fallback. Memory uses the real native InMemory Create/Update implementation. S3 and Azure ordinary SDK PutMode::Update supply If-Match using the snapshot ETag; configured endpoint, SDK credentials and transport remain intact. GCS negative writes use the captured generation in UpdateVersion as well as the canonical XML ETag; ch14:t01 separately repairs positive if_match and preserves XML/JSON receipt semantics. This shared task does not replace the GCS positive-predicate owner.

The owned OCI ordinary collaborator currently ignores PutOptions. Repair `OciObjectStore::put_opts` in `oci.rs` to honor Overwrite/Create/Update: Create sends `If-None-Match: *`; Update requires the captured ETag and sends `If-Match`; use the existing Cavage request signer/transport. Forward caller content type and metadata attributes on every request as Content-Type and opc-meta-<name> headers. Map a failed Create condition to AlreadyExists and failed Update to Precondition; preserve genuine permission and transport failures. Parse actual response ETag/opc-version-id. The shared negative loop uses this real OCI Update/Create primitive; it must not assume ignored options are atomic. No OCI multipart policy changes belong here.

The concrete private helper and real HTTP/barrier test literals are executable in the single worksheet. S3/Azure publication remains draft until cold review covers this shared prerequisite; native acceptance and original chapter reclosure depend on it. GCS positive if_match remains owned by ch14:t01 rather than being silently absorbed or dropped.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Before chapter review, isolated present-tense sync updates `.agents/bundles/knowledge/domains/storage/object-storage.md` with ordinary negative-CAS semantics, error/retry bounds and backend dispatch truth. Final supported-provider guides remain consolidated by ch04:t03; this chapter records its own implemented boundary before review.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| Original R1/R9, R1 above | [ch20:t01](tasks/t01-ordinary-negative-cas.md) | regression_tdd | Real S3/Azure/OCI HTTP predicates, Memory CAS, Local process guard and GCS generation in canonical native engine tests. |

## 5. Implementation Plan

- [ ] storage-completeness:ch20:t01 — Fence ordinary negative ETag writes through provider CAS (tasks/t01-ordinary-negative-cas.md)

**Chapter Recovery Checkpoint:** focused native verification, documentation sync, correctness review and fresh quality review precede this chapter's review stamp. Native whole-contract acceptance waits for both this ordinary path and all provider multipart repairs.

## 6. Current State & Remaining Plan Snapshot

- Verified source baseline: `bdc7799a`; this revision-6 chapter is draft pending independent cold review and has no production edits.
- Remaining: one bounded ordinary byte-put repair, with three production files and one canonical native test module.
- Documentation: `[0/1 files documented]`, updated before review of implemented behavior.
- ch16 native acceptance and reclosure cannot complete before ch20:t01 and the provider-specific repairs.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Existing ordinary conditional byte-put entry point.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK/native backend capability evidence.
