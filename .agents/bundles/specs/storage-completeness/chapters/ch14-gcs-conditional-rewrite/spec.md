---
type: Spec
parent_flow: storage-completeness
chapter_id: ch14-gcs-conditional-rewrite
title: GCS Generation CAS and Staged Rewrite Publication
description: GCS Generation CAS and Staged Rewrite Publication
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 8
plan_commit: null
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
  resource: src/rs/vertebra-storage/src/gcs_stage.rs
  title: Generation-fenced resumable staging and publication
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: SDK and provider protocol evidence
tags:
- storage
- repair
---

# GCS Generation-Fenced Staging and Rewrite

<!-- truth: start -->
Caller ETags use the existing quoted XML namespace and same-snapshot generation/metageneration fences.
Private cloud stages use JSON resumable insertion with ifGenerationMatch=0.
Publication pins source data and destination data/metadata versions, then obtains an XML receipt fenced by both versions.
Native implementation requires correctness review followed by fresh independent quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Preserve native storage conditional writes, bounded streaming, opaque keys, metadata and exact publication receipts. XML multipart completion cannot enforce Create, so private stages use the supported JSON resumable protocol. The tasks retain explicit ownership and actual protocol evidence [^research].

## 2. Requirements, Concrete Contracts & Architecture

### R1: Fence ordinary GCS predicates with the same HEAD generation

Caller ETag comparison uses SDK XML HEAD metadata. Positive Match and negative NotMatch use the shared bounded logical CAS loop with a real private GCS snapshot/request backend. SDK UpdateVersion ignores e_tag and cannot protect metadata-only changes. Create remains native Create. Rewrite receipt tests belong to t04 after publication exists.

### R2: Create bounded private stages atomically

JSON resumable initiation carries ifGenerationMatch=0, content_type and metadata. Retain opaque Location capability and bounded 8 MiB/256 KiB-aligned frames. Preserve empty, partial-tail and exact-frame EOF. Initiation collisions can change names only before producer consumption; late conflicts never replay consumed input. Use exact configured SDK transport and credentials, including skip-signature and service-account endpoint fallback. ch21:t01 first retains the exact SDK-resolved configuration with actual read/config tests. t02 depends on that task, compiles the actual private protocol under a test-only module and tests real HTTP requests.

### R3: Retain recovery and cleanup ownership

Keep the pending frame before sending and recover acknowledgement via session status probes. Bound recovery attempts; validate offsets. Session cancellation uses JSON HTTP 499. Finalized cleanup pins both object generation and ifGenerationMatch. Uncertain finalization without a captured generation retains an explicit indeterminate outcome; a random key does not authorize deleting another object. Session credentials and untrusted bodies never appear in errors. t03 finishes lifecycle behavior before runtime activation.

### R4: Publish exact staged generations and return canonical XML tokens

The production wrapper starts only after both private stage tasks pass. JSON rewrite pins source generation and BOTH destination generation/metageneration conditions across every rewriteToken continuation. Absent NotMatch uses generation-match0; absent Match fails before mutation. Caller ETags are compared only with XML SDK metadata. After completion, private XML HEAD pins the returned generation AND metageneration and returns that exact XML snapshot; a metadata-only change produces explicit receipt failure. Missing generation receipts fail explicitly; current HEAD or JSON resource.etag never substitutes for our result. Caller metadata persists and cleanup targets only the owned stage generation. Temporary cloud stages may be visible in provider listings. t04 inline real-wrapper tests compare configured SDK reads with actual resumable/rewrite requests, including custom headers and rotating external credentials. Canonical public-engine tests use only exposed configuration options. t01 removes exact config-field expectations when it activates the first production request consumer.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. ch09:t03 documents evidence-backed storage behavior after native ch16 acceptance; ch04:t03 owns user storage guides. No platform, maximum-object-size or cloud listing-invisibility guarantee is invented.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Boundary |
| --- | --- | --- | --- |
| R1 | [ch14:t01](tasks/t01-gcs-ordinary-cas.md) | regression_tdd | Actual native ordinary puts and GCS HTTP generation headers |
| R2 | [ch14:t02](tasks/t02-gcs-stage-protocol.md) | integration_acceptance | Real private SDK/HTTP resumable stage |
| R3 | [ch14:t03](tasks/t03-gcs-stage-lifecycle.md) | regression_tdd | Real private HTTP recovery/cancellation/cleanup |
| R4 | [ch14:t04](tasks/t04-gcs-rewrite-publication.md) | regression_tdd | Public native multipart publication |

## 5. Implementation Plan

- [ ] storage-completeness:ch14:t01 — Resolve ordinary GCS ETags into same-snapshot generation CAS (tasks/t01-gcs-ordinary-cas.md)
- [ ] storage-completeness:ch14:t02 — Create generation-fenced resumable stages (tasks/t02-gcs-stage-protocol.md)
- [ ] storage-completeness:ch14:t03 — Recover and clean up retained resumable sessions (tasks/t03-gcs-stage-lifecycle.md)
- [ ] storage-completeness:ch14:t04 — Publish staged generations and return canonical XML receipts (tasks/t04-gcs-rewrite-publication.md)

## 6. Current State & Remaining Plan Snapshot

All four tasks remain pending at the verified R2-removal baseline. The revised frontier awaits independent executable-plan review. ch21:t02 provides test-gated snapshot primitives activated by t01; t02/t03 provide test-gated resumable primitives activated by t04 and runs non-test compilation. ch16 acceptance depends on t04, never the incomplete staging predecessor.

[^source]: `src/rs/vertebra-storage/src/gcs_stage.rs` — Owned staging implementation introduced by this chapter.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK and primary provider protocol evidence.
