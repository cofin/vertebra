---
type: Spec
parent_flow: storage-completeness
chapter_id: ch21-sdk-resolved-config
title: GCS Configuration and Snapshot Requests
description: Preserve the actual GCS SDK endpoint and transport beside its concrete store
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: storage-completeness:ch21:t02
plan_revision: 8
plan_commit: c737ffeb
state_revision: 0
last_verified_checkpoint: 34113093
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
  title: GCS construction and concrete publication identity
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Source-grounded provider conditions
tags:
- storage
- conditional
- repair
---

# GCS Configuration and Snapshot Requests

<!-- truth: start -->
Read and publication paths share one concrete GoogleCloudStorage Arc and SDK credential owner. [^source]
Resolved endpoint precedence and final ClientOptions follow pinned object_store 0.13.2 behavior. [^research]
This chapter tests actual SDK reads and captured configuration; ch14:t04 tests final publication requests.
<!-- truth: end -->

## 1. Specification, Context & Provenance

GCS staging needs resolved configuration that the built SDK does not expose. Retaining concrete identity in ch10:t01 already consumes a bounded implementation task; this chapter owns the separate connector/configuration work without weakening either contract. The pinned builder and repository research establish accepted singleton service-account sources, endpoint fallback and final connector ordering. [^source] [^research]

## 2. Requirements, Concrete Contracts & Architecture

### R1: Retain exact SDK-resolved configuration with one SDK build

Add private `GcsStageConfig { base: Url, bucket: String, client_options: ClientOptions, skip_signature: bool }` in conditional.rs behind Arc beside `PublicationBackend::Gcs`'s existing `Arc<GoogleCloudStorage>`. ObjectStoreEngine construction resolves this record without changing public BackendConfig or authentication. Resolve BaseUrl from the fully configured builder first, then accepted service-account `gcs_base_url`, then `https://storage.googleapis.com`. Both path and inline key configured must retain the SDK rejection. Narrow endpoint-only deserialization ignores credential fields and never owns credentials or refresh. Use a real public delegating `HttpConnector` in store.rs: `connect(&ClientOptions) -> object_store::Result<HttpClient>` clones the options into shared capture and calls `ReqwestConnector.connect(options)` unchanged. Read the final build-time capture after the ONE actual `builder.build()`; earlier metadata/auth connector calls cannot become the stored object-store options. Retain the same built Arc in both fields.

Use `pub(crate) fn build_gcs_store(builder: GoogleCloudStorageBuilder) -> Result<(Arc<GoogleCloudStorage>, GcsStageConfig), StorageError>` as the actual ObjectStoreEngine construction path; the caller wraps the owned configuration in Arc for shared publication identity, and inline tests configure real ClientOptions through the same production path. Read skip_signature from the configured builder and preserve its validation; capture HTTP allowance, proxies/exclusions, TLS, timeouts and default headers exactly. Constructor work performs no network probe or second SDK build. Future publication authenticates through the same SDK credential owner, including skip-signature and empty-bearer behavior, which ch14 accepts at its actual wrapper seam.

Caller → seam → collaborator: ObjectStoreEngine GCS construction and SDK read → private GcsStageConfig/delegating connector → pinned SDK builder, ReqwestConnector and the same concrete GoogleCloudStorage credential provider.

| Edge or failure | Required result |
| --- | --- |
| Explicit/environment BaseUrl plus singleton service account | Configured BaseUrl wins; ordinary SDK read reaches that endpoint. |
| Service-account path or inline key with gcs_base_url | Exact accepted singleton fallback is retained and used by actual SDK read. |
| Path plus inline key | Construction fails with the existing SDK configuration error before external I/O. |
| Metadata/auth connector setup | Capture final object-store options; preserve one actual SDK build and same credential owner. |
| Unconsumed future config field | If non-test compilation needs an expectation, use exact field-local `#[cfg_attr(not(test), expect(dead_code, reason = "consumed by ch14:t01 GCS requests"))]`; ch14:t01 removes every such expectation. |

| Alternative | Decision |
| --- | --- |
| Capture public connector options and narrow endpoint metadata | Chosen: preserves SDK resolution behind two private production files. |
| Rebuild credentials, fork SDK, second build, new public injection API or infer all config from getters | Rejected: changes ownership or omits accepted service-account fallback/final transport options. |

### R2: Fence opaque XML ETags with data and metadata versions

A private, initially test-gated GcsRequestClient captures quoted XML ETag, generation and metageneration in one HEAD. Ordinary conditional PUT and exact receipt HEAD use both generation and metageneration predicates. SDK UpdateVersion alone cannot express this contract. ch14:t01 activates these real primitives; ch14:t04 consumes them for destination and receipt handling. Credentials and transport remain SDK-owned.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. ch09:t03 records only accepted source-backed SDK identity/configuration guarantees in `.agents/bundles/knowledge/domains/storage/object-storage.md`; ch04:t03 owns user guides after native acceptance. This chapter's real SDK/local-HTTP tests document constructor behavior without premature publication claims.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch21:t01](tasks/t01-sdk-resolved-config.md) | characterization | Inline actual SDK/local-HTTP construction, read and captured-config tests in store.rs/conditional.rs. |
| R2 | [ch21:t02](tasks/t02-gcs-snapshot-requests.md) | integration_acceptance | Inline real XML snapshot/PUT/receipt HTTP tests, metadata-only races and exact configuration. |

## 5. Implementation Plan

- [x] [fe992ae3] storage-completeness:ch21:t01 — Retain SDK-resolved GCS endpoint and transport configuration (tasks/t01-sdk-resolved-config.md)
- [~] storage-completeness:ch21:t02 — Capture and fence GCS data and metadata snapshots (tasks/t02-gcs-snapshot-requests.md)

**Chapter Recovery Checkpoint:** focused library tests, normal non-test compilation and all-target Clippy precede correctness and fresh quality review. ch14:t02 depends on this task; full read-versus-publication comparison belongs to ch14:t04.

## 6. Current State & Remaining Plan Snapshot

Two bounded tasks own SDK configuration and real snapshot requests separately; each has at most two production files, six reads and 250 implementation LOC. The concrete dispatch predecessor ch10:t01 remains independently executable. The revision8 request frontier is independently approved at `c737ffeb`; configuration capture is completed at `fe992ae3`, and snapshot requests are in progress. Full-chapter correctness and fresh quality reviews follow both tasks.

[^source]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine::new` — Actual configured GCS builder and credential precedence.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Pinned SDK builder, endpoint and transport evidence.
