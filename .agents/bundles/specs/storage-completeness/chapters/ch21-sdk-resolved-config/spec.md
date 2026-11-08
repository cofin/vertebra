---
type: Spec
parent_flow: storage-completeness
chapter_id: ch21-sdk-resolved-config
title: GCS Configuration and Snapshot Requests
description: Preserve the actual GCS SDK endpoint and transport beside its concrete store
status: draft
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: storage-completeness:ch21:t01
plan_revision: 9
plan_commit: null
state_revision: 0
last_verified_checkpoint: 533adc68
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
- id: gcs-request
  resource: src/rs/vertebra-storage/src/gcs_request.rs
  title: Test-only XML snapshots and paired request fences
tags:
- storage
- conditional
- repair
---

# GCS Configuration and Snapshot Requests

<!-- truth: start -->
Read and publication paths share one concrete GoogleCloudStorage Arc and SDK credential owner. [^source]
Resolved endpoint precedence and final ClientOptions follow pinned object_store 0.13.2 behavior. [^research]
Private test-gated requests capture one XML ETag/generation/metageneration tuple and fence mutations and receipts with both versions. [^gcs-request]
Actual SDK/loopback tests verify these primitives; ch14 retains production activation and final publication acceptance.
<!-- truth: end -->

## 1. Specification, Context & Provenance

GCS staging needs resolved configuration that the built SDK does not expose. This chapter retains that configuration beside the concrete SDK store and supplies private XML snapshot/conditional-request primitives. The pinned builder and repository research establish accepted singleton service-account sources, endpoint fallback and final connector ordering. Actual SDK and loopback HTTP tests verify the constructor and request boundaries; production activation belongs to ch14. [^source] [^research] [^gcs-request]

## 2. Requirements, Concrete Contracts & Architecture

### R1: Retain exact SDK-resolved configuration with one SDK build

Private `GcsStageConfig { base: String, bucket: String, client_options: ClientOptions, skip_signature: bool }` in conditional.rs is shared behind Arc beside `PublicationBackend::Gcs`'s `Arc<GoogleCloudStorage>`. ObjectStoreEngine construction resolves this record without changing public BackendConfig or authentication. BaseUrl comes from the fully configured builder first, then accepted singleton service-account `gcs_base_url`, then `https://storage.googleapis.com`. Simultaneous path and inline key sources retain the SDK rejection. Endpoint-only deserialization leaves credentials and refresh with the SDK. `store.rs::CapturingConnector` implements the public `HttpConnector::connect(&ClientOptions) -> object_store::Result<HttpClient>`, clones options into shared capture and delegates unchanged to `ReqwestConnector`. The final capture follows one actual `builder.build()` and retains the object-store options after metadata/auth connector calls. Reads and publication identity share the same built Arc. [^source]

`pub(crate) fn build_gcs_store(builder: GoogleCloudStorageBuilder) -> Result<(Arc<GoogleCloudStorage>, GcsStageConfig), StorageError>` is the actual ObjectStoreEngine construction path. The caller wraps the owned configuration in Arc; inline tests configure real ClientOptions through this same path. The record preserves validated skip_signature, HTTP allowance, proxies/exclusions, TLS, timeouts and default headers. Construction performs no network probe or second SDK build. The private request client uses the same SDK credential owner; ch14 retains acceptance through the actual production wrapper. [^source] [^gcs-request]

Caller → seam → collaborator: ObjectStoreEngine GCS construction and SDK read → private GcsStageConfig/delegating connector → pinned SDK builder, ReqwestConnector and the same concrete GoogleCloudStorage credential provider.

| Edge or failure | Required result |
| --- | --- |
| Explicit/environment BaseUrl plus singleton service account | Configured BaseUrl wins; ordinary SDK read reaches that endpoint. |
| Service-account path or inline key with gcs_base_url | Exact accepted singleton fallback is retained and used by actual SDK read. |
| Path plus inline key | Construction fails with the existing SDK configuration error before external I/O. |
| Metadata/auth connector setup | Capture final object-store options; preserve one actual SDK build and same credential owner. |
| Unconsumed future config field | Non-test builds retain field-local `#[cfg_attr(not(test), expect(dead_code, reason = "consumed by ch14:t01 GCS publication"))]`; ch14:t01 removes every such expectation when production requests consume the fields. |

| Alternative | Decision |
| --- | --- |
| Capture public connector options and narrow endpoint metadata | Chosen: preserves SDK resolution behind two private production files. |
| Rebuild credentials, fork SDK, second build, new public injection API or infer all config from getters | Rejected: changes ownership or omits accepted service-account fallback/final transport options. |

### R2: Fence opaque XML ETags with data and metadata versions

Private test-gated `GcsRequestClient` captures the exact quoted XML ETag, positive generation and positive metageneration in one HEAD. Conditional PUT and exact receipt HEAD use both generation and metageneration predicates; receipt HEAD also pins the generation query. Only unfenced HEAD 404 becomes absence; fenced 404/412 remain errors. Requests preserve raw-key identity through one encoding pass, segmented payloads, attributes and actual response ETag/version. Per-request SDK credentials refresh normally; skip-signature and empty-bearer behavior remain intact. Permission/authentication failures remain distinct from preconditions, and diagnostics redact sensitive context. SDK UpdateVersion has no metageneration predicate and is not a substitute. ch14:t01 activates these real primitives; ch14:t04 accepts destination publication and receipt handling. [^gcs-request]

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented]` for scoped SDK configuration and private snapshot-request knowledge in `.agents/bundles/knowledge/domains/storage/object-storage.md`. ch09:t03 retains final provider consolidation; ch04:t03 owns user guides after native acceptance. This sync records actual SDK/loopback guarantees and the test-only activation boundary. Ordinary engine and multipart publication, live GCS and rebuilt Python acceptance remain open.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch21:t01](tasks/t01-sdk-resolved-config.md) | characterization | Inline actual SDK/local-HTTP construction, capture and comparative lexical endpoint tests in store.rs/conditional.rs/gcs_request.rs. |
| R2 | [ch21:t02](tasks/t02-gcs-snapshot-requests.md) | integration_acceptance | Inline real XML snapshot/PUT/receipt HTTP tests, metadata-only races and exact configuration. |

## 5. Implementation Plan

- [ ] storage-completeness:ch21:t01 — Retain SDK-resolved GCS endpoint and transport configuration (tasks/t01-sdk-resolved-config.md)
- [!] storage-completeness:ch21:t02 — Capture and fence GCS data and metadata snapshots (tasks/t02-gcs-snapshot-requests.md)

**Chapter Recovery Checkpoint:** focused library tests, normal non-test compilation and all-target Clippy precede correctness and fresh quality review. ch14 consumers retain their declared configuration/snapshot dependencies; full read-versus-publication comparison belongs to ch14:t04.

## 6. Current State & Remaining Plan Snapshot

Configuration and snapshot artifacts remain signed at `fe992ae3` and `533adc68`. Correctness review of `85ed9f36..1388109d` identifies a verified lexical endpoint mismatch: a configured prefix with trailing slash produces different actual SDK and private request paths. Revision9 reopens t01 for one atomic three-file capture/request repair, and t02 is blocked until that signed prerequisite. The bounded repair preserves at most three production files, six reads and 250 implementation LOC. The constructor returns owned configuration; the engine and request client share it through Arc beside the same concrete SDK credential owner.

Constructor characterization records three baseline tests, then 14 library and 28 canonical tests with normal compilation, Clippy, formatting and diff checks passing. The combined pre-snapshot checkpoint passes 15 library and 39 canonical tests. Snapshot acceptance records one actual configured SDK/HTTP baseline before the primitive, then eight real request tests; formatting, normal library compilation, all-target Clippy and diff checks exit 0. These source receipts do not establish installed Python acceptance: extensions remain at `c7868b02`.

Scoped knowledge is synchronized at `[1/1 files documented]`. The chapter remains active with `current_task: storage-completeness:ch21:t01` and `last_reviewed_commit: null`; correctness recheck and fresh quality review follow the repair and refreshed Markdown. ch10 is independently completed and reviewed at `1388109d`. ch14 retains first production activation and final publication/configuration/receipt acceptance; ch09:t03/ch04:t03 retain final provider documentation and user guides.

[^source]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine::new` — Actual configured GCS builder and credential precedence.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Pinned SDK builder, endpoint and transport evidence.
[^gcs-request]: `src/rs/vertebra-storage/src/gcs_request.rs::GcsRequestClient` — Test-only XML snapshots, paired generation/metageneration fences, exact receipts and SDK-owned authentication.
