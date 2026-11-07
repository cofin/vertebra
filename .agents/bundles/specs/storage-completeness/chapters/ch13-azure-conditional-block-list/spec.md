---
type: Spec
parent_flow: storage-completeness
chapter_id: ch13-azure-conditional-block-list
title: Azure Conditional Multipart Block Commit
description: Azure Conditional Multipart Block Commit
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 6
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
  resource: src/rs/vertebra-storage/src/azure_publication.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Azure Conditional Multipart Block Commit

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Azure Conditional Multipart Block Commit addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Retain Azure SDK block identifiers and metadata for publication

Add private AzurePublicationUpload with Arc<MicrosoftAzure>, resolved account/container/endpoint, SDK MultipartId, ordered PartIds, condition, attributes/tags and SDK transport. Initiate and upload blocks through public MultipartStore APIs, preserving each returned content_id exactly for final block list. Credential access is store.credentials().get_credential() and authorization uses public AzureAuthorizer, covering access-key, bearer and SAS credential forms without handcrafted signing. Carry caller content_type/metadata/tags to t02 commit. Retain cleanup ownership after part failures and cancellation.

The real wrapper constructor accepts `Arc<MicrosoftAzure>` rather than an erased injected store: `async AzurePublicationUpload::begin(store: Arc<MicrosoftAzure>, config: AzurePublicationConfig, path: StorePath, options: PutMultipartOptions, condition: PublishCondition) -> Result<Self, ObjectStoreError>`, with private `AzurePublicationConfig { account: String, container: String, endpoint: String, client_options: ClientOptions }`. Production dispatch and inline wrapper tests use the same constructor. Build test SDK stores via `MicrosoftAzureBuilder::with_account("unit").with_container_name("assets").with_endpoint(local_url).with_allow_http(true).with_credentials(provider).build()`. For SAS use `Arc::new(StaticCredentialProvider::new(AzureCredential::SASToken(vec![("sig".into(), "test-only-signature".into())])))`; bearer uses `AzureCredential::BearerToken("test-only-token".into())`. For refresh, the external `CredentialProvider<Credential = AzureCredential>` collaborator returns distinct credentials on successive lookups. No public Python config field, engine test hook or compatibility API is added. The real SDK block requests reach a local responder and preserve endpoint/client options; ch13:t02 uses this same concrete configuration for conditional commit and error-redaction acceptance.

SDK `PartId.content_id` is raw block-ID text, not base64. Preserve that exact raw value in the ordered PartId vector throughout t01/t02. At the block-list wire boundary only, encode each raw identifier once with `base64::engine::general_purpose::STANDARD` and write `<Uncommitted>encoded-id</Uncommitted>`, matching the pinned SDK's newly uploaded-block contract. SDK block PUT already base64-encodes this same raw value in its `blockid` query. Never pre-encode stored PartIds, place raw IDs into XML, or base64 an already encoded wire token. Canonical HTTP assertions decode each captured blockid query and each ordered XML element once and compare the exact original raw SDK identifiers; this catches omitted and double encoding without asserting the random ID-generation algorithm.

### R2: Commit Azure block list atomically with metadata and conditions

Complete using PutBlockList through CloudRequestClient with `<Uncommitted>` identifiers base64-encoded exactly once from raw SDK PartIds. Send If-None-Match:* for Create and If-Match expected for Match; NotMatch sends native If-None-Match:<forbidden ETag> directly; Azure supports that exact condition, so no HEAD/unconditional publication is needed. Carry content type, metadata attributes and tags in commit headers; preserve SDK-authorized conditional/header naming. Parse actual ETag/version response into PutResult. Map create conflict versus update precondition accurately; permission errors remain genuine errors. Abort/cancellation retain staged-block ownership according to provider cleanup capability and documented Azure uncommitted-block lifecycle, with no fabricated explicit delete of competing committed blob.

Use the concrete SDK credential injection and actual wrapper constructor owned by t01 for end-to-end acceptance. Fetch `store.credentials().get_credential()` immediately before every commit attempt, run public `AzureAuthorizer::new(credential.as_ref(), &account).authorize(&mut request)`, preserve `credential.sensitive_request()` semantics, then call the live `CloudRequestClient::execute`; no custom SAS URL/signature logic or cached Authorization. Real local HTTP responses distinguish genuine 403 permission denial from 412 conditional conflict. Error context includes only static provider/operation, status and safe recognized protocol code; never serialize the signed URI, SAS query, Authorization, credential Debug or untrusted echoed response body/source chain. The real SAS fixture's responder intentionally echoes the signed request target in its rejection body, so the test proves actual remote-error redaction rather than scanning an invented error string.

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

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch13:t01](tasks/t01-azure-sdk-parts.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch13:t02](tasks/t02-azure-conditional-commit.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [ ] storage-completeness:ch13:t01 — Retain Azure SDK block identifiers and metadata for publication (tasks/t01-azure-sdk-parts.md)
- [ ] storage-completeness:ch13:t02 — Commit Azure block list atomically with metadata and conditions (tasks/t02-azure-conditional-commit.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch13-azure-conditional-block-list` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Shared ordinary negative-CAS prerequisite ch20:t01 is explicit; this provider frontier remains draft pending independent cold review of the complete ownership map.
- Remaining: all 2 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/azure_publication.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
