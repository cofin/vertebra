---
type: Spec
parent_flow: storage-completeness
chapter_id: ch10-publication-boundary-and-lifecycle
title: Native Publication Boundary and Cleanup Ownership
description: Native Publication Boundary and Cleanup Ownership
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: storage-completeness:ch10:t04
plan_revision: 7
plan_commit: 5f64d1a8
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
  resource: src/rs/vertebra-storage/src/store.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Native Publication Boundary and Cleanup Ownership

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Native Publication Boundary and Cleanup Ownership addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Retain concrete providers behind a private publication boundary

Add private `conditional.rs` and `mod conditional;` in lib.rs. `ObjectStoreEngine` retains `inner: Arc<dyn ObjectStore>` plus private `publication: PublicationBackend`. The enum retains concrete `Arc` handles for Memory, Local, S3, Azure, GCS and OCI plus an explicit Custom variant for injected stores. Build each SDK store exactly once and clone that same Arc into read and publication fields; clone/to_async/to_sync preserve its credential-provider and client identity. Ordinary reads/lists retain their existing path. No resolved GCS configuration, connector capture or service-account parsing belongs to this task; ch21:t01 owns those additions.

Private `PublishCondition { Overwrite, Create, Match(String), NotMatch(String) }` normalizes `PutConditionalOptions`, rejects simultaneous selectors before effects, and maps `if_none_match="*"` to Create. `PublicationBackend::begin(path, options) -> Result<Box<dyn MultipartUpload>, ObjectStoreError>` dispatches only implemented capabilities. Memory publication stores segmented `PutPayload` parts, builds a cheap view on completion and calls actual native InMemory `PutMode::Overwrite`, `Create` or `Update(UpdateVersion { e_tag: Some(tag), version: None })` with caller attributes. Preserve the actual successful `PutResult`; no full-object concatenation or extra byte copy. Local/S3/Azure/GCS/OCI conditional multipart fail explicitly until their provider chapters implement the required primitive; normal Overwrite remains available. Existing `with_store` keeps injected ordinary PutMode support and explicitly rejects unsupported conditional multipart.

Memory `NotMatch` deliberately returns native NotSupported at BEGIN, before consuming any producer input or changing a destination. This temporary capability boundary is mandatory to remove in ch20:t01, which adds Memory negative multipart completion using the SAME eight-attempt PutPayload CAS helper as ordinary negative puts. This task neither implements a separate negative loop nor claims final Memory negative-predicate acceptance. Retain concrete GCS Arc identity now; ch21:t01 adds the SDK-resolved configuration beside it, and ch14:t04 activates its final publication wrapper.

### R2: Reuse SDK HTTP transport and credential authorizers

Add the real private `cloud_request.rs` implementation with `pub(crate) struct CloudRequestClient { client: object_store::client::HttpClient }`. Its constructors and execution boundary are:

- `CloudRequestClient::new(options: &ClientOptions) -> Result<Self, ObjectStoreError>` calls public `ReqwestConnector::connect(options)` through `HttpConnector`; retain the exact resolved HTTP, proxy, certificate, timeout and header configuration.
- `async fn execute(&self, request: HttpRequest, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` executes the SDK request with `HttpRequestBody`. It returns non-2xx responses intact for the owning provider's status/predicate parser. It does not decide replay or retry publication.
- `async fn execute_aws(&self, request: HttpRequest, credentials: &AwsCredentialProvider, region: &str, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` obtains `credentials.get_credential().await` on every invocation, uses `AwsAuthorizer::new(credential.as_ref(), "s3", region).authorize(&mut request, None)`, then delegates to `execute`. S3 is its first production consumer. Azure and GCS wrappers perform their own SDK credential lookup/authorization before calling `execute`; do not introduce unused provider helpers here.

Map transport and credential-acquisition failures to credential-free storage errors with static operation and SDK error-kind context; never format or attach a signed request URI, Authorization, credential Debug, raw transport source chain or server body that can echo secrets. HTTP status parsing stays with the provider, which may preserve safe status/code context without forwarding untrusted response bodies. No custom signing, SDK fork or replacement reqwest client is permitted.

In `lib.rs`, register this complete module under exactly `#[cfg(test)] mod cloud_request;` for this task. Its real SDK/network tests compile through the library test target; it is not wired into runtime publication yet. ch12:t01 owns removal of this single test-only gate when the first actual S3 initiation caller lands. No broad dead-code allowance, public export, placeholder request implementation or claim of live provider publication is permitted. ch12:t01 must run normal non-test compilation and all-target Clippy after activation; unused future provider APIs are implemented only when their consuming wrapper exists.

The owned test seam is this private external-request boundary, with inline `cloud_request::tests` in `cloud_request.rs`. For credential injection, construct a real `AmazonS3` via public `AmazonS3Builder::with_bucket_name("unit").with_region("us-east-1").with_endpoint(local_url).with_allow_http(true).with_credentials(provider).build()`; `provider` is an `Arc<dyn object_store::client::CredentialProvider<Credential = AwsCredential>>` test collaborator whose `get_credential` returns distinct `Arc<AwsCredential>` values on successive calls. Its only substitution is the external credential supplier. Pass `sdk.credentials()` to `execute_aws`, not an erased `ObjectStoreEngine::with_store`, a mock request client or a mocked signing function. A real local HTTP responder captures requests and supplies bounded response/failure sequences.

Caller → seam → collaborator: inline native transport acceptance → real `CloudRequestClient::{execute,execute_aws}` → SDK connector/authorizer/credential provider and local HTTP responder. Public ObjectStore publication acceptance is assigned to ch12:t01 and ch13:t01–t02 after their implementations exist.


### R3: Retain multipart abort ownership across completion errors and cancellation

`MultipartUploadSession::complete` borrows the retained handle through `self.upload.as_mut()` across tail upload and `upload.complete().await`, and clears it only after successful completion. An equivalent RAII restore-on-drop guard is permitted only if it also preserves the handle when the future is cancelled; taking ownership out and restoring only on returned Err is insufficient. Publication implementations retain stage/upload handles until published or cleanup succeeds, with async Drop ownership spawning bounded cleanup on the existing storage runtime for dropped/cancelled sessions. Rust/PyO3 sync and async wrappers leave abort callable after a failed complete; Python put_stream finally invokes abort on producer error or cancellation and never masks original failure. Completed success clears ownership; abort remains idempotent. No shared global Python lock or unbounded Drop blocking.

### R4: Frame S3 producer chunks and preserve bounded backend policies

Pinned S3 MultipartUpload::put_part forwards each caller PutPayload as one service part, requiring at least 5 MiB for each non-final part. MultipartUploadSession frames arbitrary S3 producer chunks with a private PutPayloadMut accumulator targeting 8 MiB. Slice incoming Bytes, retain only the current partial part and caller-owned input, and await each flush before consuming more producer data. Complete flushes a short final tail and handles an empty stream while preserving abort ownership. Enforce S3's 10000-part limit before sending a disallowed part; preserve OCI's separate owned 8 MiB/count policy. Azure/Local/Memory retain their native semantics. GCS conditional publication delegates framing to ch14's bounded 8 MiB JSON resumable stage with 256 KiB-aligned non-final requests and exact-frame EOF finalization; no XML multipart 10000-part cap applies to that session. This task does not require future GCS resumable public acceptance before its implementation. No whole-object buffer, unbounded in-flight tasks, consuming finish that loses abort ownership, maximum-object-size promise or single-PUT fallback is introduced.

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

ch21:t01 owns GCS SDK-resolved service-account endpoint and ClientOptions capture after ch10:t01. ch14 owns downstream resumable staging and final read-versus-publication configuration acceptance.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch10:t01](tasks/t01-publication-dispatch.md) | regression_tdd | Canonical public S3 HTTP framing tests plus inline store.rs actual private-session count-limit test |
| R2 | [ch10:t02](tasks/t02-sdk-http-transport.md) | behavior_tdd | Real inline `cloud_request::tests`; runtime activation and provider acceptance by ch12:t01/ch13:t01–t02. |
| R3 | [ch10:t03](tasks/t03-session-cleanup.md) | regression_tdd | Canonical public S3 HTTP framing tests plus inline store.rs actual private-session count-limit test |

| R4 | [ch10:t04](tasks/t04-minimum-cloud-part-framing.md) | regression_tdd | Canonical public S3 HTTP framing tests plus inline store.rs actual private-session count-limit test |

## 5. Implementation Plan

- [x] [d50cf6a2] storage-completeness:ch10:t01 — Retain concrete providers behind a private publication boundary (tasks/t01-publication-dispatch.md)
- [x] [2bc92864] storage-completeness:ch10:t02 — Reuse SDK HTTP transport and credential authorizers (tasks/t02-sdk-http-transport.md)
- [x] [673cb696] storage-completeness:ch10:t03 — Retain multipart abort ownership across completion errors and cancellation (tasks/t03-session-cleanup.md)

- [~] storage-completeness:ch10:t04 — Frame S3 producer chunks and preserve bounded backend policies (tasks/t04-minimum-cloud-part-framing.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch10-publication-boundary-and-lifecycle` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 4 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
