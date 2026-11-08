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
current_task: null
plan_revision: 7
plan_commit: 5f64d1a8
state_revision: 0
last_verified_checkpoint: fe992ae3
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
- id: publication
  resource: src/rs/vertebra-storage/src/conditional.rs::PublicationBackend
  title: Concrete provider dispatch and Memory publication
- id: transport
  resource: src/rs/vertebra-storage/src/cloud_request.rs::CloudRequestClient
  title: Test-only SDK request transport
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Native Publication Boundary and Cleanup Ownership

<!-- truth: start -->
The four publication-core tasks are signed and verified: concrete provider identity, native Memory Create/Match, real test-only SDK transport, retained cleanup ownership and bounded S3 part framing. [^source] [^publication] [^transport]
Memory NotMatch multipart fails before producer consumption until ch20:t01; Local/cloud conditional publication remains provider-owned work.
The chapter stays active with no review stamp until correctness review and fresh quality review cover code and synchronized Markdown.
<!-- truth: end -->

## 1. Specification, Context & Provenance

The publication core retains concrete provider identity, enforces the implemented Memory predicates, and preserves multipart cleanup ownership. Each completed worksheet records its signed source commit and native verification evidence. Reopened ch01 acceptance remains independent; provider publication and installed Python acceptance remain downstream obligations. The source/Markdown review range starts at `4867dfb9` and includes all four task commits plus this documentation synchronization.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Retain concrete providers behind a private publication boundary

Private `conditional.rs` defines `PublicationBackend`, and `lib.rs` registers it with `mod conditional;`. `ObjectStoreEngine` retains `inner: Arc<dyn ObjectStore>` plus private `publication: PublicationBackend`. The enum retains concrete `Arc` handles for Memory, Local, S3, Azure, GCS and OCI plus an explicit Custom variant for injected/HTTP stores. Each SDK store is built once and shared between read and publication fields; clone/to_async/to_sync preserve its credential-provider and client identity. Ordinary reads/lists retain their existing path. SDK-resolved GCS configuration capture is separate ch21:t01 work; its signed constructor artifact is `fe992ae3`.

Private `PublishCondition { Overwrite, Create, Match(String), NotMatch(String) }` normalizes `PutConditionalOptions`, rejects simultaneous selectors before effects, and maps `if_none_match="*"` to Create. `PublicationBackend::begin(path, options) -> Result<Box<dyn MultipartUpload>, ObjectStoreError>` dispatches only implemented capabilities. Memory publication stores segmented `PutPayload` parts, builds a cheap view on completion and calls actual native InMemory `PutMode::Overwrite`, `Create` or `Update(UpdateVersion { e_tag: Some(tag), version: None })` with caller attributes. It preserves the actual successful `PutResult` without full-object concatenation or extra byte copies. Local/S3/Azure/GCS/OCI conditional multipart fail explicitly until their provider chapters implement the required primitive; normal Overwrite remains available. Existing `with_store` keeps injected ordinary PutMode support and explicitly rejects unsupported conditional multipart.

Memory `NotMatch` returns native NotSupported at BEGIN, before consuming any producer input or changing a destination. ch20:t01 owns Memory negative multipart completion through the same eight-attempt PutPayload CAS helper as ordinary negative puts. Final Memory negative-predicate acceptance remains open. Concrete GCS Arc identity is retained; ch21 owns resolved configuration/snapshots and ch14 owns production publication activation and acceptance.

### R2: Reuse SDK HTTP transport and credential authorizers

Private `cloud_request.rs` implements `pub(crate) struct CloudRequestClient { client: object_store::client::HttpClient }`. Its constructors and execution boundary are:

- `CloudRequestClient::new(options: &ClientOptions) -> Result<Self, ObjectStoreError>` calls public `ReqwestConnector::connect(options)` through `HttpConnector`; retain the exact resolved HTTP, proxy, certificate, timeout and header configuration.
- `async fn execute(&self, request: HttpRequest, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` executes the SDK request with `HttpRequestBody`. It returns non-2xx responses intact for the owning provider's status/predicate parser. It does not decide replay or retry publication.
- `async fn execute_aws(&self, request: HttpRequest, credentials: &AwsCredentialProvider, region: &str, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` obtains `credentials.get_credential().await` on every invocation, uses `AwsAuthorizer::new(credential.as_ref(), "s3", region).authorize(&mut request, None)`, then delegates to `execute`. S3 is its first production consumer. Azure and GCS wrappers perform their own SDK credential lookup/authorization before calling `execute`; do not introduce unused provider helpers here.

Transport and credential-acquisition failures map to credential-free storage errors with static operation and SDK error-kind context. Signed request URIs, Authorization, credential Debug and raw transport source chains are absent from those errors. HTTP status/body responses remain available to the owning provider parser, whose acceptance must prevent untrusted body leakage. The transport uses the public SDK connector/authorizer.

`lib.rs` registers the complete module under exactly `#[cfg(test)] mod cloud_request;`. Real SDK/network tests compile through the library test target. Runtime publication activation belongs to ch12:t01, which removes that gate with the first actual S3 initiation caller and runs normal non-test compilation plus all-target Clippy. Live provider publication remains unaccepted by this transport slice.

The owned test seam is this private external-request boundary, with inline `cloud_request::tests` in `cloud_request.rs`. For credential injection, construct a real `AmazonS3` via public `AmazonS3Builder::with_bucket_name("unit").with_region("us-east-1").with_endpoint(local_url).with_allow_http(true).with_credentials(provider).build()`; `provider` is an `Arc<dyn object_store::client::CredentialProvider<Credential = AwsCredential>>` test collaborator whose `get_credential` returns distinct `Arc<AwsCredential>` values on successive calls. Its only substitution is the external credential supplier. Pass `sdk.credentials()` to `execute_aws`, not an erased `ObjectStoreEngine::with_store`, a mock request client or a mocked signing function. A real local HTTP responder captures requests and supplies bounded response/failure sequences.

Caller → seam → collaborator: inline native transport acceptance → real `CloudRequestClient::{execute,execute_aws}` → SDK connector/authorizer/credential provider and local HTTP responder. Public ObjectStore publication acceptance is assigned to ch12:t01 and ch13:t01–t02 after their implementations exist.


### R3: Retain multipart abort ownership across completion errors and cancellation

`MultipartUploadSession::complete` borrows the retained handle through `self.upload.as_mut()` across tail upload and `upload.complete().await`, and clears it only after successful completion. `abort` retains the handle across pending/error results and clears it on success or NotFound; repeated abort after closure is a no-op. Failed/cancelled completion leaves abort callable. An abort error after an earlier operation failure preserves the original failure and retains cleanup ownership. Drop transfers the handle to one asynchronous abort attempt on `STORAGE_RUNTIME` with a 30-second timeout, without blocking the caller. Provider failure/timeout can prevent cleanup. Existing Rust/PyO3 wrappers share the same retained session, and Python put_stream already attempts abort on producer error/cancellation; installed Python and provider-specific cleanup acceptance remain later gates.

### R4: Frame S3 producer chunks and preserve bounded backend policies

Pinned S3 MultipartUpload::put_part forwards each caller PutPayload as one service part, requiring at least 5 MiB for each non-final part. MultipartUploadSession frames arbitrary S3 producer chunks with a private PutPayloadMut accumulator targeting 8 MiB. It slices incoming Bytes, retains the current partial part and caller-owned input, and awaits each flush before consuming more producer data. Complete flushes a short final tail and handles an empty stream while preserving abort ownership. Both frame and tail paths enforce S3's 10000-part limit before sending a disallowed part. OCI's separate private 8 MiB/count policy and Azure/Local/Memory semantics retain their existing behavior. GCS conditional publication framing remains ch14's bounded 8 MiB JSON resumable-stage obligation, with 256 KiB-aligned non-final requests and exact-frame EOF finalization; no XML multipart 10000-part cap applies to that planned session. S3 framing has no whole-object buffer or unbounded in-flight uploads.

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

Progress: `[1/1 files documented]` for this implemented publication core. [Object storage knowledge](../../../../knowledge/domains/storage/object-storage.md) records concrete provider identity, Memory Create/Match and negative-predicate refusal, test-only transport staging, cleanup limits and S3 framing. Canonical tests record observable source behavior. ch09:t03 retains final provider consolidation; ch04:t03 retains HTTP/Arrow consumer guides. The root final acceptance count remains `[9/13 files documented]`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch10:t01](tasks/t01-publication-dispatch.md) | regression_tdd | Canonical Memory Create/Match multipart races and concrete provider dispatch |
| R2 | [ch10:t02](tasks/t02-sdk-http-transport.md) | characterization | Real inline `cloud_request::tests`; runtime activation and provider acceptance by ch12:t01/ch13:t01–t02. |
| R3 | [ch10:t03](tasks/t03-session-cleanup.md) | regression_tdd | Canonical completion-error, cancellation and abandoned-session abort regressions |

| R4 | [ch10:t04](tasks/t04-minimum-cloud-part-framing.md) | regression_tdd | Canonical public S3 HTTP framing tests plus inline store.rs actual private-session count-limit test |

## 5. Implementation Plan

- [x] [d50cf6a2] storage-completeness:ch10:t01 — Retain concrete providers behind a private publication boundary (tasks/t01-publication-dispatch.md)
- [x] [2bc92864] storage-completeness:ch10:t02 — Reuse SDK HTTP transport and credential authorizers (tasks/t02-sdk-http-transport.md)
- [x] [673cb696] storage-completeness:ch10:t03 — Retain multipart abort ownership across completion errors and cancellation (tasks/t03-session-cleanup.md)

- [x] [85ed9f36] storage-completeness:ch10:t04 — Frame S3 producer chunks and preserve bounded backend policies (tasks/t04-minimum-cloud-part-framing.md)

**Chapter Recovery Checkpoint:** the combined source checkpoint `fe992ae3` passes 15 library tests, 39 canonical engine tests, formatting, all-target Clippy and diff checks. All four task worksheets are completed and bound to the signed commits above. Correctness review and a fresh quality review over `4867dfb9..HEAD`, including synchronized Markdown, remain required before chapter closure.

## 6. Current State & Remaining Plan Snapshot

- Concrete provider identity/Memory Create-Match, real test-only SDK request transport and multipart cleanup ownership are signed at `d50cf6a2`, `2bc92864` and `673cb696`. Memory NotMatch fails at BEGIN before input consumption until ch20:t01; cloud transport runtime activation belongs to ch12:t01.
- S3 bounded 8 MiB framing, short-tail/empty behavior, producer backpressure, abort ownership and the 10000-part guard are signed at `85ed9f36`. OCI's private policy retains its existing behavior.
- The combined source checkpoint `fe992ae3` passes 15 library and 39 canonical tests, formatting, all-target Clippy and diff checks. The GCS constructor and snapshot request boundaries have their own implementation and review receipts in [ch21](../ch21-sdk-resolved-config/spec.md). Installed Python extensions remain at the native R2-withdrawal checkpoint.
- Scoped knowledge is synchronized at `[1/1 files documented]`; final provider/consumer consolidation and root `[9/13 files documented]` acceptance remain with ch09:t03/ch04:t03.
- The chapter is active, `current_task` and `last_reviewed_commit` are null, and final review metadata re-verification remains before closure. Local/cloud publication, ch20 negative CAS and original ch01/ch02 native acceptance/reclosure remain open.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.

[^publication]: `src/rs/vertebra-storage/src/conditional.rs::PublicationBackend` — Concrete provider dispatch and native Memory conditional completion.

[^transport]: `src/rs/vertebra-storage/src/cloud_request.rs::CloudRequestClient` — Real SDK transport registered only in library tests.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
