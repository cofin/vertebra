---
type: Task
id: storage-completeness:ch10:t02
chapter_id: ch10-publication-boundary-and-lifecycle
title: Reuse SDK HTTP transport and credential authorizers
description: Reuse SDK HTTP transport and credential authorizers
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch10:t01
read_set:
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/store.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/src/cloud_request.rs
estimated_loc: 210
plan_revision: 6
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/cloud_request.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Reuse SDK HTTP transport and credential authorizers

## Objective & Scope Boundary

Reuse SDK HTTP transport and credential authorizers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/cloud_request.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Add the real private `cloud_request.rs` implementation with `pub(crate) struct CloudRequestClient { client: object_store::client::HttpClient }`. Its constructors and execution boundary are:

- `CloudRequestClient::new(options: &ClientOptions) -> Result<Self, ObjectStoreError>` calls public `ReqwestConnector::connect(options)` through `HttpConnector`; retain the exact resolved HTTP, proxy, certificate, timeout and header configuration.
- `async fn execute(&self, request: HttpRequest, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` executes the SDK request with `HttpRequestBody`. It returns non-2xx responses intact for the owning provider's status/predicate parser. It does not decide replay or retry publication.
- `async fn execute_aws(&self, request: HttpRequest, credentials: &AwsCredentialProvider, region: &str, operation: &'static str) -> Result<HttpResponse, ObjectStoreError>` obtains `credentials.get_credential().await` on every invocation, uses `AwsAuthorizer::new(credential.as_ref(), "s3", region).authorize(&mut request, None)`, then delegates to `execute`. S3 is its first production consumer. Azure and GCS wrappers perform their own SDK credential lookup/authorization before calling `execute`; do not introduce unused provider helpers here.

Map transport and credential-acquisition failures to credential-free storage errors with static operation and SDK error-kind context; never format or attach a signed request URI, Authorization, credential Debug, raw transport source chain or server body that can echo secrets. HTTP status parsing stays with the provider, which may preserve safe status/code context without forwarding untrusted response bodies. No custom signing, SDK fork or replacement reqwest client is permitted.

In `lib.rs`, register this complete module under exactly `#[cfg(test)] mod cloud_request;` for this task. Its real SDK/network tests compile through the library test target; it is not wired into runtime publication yet. ch12:t01 owns removal of this single test-only gate when the first actual S3 initiation caller lands. No broad dead-code allowance, public export, placeholder request implementation or claim of live provider publication is permitted. ch12:t01 must run normal non-test compilation and all-target Clippy after activation; unused future provider APIs are implemented only when their consuming wrapper exists.

The owned test seam is this private external-request boundary, with inline `cloud_request::tests` in `cloud_request.rs`. For credential injection, construct a real `AmazonS3` via public `AmazonS3Builder::with_bucket_name("unit").with_region("us-east-1").with_endpoint(local_url).with_allow_http(true).with_credentials(provider).build()`; `provider` is an `Arc<dyn object_store::client::CredentialProvider<Credential = AwsCredential>>` test collaborator whose `get_credential` returns distinct `Arc<AwsCredential>` values on successive calls. Its only substitution is the external credential supplier. Pass `sdk.credentials()` to `execute_aws`, not an erased `ObjectStoreEngine::with_store`, a mock request client or a mocked signing function. A real local HTTP responder captures requests and supplies bounded response/failure sequences.

Caller → seam → collaborator: inline native transport acceptance → real `CloudRequestClient::{execute,execute_aws}` → SDK connector/authorizer/credential provider and local HTTP responder. Public ObjectStore publication acceptance is assigned to ch12:t01 and ch13:t01–t02 after their implementations exist.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Configured transport | Build real SDK client with allow_http and resolved headers/proxy exclusions; send an SDK `HttpRequest` to the real local responder through CloudRequestClient | Captured method, path, configured headers and payload match the request; no S3/Azure publication wrapper is required. |
| Credential refresh | SDK credential-provider collaborator returns key/token A then B; responder returns 503 then 200 for two explicit request attempts | Actual authorization headers carry A then B, and provider is called once per attempt; no cached custom auth or hidden publication replay. |
| HTTP ownership | Real responder returns 403, 412 and a bounded body | `execute` returns the exact statuses to its caller; it does not convert permission failure into a CAS conflict or retry a mutation. |
| Transport privacy | Close the local connection for a credential-bearing request | Storage error retains static operation and safe transport kind; signed target, Authorization, tokens and raw source-chain text are absent. Provider-complete SAS/body redaction is verified by ch13:t02. |

## Verification

Record the failing real transport behavior before implementation and the passing result after. The test target is the library module, not a provider publication integration target that depends on later tasks. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib cloud_request::tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes through the real private external-request seam; runtime provider publication is not claimed before ch12:t01 activation.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/cloud_request.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
