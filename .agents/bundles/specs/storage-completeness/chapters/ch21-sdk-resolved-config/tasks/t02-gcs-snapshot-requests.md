---
type: Task
id: storage-completeness:ch21:t02
chapter_id: ch21-sdk-resolved-config
title: Capture and fence GCS data and metadata snapshots
description: Implement the real private XML request boundary for opaque ETag predicates
status: stable
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
- storage-completeness:ch21:t01
- storage-completeness:ch10:t02
read_set:
- src/rs/vertebra-storage/src/gcs_request.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/store.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/gcs_request.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/src/gcs_request.rs
estimated_loc: 230
plan_revision: 8
plan_commit: c737ffeb
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
  resource: src/rs/vertebra-storage/src/gcs_request.rs
  title: Owned private GCS request primitive
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Capture and Fence GCS Data and Metadata Snapshots

## Objective & Scope Boundary

Implement real private GCS request primitives after SDK configuration and shared transport exist. Own only gcs_request.rs and its test-only lib.rs registration. Ordinary engine routing belongs to ch14:t01; multipart publication belongs to ch14:t04. No public configuration or Python API is added.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcs_request.rs` — owned snapshot, conditional PUT and exact receipt boundary.
- `src/rs/vertebra-storage/src/lib.rs` — owned test-only module registration.
- `src/rs/vertebra-storage/src/cloud_request.rs` — actual SDK transport and safe errors.
- `src/rs/vertebra-storage/src/conditional.rs` — SDK-owned GcsStageConfig.
- `src/rs/vertebra-storage/src/store.rs` — actual pub(crate) build_gcs_store constructor.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — pinned SDK and primary GCS HTTP evidence.

## Symbol & Signature Contracts

Register exactly `#[cfg(test)] mod gcs_request;` until ch14:t01 activates the first production caller. Inline gcs_request::tests use the actual SDK/CloudRequestClient and a local HTTP responder, never a mocked transport or public test hook. Before the new wrapper, run a focused configured SDK/HTTP baseline against the real responder. After implementation, verify complete snapshot/mutation/receipt composition and negative paths through actual requests. Do not fabricate a SDK-misconfiguration Red or treat missing-interface compilation as behavioral proof.

Declare `pub(crate) struct GcsObjectVersion { pub(crate) generation: String, pub(crate) metageneration: String }` and `pub(crate) struct GcsObjectSnapshot { pub(crate) xml_etag: String, pub(crate) version: GcsObjectVersion }`. Parse positive u64 generation/metageneration values; retain the XML ETag exactly as HeaderValue::to_str()?.to_owned(), including quotes. No trim, casefold or JSON ETag substitution.

`GcsRequestClient::new(store: Arc<GoogleCloudStorage>, config: Arc<GcsStageConfig>) -> object_store::Result<Self>` retains the same SDK credential owner and an Arc<CloudRequestClient> built from its exact captured options. Its request/configuration access is pub(crate) for the real resumable/publication modules; Debug never exposes credentials, URLs, headers or ClientOptions. Each authenticated request obtains the SDK credential anew, skips lookup when configured, omits an empty bearer and marks Authorization sensitive. Session-capability requests use the same transport without injecting OAuth. Do not duplicate signing/auth resolution.

The actual primitive methods are `async fn head_snapshot(&self, path: &StorePath, expected: Option<&GcsObjectVersion>) -> object_store::Result<Option<GcsObjectSnapshot>>` and `async fn put_snapshot(&self, path: &StorePath, payload: PutPayload, options: PutOptions, expected: Option<&GcsObjectSnapshot>) -> object_store::Result<PutResult>`. Unfenced head_snapshot(None) captures a logical predicate snapshot; fenced head_snapshot(Some(G/M)) retrieves an exact publication receipt through the SAME live method. This avoids an unused future receipt method during ch14:t01 production activation. None in put_snapshot means Create, never Overwrite. Objects remain bounded metadata and segmented payload views.

Snapshot issues one XML HEAD and parses ETag, x-goog-generation and x-goog-metageneration from that SAME response; only 404 becomes None. Present conditional PUT sends BOTH x-goog-if-generation-match:G and x-goog-if-metageneration-match:M. Absent conditional PUT sends generation-match:0 and no metageneration. Preserve Content-Length, Content-Type, x-goog-meta-* and actual PutResult response headers. Follow ClientOptions::get_content_type(path).unwrap_or("application/octet-stream") when no caller content type is set. Preserve supported Attribute headers and extensions as the pinned SDK does. Do not route through SDK UpdateVersion for GCS because it ignores e_tag and has no metageneration field.

Fenced head_snapshot(Some(G/M)) issues XML HEAD with generation=G query and BOTH G/M match headers. A 200 captures that exact XML snapshot; 404/412 is an explicit provider/indeterminate receipt failure, never None/current HEAD/JSON fallback. Only unfenced predicate capture converts 404 to None. Map genuine permissions/transport/provider failures honestly and redact raw URL/body/source-chain details.

## Edge Cases & Error Semantics

Generation-only fencing cannot protect an opaque ETag against metadata-only changes. Preserve all three snapshot fields and use paired predicates. Conditional failures are typed native preconditions; ordinary absence is distinct from a failed receipt. No unfenced retry, user object deletion or whole-object byte copy is permitted.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Atomic snapshot | One HEAD returns quoted ETag A, generation1729, metageneration4 | All fields derive from that same response; quoted ETag is unchanged. |
| Opaque keys | Literal %2F versus %252F, Unicode and reserved characters in accepted StorePath keys | Actual HEAD/PUT/fenced-HEAD targets encode exactly once and identify the original key; no path/query confusion or metadata unquote. |
| Metadata-only race | After snapshot, responder changes A to F and M4 to M5 with generation1729 unchanged | PUT carries G1729/M4 and fails412; exact competitor data/metadata survives. |
| Create and attributes | Missing target with caller content type/attributes and segmented payload | PUT uses only generation-match0, exact bytes/headers and response receipt; race412 never overwrites. |
| Fenced receipt | Rewrite published G900/M1, metadata becomes M2 before HEAD | HEAD includes generation900 and G900/M1 predicates; explicit receipt failure, no successor ETag returned. |
| Real configuration | Actual build_gcs_store with headers/rotating credentials/skip-signature | Actual SDK read and primitive requests preserve transport and credential ownership; no new public injection API. |

## Verification

Run the real private library target, format check, normal compilation and all-target Clippy. Root storage Cargo coordination lock is outermost; the host SHM lock is inside it for tests.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcs_request::tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`
- `git diff --check`

## Acceptance Criteria

- [ ] Actual private SDK/HTTP tests prove snapshot tuple, paired fences, exact receipts and error identity.
- [ ] Registration remains test-only until ch14:t01; no premature public engine claim.
- [ ] Only two production files change; read_set <=6 and implementation <=250 LOC.
- [ ] Signed source/evidence recorded; durable docs belong to ch09:t03/ch04:t03 after native acceptance.

## Notes & Discoveries

The impacted frontier is independently approved at `c737ffeb`. A provider/API contradiction routes through bounded Flow revision before production edits.

[^implementation]: `src/rs/vertebra-storage/src/gcs_request.rs` — Actual private request implementation introduced by this task.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK and primary HTTP evidence.
