---
type: Task
id: storage-completeness:ch21:t01
chapter_id: ch21-sdk-resolved-config
title: Retain SDK-resolved GCS endpoint and transport configuration
description: Retain SDK-resolved GCS endpoint and transport configuration
status: draft
state: in_progress
priority: P1
verification_strategy: characterization
depends_on:
- storage-completeness:ch10:t01
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
estimated_loc: 120
plan_revision: 8
plan_commit: null
state_revision: 0
claimed_by: agent:sdk_config_capture
claimed_at: '2026-11-08T00:36:39Z'
blocked_reason: null
unblock_condition: null
next_step: Complete receipt rebinding after the impacted revision8 plan review.
last_verified_at: '2026-11-08T00:36:39Z'
last_verified_commit: fe992ae3
verification_evidence: "Frozen approved revision7 constructor contract: SDK baseline3Green; final14lib+28canonical plus normalcheck/Clippy/fmt/diff0. Signed isolated844cd4a8 imported asfe992ae3; combined15lib+39canonical0. Independent correctness at844cd4a8 findings[]. New revision8 paired-snapshot plan review and receipt rebinding remain."
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/store.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Retain SDK-Resolved GCS Endpoint and Transport Configuration

## Objective & Scope Boundary

Retain the exact resolved SDK endpoint and ClientOptions beside the existing concrete GCS Arc after ch10:t01. Own only store.rs and conditional.rs with inline actual SDK/local-HTTP tests. Constructor tests cover reads and configuration capture; full publication HTTP comparison belongs to ch14:t04. No future protocol implementation is included. [^implementation] [^research]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — ObjectStoreEngine::new GCS builder, credentials and ordinary reads.
- `src/rs/vertebra-storage/src/conditional.rs` — existing GCS publication variant and private configuration record.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — pinned object_store 0.13.2 resolution and connector API evidence.

## Symbol & Signature Contracts

Add private `GcsStageConfig { base: Url, bucket: String, client_options: ClientOptions, skip_signature: bool }` beside the existing concrete GCS publication handle. In store.rs, the private delegating public HttpConnector implements `connect(&ClientOptions) -> object_store::Result<HttpClient>` by cloning the supplied options into shared capture, then invoking `ReqwestConnector.connect(options)` unchanged. Apply it via `GoogleCloudStorageBuilder::with_http_connector`. Build the actual SDK store ONCE, then take the final captured options, which belong to the object-store client after any credential/metadata connectors. Clone the one resulting Arc into ordinary and publication fields; authentication and refresh stay in that SDK's credential provider.

The actual construction path is `pub(crate) fn build_gcs_store(builder: GoogleCloudStorageBuilder) -> Result<(Arc<GoogleCloudStorage>, Arc<GcsStageConfig>), StorageError>`, called by ObjectStoreEngine::new after applying its existing configuration; inline tests invoke that same production helper with real custom ClientOptions. Before consuming the configured builder, retrieve effective bucket/BaseUrl/skip-signature and singleton service-account path/key. Effective explicit/environment BaseUrl wins, followed by accepted singleton service-account JSON `gcs_base_url`, followed by `https://storage.googleapis.com`. A narrow serde endpoint-only struct may read metadata from that singleton source; never reconstruct or log credentials. When both path and inline key exist, preserve the SDK's own rejection by passing the unchanged builder to build; do not choose a source or silently ignore invalid configuration. Preserve SDK error classification and redact credential-bearing context. Builder getters alone do not include the service-account fallback.

Capture final allow_http, proxy/exclusions, TLS, timeouts and default headers. Constructor does not probe endpoints or rebuild the store. Do not add public APIs, fake uses, broad allows or a test-only copy of production logic. For an otherwise unused future `GcsStageConfig` or GCS variant field, only if non-test compilation requires it, place exact field-local `#[cfg_attr(not(test), expect(dead_code, reason = "consumed by ch14:t01 GCS requests"))]`; ch14:t01 owns mandatory removal in conditional.rs when its real wrapper consumes these fields. Inline tests actually read captured config, so an unconditional expect would be unfulfilled in test builds.

Caller → seam → collaborator: real ObjectStoreEngine GCS constructor/read → private capture/configuration → pinned SDK builder, ReqwestConnector, same concrete SDK credentials and real local HTTP responder. Only the external service or external credential supplier may be substituted.

## Edge Cases & Error Semantics

No endpoint network request occurs during construction. Test singleton path and inline key separately using `disable_oauth:true` fixture JSON with required SDK fields; use temporary files for path cases. Keep environment resolution deterministic and restore any modified environment safely. Preserve explicit/environment override, true service-account fallback and path-plus-key rejection. Retain transport option identity through final capture, and preserve real SDK construction/transport errors without credential or raw JSON leakage.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Explicit override | BaseUrl A, singleton service account gcs_base_url B, allow_http true | Actual SDK read reaches local A; retained base is A and B receives no request. |
| True singleton fallback | Separate path-only and inline-key-only fixtures with local gcs_base_url B and disable_oauth true | Actual SDK reads reach B; retained base/bucket/skip_signature and empty-bearer behavior match SDK configuration. |
| Dual source rejection | Both accepted service-account path and key configured | Actual constructor returns SDK configuration failure; responder count is zero and no source is silently selected. |
| Final transport capture | ClientOptions with HTTP policy, default header, timeout and proxy exclusions; SDK construction performs earlier metadata connector setup | Captured final options match configured values; actual SDK read carries the default header to the correct endpoint; metadata options never replace the final capture. |
| Shared SDK identity | Real built engine cloned and converted through existing native wrappers where available | Inline Arc identity checks and read requests show the same SDK/provider instance; no second SDK build or network probe occurs. |

## Verification

Characterize configured SDK reads before capture, then verify retained configuration and unchanged actual reads after capture; no absent-field or fabricated SDK-misconfiguration Red is claimed; do not require the future publication wrapper. Run from this worktree:

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`
- `git diff --check`

Record exact statuses, constructor request count, tested singleton sources and final capture evidence. A normal build must pass; field-local expectations, if required, have the explicit ch14:t04 removal owner.

## Acceptance Criteria

- [ ] All matrix rows exercise the real SDK/configuration path and preserve exact final transport and endpoint behavior.
- [ ] SDK construction occurs once; concrete Arc, credentials and refresh identity are shared with ordinary reads.
- [ ] Only two production files change; read_set <=6 and estimated implementation <=120 LOC.
- [ ] Any exact field expectation is temporary and ch14:t01 removes it upon actual production use.
- [ ] Signed local conventional commit, exact verification evidence, correctness and fresh quality reviews are recorded; docs remain assigned to ch09:t03/ch04:t03.

## Notes & Discoveries

The actual ch10 core already occupies 216 added implementation LOC; connector/configuration capture adds 70–90 LOC and therefore has this independent bounded owner. Any source contradiction routes through `/flow:revise storage-completeness ch21-sdk-resolved-config` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine::new` — Actual GCS construction seam.
[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Accepted SDK configuration and connector evidence.

The constructor-capture source is signed at `fe992ae3`, imported from independently verified `844cd4a8`. Its bounded implementation has 119 added production lines and shares `Arc<GcsStageConfig>` without cloning large configuration into the publication enum. The real SDK baseline passes before capture; actual configured reads and final capture pass afterward. Independent correctness review reports no verified findings. This task remains in progress only for revision8 receipt rebinding; the newly added snapshot-request task retains its separate acceptance.
