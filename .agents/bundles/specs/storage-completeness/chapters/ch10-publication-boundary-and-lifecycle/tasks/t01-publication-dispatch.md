---
type: Task
id: storage-completeness:ch10:t01
chapter_id: ch10-publication-boundary-and-lifecycle
title: Retain concrete providers behind a private publication boundary
description: Retain concrete providers behind a private publication boundary
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch08:t03
- storage-completeness:ch05:t03
- storage-completeness:ch05:t02
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 230
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
  resource: src/rs/vertebra-storage/src/store.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Retain concrete providers behind a private publication boundary

## Objective & Scope Boundary

Retain concrete providers behind a private publication boundary. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Add private `conditional.rs` and `mod conditional;` in lib.rs. `ObjectStoreEngine` keeps existing `inner: Arc<dyn ObjectStore>` and a private `publication: PublicationBackend`. `PublicationBackend` retains concrete Arc handles for Memory, Local, S3, Azure, GCS, OCI and resolved endpoint/region/bucket/client options as needed; ordinary read/list code keeps inner. Build each SDK store once and share Arc into both fields, preserving existing credential provider/refresh identity and configured transport. Private `PublishCondition { Overwrite, Create, Match(String), NotMatch(String) }` normalizes PutConditionalOptions, rejecting conflicting selectors before provider effects. The private `PublicationBackend::begin(path, options)` returns a `Box<dyn MultipartUpload>` implementing provider-specific final publish; no public backend enum or generic copy-create substitute. Existing `with_store` stays usable for injected canonical stores, retaining actual PutMode support; unsupported custom-store conditional multipart fails explicitly until it provides the required primitive. S3/Azure/GCS variant constructors carry concrete handles before their later protocol task; no silent conditional fallback is allowed.

For GCS retain a private `GcsStageConfig { base: Url, bucket: String, client_options: ClientOptions, skip_signature: bool }` beside the same concrete SDK store. Resolve the actual builder configuration before type erasure: explicit/environment BaseUrl wins, then service-account `gcs_base_url`, then the pinned SDK default. `get_config_value(BaseUrl)` alone does not include the service-account fallback. If necessary deserialize only endpoint metadata from the configured service-account source with a narrow serde struct; preserve the SDK rejection when both service-account path and inline key are configured; resolve metadata only from its accepted singleton source, ignore credential fields and leave all authentication/refresh to SDK providers. Never log the source JSON or reconstruct credentials.

Capture the exact final SDK ClientOptions with a real delegating public HttpConnector passed through `GoogleCloudStorageBuilder::with_http_connector`: its `connect(&ClientOptions)` clones the options and delegates unchanged to ReqwestConnector. In the pinned builder the final build-time connect belongs to the object store, following any credential/metadata client setup. Reuse those captured options for the private request client, including allow_http, proxy exclusions, TLS, timeouts and default headers. Read the effective skip-signature setting from the configured builder; session authentication also preserves the SDK's empty-bearer behavior. Tests compare actual configured SDK reads and publication requests against local endpoints, including each singleton service-account base-url fallback and simultaneous path-plus-inline rejection; no constructor network probe or auth fork is introduced. The retained record is consumed by the GCS resumable-stage chapter.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| One engine identity | Construct and clone/to_async/to_sync engine | Read and publication handles use same concrete store/provider credentials |
| Selector validation | if_not_exists plus if_match or both match/nonmatch selectors | InvalidInput before upload initiation |
| Memory create/update | Begin multipart, mutate destination, finish | Real memory PutMode Create/Update applies atomically; no extra whole-object copy |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
