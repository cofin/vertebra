---
type: Task
id: storage-completeness:ch10:t01
chapter_id: ch10-publication-boundary-and-lifecycle
title: Retain concrete providers behind a private publication boundary
description: Retain concrete providers behind a private publication boundary
status: stable
state: completed
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
estimated_loc: 220
plan_revision: 7
plan_commit: 5f64d1a8
state_revision: 0
claimed_by: publication_dispatch
claimed_at: '2026-11-07T20:00:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: d50cf6a2
verification_evidence: >-
  Independent core source checkpoint d50cf6a2: public publication selection Red exits
  101 with two failures before implementation; final canonical native suite exits 0
  with 28 passed. Reserved-target cargo fmt --all --check and storage all-target
  Clippy -D warnings exit 0, as does git diff --check. Actual production additions
  are 216 LOC across three files. GCS configuration capture is owned by ch21;
  Memory NotMatch rejects at begin until mandatory shared ch20 CAS support.
  Installed Python extensions are unchanged; final native Python acceptance remains
  a later gate. Bounded split revision 7 is independently approved at 5f64d1a8.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: d50cf6a2
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

Add private `conditional.rs` and `mod conditional;` in lib.rs. `ObjectStoreEngine` retains `inner: Arc<dyn ObjectStore>` plus private `publication: PublicationBackend`. The enum retains concrete `Arc` handles for Memory, Local, S3, Azure, GCS and OCI plus an explicit Custom variant for injected stores. Build each SDK store exactly once and clone that same Arc into read and publication fields; clone/to_async/to_sync preserve its credential-provider and client identity. Ordinary reads/lists retain their existing path. No resolved GCS configuration, connector capture or service-account parsing belongs to this task; ch21:t01 owns those additions.

Private `PublishCondition { Overwrite, Create, Match(String), NotMatch(String) }` normalizes `PutConditionalOptions`, rejects simultaneous selectors before effects, and maps `if_none_match="*"` to Create. `PublicationBackend::begin(path, options) -> Result<Box<dyn MultipartUpload>, ObjectStoreError>` dispatches only implemented capabilities. Memory publication stores segmented `PutPayload` parts, builds a cheap view on completion and calls actual native InMemory `PutMode::Overwrite`, `Create` or `Update(UpdateVersion { e_tag: Some(tag), version: None })` with caller attributes. Preserve the actual successful `PutResult`; no full-object concatenation or extra byte copy. Local/S3/Azure/GCS/OCI conditional multipart fail explicitly until their provider chapters implement the required primitive; normal Overwrite remains available. Existing `with_store` keeps injected ordinary PutMode support and explicitly rejects unsupported conditional multipart.

Memory `NotMatch` deliberately returns native NotSupported at BEGIN, before consuming any producer input or changing a destination. This temporary capability boundary is mandatory to remove in ch20:t01, which adds Memory negative multipart completion using the SAME eight-attempt PutPayload CAS helper as ordinary negative puts. This task neither implements a separate negative loop nor claims final Memory negative-predicate acceptance. Retain concrete GCS Arc identity now; ch21:t01 adds the SDK-resolved configuration beside it, and ch14:t04 activates its final publication wrapper.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| One engine identity | Construct and clone/to_async/to_sync engine | Read and publication handles use same concrete store/provider credentials |
| Selector validation | if_not_exists plus if_match or both match/nonmatch selectors | InvalidInput before upload initiation |
| Memory create/update | Begin multipart with Create or Match, install a competing version, finish | Actual InMemory Create/Update rejects the stale predicate and preserves competitor bytes; segmented parts retain byte identity and metadata. |
| Temporary Memory negative gate | Existing object, begin with opaque if_none_match, producer counter initially zero | BEGIN returns NotSupported, producer counter stays zero and destination bytes/metadata are unchanged; ch20:t01 owns final support. |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The bounded dispatch contract awaits independent cold review. The current core implementation fits this task; adding 70–90 LOC of GCS configuration capture would exceed the 250-LOC bound, so ch21:t01 owns that complete contract. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
