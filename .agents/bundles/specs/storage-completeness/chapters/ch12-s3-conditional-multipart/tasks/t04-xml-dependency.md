---
type: Task
id: storage-completeness:ch12:t04
chapter_id: ch12-s3-conditional-multipart
title: Declare native protocol and filesystem dependencies
description: Declare native protocol and filesystem dependencies
status: stable
state: completed
priority: P1
verification_strategy: static_validation
depends_on:
- storage-completeness:ch10:t04
read_set:
- src/rs/vertebra-storage/Cargo.toml
- src/rs/Cargo.lock
- src/rs/Cargo.toml
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/Cargo.toml
- src/rs/Cargo.lock
tests: []
estimated_loc: 30
plan_revision: 6
plan_commit: e3e344b5
state_revision: 1
claimed_by: agent:storage_flow/native_protocol_dependencies
claimed_at: '2026-11-08T00:38:14Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T00:40:20Z'
last_verified_commit: 35a4f784
verification_evidence: |
  Baseline cargo check --offline --locked exits 0. After the dependency additions, cargo check --offline exits 0 and the required second cargo check --locked exits 0. Every Cargo invocation holds flock /tmp/vertebra-flow-storage-cargo.lock and uses CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" and PYO3_PYTHON="$PWD/.venv/bin/python3.12", with --manifest-path src/rs/Cargo.toml -p vertebra-storage.
  An isolated temporary copy of the native workspace removes only uuid's resolver-required js-sys and wasm-bindgen edges; cargo check --offline --locked refuses the stale lockfile with exit 101. The live workspace is unchanged by this negative proof.
  Parsed lockfile comparison preserves all 572 package identities, versions, sources and checksums. The only added dependency edges are vertebra-storage -> quick-xml, uuid, windows-sys 0.61.2 and uuid 1.29.1 -> already-locked js-sys, wasm-bindgen. No removed edges or PostgreSQL changes. git diff --check on both owned source files exits 0.
  Final cargo check --offline --locked exits 0 with byte-identical Cargo.lock before and after (SHA256 7db061e4fa1ff0d89945ca251f5594bd99312841e13b532a351169bc9eee64c0). git diff --check including the owned worksheet exits 0.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-08T00:40:20Z'
commit: 35a4f784
sources:
- id: implementation
  resource: src/rs/vertebra-storage/Cargo.toml
  title: Owned implementation seam
tags:
- storage
- repair
---

# Declare native protocol and filesystem dependencies

## Objective & Scope Boundary

Declare native protocol and filesystem dependencies. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/Cargo.toml` — owned implementation or canonical regression surface.
- `src/rs/Cargo.lock` — owned implementation or canonical regression surface.
- `src/rs/Cargo.toml` — collaborating contract.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — collaborating contract.

## Symbol & Signature Contracts

Declare direct `uuid = { version = "=1.29.1", features = ["v4"] }` in the storage crate using the exact existing Cargo.lock package. Local and GCS stage names use `Uuid::new_v4`; no predictable process/timestamp identifier substitutes for the required unique private stage identity. Cargo also records the required uuid dependency edges to already-locked `js-sys` and `wasm-bindgen` packages when resolving the mandated features. No package identity, version, source or checksum changes, and no whole-object buffer, are permitted.

Add a Windows-target-only direct `windows-sys` workspace dependency to vertebra-storage, enabling `Win32_Foundation` and `Win32_Storage_FileSystem` in that crate. The existing locked 0.61 package identity is reused; update only its direct crate dependency edge in Cargo.lock. No workspace version or unrelated package changes. ch11:t01 consumes `GetFileInformationByHandleEx(FileIdInfo)` / `FILE_ID_INFO` and directory `FILE_FLAG_BACKUP_SEMANTICS` from that declared dependency, rather than unstable std MetadataExt methods or handwritten Win32 declarations. The dependency gate remains static validation and precedes both Local and provider protocol code.

Declare direct quick-xml 0.39.4 with serialize support in vertebra-storage/Cargo.toml, using the already locked 0.39.4 package from object_store's existing dependency graph. Add vertebra-storage's direct edges and the required uuid feature edges above through Cargo's normal resolver; do not upgrade unrelated packages, rewrite PostgreSQL package entries or add an SDK fork. S3 CreateMultipartUpload/CompleteMultipartUpload and Azure block-list serialization/parsing reuse this maintained XML codec; HTTP 200 Error bodies and XML entities/namespaces must be parsed structurally, not substring/string-split approximations. This task precedes S3 provider source imports. Preserve concurrent manifest/lock edits by other agents.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Existing locked codec | Cargo offline resolve/check after adding direct dependency | quick-xml stays 0.39.4; no unrelated package/version changes; storage compiles |
| Locked repeatability | Second cargo check --locked | Exit 0 with no Cargo.lock diff |

## Verification

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --offline`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --locked`
- `git diff --check -- src/rs/vertebra-storage/Cargo.toml src/rs/Cargo.lock`

Inspect the lock diff to ensure the existing quick-xml version and all unrelated PostgreSQL/workspace package versions are preserved. Record exact commands and exit statuses; no low-signal source-text test is required for a dependency edge.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved; an isolated stale-lockfile check refuses omission of the required uuid feature edges.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250. All 572 package identities, versions, sources and checksums are preserved; the explicit lock diff consists only of three storage edges and uuid's two required edges to already-locked WASM packages.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this manifest-only task records native Cargo parser/build and lock repeatability evidence without adding source-text tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The mandated uuid v4 declaration requires normal Cargo resolver edges to the existing js-sys and wasm-bindgen packages. The isolated stale-lockfile proof returns exit 101 when those edges are removed. Coordinator and root readiness review approve this exact resolver clarification; plan identity remains revision 6 at e3e344b5 because the declared dependencies and runtime contract are unchanged. The verified manifest and lockfile changes are signed at `35a4f784`; this task receipt and chapter marker are completed.

[^implementation]: `src/rs/vertebra-storage/Cargo.toml` — Current repository source for this task.
