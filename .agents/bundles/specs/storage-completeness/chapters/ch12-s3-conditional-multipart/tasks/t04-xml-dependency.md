---
type: Task
id: storage-completeness:ch12:t04
chapter_id: ch12-s3-conditional-multipart
title: Declare direct XML and Windows identity dependencies
description: Declare direct XML and Windows identity dependencies
status: draft
state: pending
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
plan_revision: 5
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
  resource: src/rs/vertebra-storage/Cargo.toml
  title: Owned implementation seam
tags:
- storage
- repair
---

# Declare direct XML and Windows identity dependencies

## Objective & Scope Boundary

Declare direct XML and Windows identity dependencies. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/Cargo.toml` — owned implementation or canonical regression surface.
- `src/rs/Cargo.lock` — owned implementation or canonical regression surface.
- `src/rs/Cargo.toml` — collaborating contract.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — collaborating contract.

## Symbol & Signature Contracts

Add a Windows-target-only direct `windows-sys` workspace dependency to vertebra-storage, enabling `Win32_Foundation` and `Win32_Storage_FileSystem` in that crate. The existing locked 0.61 package identity is reused; update only its direct crate dependency edge in Cargo.lock. No workspace version or unrelated package changes. ch11:t01 consumes `GetFileInformationByHandleEx(FileIdInfo)` / `FILE_ID_INFO` and directory `FILE_FLAG_BACKUP_SEMANTICS` from that declared dependency, rather than unstable std MetadataExt methods or handwritten Win32 declarations. The dependency gate remains static validation and precedes both Local and provider protocol code.

Declare direct quick-xml 0.39.4 with serialize support in vertebra-storage/Cargo.toml, using the already locked 0.39.4 package from object_store's existing dependency graph. Update only vertebra-storage's dependency edge in Cargo.lock through Cargo's normal resolver; do not upgrade unrelated packages, rewrite PostgreSQL package entries or add an SDK fork. S3 CreateMultipartUpload/CompleteMultipartUpload and Azure block-list serialization/parsing reuse this maintained XML codec; HTTP 200 Error bodies and XML entities/namespaces must be parsed structurally, not substring/string-split approximations. This task precedes S3 provider source imports. Preserve concurrent manifest/lock edits by other agents.

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

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/Cargo.toml` — Current repository source for this task.
