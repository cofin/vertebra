---
type: Task
id: storage-completeness:ch08:t01
chapter_id: ch08-remove-r2-native
title: Transfer uniform multipart accumulation into OCI ownership
description: Transfer uniform multipart accumulation into OCI ownership
status: stable
state: pending
priority: P1
verification_strategy: characterization
depends_on:
- storage-completeness:ch07:t02
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/r2.rs
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 170
plan_revision: 4
plan_commit: ddfb11f2
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
tags:
- storage
- repair
---

# Transfer uniform multipart accumulation into OCI ownership

## Objective & Scope Boundary

Transfer uniform multipart accumulation into OCI ownership. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/oci.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/r2.rs` — collaborating contract.

## Symbol & Signature Contracts

Move the incremental `UniformPartAccumulator` and fixed 8 MiB constant to `oci.rs` with OCI-specific naming (`OCI_UNIFORM_PART_SIZE`). `store.rs::create_multipart` uses uniform re-chunking only for scheme oci. Preserve existing helper exports still called by lib.rs until the next task; no Python behavior depends on moving them. Do not retain a whole-payload Vec or a 1 GiB buffer. Renaming canonical tests to OCI must retain observable part sizes and exact reconstructed payload assertions. This task stays green while r2.rs still exists; remove only its production accumulator caller, then remove the file in t03.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

Before this task completes, remove every OCI-to-R2 import/reexport: migrate or remove the complete OCI chunk helper surface, including `chunk_uniform_parts`, and rename its constant to `OCI_UNIFORM_PART_SIZE` in OCI ownership and canonical Rust tests. Preserve only the temporary `lib.rs -> r2::chunk_uniform_parts` dependency for the still-exported Python helper until t02 removes its registration/stub. t02 must not need to edit oci.rs. The orphan r2.rs file is deleted by t03, together with durable source-link cleanup.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Uniform chunks | Write irregular chunks totaling 2*8 MiB+3 bytes to OCI multipart test store | Flushed sizes [8388608,8388608,3], exact payload |
| Empty/tail | Complete empty stream and sub-part stream | Existing zero-byte semantics and exact tail retained |
| Bounded accumulator | Producer emits many parts while checking uploads advance | Only one partial 8 MiB frame retained; no whole-object accumulation |

## Verification

Characterize the existing OCI accumulator at the public multipart seam before the move and run the same assertions after it. OCI already requires uniform 8 MiB parts; this ownership transfer preserves behavior and does not invent a failing regression. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

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

Existing OCI `requires_uniform_part_size` already selects 8 MiB accumulation. Record green-before and green-after evidence for the behavior-preserving ownership transfer; do not claim a Red result. Revision 4 is independently approved by the fresh consolidated ch07/ch08 plan review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.
