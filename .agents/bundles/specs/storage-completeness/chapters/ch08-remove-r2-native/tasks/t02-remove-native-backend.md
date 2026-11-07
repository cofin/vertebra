---
type: Task
id: storage-completeness:ch08:t02
chapter_id: ch08-remove-r2-native
title: Delete native R2 builder, module, helper and stub surface
description: Delete native R2 builder, module, helper and stub surface
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch08:t01
- storage-completeness:ch07:t03
- storage-completeness:ch05:t03
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/src/oci.rs
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 130
plan_revision: 4
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
tags:
- storage
- repair
---

# Delete native R2 builder, module, helper and stub surface

## Objective & Scope Boundary

Delete native R2 builder, module, helper and stub surface. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/py/vertebra/_storage.pyi` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/r2.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.

## Symbol & Signature Contracts

Remove BackendConfig::R2, its constructor arm and from_url r2 handling; r2 URLs fail through the existing unsupported-scheme branch. Remove `pub mod r2`, helper imports, `py_chunk_r2_uniform_parts` registration/body and stub declaration/__all__ entry. Keep the now-unreferenced r2.rs file until the explicit dead-file task t03; do not delete a fourth production file here. Rust S3 signing tests use generic S3 endpoints, not R2-specific identity. Preserve OCI-owned 8 MiB accumulation and generic S3 endpoint behavior.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Native rejected URI | ObjectStoreEngine::from_url("r2://bucket", ...) | StorageError::InvalidUrl unsupported scheme |
| Stub/native parity | Build extension; stubtest and native attribute introspection | No py_chunk_r2_uniform_parts attribute or stub; all remaining symbols match |
| OCI regression | Run incremental OCI and signing canonical tests | All exact size/payload assertions remain green |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/_storage.pyi`
- `uv run --no-sync mypy src/py/vertebra/_storage.pyi`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.
