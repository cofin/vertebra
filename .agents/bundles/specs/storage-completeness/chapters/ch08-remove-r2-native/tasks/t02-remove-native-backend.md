---
type: Task
id: storage-completeness:ch08:t02
chapter_id: ch08-remove-r2-native
title: Delete native R2 builder, module, helper and stub surface
description: Delete native R2 builder, module, helper and stub surface
status: stable
state: completed
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
plan_commit: ddfb11f2
state_revision: 0
claimed_by: agent:r2-native-withdrawal
claimed_at: '2026-11-07T23:12:59Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T23:14:59Z'
last_verified_commit: 4614213f
verification_evidence: |
  Signed native source and canonical test checkpoint 4614213f.
  Red: the canonical tests test_native_r2_url_is_unsupported_with_or_without_s3_options
  and test_native_python_r2_surface_is_withdrawn fail before production edits:
  Rust accepts r2://bucket and the real native SyncObjectStore constructor
  accepts it instead of raising ValueError. No SUT or collaborator is mocked.
  Red command exits 101 (2 failed, 23 filtered out):
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12"
  flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test
  --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests
  test_native_ -- --nocapture
  Green command exits 0 (25 passed, 0.35s), preserving OCI exact multipart
  sizes/payload and generic S3 custom-endpoint signing/verification assertions:
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12"
  flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test
  --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests
  Formatting command exits 0:
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12"
  cargo fmt --manifest-path src/rs/Cargo.toml --all --check
  Clippy command exits 0 (all targets, warnings denied):
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12"
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage
  --all-targets -- -D warnings
  Native rebuild command exits 0 and installs all five CPython 3.12 extensions
  (_vertebra, _bundler, _storage, _bigquery, _spanner) from this worktree:
  UV_NO_SYNC=1 CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev
  Post-build commands exit 0:
  uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler
  vertebra._storage vertebra._bigquery vertebra._spanner (5 modules)
  uv run --no-sync ruff check src/py/vertebra/_storage.pyi
  uv run --no-sync mypy src/py/vertebra/_storage.pyi (1 source file)
  A fresh uv run --no-sync python probe verifies installed native SyncObjectStore
  and AsyncObjectStore both reject r2://bucket with the exact unsupported-scheme
  ValueError, with and without explicit generic S3 options; both construct
  s3://bucket against https://objects.example.com with scheme s3; the native
  py_chunk_r2_uniform_parts attribute is absent. Public Python sync/async
  facades also reject r2://bucket. This probe exits 0.
  git diff --check exits 0. Production diff removes 80 lines across the three
  owned files; canonical test diff is +92/-29. r2.rs and oci.rs are unchanged.
  The dead r2.rs file remains for explicit t03 deletion. Linux CPython 3.12 is
  verified; other Python/platform builds and live cloud services are not claimed.
  Signed task receipt is bound to 4614213f; chapter reviews remain pending.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T23:14:59Z'
commit: 4614213f
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

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Revision 4 is independently approved by the fresh consolidated ch07/ch08 plan review. Native R2 dispatch, public helper and stub exports are withdrawn; unsupported R2 URLs retain the existing invalid-URL error mapping. Generic S3 custom endpoints and OCI-owned 8 MiB accumulation remain verified. The unreferenced r2.rs source deletion is complete in t03 at `c7868b02`. This task is bound to signed `4614213f`; chapter correctness and fresh quality reviews remain pending.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.
