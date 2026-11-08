---
type: Task
id: storage-completeness:ch10:t03
chapter_id: ch10-publication-boundary-and-lifecycle
title: Retain multipart abort ownership across completion errors and cancellation
description: Retain multipart abort ownership across completion errors and cancellation
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch10:t01
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/vertebra/storage/_store.py
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 160
plan_revision: 7
plan_commit: 5f64d1a8
state_revision: 0
claimed_by: multipart_lifecycle
claimed_at: '2026-11-08T00:00:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T00:18:10Z'
last_verified_commit: 673cb696
verification_evidence: >-
  Native source against eb76724f: initial public lifecycle Red exits 101 (one
  success baseline passes; five ownership/cleanup regressions fail). Final
  canonical storage_engine_tests exits 0 with 34 passed, including six external
  MultipartUpload-port/public native session lifecycle cases. The cancellation
  Drop case pauses provider abort and proves cancelled-session release precedes
  cleanup, removes only upload ID 1, and preserves upload ID 0 and winning bytes.
  Reserved-target cargo fmt --all --check, storage all-target Clippy -D warnings,
  and git diff --check exit 0. Commands use CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  and PYO3_PYTHON="$PWD/.venv/bin/python3.12"; the runtime suite runs through
  flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test. Production
  additions are 21 LOC in store.rs; lib.rs wrappers need no change. Receipt logs:
  /tmp/vertebra-t03-red.log, /tmp/vertebra-t03-green-full.log,
  /tmp/vertebra-t03-fmt.log, /tmp/vertebra-t03-clippy.log. The coordinator binds
  the final source commit and completes state after its signed task commit.
  Installed extensions are unchanged; native tests compile and execute real
  PyO3 bindings but do not establish live cloud/provider acceptance.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-08T00:18:10Z'
commit: 673cb696
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

# Retain multipart abort ownership across completion errors and cancellation

## Objective & Scope Boundary

Retain multipart abort ownership across completion errors and cancellation. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`MultipartUploadSession::complete` borrows the retained handle through `self.upload.as_mut()` across tail upload and `upload.complete().await`, and clears it only after successful completion. An equivalent RAII restore-on-drop guard is permitted only if it also preserves the handle when the future is cancelled; taking ownership out and restoring only on returned Err is insufficient. Publication implementations retain stage/upload handles until published or cleanup succeeds, with async Drop ownership spawning bounded cleanup on the existing storage runtime for dropped/cancelled sessions. Rust/PyO3 sync and async wrappers leave abort callable after a failed complete; Python put_stream finally invokes abort on producer error or cancellation and never masks original failure. Completed success clears ownership; abort remains idempotent. No shared global Python lock or unbounded Drop blocking.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Cancel during completion | Public native session completes through a barrier-controlled MultipartUpload; cancel the pending completion future after the collaborator entered complete, then call abort on the retained session | Exact owned upload ID remains abortable; abort is observed once/idempotently, original destination is preserved and no handle is lost while the future is dropped. |
| Complete failure | Test MultipartUpload returns error at complete, then abort session | Underlying abort called exactly once; original completion error retained |
| Cancellation | Cancel async complete during provider publication, then drive runtime cleanup | Uncommitted upload/stage cleaned; winning object unchanged |
| Success/idempotence | Complete then abort twice | Published data intact; cleanup no-ops |

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

Completion and abort retain the external upload handle through pending futures.
Only successful completion or successful/NotFound abort clears it. Abort errors
following an earlier operation failure stay suppressed without discarding the
handle, allowing a later abort or Drop cleanup attempt. Session Drop transfers
ownership to a single async abort attempt on STORAGE_RUNTIME with a 30-second
timeout; it never blocks the caller, and provider errors/timeouts remain
best-effort cleanup limits rather than success guarantees. Python put_stream
already catches producer failure/cancellation and attempts abort; _store.py is
unchanged and no Python facade/provider-specific cleanup acceptance is claimed.
The existing sync/async PyO3 wrappers retain the same session and inherit the
native ownership repair without signature or lib.rs changes.

The executable transport staging and provider acceptance contracts are independently approved at 5f64d1a8. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
