---
type: Task
id: storage-completeness:ch11:t02
chapter_id: ch11-local-atomic-publication
title: Manage SDK-private same-parent staging artifacts
description: Manage SDK-private same-parent staging artifacts
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch11:t01
read_set:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/local_publication.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 210
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
  resource: src/rs/vertebra-storage/src/local_publication.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Manage SDK-private same-parent staging artifacts

## Objective & Scope Boundary

Manage SDK-private same-parent staging artifacts. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/local_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`LocalPublication::stage(destination)` creates an exclusive same-parent file named `.vertebra-stage-<uuid>#<counter>` whose base has no `#` and whose FIRST `#` suffix is nonempty ASCII digits. This exactly matches pinned LocalFileSystem's already unsupported/ignored filename grammar; it cannot collide with an SDK-legal object key and both list and list_with_delimiter hide it through every overlapping root. Never append a suffix to the caller's basename because legal `name#suffix` would make the SDK expose the resulting staging file. Use create_new and retry generated-name collision; retain file handle/path ownership. No OS-temp staging registry, global reserved prefix or new legal-key filtering.

Write the payload incrementally outside the persistent parent lock; same-parent staging guarantees same filesystem. Abort/cancellation/drop ownership removes only this session's actual created stage. Successful link/rename consumes or removes stage ownership. Persistent `.vtb-cas-lock#0` is never unlinked. Do not invent broad crash scavenging of similarly named files; any orphan after process death remains in the SDK's excluded keyspace and cleanup guarantees are bounded honestly. Keep normal SDK umask/access behavior. No mmap, whole-file readback, payload Vec or single-PUT size cap.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Artifact isolation | Upload staged object while listing through root and broader overlapping root | SDK-illegal lock/stage names invisible in list and delimiter; legal keep#suffix remains visible |
| Abort cleanup | Cancel multipart and repeat abort | Owned stage removed; persistent lock remains; destination untouched |
| Same filesystem | Stage destination under a mountpoint distinct from OS temp | Final native rename/link succeeds without cross-device copy fallback |

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

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/local_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
