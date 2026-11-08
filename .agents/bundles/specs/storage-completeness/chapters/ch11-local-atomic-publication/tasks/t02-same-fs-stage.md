---
type: Task
id: storage-completeness:ch11:t02
chapter_id: ch11-local-atomic-publication
title: Manage SDK-private same-parent staging artifacts
description: Manage SDK-private same-parent staging artifacts
status: stable
state: completed
priority: P1
verification_strategy: characterization
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
- src/rs/vertebra-storage/src/local_publication.rs
estimated_loc: 210
plan_revision: 5
plan_commit: bdc7799a
state_revision: 0
claimed_by: agent:runtime-ipc-specialist
claimed_at: '2026-11-08T00:56:39.799148+00:00'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T00:59:02.565672+00:00'
last_verified_commit: 61a948df
verification_evidence: |
  Linux x86_64 native source checkpoint: 61a948df.
  All Cargo commands acquire /tmp/vertebra-flow-storage-cargo.lock first and set
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" and
  PYO3_PYTHON="$PWD/.venv/bin/python3.12". Runtime commands additionally acquire
  /tmp/vertebra-flow-shm-tests.lock before uv run --no-sync cargo.
  Before primitive implementation: cargo test --manifest-path src/rs/Cargo.toml
  -p vertebra-storage --lib local_publication::tests::same_parent_filesystem_and_sdk_characterization:
  exit 0, 1 actual-filesystem/SDK characterization test. It covers hidden artifacts
  through overlapping roots, both SDK listing APIs, legal keep#suffix, raw percent
  and Unicode paths, native hard-link no-replace and same-parent atomic rename.
  Final focused gate: cargo test --manifest-path src/rs/Cargo.toml -p
  vertebra-storage --lib local_publication: exit 0, 13 tests. This includes retained
  abort ownership after cancellation/error, idempotent abort, owned-only drop
  cleanup, real permission failure, typed predicates against a controlled competing
  native SDK write, actual SDK publication receipts, and copy/rename/delete behavior.
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets
  -- -D warnings: exit 0. cargo fmt --manifest-path src/rs/Cargo.toml --all --check:
  exit 0. git diff --check: exit 0.
  The task adds 192 physical implementation lines and 389 inline test lines in
  one production file. The module remains test-only; t03 owns runtime activation
  and public engine mutation routing. No installed Python acceptance is claimed.
  macOS and Windows runtime/build evidence is unavailable on this Linux host.
  Native final link/rename/remove occurs synchronously under retained guards;
  no detached final-mutation job can outlive cancellation of those guards.
  Coordinator owns signed source commit and final state/SHA synchronization.
  All task-owned writes and build/test sessions are quiescent for commit.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-08T00:59:02.565672+00:00'
commit: 61a948df
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

The module remains test-only until t03. Implement the complete private staging/publication primitive API consumed by t03 without adding engine routing here: `async fn stage(&self, destination: &StorePath) -> Result<LocalStage, StorageError>`; `LocalStage` has `async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), StorageError>` asynchronously appends only this chunk; `async fn publish(&self, stage: &mut LocalStage, destination: &StorePath, condition: &PublishCondition) -> Result<PutResult, StorageError>` obtains ordered parent guards, checks the predicate, atomically publishes and collects the exact SDK destination metadata under the guard. `async fn abort(&mut self) -> Result<(), StorageError>` is idempotent. The owning `LocalStage` retains file/path ownership across returned errors and cancellation; successful publication clears ownership only after its actual receipt is captured. Drop removes only its owned stage, never the persistent lockfile. Complete stage writes and flush outside the lock before publication; native file write/flush/metadata errors remain visible.

Supply private `delete`, `rename` and `copy` mutation methods with StorePath operands and Result<(), StorageError>, retaining ordered guards through only their final mutation. Copy first opens a stable source snapshot and streams it into a same-parent destination stage outside publication locks, then publishes that stage with deduplicated source/destination parent guards. Ordinary puts use the same staging/publish primitives. Under a guard, Create uses native no-replace hard-link/create semantics; Match/NotMatch/Overwrite uses same-filesystem atomic rename only after evaluating actual current SDK metadata. Never fall back from unsupported LocalFileSystem PutMode::Update to an unchecked write. t03 wires these exact primitives into every public mutation and owns no changes to local_publication.rs.

Grounded contract evidence: [^research].

`LocalPublication::stage(destination)` creates an exclusive same-parent file named `.vertebra-stage-<uuid>#<counter>` whose base has no `#` and whose FIRST `#` suffix is nonempty ASCII digits. Use the declared exact-locked uuid dependency and `Uuid::new_v4()` for the unpredictable UUID component. This exactly matches pinned LocalFileSystem's already unsupported/ignored filename grammar; it cannot collide with an SDK-legal object key and both list and list_with_delimiter hide it through every overlapping root. Never append a suffix to the caller's basename because legal `name#suffix` would make the SDK expose the resulting staging file. Use create_new and retry generated-name collision; retain file handle/path ownership. No OS-temp staging registry, global reserved prefix or new legal-key filtering.

Write the payload incrementally outside the persistent parent lock; same-parent staging guarantees same filesystem. Abort/cancellation/drop ownership removes only this session's actual created stage. Successful link/rename consumes or removes stage ownership. Persistent `.vtb-cas-lock#0` is never unlinked. Do not invent broad crash scavenging of similarly named files; any orphan after process death remains in the SDK's excluded keyspace and cleanup guarantees are bounded honestly. Keep normal SDK umask/access behavior. No mmap, whole-file readback, payload Vec or single-PUT size cap.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Artifact isolation primitive | Create/write a real LocalStage while listing via actual LocalFileSystem from narrow and broader roots | Both SDK listing APIs hide the existing-illegal lock/stage grammar; legal keep#suffix remains visible. |
| Stage cleanup primitive | Cancel a staged primitive operation and repeat abort | Only its owned file disappears; destination and persistent lockfile remain. |
| Same-filesystem publish primitive | Stage and publish in a real destination directory distinct from OS temp | Atomic native rename/link succeeds; actual destination receipt matches SDK head while guarded. |
| Predicate primitive | Existing target and Create/Match/NotMatch with a controlled competing real filesystem/native-SDK write | Rejected predicate preserves exact target; successful guarded publication returns its own ETag/version. |

## Verification

Characterize real filesystem stage, link/rename and SDK listing behavior before the private adapter, then verify the inline primitive against those semantics. This task does not claim public engine acceptance before t03 routing; no fake Red or mocked filesystem is accepted. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib local_publication`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Native hard-link publication keeps two names for one inode until owned stage cleanup. Native rename between those same two inode names is a no-op, so the characterization recreates a distinct replacement stage before testing overwrite. SDK path_to_filesystem preserves raw percent and Unicode keys; empty SDK StorePath is not a valid filesystem object path.

Revision 5 is independently approved at bdc7799a; production execution follows the declared task dependencies. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/local_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
