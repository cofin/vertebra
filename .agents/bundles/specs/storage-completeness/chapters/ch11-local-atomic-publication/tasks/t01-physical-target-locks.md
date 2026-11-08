---
type: Task
id: storage-completeness:ch11:t01
chapter_id: ch11-local-atomic-publication
title: Add persistent OS locks per physical parent directory
description: Add persistent OS locks per physical parent directory
status: stable
state: completed
priority: P1
verification_strategy: characterization
depends_on:
- storage-completeness:ch12:t04
- storage-completeness:ch10:t01
- storage-completeness:ch10:t03
- storage-completeness:ch10:t04
read_set:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/store.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/src/local_publication.rs
estimated_loc: 200
plan_revision: 6
plan_commit: 9ead0d1d
state_revision: 0
claimed_by: agent:runtime-ipc-specialist
claimed_at: '2026-11-08T00:43:03+00:00'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-08T00:48:13+00:00'
last_verified_commit: 8c808beb
verification_evidence: |
  Linux x86_64 native source checkpoint: 8c808beb.
  All Cargo commands acquire /tmp/vertebra-flow-storage-cargo.lock first and set
  CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" and
  PYO3_PYTHON="$PWD/.venv/bin/python3.12". Runtime commands additionally acquire
  /tmp/vertebra-flow-shm-tests.lock before uv run --no-sync cargo.
  Characterization before the adapter: cargo test --manifest-path src/rs/Cargo.toml
  -p vertebra-storage --lib local_publication: exit 0, 3 tests including the subprocess entrypoint.
  Final focused gate: same command, exit 0, 7 tests, including independently
  launched LocalPublication child, physical alias deduplication, opposing order,
  cancellation after one acquired guard, read-only existing lockfile, and real
  permission denial. Subprocess markers/stdin and direct future polling are barriers.
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets
  -- -D warnings: exit 0. git diff --check: exit 0.
  cargo fmt --manifest-path src/rs/Cargo.toml --all --check: final exit 0.
  The initial exit 1 concerns concurrent GCS formatting and shared module order,
  corrected by that file owner before this final gate.
  Implementation is 129 lines plus 2 registration lines, within 250. The module
  remains cfg(test); no production ObjectStoreEngine routing is claimed.
  macOS and Windows runtime/build evidence is unavailable on this Linux host;
  only x86_64-unknown-linux-gnu is installed. Windows implementation uses the
  declared windows-sys FileIdInfo API and full volume/file identity.
  The coordinator owns signed commit creation and final state/SHA synchronization.
  All task-owned writes and subprocess/build sessions are quiescent for commit.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-08T00:48:13+00:00'
commit: 8c808beb
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

# Add persistent OS locks per physical parent directory

## Objective & Scope Boundary

Add persistent OS locks per physical parent directory. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/local_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Declare `#[cfg(test)] mod local_publication;` in lib.rs. This task implements real filesystem primitives and inline tests, not production engine routing; t03 removes that gate when the first real caller lands. No broad dead-code allowance or public test hook is introduced. `DirectoryIdentity` is a private comparable physical-directory identity. `DirectoryGuard` owns its opened persistent lockfile for its entire lifetime and releases the OS lock on Drop. `LocalPublication::new(store: Arc<LocalFileSystem>, root: PathBuf) -> Self` retains the exact concrete SDK store/root; `async fn lock_parents(&self, paths: &[PathBuf]) -> Result<Vec<DirectoryGuard>, StorageError>` resolves, deduplicates and sorts physical parent identities before acquiring each lock. Retain all acquired guards inside this future so cancellation releases them.

Use `std::fs::File::try_lock()` in a cancellable async wait loop with bounded `tokio::time::sleep`, treating only WouldBlock as contention and propagating other errors. File opening/metadata work may use bounded blocking jobs, but those jobs never acquire or wait for locks; no abandoned blocking lock waiter can retain a guard after cancellation. Unix identity uses stable MetadataExt dev/ino; Windows uses declared windows-sys `GetFileInformationByHandleEx(FileIdInfo)` / `FILE_ID_INFO` on an opened directory handle with `OpenOptionsExt::custom_flags(FILE_FLAG_BACKUP_SEMANTICS)`, never unstable std windows_by_handle methods. Capture the returned volume serial and full file ID for deterministic identity/order; propagate unsupported-filesystem or permission failures rather than inventing an alias hash. Platform runtime evidence is recorded separately.

Grounded contract evidence: [^research].

Private `local_publication.rs` introduces LocalPublication and RAII DirectoryGuard. Every physical destination parent directory uses the persistent SDK-illegal filename `.vtb-cas-lock#0` inside that directory. Use native OS cross-process File::lock/unlock semantics, not a Python/process mutex or OS-temp hash/registry. A single parent-directory lock serializes all leaf spellings including case/Unicode aliases without reimplementing filesystem name equivalence. Canonicalize/open the physical parent; two-directory mutations deduplicate and sort stable physical directory identities (Unix device/inode; Windows volume/file ID) before acquisition, with deterministic ordering on macOS/Linux/Windows. Overlapping LocalConfig roots and symlink aliases therefore converge on the same physical lockfile. Acquire lockfiles using SDK-normal create/umask access modes and existing-file read-lock compatibility; do not require extra user-managed infrastructure or chmod policy. Real permission/unsupported-filesystem errors propagate clearly. Never unlink/recreate persistent lockfiles: that introduces lock ABA. Stage payload outside the lock; acquire only for destination predicate, atomic publication and actual resulting metadata. t03 routes every cooperating Local mutation through this seam; direct uncooperative OS writers remain outside the guarantee.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Physical identity primitive | Open two aliases/overlapping roots for one real directory and acquire its guard in independent subprocesses | Same persistent lock inode/file ID enforces exclusion; no engine mutation routing is claimed yet. |
| Ordered acquisition primitive | Opposite ordered path vectors include duplicate aliases | Real OS guards deduplicate physical identities and acquire in the same order without deadlock. |
| Cancellation primitive | Another process holds a lock; cancel a waiting acquisition future, release holder, then reacquire | No orphan waiter takes/retains the lock; all previously acquired guards release. |
| Native platform behavior | Inline real filesystem/subprocess lock tests on supported Linux/macOS/Windows | OS exclusion and permission failures remain observable; unsupported platform/runtime evidence is stated. |

## Verification

Characterize real OS lock behavior and filesystem identities before introducing the private adapter, then run inline tests against the actual primitive. Public ObjectStoreEngine overlap/copy/rename acceptance belongs to t03/t04 after routing exists; no fabricated Red or mocked lock is accepted. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

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

Linux characterization confirms cross-process exclusion through a symlink and persistence after handle release. The adapter retains its exact SDK Arc/root, sorts and deduplicates physical parent identities, and keeps acquired guards in the cancellable future. File creation uses create_new with ordinary umask; existing files open read-only. No blocking lock waiter or lockfile unlink is introduced.

The bounded implementation read manifest contains five source/research paths. Operational prerequisite probes separately inspect src/rs/vertebra-storage/Cargo.toml and src/rs/Cargo.toml for declared Windows features and dependency versions, plus rustc/rustup for the native toolchain. These configuration-only probes provide no additional implementation context; the coordinator classifies them as operational preflight separately from the implementation read bound.


Revision 5 is independently approved at bdc7799a; production execution follows the declared task dependencies. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/local_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
