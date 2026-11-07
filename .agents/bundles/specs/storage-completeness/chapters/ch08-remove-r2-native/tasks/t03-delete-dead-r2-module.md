---
type: Task
id: storage-completeness:ch08:t03
chapter_id: ch08-remove-r2-native
title: Delete the unreferenced native R2 module
description: Delete the unreferenced native R2 module
status: stable
state: pending
priority: P1
verification_strategy: characterization
depends_on:
- storage-completeness:ch08:t02
read_set:
- src/rs/vertebra-storage/src/r2.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
- .agents/bundles/knowledge/domains/storage/object-storage.md
files:
- src/rs/vertebra-storage/src/r2.rs
- .agents/bundles/knowledge/domains/storage/object-storage.md
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 110
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
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Owned implementation seam
tags:
- storage
- repair
---

# Delete the unreferenced native R2 module

## Objective & Scope Boundary

Delete the unreferenced native R2 module. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/r2.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — owned durable provenance and relative source links.

## Symbol & Signature Contracts

Delete `src/rs/vertebra-storage/src/r2.rs` only after native module/import/helper withdrawal. Verify no active module import/registration or durable knowledge source link points to r2.rs, and keep OCI accumulator in oci.rs. Update all R2 module links, sources and footnotes in the owned storage knowledge page to the retained OCI implementation in the same change. This worksheet and its predecessor read_set/files fields may retain the deleted path as execution/deletion input until flow archive; they are not live source links. Their sources/footnotes instead cite retained OCI ownership. Do not remove generic S3 endpoint or signing support. The production file count includes this deletion.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| No dead provider module | Build storage crate after deleting r2.rs | Compile succeeds; OCI and generic S3 tests remain green |
| No public R2 surface | Native and Python config/import tests | No first-class R2 provider API is exposed |

## Verification

Record the passing native build/test baseline before this behavior-preserving dead-module deletion, then verify the same gates afterward. Do not manufacture a failing source/file-existence test; public R2 withdrawal regressions already belong to t02. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

Also execute `uv run --no-sync python -m tools.scripts.review_setup validate` for native bundle/privacy validation, then validate every relative source link in the owned knowledge page:

```bash
uv run --no-sync python - <<'PYLINK'
from pathlib import Path
import re
page = Path(".agents/bundles/knowledge/domains/storage/object-storage.md")
for target in re.findall(r"\]\(([^)]+)\)", page.read_text()):
    target = target.split("#", 1)[0]
    if target and "://" not in target:
        assert (page.parent / target).resolve().exists(), target
PYLINK
```

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Revision 4 is independently approved by the fresh consolidated ch07/ch08 plan review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/oci.rs` — Current repository source for this task.
