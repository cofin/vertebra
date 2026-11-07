---
type: Task
id: storage-completeness:ch09:t04
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 from repository and embedding overviews
description: Remove R2 from repository and embedding overviews
status: stable
state: completed
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch09:t01
read_set:
- README.md
- docs/reference/index.md
- docs/usage/embeddings-and-vectors.md
- src/py/tests/unit/tools/test_docs_tooling.py
- docs/usage/storage.md
files:
- README.md
- docs/reference/index.md
- docs/usage/embeddings-and-vectors.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 60
plan_revision: 5
plan_commit: e3e344b5
state_revision: 0
claimed_by: agent:r2-docs-t04
claimed_at: '2026-11-07T23:55:16Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T23:57:00Z'
last_verified_commit: 74afde94
verification_evidence: |
  Preflight passes at d50cf6a2: worksheet and chapter match stable revision 5
  and approval e3e344b5; t01 is completed at ancestor 7802c676. The five
  manifest files exist, and read_set/files/tests remain unchanged.
  Native artifacts reuse completed ch08:t02 checkpoint 4614213f and the
  dead-module deletion evidence at c7868b02; no unchanged extension rebuild runs.
  Baseline: UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs
  exits 0: docstring/CLI checks, Zensical, postbuild and artifact validation pass.
  Log: /tmp/vertebra-ch09-t04-docs-baseline.log.
  Baseline: UV_NO_SYNC=1 flock /tmp/vertebra-flow-shm-tests.lock uv run
  --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q exits 0
  (28 passed). Log: /tmp/vertebra-ch09-t04-tests-baseline.log.
  Final: UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs
  exits 0: docstring/CLI checks, Zensical, postbuild and artifact validation pass.
  Log: /tmp/vertebra-ch09-t04-docs-final.log.
  Final: UV_NO_SYNC=1 flock /tmp/vertebra-flow-shm-tests.lock uv run
  --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q exits 0
  (28 passed). Log: /tmp/vertebra-ch09-t04-tests-final.log.
  UV_NO_SYNC=1 uv run --no-sync python -m mypy.stubtest vertebra._storage
  exits 0 (1 module). Log: /tmp/vertebra-ch09-t04-stubtest.log.
  UV_NO_SYNC=1 uv run --no-sync ruff check
  src/py/tests/unit/tools/test_docs_tooling.py exits 0.
  Log: /tmp/vertebra-ch09-t04-ruff.log.
  UV_NO_SYNC=1 make review-check exits 0.
  Log: /tmp/vertebra-ch09-t04-review-check.log.
  UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup validate
  --rules bundles,privacy exits 0.
  Log: /tmp/vertebra-ch09-t04-bundles-privacy.log.
  Read-only final link/API probe exits 0: every owned relative Markdown link
  resolves, all seven retained backend configs export, R2Config is absent,
  and a real S3Config retains its generic custom endpoint. No R2 provider
  claim remains in the three owned pages.
  Log: /tmp/vertebra-ch09-t04-surface-links.log.
  git diff --check exits 0. The production diff is exactly three one-line
  removals (+3/-3); examples, other providers and changelog history are intact.
  No tests change. Linux CPython 3.12 documentation validation has no observed
  platform limitation; no cloud runtime acceptance is claimed. Coordinator
  owns signed commit, final task state and chapter/root synchronization.
  No Git mutation or hosted action is performed.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T23:57:00Z'
commit: 74afde94
sources:
- id: implementation
  resource: README.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from repository and embedding overviews

## Objective & Scope Boundary

Remove R2 from repository and embedding overviews. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `README.md` — owned implementation or canonical regression surface.
- `docs/reference/index.md` — owned implementation or canonical regression surface.
- `docs/usage/embeddings-and-vectors.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.
- `docs/usage/storage.md` — collaborating contract.

## Symbol & Signature Contracts

Remove first-class R2 from the README storage capability list, reference navigation configuration list, and model-bundle staging provider list. Preserve all remaining providers and examples. Keep changelog history intact; the detailed storage guide belongs to ch04:t03.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Provider surface | Build docs and inspect storage references | No R2Config/r2:// supported-provider entry; S3/GCS/Azure/OCI/local/memory/http remain |

## Verification

Validate the owned documentation against the exported API and completed native evidence. Record the documentation build and focused validation results; documentation-only edits do not require an artificial failing public-call regression. Use the verified native build checkpoint from the completed R2 removal tasks without rebuilding unchanged extensions.

- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/tests/unit/tools/test_docs_tooling.py`
- `flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`
- `make review-check`
- `make docs`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] The README, reference index and embedding guide are the three owned documentation manifest items.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The three owned pages each remove one first-class R2 claim. Remaining provider
lists and examples are preserved. The coordinator owns the signed local commit
and final task/checklist synchronization after the shared-worktree write barrier.

Revision 5 is independently approved at e3e344b5; the root documentation manifest correction is committed at 49cbfcbb. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `README.md` — Current repository source for this task.
