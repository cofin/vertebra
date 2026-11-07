---
type: Task
id: storage-completeness:ch09:t01
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 claims from public navigation and reference
description: Remove R2 claims from public navigation and reference
status: stable
state: completed
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch07:t03
- storage-completeness:ch08:t03
read_set:
- docs/index.md
- docs/usage/index.md
- docs/reference/storage.md
- src/py/tests/unit/tools/test_docs_tooling.py
- docs/usage/storage.md
files:
- docs/index.md
- docs/usage/index.md
- docs/reference/storage.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 60
plan_revision: 5
plan_commit: e3e344b5
state_revision: 0
claimed_by: agent:r2-docs-t01
claimed_at: '2026-11-07T23:35:35.133022+00:00'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T23:37:29.484590+00:00'
last_verified_commit: 7802c676
verification_evidence: |
  Preflight passes at 43654f61: worksheet and chapter are stable revision 5,
  approval e3e344b5 is an ancestor, and completed predecessors 38757809
  (ch07:t03) and c7868b02 (ch08:t03) are ancestors. All three owned docs and
  the canonical docs-tooling module exist; read_set/files remain unchanged.
  Native artifacts reuse completed ch08:t02 checkpoint 4614213f, which records
  the successful all-five CPython 3.12 extension build after native R2 removal.
  ch08:t03 deletes only an unregistered native file; no redundant make dev runs.
  Baseline: UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs
  exits 2 because mkdocstrings cannot collect vertebra.storage._store.R2Config.
  Log: /tmp/vertebra-ch09-t01-docs-baseline.log.
  Baseline: UV_NO_SYNC=1 flock /tmp/vertebra-flow-shm-tests.lock uv run
  --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q exits 0
  (28 passed). Log: /tmp/vertebra-ch09-t01-tests-baseline.log.
  Final: UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs
  exits 0: docstring/CLI checks, Zensical, postbuild and artifact verification
  pass. Log: /tmp/vertebra-ch09-t01-docs-final.log.
  Final: UV_NO_SYNC=1 flock /tmp/vertebra-flow-shm-tests.lock uv run
  --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q exits 0
  (28 passed). Log: /tmp/vertebra-ch09-t01-tests-final.log.
  UV_NO_SYNC=1 uv run --no-sync python -m mypy.stubtest vertebra._storage
  exits 0 (1 module). Log: /tmp/vertebra-ch09-t01-stubtest.log.
  UV_NO_SYNC=1 uv run --no-sync ruff check
  src/py/tests/unit/tools/test_docs_tooling.py exits 0.
  Log: /tmp/vertebra-ch09-t01-ruff.log.
  UV_NO_SYNC=1 make review-check exits 0.
  Log: /tmp/vertebra-ch09-t01-review-check.log.
  UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup validate
  --rules bundles,privacy exits 0.
  Log: /tmp/vertebra-ch09-t01-bundles-privacy.log.
  Read-only final link/API probe exits 0: every owned Markdown link and API
  directive resolves, all seven retained backend configurations export, and a
  real S3Config preserves its generic custom endpoint option. No R2 entry
  remains in the three owned pages. Log: /tmp/vertebra-ch09-t01-surface-links.log.
  git diff --check exits 0. Documentation diff is +3/-8 across exactly the
  three owned public pages; no tests change. No platform limitation is observed
  for Linux CPython 3.12 documentation validation; no cloud/platform runtime
  acceptance is claimed. Coordinator owns signed commit, final task state and
  chapter/root synchronization. No Git mutation or hosted action is performed.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T23:37:29.484590+00:00'
commit: 7802c676
sources:
- id: implementation
  resource: docs/index.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 claims from public navigation and reference

## Objective & Scope Boundary

Remove R2 claims from public navigation and reference. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `docs/index.md` — owned implementation or canonical regression surface.
- `docs/usage/index.md` — owned implementation or canonical regression surface.
- `docs/reference/storage.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.
- `docs/usage/storage.md` — collaborating contract.

## Symbol & Signature Contracts

Remove first-class R2 entries from object-storage provider tables and reference configuration list. Preserve S3 custom endpoint wording and every remaining provider. The user guide's detailed behavior is owned by ch04:t03, not duplicated here. Keep API reference symbols matching installed exported classes.

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
- [x] These three public documents are the owned documentation manifest items.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The three public provider/reference pages expose the seven retained backends. The removed R2Config directive is the baseline build failure; removing that directive restores the full documentation build. Generic S3 endpoint configuration remains intact. The coordinator owns the signed local commit and final task/checklist synchronization.

Revision 5 is independently approved at e3e344b5; the root documentation manifest correction is committed at 49cbfcbb. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `docs/index.md` — Current repository source for this task.
