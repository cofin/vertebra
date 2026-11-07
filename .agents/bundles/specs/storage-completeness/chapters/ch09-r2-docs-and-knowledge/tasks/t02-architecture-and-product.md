---
type: Task
id: storage-completeness:ch09:t02
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 from architecture and product capability claims
description: Remove R2 from architecture and product capability claims
status: stable
state: completed
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch09:t01
- storage-completeness:ch07:t03
- storage-completeness:ch08:t03
read_set:
- docs/architecture/pyo3-and-workers.md
- .agents/bundles/product/product.md
- .agents/bundles/knowledge/domains/index.md
- src/py/tests/unit/tools/test_docs_tooling.py
files:
- docs/architecture/pyo3-and-workers.md
- .agents/bundles/product/product.md
- .agents/bundles/knowledge/domains/index.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 65
plan_revision: 5
plan_commit: e3e344b5
state_revision: 0
claimed_by: agent:r2-docs-t02
claimed_at: '2026-11-07T23:42:15Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T23:49:37Z'
last_verified_commit: 4867dfb9
verification_evidence: |
  R2 withdrawal source checkpoint 06bfc64f is an ancestor of repair baseline
  34113093. Its completed validation evidence below remains applicable to the
  unchanged native surface and canonical docs-tooling tests.
  Preflight passes at e178deca: worksheet/chapter are stable revision 5,
  approval e3e344b5 and completed dependencies 7802c676 (ch09:t01),
  38757809 (ch07:t03) and c7868b02 (ch08:t03) are ancestors. All owned
  paths and the canonical docs-tooling module exist; read_set/files are unchanged.
  Native artifacts reuse completed ch08:t02 checkpoint 4614213f, which records
  the successful all-five CPython 3.12 extension build after native R2 removal.
  ch08:t03 deletes only an unregistered file; no redundant make dev runs.
  UV_NO_SYNC=1 uv run --no-sync python -m mypy.stubtest vertebra._storage
  exits 0 (1 module). Log: /tmp/vertebra-ch09-t02-stubtest.log.
  UV_NO_SYNC=1 uv run --no-sync ruff check
  src/py/tests/unit/tools/test_docs_tooling.py exits 0.
  Log: /tmp/vertebra-ch09-t02-ruff.log.
  UV_NO_SYNC=1 flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync
  pytest src/py/tests/unit/tools/test_docs_tooling.py -q exits 0
  (28 passed). Log: /tmp/vertebra-ch09-t02-tests.log.
  UV_NO_SYNC=1 make review-check exits 0.
  Log: /tmp/vertebra-ch09-t02-review-check.log.
  UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs exits 0:
  docstring/CLI checks, Zensical, postbuild and artifact verification pass.
  Log: /tmp/vertebra-ch09-t02-docs.log.
  UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup validate
  --rules bundles,privacy exits 0.
  Log: /tmp/vertebra-ch09-t02-bundles-privacy.log.
  Read-only installed API/link probe exits 0: all seven retained provider
  configs and native HMAC/S3/GCS/Azure/OCI signing functions export; generic S3
  endpoint configuration remains intact; R2Config/from_r2/native helper are
  absent. All 85 relative links resolve; no R2 claim remains in the three owned
  pages. Log: /tmp/vertebra-ch09-t02-surface-links.log.
  git diff --check exits 0. R2 withdrawal documentation diff at 06bfc64f is +3/-3 across
  exactly the three owned architecture/product/catalog pages; no tests change.
  Linux CPython 3.12 documentation validation is verified; other platforms and
  live cloud runtime acceptance are not claimed. Coordinator owns signed commit,
  final task state and chapter/root synchronization. No Git mutation or hosted
  action is performed; commit and last_verified_commit remain null for handoff.
  The bounded adjacent export correction changes only the same Object Storage
  paragraph in product.md (+1/-1). It places SyncGcpTokenCache,
  AsyncGcpTokenCache and GcpTokenSnapshot in Python exports rather than the
  vertebra._storage export list. The runtime probe verifies each is absent from
  vertebra._storage and exported by vertebra.storage as the exact
  vertebra._vertebra class. Its 85-link/R2-claim check exits 0.
  Log: /tmp/vertebra-ch09-t02-export-surface-links.log.
  Fresh UV_NO_SYNC=1 flock /tmp/vertebra-flow-docs-build.lock make docs exits 0
  with docstring/CLI checks, Zensical, postbuild and artifact verification.
  Log: /tmp/vertebra-ch09-t02-export-docs.log.
  Fresh UV_NO_SYNC=1 uv run --no-sync python -m tools.scripts.review_setup
  validate --rules bundles,privacy exits 0.
  Log: /tmp/vertebra-ch09-t02-export-bundles-privacy.log.
  Fresh UV_NO_SYNC=1 make review-check exits 0.
  Log: /tmp/vertebra-ch09-t02-export-review-check.log.
  git merge-base --is-ancestor 06bfc64f HEAD and git diff --check exit 0.
  No tests or compiled sources change in this correction; no redundant native
  rebuild or canonical test rerun is needed. Original Linux CPython 3.12 scope
  and platform/cloud acceptance limits remain. Coordinator owns repair commit.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T23:49:37Z'
commit: 4867dfb9
sources:
- id: implementation
  resource: docs/architecture/pyo3-and-workers.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from architecture and product capability claims

## Objective & Scope Boundary

Remove R2 from architecture and product capability claims. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `docs/architecture/pyo3-and-workers.md` — owned implementation or canonical regression surface.
- `.agents/bundles/product/product.md` — owned implementation or canonical regression surface.
- `.agents/bundles/knowledge/domains/index.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

Remove first-class R2 from native extension capability summaries, product supported-backend declarations and domain knowledge catalog. Keep HMAC, S3, Azure, GCS and OCI signing claims intact where implemented. Do not rewrite unrelated changelog history, email model labels or unrelated r2 variable substrings.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Capabilities reflect APIs | Search owned object-storage provider claims and build docs | No first-class R2 advertised; other capability descriptions retained |

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
- [x] These three architecture/product/catalog files are the owned documentation manifest items.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The three owned pages remove first-class R2 capability/configuration entries.
The Object Storage product paragraph places shared GCP token caches and snapshots
under the Python exports, matching runtime class identity. All remaining provider
and HMAC signing descriptions retain their existing text.
The coordinator owns the signed local commit and final task/checklist synchronization.

Revision 5 is independently approved at e3e344b5; the root documentation manifest correction is committed at 49cbfcbb. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `docs/architecture/pyo3-and-workers.md` — Current repository source for this task.
