---
type: Task
id: "driver-conformance-ci:ch05:t03"
chapter_id: ch05-docs-and-okf
title: Synchronize OKF testing, workflow, and data chapters
description: Rewrite the database driver conformance section of the test matrix chapter, add make db-conformance and the lane variables to the workflow chapter, and document the DuckDB secret lifecycle and GCS provider seam in the data drivers chapter.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-conformance-ci:ch05:t02"
read_set:
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - src/py/tests/integration/conformance_gate.py
  - .github/workflows/ci.yml
files:
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests: []
estimated_loc: 120
plan_revision: 2
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
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Database driver conformance section
  - id: workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Canonical Makefile commands
  - id: drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Driver contracts and conformance section
  - id: gate
    resource: src/py/tests/integration/conformance_gate.py::ConformanceGate
    title: Conformance gate (ch01 t01)
tags:
  - okf
  - testing
---

# Task t03: Synchronize OKF Testing, Workflow, and Data Chapters

## Objective & Scope Boundary

1. `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`: rewrite `## Database driver conformance` in present tense covering the engine set (including `mariadb`), offline skip reasons, `ConformanceGate` selection and zero-skip semantics, `driver_engine` tagging, `ServiceVersionMatrix` variables, the engine live suites (`test_mysql_native.py`, `test_mssql_native.py`, `test_oracle_native.py`, `test_duckdb_native.py`), and the `test-db-conformance` CI matrix; add `make db-conformance` and the updated `make oracle-test` rows to the targets table; add `sources:` entries and `[^id]` footnotes for `conformance_gate.py` and the new suites; append a `verified:` entry [^test-matrix] [^gate].
2. `.agents/bundles/knowledge/workflow.md`: add `make db-conformance` with its variables to the canonical commands block and a short paragraph on running one live lane locally (Docker required); remove `--group duckdb` from the `make install`, `make upgrade`, and `make py-test` comments and rewrite the dependency-group isolation item so PyPI `duckdb` installs only through `--group bench` (`ch04 / t03`) [^workflow]. In `polyglot-test-matrix.md`, replace the `PY_TEST_GROUPS` paragraph with a statement that test targets install no PyPI `duckdb` [^test-matrix].
3. `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`: under `## Database Driver Conformance` (or the DuckDB subsection), document the `value_provider` rotation triggers, `(HTTP 401)`/`(HTTP 403)` invalidation, validation rules, and the `vertebra.storage` GCS provider seam keeping the engine adapter cloud-agnostic; add sources/footnotes and a `verified:` entry [^drivers].

Out of scope: limitations record and indexes (`t04`).

## Bounded Read Manifest

- The three OKF chapters above [^test-matrix] [^workflow] [^drivers].
- `src/py/tests/integration/conformance_gate.py` — exact names [^gate].
- `.github/workflows/ci.yml` — matrix rows.

## Symbol & Signature Contracts

- Every cited symbol uses `path/to/file::Symbol` in `sources:` and footnotes; links are relative from the chapter file.
- `verified:` entries use `by: agent:flow-implement`, the implementation timestamp, and the HEAD commit at edit time (an ancestor of HEAD).

## Edge Cases & Error Semantics

- Remove the "eight adapters" count wording; describe the matrix without numeric labels.
- No historical narrative; no reviewer IDs; no internal product names.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` through `make review-check` (frontmatter, link integrity, provenance ancestry, privacy).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Gate documented | `git grep -n "VERTEBRA_CONFORMANCE_ENGINES" -- .agents/bundles/knowledge/testing/polyglot-test-matrix.md .agents/bundles/knowledge/workflow.md` | Matches in both |
| Count label removed | `git grep -n "eight adapters" -- .agents/bundles/knowledge/testing/polyglot-test-matrix.md` | No matches |
| Provider seam documented | `git grep -n "SyncGcsBearerSecretProvider" -- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` | One or more matches |

## Verification

- `make review-check`
- `! git grep -n "eight adapters" -- .agents/bundles/knowledge/testing/polyglot-test-matrix.md`

## Acceptance Criteria

- [ ] Test matrix, workflow, and data driver chapters describe the shipped gate, lanes, suites, and DuckDB secret lifecycle (R15).
- [ ] `make review-check` exits `0` (R15).

## Notes & Discoveries

- [2026-11-12 00:30] The test matrix targets table also lists stale extension sets for `make dev`/`py-test`; refresh those rows to the current ten extensions while editing the table.

[^test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Conformance section.
[^workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical commands.
[^drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Driver contracts chapter.
[^gate]: `src/py/tests/integration/conformance_gate.py::ConformanceGate` — Conformance gate.
