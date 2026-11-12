---
type: Task
id: "driver-conformance-ci:ch05:t04"
chapter_id: ch05-docs-and-okf
title: Record driver conformance limitations and link them from the knowledge indexes
description: Create the driver conformance limitations record listing every verified caveat of the live lanes and DuckDB GCS rotation, and link it from the domains and knowledge indexes.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-conformance-ci:ch05:t03"
read_set:
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/index.md
  - .agents/bundles/specs/driver-conformance-ci/spec.md
files:
  - .agents/bundles/knowledge/domains/data/driver-conformance-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/index.md
tests: []
estimated_loc: 90
plan_revision: 1
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
  - id: precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations record format precedent (type Gotcha, truth block)
  - id: domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domains index data section
  - id: knowledge-index
    resource: .agents/bundles/knowledge/index.md
    title: Knowledge index data section
tags:
  - okf
  - limitations
---

# Task t04: Record Driver Conformance Limitations and Link Them from the Knowledge Indexes

## Objective & Scope Boundary

1. Create `.agents/bundles/knowledge/domains/data/driver-conformance-limitations.md` (`type: Gotcha`, `status: stable`, `tags`, `generated`, `verified`, `sources` with matching footnotes, `<= 40`-line truth block) following the storage limitations record [^precedent]. Record each caveat that holds at implementation time, at minimum:
   - Credential rejection is detected from DuckDB error text `(HTTP 401)` / `(HTTP 403)` (DuckDB exposes no HTTP status field); `403` from permission denial also triggers one extra token mint.
   - Rejected statements are not retried; callers re-run them.
   - Live `httpfs` acceptance installs the extension from the DuckDB extension repository and needs network access; it runs only with `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`.
   - GCS bearer verification runs against a loopback XML endpoint, not a real bucket; the exact request paths observed in `ch02 / t03` are listed.
   - SQL Server images are `linux/amd64` only; Oracle runs only `23ai` Free (`gvenzl/oracle-free:23-slim-faststart`).
   - Free-threaded CI rows cover DuckDB and MySQL 8.4 only.
   - PostgreSQL live conformance is owned by `native-postgres-driver`; `VERTEBRA_CONFORMANCE_ENGINES=postgres` is rejected.
   - Offline MySQL conformance uses the `MySQLWireTestServer` loopback, which proves wire handling only.
   - Any deviation, blocked task, or live failure recorded during ch01–ch04 implementation.
2. Link the record from `.agents/bundles/knowledge/domains/index.md` (data section) and `.agents/bundles/knowledge/index.md` (data list) with one-line present-tense descriptions [^domains-index] [^knowledge-index].

Out of scope: other OKF chapters (`t03`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — format [^precedent].
- `.agents/bundles/knowledge/domains/index.md`, `.agents/bundles/knowledge/index.md` — link placement.
- `.agents/bundles/specs/driver-conformance-ci/spec.md` — caveat inventory and task Notes.

## Symbol & Signature Contracts

- File path and title: `Driver Conformance and Live Verification Limitations`.
- Every caveat cites a `path/to/file::Symbol` source footnote (`conformance_gate.py::ConformanceGate`, `duckdb/driver.py::_CREDENTIAL_REJECTION_PATTERN`, `test_duckdb_native.py::LoopbackGcsEndpoint`, `.github/workflows/ci.yml`).

## Edge Cases & Error Semantics

- Only verified caveats; speculative items are excluded.
- Bundle files carry no standalone horizontal rules and no history.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation` via `make review-check`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Review gate | `make review-check` | Exit `0` |
| Linked from both indexes | `git grep -n "driver-conformance-limitations.md" -- .agents/bundles/knowledge/domains/index.md .agents/bundles/knowledge/index.md` | One match in each |
| Truth block bounded | Count lines between `<!-- truth: start -->` and `<!-- truth: end -->` | `<= 40` |

## Verification

- `make review-check`
- `git grep -n "driver-conformance-limitations.md" -- .agents/bundles/knowledge/domains/index.md .agents/bundles/knowledge/index.md`

## Acceptance Criteria

- [ ] The limitations record lists every verified caveat with source footnotes (R15).
- [ ] Both knowledge indexes link it and `make review-check` exits `0` (R15).

## Notes & Discoveries

- [2026-11-12 00:30] Pull caveats from every completed task's `Notes & Discoveries` before writing the record.

[^precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations record format.
[^domains-index]: `.agents/bundles/knowledge/domains/index.md` — Domains index.
[^knowledge-index]: `.agents/bundles/knowledge/index.md` — Knowledge index.
