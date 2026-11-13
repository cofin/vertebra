---
type: Spec
flow_id: driver-conformance-ci
chapter_id: ch04-docs-and-okf-sync
title: "Chapter 04: User Documentation, Workflow, Testing Matrix, and Limitations OKF Synchronization"
description: "Document SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider, update database/changelog/AGENTS docs, synchronize testing and workflow OKF chapters including benchmarks-and-free-threaded.md, and update database-driver-limitations.md in place."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - documentation
  - okf
  - conformance
  - duckdb
  - limitations
sources:
  - id: doc-storage
    resource: docs/usage/storage.md
    title: "Object storage user guide"
  - id: doc-analytics
    resource: docs/usage/analytics-and-cdc.md
    title: "Analytics, DuckDB secrets, and CDC user guide"
  - id: doc-databases
    resource: docs/usage/databases.md
    title: "Database drivers user guide"
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: "Polyglot test matrix and conformance OKF chapter"
  - id: okf-bench-ft
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: "Benchmarks and free-threaded Python OKF chapter"
  - id: okf-driver-limits
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Database driver limitations OKF chapter updated in place"
---

# Specification: Chapter 04 — User Documentation, Workflow, Testing Matrix, and Limitations OKF Synchronization

<!-- truth: start -->
- **User & Reference Docs (`t01`–`t02`)**: `docs/usage/storage.md`, `docs/usage/analytics-and-cdc.md`, and `docs/reference/storage.md` document `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` and DuckDB secret validation/invalidation; `docs/usage/databases.md`, `docs/changelog/index.md`, and `AGENTS.md` (`<= 100L`) align native wheels, `make db-conformance`, `make oracle-test`, and `bench`-only PyPI `duckdb` [^doc-storage] [^doc-analytics] [^doc-databases].
- **OKF Sync Without Duplicate Limitations Files (`t03`–`t04`)**: `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md` (removing stale `PY_TEST_GROUPS` `--group duckdb` prose), `workflow.md`, `database-drivers-and-dialers.md`, and existing `database-driver-limitations.md` (`<= 200L` each) synchronize in present tense with zero duplicate `driver-conformance-limitations.md` file [^okf-test-matrix] [^okf-bench-ft] [^okf-driver-limits].
<!-- truth: end -->

## 1. Specification, Context & Provenance

After `ch01`–`ch03` land GCS secret providers, Oracle AQ liveness, live conformance suites, `make db-conformance`, and `bench`-only `duckdb` isolation, `11` user doc, agent, and OKF files require present-tense updates—including `benchmarks-and-free-threaded.md` (which previously claimed `PY_TEST_GROUPS` adds `--group duckdb` on GIL builds) and existing `database-driver-limitations.md` [^okf-bench-ft] [^okf-driver-limits].

## 2. Requirements, Concrete Contracts & Architecture

- **Public Docs Rules (`t01`–`t02`)**: Linked `pymdownx.tabbed` install snippets (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`), `4`–`6` node conceptual Mermaid diagrams, concise `msgspec.Struct` examples without `frozen=True, gc=False` boilerplate, `H2+H3 <= 14` and reference HTML `<= 120,000B` on `docs/reference/storage.md`, and `AGENTS.md <= 100` lines.
- **OKF Rules (`t03`–`t04`)**: Every OKF chapter `<= 200` lines, truth blocks `<= 40` lines, multi-line YAML frontmatter (`verified[].commit` ancestor of `HEAD`), 1:1 `sources:`-to-footnote parity, zero standalone `---` body rules, and in-place update of `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` removing the resolved DuckDB GCS secret-provider gap and `oracle-test` stub caveat while recording:
  1. DuckDB `(HTTP 401)`/`(HTTP 403)` credential invalidation matches DuckDB `httpfs` error text (`\(HTTP (?:401|403)\)`) without auto-retrying the failed statement.
  2. Live `httpfs` integration test (`VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1`) requires network access on first extension download when not pre-cached.
  3. `conformance-oracle` (`oracle_23ai_service`) runs with `continue-on-error: true` in GitHub Actions due to container image size and x86_64 runner constraints.

## 3. Documentation & OKF Knowledge Plan

- `t01`: `docs/usage/storage.md`, `docs/usage/analytics-and-cdc.md`, `docs/reference/storage.md`
- `t02`: `docs/usage/databases.md`, `docs/changelog/index.md`, `AGENTS.md`
- `t03`: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/bundles/knowledge/workflow.md`
- `t04`: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/database-driver-limitations.md`

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R6` Storage & analytics user/ref docs | `t01` | `docs/usage/{storage,analytics-and-cdc}.md`, `docs/reference/storage.md` | `make docs` |
| `R6` Databases doc, changelog & `AGENTS.md` | `t02` | `docs/usage/databases.md`, `docs/changelog/index.md`, `AGENTS.md` | `make docs && make review-check` |
| `R6` Testing & workflow OKF chapters | `t03` | `knowledge/testing/{polyglot-test-matrix,benchmarks-and-free-threaded}.md`, `knowledge/workflow.md` | `make review-check` |
| `R6` Data drivers & limitations OKF | `t04` | `knowledge/domains/data/{database-drivers-and-dialers,database-driver-limitations}.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [ ] [t01-gcs-secret-provider-user-docs.md](tasks/t01-gcs-secret-provider-user-docs.md) — Document `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` and DuckDB secret rotation across `storage.md`, `analytics-and-cdc.md`, and `docs/reference/storage.md`.
- [ ] [t02-databases-changelog-and-agents-docs.md](tasks/t02-databases-changelog-and-agents-docs.md) — Synchronize `docs/usage/databases.md`, `docs/changelog/index.md`, and `AGENTS.md` (`<= 100L`).
- [ ] [t03-testing-and-workflow-okf-sync.md](tasks/t03-testing-and-workflow-okf-sync.md) — Synchronize `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, and `workflow.md` (`<= 200L` each).
- [ ] [t04-data-drivers-and-limitations-okf-sync.md](tasks/t04-data-drivers-and-limitations-okf-sync.md) — Synchronize `database-drivers-and-dialers.md` and update `database-driver-limitations.md` in place (`<= 200L` each).

## 6. Current State & Remaining Plan Snapshot

- **Fast `< 60s` Offline Chapter Gate**: `make docs && make review-check` (offline Zensical build + `verify_docs_artifacts.py` + OKF/link/SHA validation).

[^doc-storage]: `docs/usage/storage.md` — Object storage user guide.
[^doc-analytics]: `docs/usage/analytics-and-cdc.md` — Analytics, DuckDB secrets, and CDC user guide.
[^doc-databases]: `docs/usage/databases.md` — Database drivers user guide.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix and conformance OKF chapter.
[^okf-bench-ft]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — Benchmarks and free-threaded Python OKF chapter.
[^okf-driver-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database driver limitations OKF chapter updated in place.
