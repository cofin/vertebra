---
type: Spec
parent_flow: driver-conformance-ci
chapter_id: ch05-docs-and-okf
title: "Chapter 5: Docs and OKF"
description: User docs for GCS bearer secret providers and DuckDB rotation, truthful database engine docs, changelog and AGENTS.md updates, OKF testing/workflow/data chapters, and a driver conformance limitations record.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch04-make-and-ci-lanes
replan_trigger: null
current_task: "driver-conformance-ci:ch05:t01"
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T00:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T00:30:00Z"
    commit: c8722c76
sources:
  - id: storage-auth-doc
    resource: docs/usage/storage-auth-and-signing.md
    title: Google Cloud credentials and token caches guide
  - id: analytics-doc
    resource: docs/usage/analytics-and-cdc.md
    title: DuckDB secrets and Arrow analytics guide
  - id: databases-doc
    resource: docs/usage/databases.md
    title: Database adapters overview with a stale Python-client statement
  - id: test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: Test matrix and database driver conformance chapter
  - id: limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations record format precedent
tags:
  - documentation
  - okf
---

# Chapter 5: Docs and OKF

<!-- truth: start -->
- User docs show `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` with `DuckDBSecretConfig(secret_type="gcs")`, rotation triggers, credential-rejection invalidation, and the persistent/interval validation rules [^storage-auth-doc] [^analytics-doc].
- `docs/usage/databases.md` states that MySQL, SQL Server, Oracle, PostgreSQL, DuckDB, BigQuery, and Spanner run on native wheels [^databases-doc].
- OKF chapters document `VERTEBRA_CONFORMANCE_ENGINES`, version variables, `make db-conformance`, the CI job, and engine live suites [^test-matrix].
- `.agents/bundles/knowledge/domains/data/driver-conformance-limitations.md` records every caveat in the format of the storage limitations record [^limitations-precedent].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`docs/usage/databases.md` still says "the other live engines use their Python client packages" although MySQL, SQL Server, Oracle, and PostgreSQL run on native wheels, and its DuckDB row claims "active ADC `httpfs` secret rotation" that ch02 makes true [^databases-doc]. The storage auth guide never mentions DuckDB [^storage-auth-doc]. The OKF test matrix describes conformance across "eight adapters" without live lanes [^test-matrix]. No record lists the caveats of live verification (image architecture, `httpfs` network install, message-based credential detection).

### 1.2 Non-Goals

- Rewriting unrelated docs sections; edits are targeted at the paragraphs named in each task.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| User docs | `docs/usage/storage-auth-and-signing.md`, `docs/usage/analytics-and-cdc.md`, `docs/reference/storage.md`, `docs/usage/databases.md`, `docs/changelog/index.md` [^storage-auth-doc] [^analytics-doc] [^databases-doc] | Zensical site built by `make docs`. |
| Agent docs | `AGENTS.md` Quick Verification Commands | Agent entry point. |
| OKF | `.agents/bundles/knowledge/testing/polyglot-test-matrix.md`, `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/index.md`, `.agents/bundles/knowledge/index.md` [^test-matrix] | Validated by `make review-check`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- `flow-docs` and `okf` skills; `.agents/skills/review-docs-okf/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R15**: Docs and OKF describe the shipped behavior (`t01`–`t04`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- Documented public names: `vertebra.storage.SyncGcsBearerSecretProvider`, `vertebra.storage.AsyncGcsBearerSecretProvider`, `DuckDBSecretConfig`, `SyncDuckDBDriver.refresh_secrets`, `make db-conformance`, `VERTEBRA_CONFORMANCE_ENGINES`, `VERTEBRA_CONFORMANCE_{MYSQL,MARIADB,MSSQL,ORACLE}_VERSION`, `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS`, `VERTEBRA_CONFORMANCE_MARIADB_DSN`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Reader → `docs/usage/analytics-and-cdc.md` example → `docs/reference/storage.md` mkdocstrings entries for `vertebra.storage._secrets.SyncGcsBearerSecretProvider` → source docstrings.
- Agent → `AGENTS.md` → `knowledge/workflow.md` → `knowledge/testing/polyglot-test-matrix.md` → `knowledge/domains/data/driver-conformance-limitations.md`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Example drift | Docs example uses a removed name | `make docs` / docstring scan fails | Examples match code |
| Broken OKF link | New limitations record not linked | `make review-check` fails | Index integrity |
| Provenance SHA | `verified.commit` not an ancestor of HEAD | `make review-check` fails | Valid provenance |
| Mermaid size | Diagrams in `docs/` | `4–6` nodes, conceptual | Docs stay high-level |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the finished flow, when `make docs` and `make review-check` run, then both exit `0` and every new name appears in the reference page.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Contributor docs location | `AGENTS.md` + OKF `workflow.md` / `polyglot-test-matrix.md` | A new `docs/` contributor page: user docs stay focused on library usage. |
| Limitations | Dedicated data-domain limitations record | Appending caveats to `database-drivers-and-dialers.md`: buries them in a long chapter. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/12 files documented]`
- `[ ]` `docs/usage/storage-auth-and-signing.md` (`t01`)
- `[ ]` `docs/usage/analytics-and-cdc.md` (`t01`)
- `[ ]` `docs/reference/storage.md` (`t01`)
- `[ ]` `docs/usage/databases.md` (`t02`)
- `[ ]` `docs/changelog/index.md` (`t02`)
- `[ ]` `AGENTS.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/workflow.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/data/driver-conformance-limitations.md` (`t04`)
- `[ ]` `.agents/bundles/knowledge/domains/index.md` (`t04`)
- `[ ]` `.agents/bundles/knowledge/index.md` (`t04`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R15 (storage and DuckDB docs) | [t01](tasks/t01-gcs-secret-provider-user-docs.md) | `documentation_validation` | `make docs` |
| R15 (engine docs, changelog, AGENTS.md) | [t02](tasks/t02-database-docs-changelog-and-agents.md) | `documentation_validation` | `make docs` |
| R15 (OKF chapters) | [t03](tasks/t03-okf-testing-workflow-and-data-sync.md) | `documentation_validation` | `make review-check` |
| R15 (limitations record) | [t04](tasks/t04-driver-conformance-limitations-record.md) | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [ ] driver-conformance-ci:ch05:t01 — Document GCS bearer secret providers and DuckDB rotation in user docs (tasks/t01-gcs-secret-provider-user-docs.md)
- [ ] driver-conformance-ci:ch05:t02 — Correct database engine docs and update the changelog and AGENTS.md (tasks/t02-database-docs-changelog-and-agents.md)
- [ ] driver-conformance-ci:ch05:t03 — Synchronize OKF testing, workflow, and data chapters (tasks/t03-okf-testing-workflow-and-data-sync.md)
- [ ] driver-conformance-ci:ch05:t04 — Record driver conformance limitations and link them from the knowledge indexes (tasks/t04-driver-conformance-limitations-record.md)

**Chapter Recovery Checkpoint:** `make docs && make review-check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, `plan_commit: null`, HEAD `c8722c76`; depends on `ch04-make-and-ci-lanes`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed); `[0/12 files documented]`.
- **Active Invariants**: Targeted section edits only; present tense; no internal product names; `verified.commit` values are ancestors of HEAD.

[^storage-auth-doc]: `docs/usage/storage-auth-and-signing.md` — Google Cloud credentials and token caches guide.
[^analytics-doc]: `docs/usage/analytics-and-cdc.md` — DuckDB secrets and analytics guide.
[^databases-doc]: `docs/usage/databases.md` — Database adapters overview.
[^test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Test matrix and conformance chapter.
[^limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations record format precedent.
