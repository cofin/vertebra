---
type: Task
id: "driver-conformance-ci:ch04:t02"
flow_id: driver-conformance-ci
chapter_id: ch04-docs-and-okf-sync
title: "Synchronize databases.md, changelog, and AGENTS.md for live conformance and DuckDB isolation"
description: "Update docs/usage/databases.md, docs/changelog/index.md, and AGENTS.md for native wheels, GCS secrets, Oracle notify liveness, make db-conformance, and bench-only duckdb."
status: stable
state: completed
commit: cc75373f
last_verified_commit: cc75373f
updated_at: "2026-11-14T18:08:00Z"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_revision: 4
plan_commit: 37972f2a
depends_on:
  - "driver-conformance-ci:ch04:t01"
files:
  - docs/usage/databases.md
  - docs/changelog/index.md
  - AGENTS.md
tests: []
read_set:
  - docs/usage/databases.md
  - docs/changelog/index.md
  - AGENTS.md
  - Makefile
estimated_loc: 120
tags:
  - documentation
  - changelog
  - workflow
sources:
  - id: doc-databases
    resource: docs/usage/databases.md
    title: "Database drivers overview and engine capability notes"
  - id: doc-changelog
    resource: docs/changelog/index.md
    title: "Release changelog Unreleased section"
  - id: repo-agents
    resource: AGENTS.md
    title: "Repository agent instructions and verification command reference"
---

# Task `t02`: Synchronize `databases.md`, Changelog, and `AGENTS.md` for Live Conformance and DuckDB Isolation

## Objective & Scope Boundary

Update `docs/usage/databases.md`, `docs/changelog/index.md`, and `AGENTS.md` (`<= 100` lines) per [`../spec.md` §2](../spec.md) [^doc-databases] [^doc-changelog] [^repo-agents]. Out of scope: OKF knowledge chapters (`t03`–`t04`).

## Bounded Read Manifest

- `docs/usage/databases.md`, `docs/changelog/index.md`, `AGENTS.md`, and `Makefile`.

## Contracts & Edge Cases

- **`docs/usage/databases.md`**: Ensure native wheel descriptions cover PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, DuckDB, BigQuery, and Spanner accurately, link `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` in the DuckDB secret note, and document `OracleNotifyConfig.wait_timeout_seconds` idle session liveness probing.
- **`docs/changelog/index.md`**: Record user-visible additions/fixes under `## Unreleased` (`SyncGcsBearerSecretProvider`, `AsyncGcsBearerSecretProvider`, `DuckDBSecretConfig` validation and `401`/`403` invalidation, `OracleNotifyConfig.wait_timeout_seconds` idle liveness) without version-bump noise or internal task IDs.
- **`AGENTS.md` (`<= 100` lines)**: Update `make install` (removing `--group duckdb`), `make oracle-test`, and add `make db-conformance`.

## Test Matrix

- `wc -l AGENTS.md` (`<= 100`), `make docs`, and `make review-check`.

## Verification

```bash
test "$(wc -l < AGENTS.md)" -le 100 && make docs && make review-check
```

## Acceptance Criteria

- [x] `AGENTS.md` is `<= 100` lines and matches `Makefile` targets (`make install` without `--group duckdb`, `make db-conformance`, `make oracle-test`).
- [x] `make docs` and `make review-check` exit `0`.

[^doc-databases]: `docs/usage/databases.md` — Database drivers overview and engine capability notes.
[^doc-changelog]: `docs/changelog/index.md` — Release changelog `Unreleased` section.
[^repo-agents]: `AGENTS.md` — Repository agent instructions and verification command reference.
