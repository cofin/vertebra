---
type: Task
id: "driver-conformance-ci:ch05:t02"
chapter_id: ch05-docs-and-okf
title: Correct database engine docs and update the changelog and AGENTS.md
description: Fix the stale Python-client statement in the databases guide, keep the DuckDB row accurate, add Unreleased changelog entries, and list make db-conformance in AGENTS.md.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-conformance-ci:ch05:t01"
read_set:
  - docs/usage/databases.md
  - docs/changelog/index.md
  - AGENTS.md
  - Makefile
files:
  - docs/usage/databases.md
  - docs/changelog/index.md
  - AGENTS.md
tests: []
estimated_loc: 60
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
  - id: databases-doc
    resource: docs/usage/databases.md
    title: Database adapters overview and capability table
  - id: changelog
    resource: docs/changelog/index.md
    title: Changelog with Added/Changed sections
  - id: agents
    resource: AGENTS.md
    title: Quick Verification Commands
tags:
  - documentation
  - changelog
---

# Task t02: Correct Database Engine Docs and Update the Changelog and AGENTS.md

## Objective & Scope Boundary

1. `docs/usage/databases.md`: replace the first paragraph's clause "BigQuery, Spanner, and DuckDB run on their own native wheels, the other live engines use their Python client packages" with a present-tense statement that PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, DuckDB, BigQuery, and Spanner run on their own native wheels (`vertebra-<engine>`), and update the DuckDB table row's rotation phrase to "GCS ADC `httpfs` secret rotation via `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider`" [^databases-doc].
2. `docs/changelog/index.md`: add `## Unreleased` above `## 0.3.0` with `### Added` (GCS bearer secret providers; live `pytest-databases` conformance lanes and `test-db-conformance` CI job; MariaDB conformance engine) and `### Changed` (`DuckDBSecretConfig` rejects persistent dynamic secrets and invalid intervals; DuckDB drivers invalidate providers on `401`/`403`; `make oracle-test` runs conformance and native suites), each bullet naming exact symbols, no version-bump entries, no PR links until the PR exists [^changelog].
3. `AGENTS.md`: add `make db-conformance ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]` with a one-line comment to the Quick Verification Commands block after `make oracle-wheel-check`, update the `make oracle-test` line to mention conformance and native suites, and drop `--group duckdb` from the `make install` line [^agents].
4. `docs/usage/databases.md`: in the Oracle notification section that shows `OracleNotifyConfig(queue_prefix="VTB_AQ_", wait_timeout_seconds=5)`, add one sentence stating that `wait_timeout_seconds` is the idle wait budget before the listener pings its session, with dequeues sliced to 1 second so `close()` stays responsive (`ch03 / t04`) [^databases-doc].
5. `docs/changelog/index.md` `### Changed`: add bullets for `OracleNotifyConfig.wait_timeout_seconds` now bounding the idle dequeue cycle with a liveness ping, and for test and install targets no longer syncing PyPI `duckdb` [^changelog].

Out of scope: storage/analytics docs (`t01`), OKF (`t03`, `t04`).

## Bounded Read Manifest

- `docs/usage/databases.md` — first paragraph and engine table [^databases-doc].
- `docs/changelog/index.md` — `## 0.3.0` structure [^changelog].
- `AGENTS.md` — Quick Verification Commands [^agents].
- `Makefile` — `db-conformance`, `oracle-test` help text.

## Symbol & Signature Contracts

- Changelog bullets follow the existing `- **Title (`scope`)**:` + nested bullet style.
- `AGENTS.md` code block keeps aligned `#` comments as the surrounding lines do (shell comments inside a code block, not Python).

## Edge Cases & Error Semantics

- `make docs` link checks pass; no internal tool names appear.
- `AGENTS.md` lines stay under the block's existing column alignment.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Stale statement removed | `git grep -n "use their Python client packages" -- docs/usage/databases.md` | No matches |
| Changelog section | `git grep -n "^## Unreleased" -- docs/changelog/index.md` | One match above `## 0.3.0` |
| AGENTS entry | `git grep -n "make db-conformance" -- AGENTS.md` | One match |
| Site build | `make docs` | Exit `0` |

## Verification

- `! git grep -n "use their Python client packages" -- docs/usage/databases.md`
- `git grep -n "make db-conformance" -- AGENTS.md`
- `make docs`

## Acceptance Criteria

- [ ] Databases guide states native-wheel engines accurately and names the GCS providers (R15).
- [ ] Changelog `Unreleased` and `AGENTS.md` list the new behavior and command (R15).

## Notes & Discoveries

- [2026-11-12 00:30] Add PR links to the changelog bullets when the pull request number is known at delivery.

[^databases-doc]: `docs/usage/databases.md` — Database adapters overview.
[^changelog]: `docs/changelog/index.md` — Changelog.
[^agents]: `AGENTS.md` — Quick Verification Commands.
