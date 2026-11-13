---
type: Task
id: "driver-conformance-ci:ch04:t04"
flow_id: driver-conformance-ci
chapter_id: ch04-docs-and-okf-sync
title: "Synchronize database-drivers-and-dialers.md and update database-driver-limitations.md in place"
description: "Document DuckDB GCS secret lifecycle and Oracle AQ liveness in database-drivers-and-dialers.md and replace resolved caveats with live conformance boundaries in database-driver-limitations.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "driver-conformance-ci:ch04:t03"
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/database-driver-limitations.md
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/rs/vertebra-oracle/src/notify.rs
estimated_loc: 135
tags:
  - okf
  - data
  - limitations
  - duckdb
  - oracle
sources:
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Cross-engine database drivers and cloud dialers OKF chapter"
  - id: okf-limits
    resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
    title: "Canonical database driver limitations OKF chapter"
---

# Task `t04`: Synchronize `database-drivers-and-dialers.md` and Update `database-driver-limitations.md` in Place

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` and `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` (`<= 200` lines each) in place per [`../spec.md` §2](../spec.md) [^okf-drivers] [^okf-limits]. Out of scope: creating any new `driver-conformance-limitations.md` file.

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,database-driver-limitations}.md`.
- `src/py/vertebra/storage/_secrets.py`, `src/py/vertebra/db/adapters/duckdb/driver.py`, and `src/rs/vertebra-oracle/src/notify.rs`.

## Contracts & Edge Cases

- **`database-drivers-and-dialers.md` (`<= 200L`)**: Document `SyncGcsBearerSecretProvider` / `AsyncGcsBearerSecretProvider` (`vertebra.storage`) feeding `DuckDBSecretConfig.value_provider`, `(HTTP 401)`/`(HTTP 403)` provider invalidation, startup `_client` cleanup, and `OracleNotifyConfig.wait_timeout_seconds` `SELECT 1 FROM DUAL` idle liveness probing.
- **`database-driver-limitations.md` (`<= 200L`)**: Remove the resolved DuckDB missing-GCS-secret-provider and `oracle-test` `test_freethreaded_gil.py` fallback caveats, and record the confirmed boundaries: DuckDB `(HTTP 401)`/`(HTTP 403)` error-text regex detection without implicit query retry, `VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1` extension download network requirement, and `conformance-oracle` (`continue-on-error: true`) CI runner isolation.

## Test Matrix

- Line-count checks (`<= 200` lines each, truth blocks `<= 40` lines), `make docs`, and `make review-check`.

## Verification

```bash
for f in .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/domains/data/database-driver-limitations.md; do test "$(wc -l < "$f")" -le 200; done && make docs && make review-check
```

## Acceptance Criteria

- [ ] Both OKF files are `<= 200` lines with valid `sources:` and footnotes, and zero duplicate limitations files exist.
- [ ] `make docs` and `make review-check` exit `0`.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Cross-engine database drivers and cloud dialers OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Canonical database driver limitations OKF chapter.
