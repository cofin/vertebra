---
type: Task
id: "native-adapter-organization:t04"
flow_id: native-adapter-organization
title: "Synchronize analytics guide and data-domain OKF chapters for adapter layout"
description: "Update docs/usage/analytics-and-cdc.md and data-domain OKF chapters to reflect uniform adapter module boundaries and in-process DataFusion registration."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "native-adapter-organization:t01"
  - "native-adapter-organization:t02"
  - "native-adapter-organization:t03"
files:
  - docs/usage/analytics-and-cdc.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
tests: []
read_set:
  - docs/usage/analytics-and-cdc.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - src/py/vertebra/db/adapters/bigquery/object_tables.py
  - src/py/vertebra/db/adapters/spanner/mutations.py
  - src/py/vertebra/db/adapters/datafusion/driver.py
estimated_loc: 135
tags:
  - documentation
  - okf
  - adapters
  - datafusion
sources:
  - id: doc-analytics
    resource: docs/usage/analytics-and-cdc.md
    title: "User-facing Arrow analytics, Object Tables, and DataFusion federation guide"
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Cross-engine database drivers and adapter layout OKF chapter"
  - id: okf-cdc
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: "Query pipeline, CDC, and federated analytics OKF chapter"
---

# Task `t04`: Synchronize Analytics Guide and Data-Domain OKF Chapters for Adapter Layout

## Objective & Scope Boundary

Implement `R4` in [`../spec.md` §3](../spec.md) by updating `docs/usage/analytics-and-cdc.md` and synchronizing [`database-drivers-and-dialers.md`](../../../knowledge/domains/data/database-drivers-and-dialers.md) and [`query-pipeline-cdc-and-tenancy.md`](../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) (`<= 200` lines each) in present tense [^doc-analytics] [^okf-drivers] [^okf-cdc]. Out of scope: non-data OKF chapters or reference stub pages.

## Bounded Read Manifest

- `docs/usage/analytics-and-cdc.md` — DataFusion federation and BigQuery Object Table examples.
- `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy}.md` — Adapter organization and pipeline/federation provenance.
- `src/py/vertebra/db/adapters/{bigquery/object_tables,spanner/mutations,datafusion/driver}.py` — Updated symbol locations for `sources:` citations.

## Contracts & Edge Cases

- **`docs/usage/analytics-and-cdc.md`**: Replace any `register_bigquery_*`, `register_spanner_*`, or `pgwire_fdw` snippets with `engine.register_arrow_table(name, driver.select_to_arrow(...))` over `DataFusionConfig(mode="in_process")`; keep Mermaid diagrams `4`–`6` conceptual nodes and `msgspec.Struct` examples free of `frozen=True, gc=False` boilerplate.
- **OKF Chapters (`<= 200` lines, `<= 40`-line truth blocks)**: Update `sources:` resource paths (`bigquery/object_tables.py`, `spanner/mutations.py`, `oracle/pipeline.py`, `_sync.py`/`_async.py`), document uniform per-adapter module boundaries and `DataFusionEngine`'s embedded `in_process` exemption, stamp `verified[].commit` to the current `HEAD` ancestor, and include zero standalone `---` body rules or inline `{...}` YAML mappings.

## Test Matrix

- `scan_public_docstrings` leak/overload validation across `src/py/vertebra/db/adapters`, full Zensical build + `verify_docs_artifacts.py` (`make docs`), and OKF/link/SHA validation (`make review-check`).

## Verification

```bash
PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters && make docs && make review-check
```

## Acceptance Criteria

- [ ] `docs/usage/analytics-and-cdc.md` builds cleanly via `make docs` with zero references to removed DataFusion wrappers or `pgwire_fdw`.
- [ ] Both updated OKF chapters are `<= 200` lines with resolving `sources:` paths and `make review-check` exiting `0`.

[^doc-analytics]: `docs/usage/analytics-and-cdc.md` — User-facing Arrow analytics, Object Tables, and DataFusion federation guide.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Cross-engine database drivers and adapter layout OKF chapter.
[^okf-cdc]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — Query pipeline, CDC, and federated analytics OKF chapter.
