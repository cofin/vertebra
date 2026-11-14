---
type: Task
id: sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t04
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-cutover-storage-events-docs
title: Synchronize SQLSpec Adapter Parity Documentation and OKF Knowledge Chapters
description: Update docs/usage/extensions.md, docs/reference/extensions.md, sqlspec-alchemy-and-dialects.md, and database-driver-limitations.md.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 1b191388
depends_on:
- sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t03
files:
- docs/usage/extensions.md
- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
- .agents/bundles/knowledge/domains/data/database-driver-limitations.md
tests:
- src/py/tests/unit/extensions/test_vite_and_docs_portal.py
read_set:
- docs/usage/extensions.md
- docs/reference/extensions.md
- .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
- .agents/bundles/knowledge/domains/data/database-driver-limitations.md
estimated_loc: 170
tags:
- documentation
- okf
- sqlspec
sources:
- id: doc-ext
  resource: docs/usage/extensions.md
  title: SQLSpec extension usage documentation
- id: okf-dialects
  resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  title: SQLSpec and dialects OKF chapter
- id: okf-limits
  resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
  title: Database driver and SQLSpec limitations ledger
---

# Task `t04`: Synchronize SQLSpec Adapter Parity Documentation and OKF Knowledge Chapters

## Objective & Scope Boundary

Synchronize public SQLSpec documentation and OKF chapters per [`../spec.md`](../spec.md) [^doc-ext] [^okf-dialects] [^okf-limits].

## Bounded Read Manifest

- `docs/usage/extensions.md` — per-engine `vertebra.extensions.sqlspec.adapters.<engine>` guides, ADK/Events/Litestar stores, and `ArtifactReference`.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` & `domains/data/database-driver-limitations.md` — OKF truth blocks and caveats.

## Contracts & Edge Cases

- Document `vertebra.extensions.sqlspec.adapters.<engine>`, `adapter_name = 'vertebra_<engine>'`, explicit `*_store_class` attributes (`sqlspec>=0.71.1`), `vertebra.storage.ArtifactReference`, and `adk` optional extra (`pymdownx.tabbed` install tabs).
- Record caveats in `database-driver-limitations.md` (`google-adk>=1.0.0` optional extra required for ADK memory/artifact services; Oracle TxEventQ commit-at-dequeue semantics; DuckDB `:memory:` vs `:memory:<name>` isolation), keeping every OKF chapter `<= 200` lines.

## Test Matrix

- Run `python -m tools.scripts.review_setup validate` and `pytest test_vite_and_docs_portal.py`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] Public docs and OKF chapters (`<= 200` lines each) pass `review_setup.py validate` with zero issues.
- [ ] Zero regressions in the fast verification gate.

[^doc-ext]: `docs/usage/extensions.md` — SQLSpec extension usage documentation.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec and dialects OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database driver and SQLSpec limitations ledger.
