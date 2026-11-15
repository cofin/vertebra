---
type: Task
id: broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t04
flow_id: broker-pool-sharing
chapter_id: ch04-python-pooling-sqlspec-sqlalchemy-and-docs
title: Synchronize Broker Pool Sharing Documentation, Changelog, and OKF Chapters
description: Update databases/extensions/dbapi docs, changelog, and OKF broker, driver, SQLSpec/SQLAlchemy, and broker-pool-limitations chapters.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch04-python-pooling-sqlspec-sqlalchemy-and-docs:t03
files:
- docs/usage/extensions.md
- docs/usage/databases.md
- docs/changelog/index.md
- .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
- .agents/bundles/knowledge/domains/data/shm-broker-limitations.md
- .agents/bundles/knowledge/domains/index.md
tests: []
read_set:
- docs/usage/extensions.md
- docs/usage/databases.md
- docs/changelog/index.md
- src/py/vertebra/extensions/sqlalchemy/pool.py
- .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
- .agents/bundles/knowledge/domains/data/postgres-limitations.md
estimated_loc: 220
tags:
- docs
- changelog
- broker
- okf
- knowledge
- limitations
sources:
- id: docs-ext
  resource: docs/usage/extensions.md
  title: Extensions usage documentation
- id: docs-db
  resource: docs/usage/databases.md
  title: Databases usage documentation
- id: docs-changelog
  resource: docs/changelog/index.md
  title: Release changelog
- id: broker-okf
  resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
  title: Broker and SHM leases OKF chapter
- id: domains-idx
  resource: .agents/bundles/knowledge/domains/index.md
  title: OKF domains catalog index
---

# Task `t04`: Synchronize Broker Pool Sharing Documentation, Changelog, and OKF Chapters

## Objective & Scope Boundary

Update `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` per [`../spec.md` §2](../spec.md) [^docs-ext] [^docs-db] [^docs-changelog].

Refresh `broker-and-shm-leases.md`, author `shm-broker-limitations.md` (`<= 200` lines each), and link `shm-broker-limitations.md` in `domains/index.md` per [`../spec.md` §2](../spec.md) [^broker-okf] [^domains-idx].

## Bounded Read Manifest

- `docs/usage/extensions.md` — Broker, SQLSpec proxy, and SQLAlchemy extension docs.
- `docs/usage/databases.md` — Database driver and broker capability matrix.
- `docs/changelog/index.md` — `Unreleased` changelog section.
- `src/py/vertebra/extensions/sqlalchemy/pool.py` — `BrokerPool` and `broker_creator` public signatures.

- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Current broker OKF chapter.
- `.agents/bundles/knowledge/domains/data/postgres-limitations.md` — Canonical OKF limitations chapter format.
- `.agents/bundles/knowledge/domains/index.md` — Domains index catalog.

## Contracts & Edge Cases

- Use `pymdownx.tabbed` (`uv`, `pip`, `pdm`, `poetry`) for any install snippets, keep Mermaid diagrams conceptual (`4–6` nodes), and remove all `SyncInMemoryBrokerTransport` / `use_shm=False` references.
- Record concise `Added`, `Changed`, and `Removed` entries under `Unreleased` in `docs/changelog/index.md`.
- **Edge Case**: Declare `msgspec.Struct` examples concisely without internal `frozen=True, gc=False` boilerplate in public docs.

- Document `ShmOwnerHeader`, `BrokerEngineCapsule` ABI v1, `LeaseState::Quarantined`, UDS `SCM_RIGHTS` attach, `BrokerHost` crash `ROLLBACK`, `vertebra.db.dbapi`, and `BrokerPool` in `broker-and-shm-leases.md`.
- Record BigQuery/Spanner native broker exclusion, Windows named-SHM exclusion, `MAX_POOL_PARTITIONS = 64`, 256-slot signal cleanup cap, and macOS 31-byte `shm_open` name limit in `shm-broker-limitations.md`.
- **Edge Case**: Every `sources:` path and relative link resolves on disk; `domains/index.md` carries zero YAML frontmatter.

## Test Matrix

- `make docs` builds Zensical site cleanly and `scan_public_docstrings` reports zero issues.

- `make review-check` exits `0` with zero OKF, link, truth-budget, or privacy errors.

## Verification

```bash
make review-check
```

## Acceptance Criteria

- [ ] Public docs and changelog accurately describe shipped broker pool-sharing APIs and pass `make docs`.
- [ ] Both OKF chapters are `<= 200` lines, present-tense, and pass `make review-check`.
- [ ] Zero regressions in the fast verification gate.

[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
