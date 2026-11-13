---
type: Task
id: "broker-pool-sharing:ch08:t01"
flow_id: broker-pool-sharing
chapter_id: ch08-docs-and-knowledge-sync
title: "Update Extensions and Database Usage Guides and Unreleased Changelog"
description: "Document shared broker pools, UDS attach, quarantine, vertebra.db.dbapi, BrokerPool, and Unreleased changelog entries."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch06:t03"
  - "broker-pool-sharing:ch07:t03"
files:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/changelog/index.md
tests: []
read_set:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/changelog/index.md
  - src/py/vertebra/extensions/sqlalchemy/pool.py
estimated_loc: 190
tags:
  - docs
  - changelog
  - broker
sources:
  - id: docs-ext
    resource: docs/usage/extensions.md
    title: "Extensions usage documentation"
  - id: docs-db
    resource: docs/usage/databases.md
    title: "Databases usage documentation"
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
---

# Task `t01`: Update Extensions and Database Usage Guides and Unreleased Changelog

## Objective & Scope Boundary

Update `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` per [`../spec.md` §2](../spec.md) [^docs-ext] [^docs-db] [^docs-changelog].

## Bounded Read Manifest

- `docs/usage/extensions.md` — Broker, SQLSpec proxy, and SQLAlchemy extension docs.
- `docs/usage/databases.md` — Database driver and broker capability matrix.
- `docs/changelog/index.md` — `Unreleased` changelog section.
- `src/py/vertebra/extensions/sqlalchemy/pool.py` — `BrokerPool` and `broker_creator` public signatures.

## Contracts & Edge Cases

- Use `pymdownx.tabbed` (`uv`, `pip`, `pdm`, `poetry`) for any install snippets, keep Mermaid diagrams conceptual (`4–6` nodes), and remove all `SyncInMemoryBrokerTransport` / `use_shm=False` references.
- Record concise `Added`, `Changed`, and `Removed` entries under `Unreleased` in `docs/changelog/index.md`.
- **Edge Case**: Declare `msgspec.Struct` examples concisely without internal `frozen=True, gc=False` boilerplate in public docs.

## Test Matrix

- `make docs` builds Zensical site cleanly and `scan_public_docstrings` reports zero issues.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] Public docs and changelog accurately describe shipped broker pool-sharing APIs and pass `make docs`.
- [ ] Zero regressions in the chapter fast verification gate.

[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
