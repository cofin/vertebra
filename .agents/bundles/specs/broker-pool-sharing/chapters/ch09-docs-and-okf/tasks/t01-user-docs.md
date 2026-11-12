---
type: Task
id: "broker-pool-sharing:ch09:t01"
chapter_id: ch09-docs-and-okf
title: Document broker pools, attach security, quarantine, engine matrix, and architecture
description: Update docs/usage/extensions.md, docs/usage/databases.md, and docs/architecture/pyo3-and-workers.md to describe shared broker pools, the attach security boundary, quarantine semantics, the native engine registration matrix, segment ownership, the capsule registry, and the broker host.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "broker-pool-sharing:ch08:t04"
  - "broker-pool-sharing:ch07:t04"
read_set:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/architecture/pyo3-and-workers.md
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - .agents/bundles/specs/broker-pool-sharing/spec.md
files:
  - docs/usage/extensions.md
  - docs/usage/databases.md
  - docs/architecture/pyo3-and-workers.md
tests: []
estimated_loc: 220
plan_revision: 3
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
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: docs-extensions
    resource: docs/usage/extensions.md
    title: Extensions usage guide
  - id: docs-databases
    resource: docs/usage/databases.md
    title: Databases usage guide
  - id: docs-arch
    resource: docs/architecture/pyo3-and-workers.md
    title: PyO3 and worker architecture
tags:
  - docs
---

# Task t01: Document Broker Pools, Attach Security, Quarantine, Engine Matrix, and Architecture

## Objective & Scope Boundary

1. `docs/usage/extensions.md` [^docs-extensions]: a "Shared broker pools" section — wrapping an upstream SQLSpec config in `ProxyAsyncConfig` / `ProxySyncConfig`, `BrokerPlugin` setup, SQLAlchemy and Advanced Alchemy via `BrokerPool` / `AsyncBrokerPool` / `broker_creator`, the attach security boundary (private Unix socket, `0600`, secret compare; same-UID processes without the secret cannot attach), `LeaseQuarantinedError` semantics (the operation outcome is unknown; retry at the application level on a fresh lease), and `vtb broker serve`.
2. `docs/usage/databases.md` [^docs-databases]: native engine broker registration matrix (PostgreSQL `1`, MySQL `2`, Oracle `4`, SQL Server `5`, DuckDB `6` register; BigQuery and Spanner use per-worker clients; SQLite not covered) and which wheel provides each.
3. `docs/architecture/pyo3-and-workers.md` [^docs-arch]: atomic create-or-attach with owner PID and generation, owned-segment cleanup on panic/SIGTERM, the `BrokerEngineCapsule` registry across cdylibs, and the broker host lifecycle; one conceptual Mermaid diagram with 4–6 nodes.
4. Remove references to `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, and `use_shm=False`.

## Bounded Read Manifest

- The three doc files [^docs-extensions] [^docs-databases] [^docs-arch].
- `extensions/broker/plugin.py`, `extensions/sqlspec/plugin.py` — current constructor signatures to quote.
- Root spec contracts section 2.2.

## Symbol & Signature Contracts

Every code example imports names that exist at the time of writing; examples declare models as `class Model(msgspec.Struct):` and pass configuration inline. Install snippets use `pymdownx.tabbed` blocks `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`.

## Edge Cases & Error Semantics

- macOS: note that the broker uses `ulock` wait/wake and link to the limitations record for caveats.
- Windows: state that shared broker pools are unsupported and link to the limitations record.

## Public-Seam Test Blueprint

| Check | Command | Expected |
| --- | --- | --- |
| Docs build | `make docs` | exit `0` |
| No stale names | `git grep -n "InMemoryBrokerTransport\|use_shm=False" docs` | no output |
| Example names exist | `uv run --no-sync python -c "from vertebra.extensions.sqlalchemy import BrokerPool, AsyncBrokerPool, broker_creator; from vertebra.extensions.broker.errors import LeaseQuarantinedError"` | exit `0` |

## Verification

- `make docs`
- `git grep -n "InMemoryBrokerTransport\|use_shm=False" docs` prints nothing.

## Acceptance Criteria

- The three files describe shipped behavior; checks in the matrix pass.

## Notes & Discoveries

- None yet.
