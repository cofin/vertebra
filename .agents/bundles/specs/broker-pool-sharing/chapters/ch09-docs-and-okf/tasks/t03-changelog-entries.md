---
type: Task
id: "broker-pool-sharing:ch09:t03"
chapter_id: ch09-docs-and-okf
title: Add Unreleased changelog entries
description: Add Unreleased entries to docs/changelog/index.md for shared broker pools, atomic segment ownership, authenticated attach, quarantine, the engine capsule registry, the SQLSpec proxy rebase, vertebra.db.dbapi, BrokerPool, crash reclamation, metrics, and the removed in-memory transports.
status: draft
state: pending
priority: P3
verification_strategy: documentation_validation
depends_on:
  - "broker-pool-sharing:ch09:t01"
read_set:
  - docs/changelog/index.md
  - .agents/bundles/specs/broker-pool-sharing/spec.md
files:
  - docs/changelog/index.md
tests: []
estimated_loc: 60
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
  - id: changelog
    resource: docs/changelog/index.md
    title: Changelog
tags:
  - changelog
---

# Task t03: Add `Unreleased` Changelog Entries

## Objective & Scope Boundary

In `docs/changelog/index.md` [^changelog], under `Unreleased`, following the file's existing heading and bullet style:

- **Added**: `BrokerEngineRegistry` and `broker_engine_capsule()` on `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, `vertebra._duckdb`, `vertebra._postgres`; `stable_hash64`; `BrokerHost` and `vtb broker serve`; `vertebra.db.dbapi`; `BrokerPool`, `AsyncBrokerPool`, `broker_creator`; per-pool and quarantine metrics.
- **Changed**: atomic create-or-attach for named segments with owner-aware cleanup ([#95](https://github.com/cofin/vertebra/issues/95), [#96](https://github.com/cofin/vertebra/issues/96)); authenticated attach ([#97](https://github.com/cofin/vertebra/issues/97)); `ProxySyncDriver` / `ProxyAsyncDriver` subclass SQLSpec adapter bases; DB-API classes move out of `vertebra.extensions.sqlalchemy.dialects`.
- **Fixed**: uncertain release outcomes quarantine the connection and raise `LeaseQuarantinedError` ([#98](https://github.com/cofin/vertebra/issues/98)); crashed worker leases roll back before reuse.
- **Removed**: `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport`, `BrokerPlugin(use_shm=False)`, `extensions/alchemy/broker.py::StatementCache`.

PR links are added once PR numbers exist; no version bump entry.

## Bounded Read Manifest

- `docs/changelog/index.md` [^changelog]; root spec requirements.

## Symbol & Signature Contracts

Every named class or function exists at HEAD when the entry is written (`git grep -n` each name).

## Edge Cases & Error Semantics

- A name renamed during implementation uses the shipped name.

## Public-Seam Test Blueprint

| Check | Command | Expected |
| --- | --- | --- |
| Docs build | `make docs` | exit `0` |
| Removed names absent from code | `git grep -n "class SyncInMemoryBrokerTransport" src/py` | no output |

## Verification

- `make docs`

## Acceptance Criteria

- `Unreleased` lists the entries above with exact names and issue links.

## Notes & Discoveries

- None yet.
