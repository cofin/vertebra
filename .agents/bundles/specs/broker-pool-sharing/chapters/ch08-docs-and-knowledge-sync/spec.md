---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch08-docs-and-knowledge-sync
title: "Chapter 08: Public Documentation, Changelog, and OKF Knowledge Synchronization"
description: "Update public extensions and database guides, add Unreleased changelog entries, refresh broker-and-shm-leases.md, and record caveats in shm-broker-limitations.md."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - docs
  - okf
  - broker
  - limitations
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
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: "Broker and SHM leases OKF chapter"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
---

# Chapter 08: Public Documentation, Changelog, and OKF Knowledge Synchronization

<!-- truth: start -->
- `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` document `ProxySyncConfig`/`ProxyAsyncConfig`, `BrokerPool`/`AsyncBrokerPool`, `vtb broker serve`, UDS attach, quarantine, `vertebra.db.dbapi`, and the capsule registration matrix [^docs-ext] [^docs-db] [^docs-changelog].
- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` and `.agents/bundles/knowledge/domains/data/shm-broker-limitations.md` stay `<= 200` lines each and are indexed in `.agents/bundles/knowledge/domains/index.md` [^broker-okf] [^domains-idx].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Public documentation and OKF chapters must reflect the shipped SHM ownership model, UDS attach boundary, cross-wheel `BrokerEngineCapsule` registry, fail-loud quarantine, `vertebra.db.dbapi`, and explicit operational caveats while removing deleted `SyncInMemoryBrokerTransport` / `use_shm=False` references [^docs-ext] [^broker-okf].

**Non-Goals:** SQLite or Redis driver documentation (owned by `native-sqlite-driver` and `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Public docs | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md` [^docs-ext] [^docs-db] [^docs-changelog] | Zensical user docs and `Unreleased` changelog |
| OKF knowledge | `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,shm-broker-limitations}.md` [^broker-okf] [^domains-idx] | Present-tense OKF architecture and limitations chapters (`<= 200` lines) |

**Knowledge Anchors:** [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R22a**: Update `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/changelog/index.md` with tabbed install snippets, conceptual 4–6 node Mermaid overview, and exact public class/function names (`t01`).
- **R22b**: Update `broker-and-shm-leases.md`, create `shm-broker-limitations.md` (`<= 200` lines each), and link from `domains/index.md` (`t02`).

```text
Documented public symbols:
- vertebra.extensions.broker: BrokerConfig, BrokerPlugin, BrokerHost, BrokerEngineDiscovery,
  BrokerAttachError, BrokerSegmentInUseError, BrokerAuthenticationError, LeaseQuarantinedError, PoolMetrics
- vertebra._vertebra: LeaseBroker.open_or_create_named, BrokerAttachServer, BrokerAttachClient,
  BrokerEngineRegistry, stable_hash64
- vertebra.extensions.sqlspec: ProxySyncConfig, ProxyAsyncConfig, ProxySyncDriver, ProxyAsyncDriver
- vertebra.extensions.sqlalchemy: BrokerPool, AsyncBrokerPool, broker_creator
- vertebra.db.dbapi: SyncDBAPIConnection, AsyncDBAPIConnection, SyncDBAPIModule, AsyncDBAPIModule
```

**Integration Flow:**
- `make docs` validates Zensical rendering, docstring scans, and artifact budgets.
- `make review-check` validates OKF v0.2 frontmatter, truth blocks, relative links, and privacy hygiene.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Stale transport names | Mention of `SyncInMemoryBrokerTransport` or `use_shm=False` | Removed across `docs/` and `knowledge/` | `DOC-02` / `DOC-09` parity |
| OKF line cap | Knowledge chapter `> 200` lines | Split cleanly between `broker-and-shm-leases.md` and `shm-broker-limitations.md` | `<= 200` lines per chapter |

## 3. Documentation & OKF Knowledge Plan

This chapter completes `[6/6]` documentation and OKF deliverables for `broker-pool-sharing`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R22a` | `t01` | `docs/usage/{extensions,databases}.md`, `docs/changelog/index.md` | `make docs` |
| `R22b` | `t02` | `.agents/bundles/knowledge/domains/data/{broker-and-shm-leases,shm-broker-limitations}.md`, `domains/index.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-public-docs-and-changelog.md](tasks/t01-public-docs-and-changelog.md) — Document shared broker pools, UDS attach, quarantine, vertebra.db.dbapi, BrokerPool, and Unreleased changelog entries.
- [ ] [t02-okf-broker-chapter-and-limitations.md](tasks/t02-okf-broker-chapter-and-limitations.md) — Update broker-and-shm-leases.md, create shm-broker-limitations.md (<= 200 lines each), and index in domains/index.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/2` tasks completed.
- **Active Invariants**: Every OKF chapter stays `<= 200` lines in present tense with zero private paths or historical prose.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && uv run --no-sync python -m tools.docs.scan_public_docstrings`

[^docs-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^docs-db]: `docs/usage/databases.md` — Databases usage documentation.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker and SHM leases OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
