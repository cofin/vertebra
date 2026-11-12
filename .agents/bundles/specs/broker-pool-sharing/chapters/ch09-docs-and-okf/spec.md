---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch09-docs-and-okf
title: "Chapter 9: Docs and OKF"
description: User docs for broker pools, attach security, quarantine, and the native engine registration matrix; architecture docs for segment ownership and the capsule registry; changelog entries; and the OKF broker chapter plus a limitations record.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-shm-segment-ownership
  - ch02-pool-partitions-and-dbrpc
  - ch03-engine-registry-capsule
  - ch04-engine-backend-registration
  - ch05-lease-safety-and-host
  - ch06-sqlspec-proxy
  - ch07-dbapi-and-sqlalchemy-pool
  - ch08-reclamation-and-acceptance
replan_trigger: null
current_task: "broker-pool-sharing:ch09:t01"
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
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
  - id: changelog
    resource: docs/changelog/index.md
    title: Changelog
  - id: broker-okf
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: Broker and SHM leases knowledge chapter
  - id: domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domains knowledge index
tags:
  - docs
  - okf
  - broker
---

# Chapter 9: Docs and OKF

<!-- truth: start -->
- `docs/usage/extensions.md`, `docs/usage/databases.md`, and `docs/architecture/pyo3-and-workers.md` describe the broker as it exists at HEAD (name-only attach, in-memory transport option, static engine registry) [^docs-extensions] [^docs-databases] [^docs-arch].
- `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` is the broker knowledge chapter; no limitations record exists for broker pools [^broker-okf] [^domains-index].
- After this chapter every doc target in the root spec section 3 describes shipped behavior from `ch01`–`ch08`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Users need to know how to share pools across workers, what the attach security boundary is, what `LeaseQuarantinedError` means, and which engines register native broker backends. Maintainers need the OKF chapter and a limitations record that explains BigQuery/Spanner exclusion and platform caveats.

### 1.2 Non-Goals

- Docs for `vertebra.db.dbapi` consumers beyond broker pooling (owned by `sqlalchemy-and-django-integration`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| User docs | `docs/usage/extensions.md`, `docs/usage/databases.md` [^docs-extensions] [^docs-databases] | Zensical site (`make docs`). |
| Architecture | `docs/architecture/pyo3-and-workers.md` [^docs-arch] | Zensical site. |
| Changelog | `docs/changelog/index.md` [^changelog] | `Unreleased` section. |
| OKF | `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`, `.agents/bundles/knowledge/domains/index.md` [^broker-okf] [^domains-index] | `make review-check`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Project Workflow](../../../../knowledge/workflow.md).
- Skills: `okf`, `flow-docs`, `litestar-docs`, `.agents/skills/review-docs-okf/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R22a**: User and architecture docs describe broker pools, attach security, quarantine, the engine registration matrix, segment ownership, the capsule registry, and the broker host (`t01`).
- **R22b**: OKF broker chapter, limitations record, and domains index link (`t02`).
- **R22c**: `Unreleased` changelog entries with exact class/function names (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

Documented names (must match code at the time of writing): `ProxySyncConfig`, `ProxyAsyncConfig`, `BrokerPlugin`, `BrokerHost`, `vtb broker serve`, `LeaseQuarantinedError`, `BrokerAttachError`, `BrokerSegmentInUseError`, `BrokerAuthenticationError`, `BrokerEngineRegistry`, `broker_engine_capsule`, `stable_hash64`, `BrokerPool`, `AsyncBrokerPool`, `broker_creator`, `vertebra.db.dbapi`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `make docs` → Zensical build → `docs/usage/*.md`, `docs/architecture/*.md`, `docs/changelog/index.md`.
- `make review-check` → OKF frontmatter, relative links, privacy checks over `.agents/bundles/knowledge/`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Stale name | Doc mentions `SyncInMemoryBrokerTransport` or `use_shm=False` | Removed; `git grep` over `docs/` prints nothing | Docs match code |
| Install commands | Package install snippet | Tabbed `uv` / `pip` / `pdm` / `poetry` blocks | Repo doc convention |
| Mermaid in `docs/` | Architecture overview | 4–6 conceptual nodes; detailed diagram lives in OKF | Doc layering |
| Broken relative link | OKF link to limitations record | `make review-check` fails | Link integrity |
| Windows | Named segments | Limitations record states unsupported | Honest scope |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given the finished flow, when `make docs` and `make review-check` run, then both exit `0` and every root-spec doc target is checked.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Limitations location | Dedicated `broker-pool-limitations.md` | Inline in the broker chapter: limitations grow independently and need a stable link target. |
| Changelog task | Separate task | Bundling with user docs: exceeds the per-task file bound. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/7 files documented]`
- `[ ]` `docs/usage/extensions.md` (`t01`)
- `[ ]` `docs/usage/databases.md` (`t01`)
- `[ ]` `docs/architecture/pyo3-and-workers.md` (`t01`)
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/data/broker-pool-limitations.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/index.md` (`t02`)
- `[ ]` `docs/changelog/index.md` (`t03`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R22a | [t01](tasks/t01-user-docs.md) | `documentation_validation` | `make docs` |
| R22b | [t02](tasks/t02-okf-and-limitations.md) | `documentation_validation` | `make review-check` |
| R22c | [t03](tasks/t03-changelog-entries.md) | `documentation_validation` | `make docs` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch09:t01 — Document broker pools, attach security, quarantine, engine matrix, and architecture (tasks/t01-user-docs.md)
- [ ] broker-pool-sharing:ch09:t02 — Update the OKF broker chapter and add the limitations record (tasks/t02-okf-and-limitations.md)
- [ ] broker-pool-sharing:ch09:t03 — Add `Unreleased` changelog entries (tasks/t03-changelog-entries.md)

**Chapter Recovery Checkpoint:** `make docs && make review-check` exits `0`, and `git grep -n "SyncInMemoryBrokerTransport\|AsyncInMemoryBrokerTransport\|use_shm=False" docs .agents/bundles/knowledge` prints nothing.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`0/3` completed).
- **Active Invariants**: Docs describe only shipped behavior; no private paths or internal product names.

[^docs-extensions]: `docs/usage/extensions.md` — Extensions usage guide.
[^docs-databases]: `docs/usage/databases.md` — Databases usage guide.
[^docs-arch]: `docs/architecture/pyo3-and-workers.md` — PyO3 and worker architecture.
[^changelog]: `docs/changelog/index.md` — Changelog.
[^broker-okf]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` — Broker knowledge chapter.
[^domains-index]: `.agents/bundles/knowledge/domains/index.md` — Domains index.
