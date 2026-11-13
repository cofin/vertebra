---
type: Spec
flow_id: native-redis-driver
chapter_id: ch08-changelog-and-okf-sync
title: "Chapter 08: Changelog, Product Manifest, Review Skill, and OKF Knowledge Synchronization"
description: "Update docs/changelog/index.md, AGENTS.md, product.md, redis-limitations.md, database-drivers-and-dialers.md, distributed-queues-and-email.md, review-pyo3-bridge/SKILL.md, domains/index.md, and workflow.md."
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
  - redis
  - valkey
  - limitations
sources:
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
  - id: agents-md
    resource: AGENTS.md
    title: "Repository agent instructions and Makefile commands"
  - id: product-md
    resource: .agents/bundles/product/product.md
    title: "OKF product definition and crate capability table"
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers OKF chapter"
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: "Distributed queues and email OKF chapter"
  - id: review-pyo3-skill
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: "PyO3 bridge review skill"
  - id: domains-idx
    resource: .agents/bundles/knowledge/domains/index.md
    title: "OKF domains catalog index"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project Makefile and verification workflow chapter"
---

# Chapter 08: Changelog, Product Manifest, Review Skill, and OKF Knowledge Synchronization

<!-- truth: start -->
- `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` record `vertebra-redis` (`[redis]`/`[valkey]`), `SyncNotificationSource`/`AsyncNotificationSource`, and removal of `vertebra_db::resp3` [^docs-changelog] [^agents-md] [^product-md].
- `redis-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, and `distributed-queues-and-email.md` capture present-tense Redis/Valkey architecture and operational caveats [^okf-drivers] [^okf-queues].
- `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` list `vertebra-redis`, `redis-limitations.md`, `make redis-test`, and `make redis-wheel-check` [^review-pyo3-skill] [^domains-idx] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Removing `vertebra_db::resp3` changes the `vertebra-db` capability row in `.agents/bundles/product/product.md` [^product-md], and adding `vertebra-redis` + `make redis-test` / `make redis-wheel-check` requires synchronized updates across `AGENTS.md`, `workflow.md`, `review-pyo3-bridge/SKILL.md`, and a dedicated `redis-limitations.md` OKF chapter (`<= 200` lines) [^agents-md] [^okf-workflow].

**Non-Goals:** Distributed channel extensions owned by `queues-tasks-and-channels`.

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Changelog & product | `docs/changelog/index.md`, `AGENTS.md`, `product/product.md` [^docs-changelog] [^agents-md] [^product-md] | Release notes, crate inventory, and `vertebra-db` table accuracy |
| OKF chapters | `knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md` [^okf-drivers] [^okf-queues] | Present-tense OKF chapters (`<= 200` lines each) |
| Skills & indexes | `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` [^review-pyo3-skill] [^domains-idx] [^okf-workflow] | `DOC-08` `Makefile` parity and OKF index links |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R19b**: Update `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` (`t01`).
- **R19c**: Author `.agents/bundles/knowledge/domains/data/redis-limitations.md` (`<= 200` lines) and update `database-drivers-and-dialers.md` and `distributed-queues-and-email.md` (`t02`).
- **R19d**: Update `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, and `.agents/bundles/knowledge/workflow.md` (`t03`).

```text
redis-limitations.md required caveats (<= 200 lines):
1. Dedicated socket per SyncRedisListener / AsyncRedisListener handle (subscribed sockets are isolated from multiplexed command pipelines).
2. Multi-key pipelines and Lua scripts on Redis/Valkey Cluster require matching {hash_tag} keys to avoid CROSSSLOT ProgrammingError.
3. Server-assisted CLIENT TRACKING invalidation requires RESP3 (protocol=3) on Redis 7.2+/8.0 or Valkey 8.0+/9.0.
4. unix:// sockets apply strictly to standalone single-node deployments, never Cluster or Sentinel TCP topologies.
```

**Integration Flow:**
- `make review-check && make docs` validates every link, `sources:` path, line budget (`<= 200`), and `Makefile` target.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Stale `product.md` row | `parse_resp3_frame` left in `vertebra-db` table | Updated to `vertebra-redis` in `t01` | `DOC-02` / `DOC-04` truth |
| Skill/OKF line cap | File exceeds `200` lines | Kept concise and `<= 200` lines | `ReviewSetupValidator` budget |

## 3. Documentation & OKF Knowledge Plan

This chapter completes `[12/12]` documentation, skill, product, and OKF deliverables for `native-redis-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R19b` | `t01` | `docs/changelog/index.md`, `AGENTS.md`, `.agents/bundles/product/product.md` | `make review-check && make docs` |
| `R19c` | `t02` | `knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md` | `make review-check` |
| `R19d` | `t03` | `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-changelog-agents-and-product-sync.md](tasks/t01-changelog-agents-and-product-sync.md) — Record vertebra-redis, NotificationSource split, and resp3 removal in changelog, AGENTS.md, and product.md.
- [ ] [t02-okf-redis-limitations-and-domain-docs.md](tasks/t02-okf-redis-limitations-and-domain-docs.md) — Create redis-limitations.md (<= 200 lines) and update database-drivers-and-dialers.md and distributed-queues-and-email.md.
- [ ] [t03-review-skill-and-okf-indexes-sync.md](tasks/t03-review-skill-and-okf-indexes-sync.md) — Add vertebra-redis, redis-limitations.md, and make redis-test/redis-wheel-check to review-pyo3-bridge/SKILL.md, domains/index.md, and workflow.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: Every OKF chapter and review skill stays `<= 200` lines with zero broken relative links or stale `resp3` references.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && make docs`

[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^agents-md]: `AGENTS.md` — Repository agent instructions and Makefile commands.
[^product-md]: `.agents/bundles/product/product.md` — OKF product definition and crate capability table.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
