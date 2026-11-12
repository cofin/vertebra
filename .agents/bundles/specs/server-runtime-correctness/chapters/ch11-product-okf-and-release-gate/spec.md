---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch11-product-okf-and-release-gate
title: "Chapter 11: Packaging OKF, Product Docs, Knowledge Indexes, and Final Release Gate"
description: Sync the packaging and PyO3 runtime OKF chapters, the domains and knowledge indexes, and the product and tech-stack docs to the delivered runtime, then run the full verification gates and the correctness and fresh quality reviews over c8722c76..HEAD.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch10-docs-and-okf-sync
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: packaging-okf
    resource: .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
    title: Packaging and PGO domain chapter
  - id: pyo3-gotchas
    resource: .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
    title: PyO3 and Python runtime gotchas including binary freshness
  - id: tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Distribution, maturin include globs, and CLI entrypoint statements
  - id: product
    resource: .agents/bundles/product/product.md
    title: Runtime topology and capability statements
tags:
  - okf
  - product
  - release-gate
---

# Chapter 11: Packaging OKF, Product Docs, Knowledge Indexes, and Final Release Gate

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`packaging-and-pgo.md` and `pyo3-and-python-runtime.md` describe binary resolution only for development targets and the absolute rpath as a feature [^packaging-okf] [^pyo3-gotchas]. `tech-stack.md` lists the maturin `include` globs without the server binary and `product.md` describes the embedded topology without the wheel-shipped binary or streaming guarantees [^tech-stack] [^product]. The domains and knowledge indexes do not list the new limitations chapter. This chapter syncs those files and runs the release gate for the flow.

### 1.2 Non-Goals

- Archiving the flow (`/flow:archive` runs after finish).
- Editing `.agents/bundles/index.md`, `specs/index.md`, or `log.md` (coordinator-owned).

### 1.3 Upstream Baseline & Code Analysis

`.agents/bundles/knowledge/domains/index.md` lists server chapters under "Server & Worker Runtime (`server/`)"; `.agents/bundles/knowledge/index.md` lists them under "Server & Worker Runtime (`domains/server/`)". `make review-route BASE=<sha>` maps changed files to review areas defined in `.agents/skills/review-core/references/areas.toml`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R18e** (`t01`): packaging OKF, PyO3 gotchas, domains index.
- **R18f** (`t02`): tech stack, product, knowledge index.
- **Release gate** (`t03`): full verification and both reviews.

### 2.2 Concrete Type & Signature Contracts

Documentation and verification only; OKF v0.2 frontmatter and provenance rules from `ch10` apply.

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| `/flow:finish` | `ch11:t03` gate | `make lint-all`, `make rs-test`, `make py-test`, `make core-wheel-check`, `make docs-check`, `make review-check`, `make review-route`, correctness reviewer, quality reviewer |

### 2.4 Edge-Case & Failure-Mode Matrix

| Risk | Control |
| --- | --- |
| Index link to a missing chapter | `make review-check` link validation. |
| Review finds a defect | Fix in place on the branch, rerun the failing gate, rerun the review. |
| Product docs drift from code | Each statement cites a verified symbol. |

### 2.5 Alternatives Considered

Merging product and index edits into `ch10` was rejected to keep every task at three owned files.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented in ch11]`.

| File | Task |
| --- | --- |
| `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` | `t01` |
| `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` | `t01` |
| `.agents/bundles/knowledge/domains/index.md` | `t01` |
| `.agents/bundles/product/tech-stack.md` | `t02` |
| `.agents/bundles/product/product.md` | `t02` |
| `.agents/bundles/knowledge/index.md` | `t02` |

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R18e | [t01](tasks/t01-packaging-okf-and-domains-index.md) | `documentation_validation` | `make review-check` |
| R18f | [t02](tasks/t02-product-docs-and-knowledge-index.md) | `documentation_validation` | `make review-check`, `make docs-check` |
| Release gate | [t03](tasks/t03-final-verification-and-reviews.md) | `static_validation` | Flow recovery checkpoint and reviews |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch11:t01 — Sync packaging OKF, PyO3 runtime gotchas, and the domains index (tasks/t01-packaging-okf-and-domains-index.md)
- [ ] server-runtime-correctness:ch11:t02 — Sync tech stack, product docs, and the knowledge index (tasks/t02-product-docs-and-knowledge-index.md)
- [ ] server-runtime-correctness:ch11:t03 — Run the full verification gates and correctness plus fresh quality reviews (tasks/t03-final-verification-and-reviews.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make core-wheel-check && make docs-check && make review-check && make review-route BASE=c8722c76` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t03` (`0/3`); `[0/6 files documented in ch11]`.
- **Downstream Dependents**: `/flow:finish server-runtime-correctness`.

[^packaging-okf]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Packaging and PGO domain chapter.
[^pyo3-gotchas]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 and Python runtime gotchas including binary freshness.
[^tech-stack]: `.agents/bundles/product/tech-stack.md` — Distribution, maturin include globs, and CLI entrypoint statements.
[^product]: `.agents/bundles/product/product.md` — Runtime topology and capability statements.
