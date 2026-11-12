---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch33-final-review-and-release-gate
title: "Chapter 33: Complete-Driver Verification Gates, Routed Correctness Review, and Fresh Quality Review"
description: Reconcile all 33 chapters (119 tasks) and 33 documented files, run the complete verification and benchmark matrix, execute routed correctness review and fresh zero-context quality review over c8722c76..HEAD, and prepare active state for finish/archive.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch32-ffi-okf-and-review-routing-sync
replan_trigger: null
current_task: "native-postgres-driver:ch33:t01"
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-postgres-driver/spec.md
    title: Root native-postgres-driver 33-chapter roadmap and requirement matrix
  - id: root-review
    resource: .agents/bundles/specs/native-postgres-driver/review.md
    title: Native PostgreSQL planning and implementation review ledger
  - id: review-core-skill
    resource: .agents/skills/review-core/SKILL.md
    title: Shared review routing, checklist, and read-only reviewer protocol
  - id: review-script
    resource: tools/scripts/review_setup.py
    title: Automated review capability, DOC-07 resident-spec, and routing validator
  - id: root-makefile
    resource: Makefile
    title: Polyglot lint, test, coverage, postgres-*, wheel, benchmark, docs, and review gates
tags:
  - postgres
  - review
  - verification
  - acceptance
  - quality
---

# Chapter 33: Complete-Driver Verification Gates, Routed Correctness Review, and Fresh Quality Review

<!-- truth: start -->
- **Complete Roadmap & Documentation Gate (`33` Chapters, `119` Tasks, `[33/33]` Docs)**: Verifies `ch01`–`ch32` (`118` preceding tasks) and all `33` documentation, OKF, and skill files (`ch30`–`ch32`) are completed and runs every polyglot, zero-skip PostgreSQL `14`–`18`/service, outside-checkout wheel, benchmark, documentation, and review-routing command before sign-off [^root-spec] [^root-makefile].
- **Sequential Correctness Then Fresh Quality Review (`c8722c76..HEAD`)**: Routes the exact complete-driver range `c8722c76..HEAD` via `make review-route BASE=c8722c76`, completes multi-area correctness review first, spawns a fresh independent zero-context quality review second, and resolves any findings before recording final evidence in `review.md` [^root-review] [^review-core-skill] [^review-script].
- **Resident Active State for `DOC-07` Compliance**: Root `spec.md` remains `state: active` (`current_task: null`, `119/119` `[x]`) while resident under `specs/native-postgres-driver/` so `make review-check` (`DOC-07`) passes cleanly immediately ahead of atomic `/flow:finish` -> `/flow:archive` contraction [^review-script].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The native wheel (`ch01`–`ch21`, `ch22:t01`–`t02`) is on `main` in squash commit `c8722c76`; the remaining chapters ship in one follow-up pull request, so final release readiness requires one dedicated chapter after code (`ch22`–`ch27`), live service acceptance (`ch28`), wheel/CI/benchmark gates (`ch29`), and `33`-file documentation/OKF/skill synchronization (`ch30`–`ch32`) all land on the follow-up branch [^root-spec]. Pre-squash task SHAs recorded in `ch01`–`ch22` are reachable only through [PR #104](https://github.com/cofin/vertebra/pull/104) and are reconciled as mapping to `c8722c76`; follow-up task SHAs are reconciled directly.

This single-worksheet chapter (`ch33:t01`) reconciles all `33` chapters (`119` tasks) and `[33/33 files documented]`, executes the full polyglot and strict zero-skip PostgreSQL verification/benchmark command suite, routes `c8722c76..HEAD` through `make review-route BASE=c8722c76` for multi-area correctness review followed by an independent fresh zero-context quality review, records verified SHAs and outcomes in `review.md` and `spec.md`, and preserves `state: active` (`current_task: null`) so `make review-check` (`DOC-07`) exits `0` prior to `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` [^root-review] [^review-core-skill] [^review-script].

### 1.2 Non-Goals

- Implementing missing features or silently editing production code inside `ch33:t01` (any Critical or Important defect discovered during gates or review triggers a bounded remediation revision and gate re-run before closing `ch33:t01`).
- Setting `state: completed` on resident `specs/native-postgres-driver/spec.md` before `/flow:archive` (forbidden by `review_setup.py` `DOC-07` [^review-script]).
- Pushing branches, opening or updating GitHub PRs, merging PRs, or touching Git tags without separate explicit user instruction.

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); review ledger: [review.md](../../review.md); depends on [ch32-ffi-okf-and-review-routing-sync](../ch32-ffi-okf-and-review-routing-sync/spec.md). `tools/scripts/review_setup.py` fails `make review-check` if any resident `specs/*/spec.md` declares `state: completed`, making `state: active` with `current_task: null` and `119/119` `[x]` reconciled tasks the canonical pre-archive resting state [^review-script].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8.2 / R27.12 (`ch33:t01`)**: Verify `118/118` tasks across `ch01`–`ch32` and `[33/33 files documented]` are `[x] [<sha>]`, execute `uv lock --check`, `make lint-all`, `make rs-test` (with `/dev/shm` coordination), `make py-test`, `make ts-test`, `make coverage`, `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, `make postgres-bench-check`, `make rs-bench`, `make bench-check`, `make docs-check`, `make review-check`, and `make review-route BASE=c8722c76`, complete routed multi-area correctness review and a subsequent fresh zero-context quality review over `c8722c76..HEAD` with zero unresolved Critical/Important findings, record attested local verification and review summaries in `review.md` and `spec.md`, and keep root `spec.md` at `state: active` (`current_task: null`) for `DOC-07` compliance.

### 2.2 Gate & Review Sequence

1. Reconcile `ch01`–`ch32` task/chapter SHAs and `[33/33 files documented]`.
2. Execute full static, unit, coverage, live zero-skip PostgreSQL `14`–`18`/service, wheel isolation, benchmark, documentation, and review-routing commands.
3. Run bounded read-only correctness review across all areas emitted by `make review-route BASE=c8722c76` (`runtime-ipc`, `pyo3-bridge`, `python-framework`, `performance`, `tests`, `docs-okf`) against `contracts.md` and `R1`–`R27` (zero swarm board usage; sequential or small bounded subagent batch).
4. Run a fresh independent zero-context quality review over `c8722c76..HEAD` inspecting public API surface, dead code, test strength, line budgets (`AGENTS.md <= 100`, `knowledge/** <= 200`, `SKILL.md <= 200`), and present-tense prose.
5. Record gate results and review verdicts in `review.md` and synchronize `spec.md` (`state: active`, `current_task: null`, `last_reviewed_chapter: ch33-final-review-and-release-gate`), re-running `make review-check` to confirm exit `0`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Setting `state: completed` on root `spec.md` inside `ch33:t01` | Fails `make review-check` (`resident spec has 'state: completed'`); keep root `spec.md` `state: active` (`current_task: null`) until atomic `/flow:finish` -> `/flow:archive` curates and deletes `specs/native-postgres-driver/`. |
| Default `make review-route` diffing against a moving `origin/main` | Always pass `BASE=c8722c76` so review routing isolates the follow-up `native-postgres-driver` commit range (`c8722c76..HEAD`). |
| Any skipped PostgreSQL conformance/service test during `make postgres-test` | `VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` fail the suite on any skip or xfail; keep `ch33:t01` open until every service passes with zero skips. |
| Concurrent `/dev/shm` consumers during `make rs-test` | Check `/dev/shm` ownership first (or run in an isolated `/dev/shm` tmpfs mount namespace) because `make rs-test` removes `/dev/shm/test-*` and `/dev/shm/*vertebra*`. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Exact review diff range | `c8722c76..HEAD` (`make review-route BASE=c8722c76`) | Diffing against pre-squash SHAs that are not ancestors of `main`, or only `ch32..HEAD` — the first fails to resolve on `main` and the second misses earlier follow-up chapters. |
| Root `spec.md` post-`ch33:t01` `state` | Keep `state: active`, `current_task: null`, `33/33` `[x]` until `/flow:finish` + `/flow:archive` | Writing `state: completed` to disk while `specs/native-postgres-driver/` still resides in the tree — immediately breaks `make review-check` (`DOC-07`). |

## 3. Documentation & OKF Knowledge Plan

All `33` repository documentation, OKF knowledge, product, and skill files are completed across [ch30-user-and-workflow-docs](../ch30-user-and-workflow-docs/spec.md) (`12` files), [ch31-data-and-testing-okf-sync](../ch31-data-and-testing-okf-sync/spec.md) (`12` files), and [ch32-ffi-okf-and-review-routing-sync](../ch32-ffi-okf-and-review-routing-sync/spec.md) (`9` files). This chapter records final verification and review evidence in `.agents/bundles/specs/native-postgres-driver/review.md` and `.agents/bundles/specs/native-postgres-driver/spec.md`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Gate |
| --- | --- | --- | --- |
| R8, R27 (Complete-driver gates, correctness review, & fresh quality review) | [t01](tasks/t01-complete-driver-review-gates.md) | `static_validation` | Complete polyglot/PostgreSQL/wheel/bench/docs suite + `make review-check` + `make review-route BASE=c8722c76` + `review.md` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch33:t01 — Execute complete-driver verification gates, routed correctness review, and fresh zero-context quality review (tasks/t01-complete-driver-review-gates.md)

**Chapter Recovery Checkpoint:** `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check && make docs-check && make review-check && make review-route BASE=c8722c76` exits `0` with clean correctness and fresh quality reviews recorded in `review.md` and `spec.md` (`state: active`, `current_task: null`).

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `c8722c76`; `depends_on_chapters: [ch32-ffi-okf-and-review-routing-sync]`, `state: planned`, `plan_revision: 7`.
- **Remaining Tasks in Chapter**: `t01` (`1/1` pending).
- **Downstream Lifecycle Transition**: After `ch33:t01` records its signed local commit with `make review-check` exiting `0`, run `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` atomically on the feature branch.

[^root-spec]: `.agents/bundles/specs/native-postgres-driver/spec.md` — Root `native-postgres-driver` `33`-chapter roadmap and requirement matrix.
[^root-review]: `.agents/bundles/specs/native-postgres-driver/review.md` — Native PostgreSQL planning and implementation review ledger.
[^review-core-skill]: `.agents/skills/review-core/SKILL.md` — Shared review routing, checklist, and read-only reviewer protocol.
[^review-script]: `tools/scripts/review_setup.py` — Automated review capability, `DOC-07` resident-spec, and routing validator.
[^root-makefile]: `Makefile` — Polyglot `lint`, `test`, `coverage`, `postgres-*`, `wheel`, `benchmark`, `docs`, and `review` gates.
