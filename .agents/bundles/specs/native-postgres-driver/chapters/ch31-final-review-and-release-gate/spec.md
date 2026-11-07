---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch31-final-review-and-release-gate
title: "Chapter 31: Routed Correctness Review, Fresh Quality Review, and Complete-Driver Release Gate"
description: Reconcile all 31 chapters (111 tasks) and 11 documented files, run routed multi-area correctness review followed by a fresh zero-context quality review, execute the complete polyglot/service/wheel/benchmark/docs verification suite, and record attested acceptance in review.md and spec.md.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch30-user-and-domain-docs
replan_trigger: null
current_task: "native-postgres-driver:ch31:t01"
plan_revision: 5
plan_commit: 399e068c
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:45:00Z"
    commit: dd432f18
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-postgres-driver/spec.md
    title: Root native-postgres-driver roadmap and requirement traceability
  - id: root-review
    resource: .agents/bundles/specs/native-postgres-driver/review.md
    title: Native PostgreSQL planning and implementation review ledger
  - id: review-core-skill
    resource: .agents/skills/review-core/SKILL.md
    title: Shared review routing, checklist, and read-only reviewer protocol
  - id: review-script
    resource: tools/scripts/review_setup.py
    title: Repository review routing, OKF frontmatter, and privacy validator
  - id: root-makefile
    resource: Makefile
    title: Full polyglot lint, test, coverage, docs, wheel, and benchmark gates
tags:
  - postgres
  - review
  - acceptance
  - verification
---

# Chapter 31: Routed Correctness Review, Fresh Quality Review, and Complete-Driver Release Gate

<!-- truth: start -->
- **Two-Stage Branch Review (`R8`)**: `ch31:t01` runs `make review-route` correctness review across all touched review areas (`runtime-ipc`, `pyo3-bridge`, `python-framework`, `performance`, `tests`, `docs-okf`), resolves any Critical/Important findings, and then spawns a fresh zero-context quality review auditing semantic surface, dead code, typing, test strength, and zero-slop prose [^review-core-skill] [^review-script].
- **Full Complete-Driver Verification Gate (`R8`, `R27`)**: Reconciles all `31` chapters (`111` tasks) and `[11/11 files documented]`, coordinates `/dev/shm` ownership before `make rs-test`, and passes every polyglot lint/test/coverage, live PG14–18/replication/`pgvector`/`PgBouncer`, outside-checkout wheel, `3.14t`/`3.15t` GIL, PG18 benchmark, Zensical docs, and `review-check` gate with zero PostgreSQL skips or xfails [^root-spec] [^root-review] [^root-makefile].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

After Chapters 01–30 implement, cut over, live-test, package, benchmark, and document `vertebra-postgres` across all `11` user and OKF files, one dedicated final gate is required before `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` [^root-spec] [^root-review]. Separating the final review and branch sign-off (`ch31:t01`) from prose authoring (`ch30:t01`–`t04`) ensures the complete branch diff (`origin/main...HEAD`) is audited as a single finished artifact [^review-core-skill] [^review-script] [^root-makefile].

### 1.2 Non-Goals

- Editing production Rust, Python, or documentation files directly inside `ch31:t01` without opening a bounded remediation revision and re-running impacted gates if a review finding requires code changes.
- Deleting `.agents/bundles/specs/native-postgres-driver/` inside `ch31:t01` (spec archival belongs exclusively to `/flow:archive native-postgres-driver` after `/flow:finish`).
- Pushing branches, opening pull requests, dispatching hosted workflows, or creating/modifying Git tags without explicit user authorization.

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); review ledger: [review.md](../../review.md); depends on [ch30-user-and-domain-docs](../ch30-user-and-domain-docs/spec.md). Skills: [review-core](../../../../../skills/review-core/SKILL.md) and all routed `review-*` domain skills.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R8 & R27 (`ch31:t01`)**: Reconcile all `31` chapters (`111` tasks) task-first and confirm `[11/11 files documented]`, run `make review-route` correctness review across all touched areas (`runtime-ipc`, `pyo3-bridge`, `python-framework`, `performance`, `tests`, `docs-okf`) and resolve every Critical/Important finding before running a fresh zero-context quality review, coordinate `/dev/shm` cleanup before `make rs-test`, execute every canonical verification, live-service, wheel, benchmark, and docs command with zero PostgreSQL skips/xfails, and record commit/environment/artifact evidence in `review.md` and `spec.md`.

### 2.2 Gate & Collaborator Map

- `make review-route` (`BASE=origin/main`) -> read-only area correctness reviewers -> remediation if needed -> fresh zero-context `@quality-reviewer` + read-only domain reviewers.
- Full verification suite (`uv lock --check`, `make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, `make coverage`, `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, `make postgres-bench-check`, `make rs-bench`, `make bench-check`, `make docs-check`, `make review-check`) -> attested evidence in `.agents/bundles/specs/native-postgres-driver/review.md` and `spec.md`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Concurrent `/dev/shm` usage on shared host before `make rs-test` | Inspect `/dev/shm` ownership prior to running `make rs-test` (which cleans `/dev/shm/test-*` and `/dev/shm/*vertebra*`) or run in an isolated mount namespace so active sibling processes are never disrupted. |
| Read-only reviewer subagent command boundary | Reviewer subagents never execute `make dev`, `make rs-test`, `make lint`, or mutating Git commands; they inspect fresh executor build/test/benchmark outputs and permitted read-only checks. |
| Skipped Docker/wheel/benchmark test or unresolved Critical/Important finding | Blocks `ch31:t01` completion until remediated and re-verified with `0` skips/xfails. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Quality review context | Spawn a fresh zero-context quality reviewer after all correctness fixes and `ch30` doc/OKF updates land | Reusing the correctness reviewer conversation or reviewing before `ch30` completes — causes confirmation bias or misses `DOC-04`/`DOC-08` drift. |
| Dedicated Chapter 31 sign-off | Isolate `review.md` + `spec.md` final reconciliation and full-branch verification in `ch31:t01` | Combining `11`-file doc updates with final review gates in a single chapter — obscures whether documentation is complete before final branch review begins. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (`ch31:t01` verifies that all `[11/11 files documented]` from [ch30-user-and-domain-docs](../ch30-user-and-domain-docs/spec.md) pass `make docs-check` and `make review-check`, and records final acceptance evidence in `review.md` and `spec.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Gate |
| --- | --- | --- | --- |
| R8, R27 | [t01](tasks/t01-complete-driver-review-gates.md) | `static_validation` | `make lint-all`, `make test`, `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench-check`, `make docs-check`, `make review-check`, `make review-route` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch31:t01 — Execute routed correctness review, fresh quality review, and complete-driver acceptance gates (tasks/t01-complete-driver-review-gates.md)

**Chapter Recovery Checkpoint:** `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage && make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check && make docs-check && make review-check && make review-route` exits `0` with both correctness and fresh quality reviews clean.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; `depends_on_chapters: [ch30-user-and-domain-docs]`, `state: planned`, `plan_revision: 3`.
- **Remaining Tasks in Chapter**: `t01` (`1/1` pending).
- **Active Invariants**: `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` run only after `ch31:t01` records complete verified evidence across all `31` chapters (`111` tasks) on the local feature branch.

[^root-spec]: `.agents/bundles/specs/native-postgres-driver/spec.md` — Root `native-postgres-driver` roadmap and requirement traceability.
[^root-review]: `.agents/bundles/specs/native-postgres-driver/review.md` — Native PostgreSQL planning and implementation review ledger.
[^review-core-skill]: `.agents/skills/review-core/SKILL.md` — Shared review routing, checklist, and read-only reviewer protocol.
[^review-script]: `tools/scripts/review_setup.py` — Repository review routing, OKF frontmatter, and privacy validator.
[^root-makefile]: `Makefile` — Full polyglot lint, test, coverage, docs, wheel, and benchmark gates.
