---
type: Task
id: "server-runtime-correctness:ch11:t03"
chapter_id: ch11-product-okf-and-release-gate
title: Run the full verification gates and correctness plus fresh quality reviews
description: Run lint, Rust, Python, wheel smoke, docs, review-check, and review-route gates over c8722c76..HEAD, then a routed correctness review and a fresh zero-context quality review, fixing findings in place until both are clean.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "server-runtime-correctness:ch11:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/spec.md
  - .agents/bundles/specs/server-runtime-correctness/chapters/index.md
  - .agents/bundles/knowledge/workflow.md
  - .agents/skills/review-core/SKILL.md
  - .agents/skills/review-core/references/areas.toml
files: []
tests: []
estimated_loc: 0
plan_revision: 1
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
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Canonical Makefile verification commands
  - id: review-core
    resource: .agents/skills/review-core/SKILL.md
    title: Shared review areas, checklists, and finding ledger format
tags:
  - release-gate
  - review
---

# Task t03: Run the Full Verification Gates and Correctness plus Fresh Quality Reviews

## Objective & Scope Boundary

Prove the flow end to end and record evidence. Defects found by gates or reviews are fixed on the branch in the file owned by the originating task (reopening that worksheet's checklist state), never by weakening tests or gates. Out of scope: pushing, opening PRs, or archiving.

## Bounded Read Manifest

- [Root spec](../../../spec.md) and [chapter catalog](../../index.md).
- `.agents/bundles/knowledge/workflow.md` — gate commands [^workflow].
- `.agents/skills/review-core/SKILL.md` and `references/areas.toml` — routed review areas [^review-core].

## Symbol & Signature Contracts

- Gate sequence (all must exit `0`): `make lint-all`, `make rs-test`, `make py-test`, `make core-wheel-check`, `make docs-check`, `make review-check`, `make review-route BASE=c8722c76`.
- Correctness review over `c8722c76..HEAD` dispatched per routed area (`runtime-ipc`, `http-ingress`, `pyo3-bridge`, `python-framework`, `tests`, `docs-okf`) with the spec bundle as input.
- Fresh quality review (new zero-context reviewer) over the same range after correctness findings are resolved.

## Edge Cases & Error Semantics

- `/dev/shm` residue from interrupted runs is cleaned by `make rs-test` and the pytest autouse fixture before reruns.
- A gate that needs network (wheel deps) runs with the environment configured by `make setup-env`.

## Public-Seam Test Blueprint

- **Verification category**: `static_validation` (gate evidence).

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Full gates | Sequence above | All exit `0`; zero skips in `src/py/tests/integration/server` on Linux |
| Streaming acceptance | `uv run --no-sync pytest src/py/tests/integration/server -q` | All cases pass in embedded and SHM topologies |
| Wheel smoke | `make core-wheel-check` | Four scenarios pass |
| Review ledgers | Correctness and quality reviews | No open Critical or Important findings |

## Verification

- Final:
  ```bash
  make lint-all
  make rs-test
  make py-test
  make core-wheel-check
  make docs-check
  make review-check
  make review-route BASE=c8722c76
  ```
- Planned local commit: none unless review fixes are required (each fix uses the originating task's commit scope).

## Acceptance Criteria

- [ ] Every gate exits `0` (all requirements).
- [ ] Correctness and fresh quality reviews report no open Critical or Important findings (all requirements).
- [ ] Root spec Section 3 shows `[18/18 files documented]` and Section 6 records the verified checkpoint.

## Notes & Discoveries

[^workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical `Makefile` verification commands.
[^review-core]: `.agents/skills/review-core/SKILL.md` — Shared review areas, checklists, and finding ledger format.
