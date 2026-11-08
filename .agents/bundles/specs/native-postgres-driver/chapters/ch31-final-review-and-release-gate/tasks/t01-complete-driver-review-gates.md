---
type: Task
id: "native-postgres-driver:ch31:t01"
chapter_id: ch31-final-review-and-release-gate
title: Execute routed correctness review, fresh quality review, and complete-driver acceptance gates
description: Reconcile all 31 chapters (111 tasks) and 11 documented files, run routed correctness review and fresh zero-context quality review across the full branch diff, execute every canonical verification and benchmark gate, and record final acceptance in review.md and spec.md.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch30:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/spec.md
  - .agents/bundles/specs/native-postgres-driver/review.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/skills/review-core/SKILL.md
  - tools/scripts/review_setup.py
  - Makefile
files:
  - .agents/bundles/specs/native-postgres-driver/review.md
  - .agents/bundles/specs/native-postgres-driver/spec.md
tests: []
estimated_loc: 120
plan_revision: 7
plan_commit: 1c75af65
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:45:00Z"
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-postgres-driver/spec.md
    title: Root native-postgres-driver roadmap and requirement traceability
  - id: root-review
    resource: .agents/bundles/specs/native-postgres-driver/review.md
    title: Native PostgreSQL planning and implementation review ledger
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen public native PostgreSQL contracts
  - id: review-core-skill
    resource: .agents/skills/review-core/SKILL.md
    title: Shared review routing, checklist, and read-only reviewer protocol
  - id: review-script
    resource: tools/scripts/review_setup.py
    title: Automated review capability and OKF bundle validator
  - id: root-makefile
    resource: Makefile
    title: Full polyglot lint, test, coverage, docs, wheel, and benchmark gates
tags:
  - postgres
  - review
  - acceptance
  - verification
---

# Task t01: Execute Routed Correctness Review, Fresh Quality Review, and Complete-Driver Acceptance Gates

## Objective & Scope Boundary

Perform final task-first state reconciliation, multi-area correctness review, fresh independent quality review, and complete local verification across `native-postgres-driver`, recording attested acceptance evidence in `.agents/bundles/specs/native-postgres-driver/review.md` and `.agents/bundles/specs/native-postgres-driver/spec.md` [^root-spec] [^root-review] [^pg-contracts] [^review-core-skill] [^review-script] [^root-makefile]:
1. Verify every preceding chapter (`ch01`–`ch30`, `110` tasks) is `[x] [<sha>]` (`completed`) on the dedicated feature branch with `[11/11 files documented]`, exact `_postgres.pyi` / wheel / adapter / `change_stream` parity, and zero pending native features or legacy fallbacks. Any code or doc defect uncovered during review or verification requires opening a bounded remediation revision and re-running affected gates rather than editing production files inside `ch31:t01`.
2. Route the complete branch diff via `make review-route` (`runtime-ipc`, `pyo3-bridge`, `python-framework`, `performance`, `tests`, `docs-okf`) and execute correctness review verifying protocol framing/drain safety, GIL detachment & `ShmCapsule` lifetimes, pool/cancel generation fencing, `PgBouncer` cache rules, cold/warm OID formats, lossless `Pg*` codecs, binary `COPY`/`VTBARROW`, dedicated `LISTEN` & `pgoutput` v1 `change_stream` checkpoint CAS, and lexer-aware bind boundaries; resolve all Critical/Important findings before proceeding.
3. Spawn a fresh zero-context quality review (`@quality-reviewer` + paired read-only domain reviewers obeying `review-core` read-only command rules) auditing semantic surface, dead code, public typing, test strength, `DOC-08` line budgets (`AGENTS.md <= 100`, `knowledge/** <= 200`), and zero-slop present-tense prose without recycling prior review state.
4. Coordinate `/dev/shm` ownership before running `make rs-test` (using an isolated mount namespace if concurrent processes hold `/dev/shm` segments), run the complete verification and benchmark command suite below with zero PostgreSQL skips or xfails, and record commit SHAs, server/interpreter versions, benchmark artifact summaries, and review verdicts (separating local verified outcomes from hosted CI status) in `review.md` and `spec.md` ahead of `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver`.

Out of scope: Pushing commits, opening PRs, dispatching hosted workflows, mutating Git tags, or deleting the spec directory before `/flow:archive`.

## Bounded Read Manifest

- [.agents/bundles/specs/native-postgres-driver/spec.md](../../../spec.md) — Root checklist, `[11/11]` documentation manifest, and requirement matrix (`R1`–`R27`) [^root-spec].
- [.agents/bundles/specs/native-postgres-driver/review.md](../../../review.md) — Adjudicated planning ledger to extend with implementation correctness/quality verdicts [^root-review].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Authoritative public contracts verified against final code [^pg-contracts].
- [.agents/skills/review-core/SKILL.md](../../../../../../../.agents/skills/review-core/SKILL.md) — Area routing, severity criteria, and read-only reviewer constraints [^review-core-skill].
- [tools/scripts/review_setup.py](../../../../../../../tools/scripts/review_setup.py) — `review-check` and `review-route` validation rules [^review-script].
- [Makefile](../../../../../../../Makefile) — Canonical verification, wheel, PostgreSQL service, and benchmark targets [^root-makefile].

## Symbol & Signature Contracts

- `.agents/bundles/specs/native-postgres-driver/review.md` & `.agents/bundles/specs/native-postgres-driver/spec.md`:
  - Synchronize `last_reviewed_chapter: ch31-final-review-and-release-gate`, `last_reviewed_commit: <head_sha>`, `last_verified_checkpoint: <head_sha>`, `[11/11 files documented]`, and complete `ch01`–`ch31` (`111/111` tasks) evidence in present tense with multi-line YAML frontmatter and `<= 40`-line truth blocks.

## Edge Cases & Error Semantics

- Read-only reviewer subagents never run `make dev`, `make rs-test`, `make lint`, or mutating Git commands; they inspect fresh executor build/test/benchmark artifacts and permitted read-only checks.
- Any skipped PostgreSQL 14–18, logical replication, `pgvector` 0.8.1, `PgBouncer` 1.26.0, outside-checkout wheel, free-threaded `3.14t`/`3.15t`, or PG18 benchmark gate keeps `ch31:t01` open until executed and passing.
- `make rs-test` deletes `/dev/shm/test-*` and `/dev/shm/*vertebra*`; verify no sibling process owns active `/dev/shm` segments (or run under an isolated `/dev/shm` mount) before invoking `make rs-test`.

## Public-Seam Test Blueprint

- **Canonical validation gates**: Full repository, live-service, wheel, benchmark, documentation, and review suite (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full lint, type, stubtest, & polyglot test suite | `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage` | All exit `0`; `mypy.stubtest` passes across all 6 native extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`). |
| Strict live PG14–18, feature, wheel, & benchmark suite | `make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check` | All exit `0` with zero PostgreSQL skips/xfails and `comparison.json` `passed == true`. |
| Docs, OKF, & two-stage review sign-off | `make docs-check && make review-check && make review-route` + correctness review + fresh quality review | Zero Critical/Important findings remain; `review.md` and `spec.md` record exact commit SHAs and gate outputs across all `31` chapters (`111` tasks). |

## Verification

- Static, unit, & coverage gates: `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage`
- Native PostgreSQL live, wheel, benchmark, & docs/review gates: `make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check && make docs-check && make review-check && make review-route`

## Acceptance Criteria

- [ ] Every requirement `R1`–`R27` across all `31` chapters (`111` tasks) is verified complete with passing polyglot tests, strict PG14–18/feature acceptance, outside-checkout wheel isolation, free-threaded probes, paired PG18 benchmarks, and `[11/11 files documented]` (R8, R27).
- [ ] Routed correctness review and a subsequent fresh zero-context quality review pass with zero unresolved Critical/Important findings, gating `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` on the local feature branch (R8).

## Notes & Discoveries

- [2026-11-07 16:45] After `ch31:t01` commits local acceptance evidence, the next canonical lifecycle command is `/flow:finish native-postgres-driver` followed by `/flow:archive native-postgres-driver`.

[^root-spec]: `.agents/bundles/specs/native-postgres-driver/spec.md` — Root `native-postgres-driver` roadmap and requirement traceability.
[^root-review]: `.agents/bundles/specs/native-postgres-driver/review.md` — Native PostgreSQL planning and implementation review ledger.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public native PostgreSQL contracts.
[^review-core-skill]: `.agents/skills/review-core/SKILL.md` — Shared review routing, checklist, and read-only reviewer protocol.
[^review-script]: `tools/scripts/review_setup.py` — Automated review capability and OKF bundle validator.
[^root-makefile]: `Makefile` — Full polyglot lint, test, coverage, docs, wheel, and benchmark gates.
