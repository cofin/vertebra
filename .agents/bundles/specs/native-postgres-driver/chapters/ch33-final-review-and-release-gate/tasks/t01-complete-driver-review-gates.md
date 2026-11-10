---
type: Task
id: "native-postgres-driver:ch33:t01"
chapter_id: ch33-final-review-and-release-gate
title: Execute complete-driver verification gates, routed correctness review, and fresh zero-context quality review
description: Reconcile all 33 chapters (118 tasks) and 33 documented files, run the complete polyglot/PostgreSQL/wheel/benchmark/docs/review gate matrix, execute routed correctness review and fresh quality review over 88c30004..HEAD, and record final evidence in review.md and active spec.md.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch32:t03"
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
estimated_loc: 135
plan_revision: 6
plan_commit: ef86844f
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-postgres-driver/spec.md
    title: Root native-postgres-driver 33-chapter checklist, [33/33] doc manifest, and R1-R27 matrix
  - id: root-review
    resource: .agents/bundles/specs/native-postgres-driver/review.md
    title: Native PostgreSQL planning and implementation review ledger
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Authoritative native PostgreSQL public contracts
  - id: review-core-skill
    resource: .agents/skills/review-core/SKILL.md
    title: Shared review routing, checklist, and read-only reviewer protocol
  - id: review-script
    resource: tools/scripts/review_setup.py
    title: Automated review capability, DOC-07 resident-spec, and diff routing validator
  - id: root-makefile
    resource: Makefile
    title: Full polyglot lint, test, coverage, postgres-*, wheel, benchmark, docs, and review gates
tags:
  - postgres
  - review
  - acceptance
  - verification
  - quality
---

# Task t01: Execute Complete-Driver Verification Gates, Routed Correctness Review, and Fresh Zero-Context Quality Review

## Objective & Scope Boundary

Reconcile all `33` chapters (`118` tasks) and `[33/33 files documented]`, execute the complete local verification and benchmark matrix, run routed multi-area correctness review followed by a fresh zero-context quality review across `88c30004..HEAD`, and record attested review/verification summaries in `.agents/bundles/specs/native-postgres-driver/review.md` and `.agents/bundles/specs/native-postgres-driver/spec.md` [^root-spec] [^root-review] [^pg-contracts] [^review-core-skill] [^review-script] [^root-makefile]:
1. Confirm every preceding chapter (`ch01`–`ch32`, `117` tasks) is `[x] [<sha>]` (`completed`) on `feat/native-postgres-driver`, `[33/33 files documented]` is complete across `ch30` (`12`), `ch31` (`12`), and `ch32` (`9`), and zero legacy `psycopg`/`asyncpg` runtime paths or `memory://` PostgreSQL branches exist. Any defect uncovered during gates or review opens a bounded remediation revision and re-runs affected gates rather than editing source/doc files ad hoc inside `ch33:t01`.
2. Coordinate `/dev/shm` before `make rs-test` (verifying no concurrent process holds live `/dev/shm` segments or using an isolated `/dev/shm` mount), then run `uv lock --check`, `make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, `make coverage`, `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, `make postgres-bench-check`, `make rs-bench`, `make bench-check`, `make docs-check`, `make review-check`, and `make review-route BASE=88c30004` with zero PostgreSQL skips or xfails [^root-makefile].
3. Execute routed multi-area correctness review across the exact git range `88c30004..HEAD` (`runtime-ipc`, `pyo3-bridge`, `python-framework`, `performance`, `tests`, `docs-okf` from `make review-route BASE=88c30004`, using sequential or a small bounded set of read-only reviewer subagents and never invoking the swarm board) auditing Protocol 3.0 framing/drain safety, GIL detachment, `ShmCapsule` ownership, `PendingDeliveryGuard` async drop safety, physical pool/cancel generation fencing, `PoolerMode` cache rules, `'Y'` catalog retention, lossless `Pg*` codecs, delayed-commit single-`Sync` `pipeline()`, binary `COPY`/`copy_from_arrow`/`select_to_arrow`, dedicated `LISTEN`/`NOTIFY` & `pgoutput` v1 `change_stream` CAS checkpointing, `$n`/`numeric_dollar` compilation, and `PostgreSQLQueueBackend` execution against `contracts.md` and `R1`–`R27` [^pg-contracts] [^review-core-skill].
4. After zero Critical/Important correctness findings remain, run a fresh independent zero-context quality review (`@quality-reviewer` obeying read-only `reviewer_forbidden_commands`) over `88c30004..HEAD` evaluating public API surface, dead code, typing, test strength vs tautology, `DOC-08` line budgets (`AGENTS.md <= 100`, `knowledge/** <= 200`, `SKILL.md <= 200`), Zensical HTML/TOC ceilings, and present-tense zero-slop prose.
5. Record the final verification inventory (commit SHAs, PostgreSQL `14`–`18` / `pgvector` `0.8.1` / `PgBouncer` `1.26.0` / Python `3.12`–`3.15t` versions, benchmark comparison summary, correctness and quality review verdicts, clearly distinguishing local verified runs from hosted CI) in `review.md`, synchronize `spec.md` keeping `state: active` (`current_task: null`, `118/118` `[x]`, `last_reviewed_chapter: ch33-final-review-and-release-gate`, `last_reviewed_commit`, `last_verified_checkpoint`) so `DOC-07` (`make review-check`) passes cleanly before atomic `/flow:finish` -> `/flow:archive` contraction [^root-spec] [^root-review] [^review-script].

Out of scope: Writing `state: completed` to resident `spec.md`, pushing branches, opening/merging PRs, dispatching hosted workflows, or modifying Git tags.

## Bounded Read Manifest

- [.agents/bundles/specs/native-postgres-driver/spec.md](../../../spec.md) — Root `33`-chapter checklist, `[33/33]` doc list, and `R1`–`R27` traceability [^root-spec].
- [.agents/bundles/specs/native-postgres-driver/review.md](../../../review.md) — Review ledger receiving final gate and dual-review summaries [^root-review].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Public constructor, pool, codec, pipeline, Arrow, event, and CDC contracts [^pg-contracts].
- [.agents/skills/review-core/SKILL.md](../../../../../../../.agents/skills/review-core/SKILL.md) — Area manifests, severity criteria, and read-only reviewer constraints [^review-core-skill].
- [tools/scripts/review_setup.py](../../../../../../../tools/scripts/review_setup.py) — `DOC-07` resident-spec check and `review-route` CLI [^review-script].
- [Makefile](../../../../../../../Makefile) — Canonical verification, wheel, live-service, benchmark, docs, and review targets [^root-makefile].

## Symbol & Signature Contracts

- `.agents/bundles/specs/native-postgres-driver/spec.md` retains `state: active` with `current_task: null`, `[33/33 files documented]`, all `33` chapter checklist lines marked `[x] [<sha>]`, `<= 40`-line truth block, zero standalone `---` body rules, and valid ancestor SHAs in `verified:`.
- `.agents/bundles/specs/native-postgres-driver/review.md` retains `state: active` (or valid `Review` frontmatter without resident `completed` spec violation), `<= 40`-line truth block, zero standalone `---` body rules, and concise present-tense gate and correctness/quality review summaries across `88c30004..HEAD`.

## Edge Cases & Error Semantics

- Read-only reviewer subagents never run `make dev`, `make lint`, `make rs-test`, `make py-test`, or mutating Git/file commands; they inspect fresh executor artifacts and run permitted read-only checks.
- Any skip or xfail during `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test`, wheel isolation failure, benchmark regression failure, or `verify_docs_artifacts.py` failure blocks completion of `ch33:t01`.
- Re-run `make review-check` as the final step after editing `review.md` and `spec.md` to confirm `DOC-07`, truth-block bounds, footnote parity, link resolution, and privacy checks exit `0` right before signing the commit.

## Public-Seam Test Blueprint

- **Canonical validation gates**: Full polyglot, zero-skip PostgreSQL `14`–`18`/service, wheel, benchmark, documentation, and review-routing suite (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Static, stubtest, polyglot test, & coverage gates | `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage` | All exit `0`; `mypy.stubtest` passes across `_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_postgres`, and `_spanner`. |
| Zero-skip live PG14–18/services, wheel isolation, & benchmarks | `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check` | All exit `0` with `0` PostgreSQL skips, `0` xfails, isolated wheel/sdist checks green across runtimes (including `3.14t`/`3.15t` disabled-GIL probes), and `comparison.json` `passed == true`. |
| Documentation, `DOC-07` bundle check, & `88c30004..HEAD` reviews | `make docs-check && make review-check && make review-route BASE=88c30004` + routed correctness review + fresh quality review | All exit `0`; zero unresolved Critical/Important findings across `88c30004..HEAD`; `review.md` records evidence while root `spec.md` stays `state: active` (`current_task: null`). |

## Verification

- Static, unit, & coverage gates: `uv lock --check && make lint-all && make rs-test && make py-test && make ts-test && make coverage`
- Live zero-skip PostgreSQL, wheel, benchmark, docs, & review gates: `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 make postgres-test && make postgres-wheel-check && make postgres-bench && make postgres-bench-check && make rs-bench && make bench-check && make docs-check && make review-check && make review-route BASE=88c30004`

## Acceptance Criteria

- [ ] Every requirement `R1`–`R27` across all `33` chapters (`118` tasks) and `[33/33 files documented]` is verified green with zero PostgreSQL skips or xfails across polyglot tests, PG14–18/replication/`pgvector`/`PgBouncer` services, outside-checkout wheels, `3.14t`/`3.15t` disabled-GIL probes, benchmarks, docs, and `make review-check` (R8, R27).
- [ ] Routed multi-area correctness review and a subsequent fresh zero-context quality review over `88c30004..HEAD` conclude with zero unresolved Critical/Important findings recorded in `review.md`, leaving root `spec.md` cleanly at `state: active` (`current_task: null`) ready for `/flow:finish native-postgres-driver` and `/flow:archive native-postgres-driver` (R8).

## Notes & Discoveries

- Immediately after committing `ch33:t01`, execute `/flow:finish native-postgres-driver` followed by `/flow:archive native-postgres-driver` on the local working branch so final OKF curation is verified and `specs/native-postgres-driver/` contracts cleanly with `make review-check` remaining green.

[^root-spec]: `.agents/bundles/specs/native-postgres-driver/spec.md` — Root `native-postgres-driver` `33`-chapter checklist, `[33/33]` doc manifest, and `R1`–`R27` matrix.
[^root-review]: `.agents/bundles/specs/native-postgres-driver/review.md` — Native PostgreSQL planning and implementation review ledger.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Authoritative native PostgreSQL public contracts.
[^review-core-skill]: `.agents/skills/review-core/SKILL.md` — Shared review routing, checklist, and read-only reviewer protocol.
[^review-script]: `tools/scripts/review_setup.py` — Automated review capability, `DOC-07` resident-spec, and diff routing validator.
[^root-makefile]: `Makefile` — Full polyglot `lint`, `test`, `coverage`, `postgres-*`, `wheel`, `benchmark`, `docs`, and `review` gates.
