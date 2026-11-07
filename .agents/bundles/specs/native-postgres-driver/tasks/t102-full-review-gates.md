---
type: Task
id: "native-postgres-driver:t102"
title: "Run complete native PostgreSQL correctness and fresh quality acceptance"
description: "Run complete native PostgreSQL correctness and fresh quality acceptance for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:t101"
files:
  - .agents/bundles/specs/native-postgres-driver/spec.md
  - .agents/bundles/specs/native-postgres-driver/tasks/t102-full-review-gates.md
tests: []
plan_revision: 2
plan_commit: cdb978d3
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
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - cutover
  - native
---

# Run complete native PostgreSQL correctness and fresh quality acceptance

## Objective

Run complete native PostgreSQL correctness and fresh quality acceptance through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 50–120 evidence/state lines; no production edits. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

1. Confirm every preceding implementation/acceptance worksheet, including dedicated event hooks, benchmark harness and checkpoint acknowledgment reconciliation, is completed on one dedicated feature branch with one local signed conventional commit per invocation, exact final source/stub/package contracts and no remaining pending native feature. Inspect git status/diff preserving unrelated work; task-first reconcile checklist/state and actual evidence. This task records evidence only; any production correction is a revised bounded worksheet followed by a rerun of impacted gates.
2. Run correctness review against final branch diff using review-core route and all owning runtime/PyO3/Python/performance/testing/docs areas. Require actual native source ownership, protocol ordering/error/drain cases, GIL and SHM lifetime, pool/cancel generation, cache/proxy safety, cold/warm formats, COPY/Arrow, notifications/pgoutput, bind compiler boundaries and regression preservation. Record findings with file/line and test evidence; resolve all critical/important findings before quality review.
3. After correctness fixes finish, run mandatory fresh quality review from a new reviewer context using repository review skills and applicable paired reviewers; reviewer must not recycle an earlier checklist verdict. Evaluate line-budget/duplication/dead-code/public typing, test strength and truthful docs. No 'looks complete' substitutes for named native/live/wheel evidence. Read-only reviewers follow review-core command restrictions: they inspect executor-supplied fresh build/test evidence and may run permitted focused read-only checks, never make dev/make rs-test/make lint or Git mutations. The coordinator owns review lifecycle/agents and final knowledge curation; this worksheet does not grant external submission permission.
4. Execute full fresh gates below, strict no-skip PG14–18/features, installed wheel/free-threaded probes and benchmark report. Record commit hashes, environment/server versions and artifacts with local versus hosted outcomes separated. `make rs-test` cleans shared /dev/shm globs: coordinate test ownership before running so other users' SHM state is not destroyed. Use safe isolated namespace/process if shared state exists and document equivalent command. Every skip/failure/missing environment remains an open acceptance item.
5. Only after complete runtime/native/wheel/database/benchmark/docs evidence and both reviews pass, the coordinator may `/flow:finish native-postgres-driver` then curate evidence-backed nested OKF chapters and `/flow:archive native-postgres-driver` on this branch. Preserve complete scope until then; no first-slice archive or separate partial PR. Commit test(postgres): record complete native driver acceptance. No PR/push/hosted comment/release/tag action is authorized by this worksheet.

## Verification

This task changes packaging, docs, evidence or static gates; no fabricated behavioral RED is required. Validate the owned artifacts with the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
uv lock --check
make lint-all
make rs-test
make py-test
make ts-test
make coverage
make postgres-test
make postgres-wheel-check
make postgres-bench
make postgres-bench-check
make rs-bench
make bench-check
make docs-check
make review-check
make review-route
```

## Acceptance Criteria

- [ ] R8, R27: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
