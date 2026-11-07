---
type: Task
id: "native-postgres-driver:t98"
title: "Verify complete native PostgreSQL wheels and strict CI service lanes"
description: "Verify complete native PostgreSQL wheels and strict CI service lanes for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:t97"
files:
  - .github/workflows/ci.yml
  - .github/workflows/release.yaml
  - Makefile
  - tools/scripts/verify_postgres_wheel.py
  - src/py/vertebra/_postgres.pyi
tests:
  - tools/scripts/verify_postgres_wheel.py
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

# Verify complete native PostgreSQL wheels and strict CI service lanes

## Objective

Verify complete native PostgreSQL wheels and strict CI service lanes through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 280–370; builds reuse t26 harness. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

Every test-owned vanilla PostgreSQL, pgvector, PgBouncer, administrative, cancellation observer and installed-wheel native config in this worksheet explicitly sets `sslmode="disable"`; these local images do not provide TLS. This is scoped to PostgreSQL fixtures and never changes other engine defaults or dedicated t05 TLS authentication fixtures.

1. Implement the final `tools/scripts/verify_postgres_wheel.py` harness by factoring the t26 installed-wheel recipe; reuse its frontend bootstrap, outside-checkout namespace checks and existing test_freethreaded_gil.py actual GIL-disabled import probe rather than duplicating that probe. Verify final Sync/AsyncPostgresClient and Sync/AsyncPostgresPool exports, native query/bulk/pipeline/cancel/stubs, adapter PostgresQueryResult classes and packaging parity. Build optional wheel plus core and sibling wheels, install with --no-deps into a fresh env outside checkout, set no PYTHONPATH and assert imported files belong to installed distributions. Run a real SELECT and transaction against test-owned service using installed adapter; uninstall postgres wheel then prove core/sibling imports retained and PostgreSQL missing-extra diagnostic correct.
2. Update release.yaml final exports and existing platform matrix for postgres on Linux x86_64/aarch64, macOS universal platform choices and Windows, cp312..cp315 plus cp314t/cp315t as supported by existing build matrix. Ensure all wheel/sdist/combined-install/version checks from scaffold include postgres; sdist builds without local checkout assets. No release, tag, dispatch or package publication is part of local acceptance.
3. Add strict CI native PostgreSQL service job parameterized14..18 with native extra installed, Docker available and command from t94; run feature acceptance on a designated version with logical replication/pgvector/PgBouncer fixtures. Existing test-python matrix3.12,3.13,3.14,3.14t,3.15,3.15t remains; ensure optional module built before stubtest and t26 fresh child asserts GIL disabled both before and after actual _postgres import. Add native query/pool concurrency smoke on both free-threaded versions using installed wheels; simultaneous threads cannot share Python refs unsafely.
4. Makefile dev/build/py-test/lint/lint-all already gain optional module in t01/t26; reconcile only omissions and add canonical postgres-test/postgres-wheel-check targets invoking these actual scripts/tests. Validate workflow syntax/actionlint if available and all static package matrices locally. Hosted green evidence is required only after the user authorizes exact PR delivery; record local and hosted results separately. Commit ci(postgres): verify complete native wheels and services.

## Verification

This task changes packaging, docs, evidence or static gates; no fabricated behavioral RED is required. Validate the owned artifacts with the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
make ts-install
make ts-build
uv lock --check
make dev
uv run --no-sync python -m mypy.stubtest vertebra._postgres
uv run --no-sync python tools/scripts/verify_postgres_wheel.py --help
make postgres-wheel-check
make postgres-test
make review-check
```

## Acceptance Criteria

- [ ] R26: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
