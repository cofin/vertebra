---
type: Task
id: native-postgres-driver:t94
title: Activate strict native PostgreSQL 14 through 18 driver conformance
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t93
files:
- src/py/tests/integration/conftest.py
- src/py/tests/integration/db/test_driver_conformance.py
- src/py/tests/integration/db/test_postgres_native.py
tests:
- src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
- src/py/tests/integration/db/test_postgres_native.py::test_active_query_cancel
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- cutover
- native
requirements:
- R25
---

# Activate strict native PostgreSQL 14 through 18 driver conformance

## Objective

Activate strict native PostgreSQL 14 through 18 driver conformance through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 300–380. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

Every test-owned vanilla PostgreSQL, pgvector, PgBouncer, administrative, cancellation observer and installed-wheel native config in this worksheet explicitly sets `sslmode="disable"`; these local images do not provide TLS. This is scoped to PostgreSQL fixtures and never changes other engine defaults or dedicated t05 TLS authentication fixtures.

1. Add PostgreSQL-specific types DDL with NUMERIC, BOOLEAN, BYTEA, TIMESTAMPTZ, JSONB rather than generic TEXT/INTEGER/BLOB, and enable supports_pipeline=True for PostgreSQL. Remove only PostgreSQL pending_flow_id/strict xfail after prerequisites exist; leave other pending engines unchanged. Existing typed-null, Decimal, UTF8, binary, JSON, affected-row, schema hydration, transaction, typed error and Arrow assertions must all execute natively.
2. Change only vtb_postgres_config/its versioned PostgreSQL configurations to pass explicit sslmode="disable" (no generic fixture default mutation for other engines), then parameterize container-backed PostgreSQL conformance through exact installed postgres_14_service..postgres_18_service fixtures, using PostgresService coordinates with native clients and psql readiness. Keep nearest plugin declarations and dynamic service ports; preserve explicit environment DSN as a separate opt-in lane. Strict VERTEBRA_CONFORMANCE_DOCKER=1 acceptance has no skip/xfail for native postgres, no psycopg connection fixture, and reports server_version_num for all five versions. Use test-owned schema/database names and teardown in finally.
3. Add active-query cancellation for sync (worker thread) and async: start SELECT pg_sleep, observe the exact backend PID active with matching query via separate native observer and bounded polling, then exercise explicit driver.cancel and async task.cancel separately. Explicit driver.cancel yields the active query's canonical QueryCancelledError with actual server SQLSTATE57014. Async task.cancel preserves asyncio.CancelledError at the caller; any server57014 belongs to independent native cleanup and never replaces that caller outcome. Both paths require observed CancelRequest, bounded completion, bounded original-response drain or close followed by mandatory physical-session retirement whenever cancellation may have been sent, and a subsequent SELECT1 using a fresh backend PID. Ambient cancelled transaction stays failed until rollback; no replacement mid-transaction. Include concurrent second borrower so delayed cancel cannot hit it; use events/barriers and bounded deadlines, never idle cancel alone.
4. Run existing driver conformance tests for each version, including pipeline atomic rollback and one-Sync protocol tests from predecessors. Fixture setup failures are failures in strict lane. Commit test(postgres): enforce native PostgreSQL version conformance.

## Verification

Write the named observable RED tests first; record the actual failure. Implement only the owned behavior and run the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
make dev
VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py -q -k postgres -rs
uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py
```

## Acceptance Criteria

- [ ] R25: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
