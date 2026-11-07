---
type: Task
id: native-postgres-driver:t97
title: Accept native bulk notifications vectors and transaction poolers live
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t96
files:
- src/py/tests/integration/db/test_postgres_features.py
- src/py/tests/integration/conftest.py
tests:
- src/py/tests/integration/db/test_postgres_features.py
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

# Accept native bulk notifications vectors and transaction poolers live

## Objective

Accept native bulk notifications vectors and transaction poolers live through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 300–380 using completed t95/t96 live service fixtures. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

Every test-owned vanilla PostgreSQL, pgvector, PgBouncer, administrative, cancellation observer and installed-wheel native config in this worksheet explicitly sets `sslmode="disable"`; these local images do not provide TLS. This is scoped to PostgreSQL fixtures and never changes other engine defaults or dedicated t05 TLS authentication fixtures.

1. Reuse t96 test-owned services and add strict native live acceptance: binary COPY in/out and Arrow schema/Decimal/time/JSON/arrays roundtrip, bounded portal/Arrow stream early-close and cancellation, live LISTEN/NOTIFY delivery plus rollback suppression, and no Python tuple materialization in native Arrow route. Reuse predecessor focused tests rather than reimplementing their protocol fixtures; this task joins actual adapter+native+server boundaries.
2. Require completed t95 live logical-replication adapter acceptance on PostgreSQL14–18; reuse its explicit source/ack/replay/gap/backpressure/slot-cleanup report in the full feature evidence. Do not reimplement replication protocol or duplicate those tests here. Cross-check direct_dsn selection through adapter feature configuration.
3. Use completed t96 digest-pinned pgvector0.8.1 PG14/18 fixtures and assert observed extversion0.8.1, CREATE EXTENSION vector in test-owned database, and verify native vector/halfvec/bit binds/results, dimensions/error handling, NULL and warm/cold OID fallback through adapter SQL. Test PostgresEmbeddingPushdown with a test-owned SQL embedding function returning vector; this proves binding/order/search but is not a live AlloyDB model claim.
4. Reuse t96 local PgBouncer fixture with transaction pooling, max_prepared_statements=0 and >0 cases, session/direct controls, and capability negotiation for auto/PgCat/pgdog policy. Verify disabled unsafe cache, safe named statement reuse, transaction stickiness, stale plan invalidation and dedicated direct_dsn for notifications/CDC. Use t96 completed explicit service lifecycle and assert cache0/cache200 controls; no inferred service capability or skipped proxy lane enters acceptance. Commit test(postgres): accept complete native feature paths.

## Verification

Write the named observable RED tests first; record the actual failure. Implement only the owned behavior and run the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
make dev
VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_features.py -q -rs
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
uv run --no-sync ruff check src/py/tests/integration/db/test_postgres_features.py src/py/tests/integration/conftest.py
```

## Acceptance Criteria

- [ ] R25: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
