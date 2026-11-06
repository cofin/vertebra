---
type: Task
id: native-postgres-driver:t101
title: Document complete native PostgreSQL installation behavior and operations
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
- native-postgres-driver:t100
files:
- docs/usage/databases.md
- docs/reference/database-capabilities.md
- src/py/vertebra/db/adapters/postgres/driver.py
- src/py/vertebra/db/adapters/postgres/config.py
- README.md
- Makefile
tests: []
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
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- cutover
- native
requirements:
- R27
---

# Document complete native PostgreSQL installation behavior and operations

## Objective

Document complete native PostgreSQL installation behavior and operations through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 180–280. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

1. Update PostgreSQL installation to vertebra-core[postgres]/vertebra-postgres and native ownership; document synchronous/asynchronous `$n` binds, SQLSpec default NUMERIC with explicit configs preserved and final transport compilation, SQLAlchemy compiled numeric_dollar and raw DBAPI contract. Remove psycopg/asyncpg/memory:// executor examples only for PostgreSQL, preserving other adapters. Constructor clean breaks and supplied pool ownership are explicit. Document ordinary one-statement extended execute/description versus explicit sync/async query_multi(sql) returning all simple-protocol PostgresQueryResult entries without parameters; multiSQL+parameters raises42601 and no path silently drops results.
2. Document transaction/savepoint pipeline scope, queued methods returning None, results never committing, rollback on result hydration/mismatch and cancellation/reuse rules; one logical flush/one Sync applies to batch, not transaction controls or one TCP packet. Document cold unknown OID text/warm binary per-column fallback, Decimal/interval/array/composite/pgvector supported values and limitations proven by tests.
3. Document native COPY/Arrow streaming resource closure, core ArrowCursor SHM ownership, direct_dsn dedicated LISTEN/CDC sessions, protocol3.0 PG14–18 baseline, logical replication setup/ack/replay/backpressure and test-owned service requirements. Pooler capability/cache settings distinguish verified PgBouncer protocol cache from an unverified proxy flag. Explain optional native wheel and no default live cloud acceptance; include local cloud transport/auth test evidence only.
4. Document precise user/PGUSER/OS-user and database/PGDATABASE/resolved-user defaults, supported explicit empty-field errors, raw pyformat ambiguous backslash-quote rejection requiring E strings and unchanged native `$n` SQL passthrough. Document dedicated event hooks/driver shutdown and explicit sslmode=disable only for non-TLS local fixtures. Add canonical verification commands for strict version/features, installed wheel/free-threaded and PostgreSQL connection/query benchmarks alongside existing gates. Update public docstrings in touched source only as needed for accurate options and Google style; no inline Python comments. Run docs build/link/artifact checks and inspect public examples against actual stub/API. Commit docs(postgres): document complete native driver contracts. Final evidence-backed knowledge curation and archive follow t102; do not delete spec here.

## Verification

This task changes packaging, docs, evidence or static gates; no fabricated behavioral RED is required. Validate the owned artifacts with the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
make docs
make docs-check
uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres
make review-check
```

## Acceptance Criteria

- [ ] R27: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
