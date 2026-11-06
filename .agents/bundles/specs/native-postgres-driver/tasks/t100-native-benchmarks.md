---
type: Task
id: native-postgres-driver:t100
title: Measure native PostgreSQL connection query and pipeline performance
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
- native-postgres-driver:t99
files:
- src/rs/vertebra-postgres/Cargo.toml
- src/rs/vertebra-postgres/benches/postgres_native.rs
- tools/benchmarks/postgres.py
- src/py/tests/integration/db/test_postgres_benchmarks.py
- Makefile
tests:
- src/rs/vertebra-postgres/benches/postgres_native.rs
- src/py/tests/integration/db/test_postgres_benchmarks.py
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

# Measure native PostgreSQL connection query and pipeline performance

## Objective

Measure native PostgreSQL connection query and pipeline performance through the existing public adapter and consumer contracts.

## Context

Read `../spec.md` and the frozen complete public contracts integrated there, dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`, `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/pyo3-bridge/SKILL.md` and owning review skills. This is the complete native driver on one feature branch and one PR; no partial completion or early archive is permitted. No implementation starts while plan_commit is null.

Ownership is exactly the frontmatter list and named symbols. Shared files change sequentially after predecessors; preserve unrelated edits. One executor invocation produces one signed conventional local commit. Estimated changed handwritten lines including tests: 230–330. Include removals in the measured diff; if the task cannot remain near400 lines, stop and `/flow:revise native-postgres-driver` to split boundaries while preserving complete one-PR scope. Do not hide deferred implementation in a verification task. Never submit hosted artifacts, dispatch workflows, push or mutate tags.

Native contracts are prerequisite seams, not new engines to implement here: SyncPostgresClient/AsyncPostgresClient own Rust sessions; SyncPostgresPool/AsyncPostgresPool own concrete Rust leases; PostgresQueryResult has frozen adapter-owned PostgresColumn metadata, tuple rows, affected_rows, command_tag and I/T/E transaction status. Resolve exact implemented signatures from `_postgres.pyi` and predecessor acceptance; do not duck-type psycopg/asyncpg or invent parallel runtime APIs.

## Steps

1. Implement native capture dispatch in the completed t99 module using exact release-built Sync/AsyncPostgresClient and high-level driver APIs, no alternate engine. Cold direct client constructor/connect/query-probe/close is100 measured connects,10 warmups,5 groups20; reference psycopg3/asyncpg uses the same service/TLS/user/database and includes equivalent lifecycle. Retained raw lease prepared `SELECT $1::bigint + 1` (reference `SELECT %s::bigint + 1` for psycopg, native `$1` for asyncpg) uses1000 fully consumed queries/sample×5,10 unmeasured warmups; native values/counts must match reference before timing is accepted. High-level transient pool acquire/release/DISCARD ALL and retained-transaction query latency are separate named scenarios; do not benchmark one permanent native lease against a reset-heavy reference or claim reset cost as warm-query speed.
2. Pipeline capture runs64 parameterized SELECT statements/batch×100 batches/sample×5 and10 warmups through actual native pipeline context versus pinned psycopg3 sync/async pipeline. Keep one explicit ambient transaction/physical lease across the warmups and samples for both implementations; each measured batch uses matching nested savepoint scope, all64 commands queued before reference Pipeline.sync/result fetch, complete row consumption and clean savepoint release. Outer commit/lease reset occurs after timing and is separately reported; assert64 results and expected sums. Record measured batch ns and normalized statement ns/throughput with explicit counts. Optional Arrow100k-row native read is diagnostic with exact typed schema/count/checksum; no Arrow performance gate uses a nonmatching reference. Criterion postgres_native.rs benchmarks real Rust cold/retained-query paths against the same fixture DSN; do not replace database I/O with CPU frame encoding.
3. Add dedicated integration benchmark orchestrator `test_postgres_benchmarks.py` excluded from ordinary fast suites by registered postgres_benchmark marker. Its function-scoped postgres_18_service fixture owns the ephemeral server, native test config sslmode="disable" and a uniquely named benchmark schema. Clientless psql setup creates bench_values(id bigint primary key,payload text), populates100000 deterministic rows, records checksum and truncates/repopulates before each paired run. All benchmark write samples execute in rollback scopes; teardown drops only owned schema and closes every raw/pool connection. The orchestrator exports VERTEBRA_POSTGRES_BENCH_DSN only to child environments, uses a single server/dataset/hardware/paired_run_id for reference/native (child env VERTEBRA_POSTGRES_BENCH_RUN_ID and VERTEBRA_POSTGRES_BENCH_SCHEMA supply paired identity and validated generated schema), and invokes the exact capture commands below. A whole paired run with CV>0.15 is repeated once fresh, then fails. It invokes comparison and asserts no missing gated scenario, never compares historical pre-native reports or fabricates a baseline.
4. Add new Makefile targets explicitly: `postgres-bench` runs `UV_PYTHON=3.12 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q -s -m postgres_benchmark` after release build and bench extras setup; `postgres-bench-check` invokes completed comparator on artifacts/postgres/reference.json and native.json with exact thresholds from t99. Keep source configuration private, output structured artifacts and versions only. Existing rs-bench/bench-check remain required and are labeled core/HTTP coverage. Record command exits, hardware/server/package/git versions, raw samples and comparison JSON. Commit perf(postgres): measure complete native driver against pinned references.

## Verification

t99 already supplies deterministic comparator RED coverage; this worksheet wires native runtime scenarios and measures them, without fabricating benchmark failures as unit-test RED. Validate the owned artifacts with the commands below. Commands are planned, not evidence. Record command, exit status and the actual public assertion. Rebuild with `make dev` after Rust changes and before Python tests; preserve optional installed extras with `--no-sync`. A skipped service test is not acceptance.

```bash
make build
uv sync --python 3.12 --all-extras --dev --group bench
UV_PYTHON=3.12 UV_NO_SYNC=1 make build
cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --no-run
make postgres-bench
make postgres-bench-check
make rs-bench
make bench-check
```

Capture module commands execute only inside the dedicated orchestrator's owned PG18 fixture environment; the shell verification invokes the orchestrator and artifact comparison, never a cleaned-up or arbitrary DSN. `make postgres-bench-check` compares existing paired artifacts without opening a network connection.

## Acceptance Criteria

- [ ] R27: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] All Steps contracts and named negative cases pass without weakening existing behavior.
- [ ] No old Python client, in-memory fallback, stale extension or synthetic success substitutes for the native public seam.
- [ ] This task has one reviewed local signed conventional commit and synchronized worksheet/spec state.

## Notes & Discoveries

None.
