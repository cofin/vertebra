---
type: Task
id: native-postgres-driver:t44
title: Session statement cache with modern pooler capability
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t43
files:
- src/rs/vertebra-postgres/src/statements.rs
- src/rs/vertebra-postgres/src/config.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/src/extended.rs
- src/rs/vertebra-postgres/src/transaction.rs
- src/py/vertebra/db/adapters/postgres/config.py
- src/rs/vertebra-postgres/tests/statement_cache.rs
- src/py/tests/unit/db/test_postgres_types.py
tests:
- src/rs/vertebra-postgres/tests/statement_cache.rs
- src/py/tests/unit/db/test_postgres_types.py
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-05T20:03:00Z'
requirements:
- R15
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
---

# Session statement cache with modern pooler capability

## Objective

Cache real server prepared statements only when connection and pooler capabilities make them safe.

## Context

Read `../spec.md`, direct dependency worksheets and notes, `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,query-pipeline-cdc-and-tenancy,broker-and-shm-leases}.md`, and applicable `.agents/skills/{pyo3-bridge,runtime-ipc}/SKILL.md`. New paths listed in this worksheet are intentional additions under the t01 scaffold; inspect existing parents and dependency APIs before creating them. This is one invocation and one signed conventional local commit on the same feature branch and in the complete-driver PR. Estimated 390 handwritten changed lines including tests; cap approximately400. Preserve unrelated edits. No external repository writes. If the contract/budget does not hold, route `/flow:revise native-postgres-driver` before changing scope. This task is pending and is not implementation approval.

## Steps

1. Write RED repeated-query transcripts proving named Parse once then Bind reuse, LRU eviction Close/CloseComplete, capacity0 unnamed statements, SQL/OID-distinct cache keys, reconnect/reset/schema invalidation and first-use failure cleanup, catalog Query destroying unnamed state, explicit metadata-scope pinning/re-Parse, and idle backend rotation with cache disabled. Test direct/session/transaction/statement/auto pooler modes with actual ParameterStatus capability tuples and configured older-PgBouncer fallback.

2. Implement statements.rs::{StatementCache,StatementKey,PoolerCapabilities}. Key by SQL, explicit/inferred parameter OIDs and session schema generation. Entries retain actual prepared name, parameter/column descriptions and negotiated codec metadata. Keep capacity bounded; close evicted names at a safe protocol boundary and never reuse an invalid generation. SHM compiled SQL metadata remains separate from physical server statement ownership. Default cold/cache0 execution follows t41/t40 one-group text and post-result normalization. If a named-statement preparation path requires separate Describe plus catalog lookup before Bind, use the established TransactionScope from t42 BEFORE Parse/Describe: own BEGIN at I or a savepoint at T, keep every intermediate ReadyForQuery at T, and finish only after execution/result decoding. For an unnamed fallback, re-Parse after every catalog Query before final Bind/Execute/Sync because Query destroys unnamed state. Never assume a named statement belongs to the next idle-rotated backend without verified protocol cache support. Error paths roll back only the owned scope; never commit ambient work.

3. Resolve effective capacity from pooler mode plus verified ParameterStatus pgbouncer.version/pgbouncer.pool_mode/pgbouncer.max_prepared_statements when advertised, with explicit protocol_level_pooler_cache configuration fallback for older servers. Transaction mode permits named protocol statements only when support is verified/configured; a server-advertised0 disables it. Statement mode disables scoped transactions/portals/pipelines and uses conservative unnamed queries. In auto mode, absent advertised pool mode/cache support and absent an explicit capability override, use capacity0 rather than assuming an older pooler is a direct server; an advertised mode applies the same per-mode rules. Do not issue SQL PREPARE/EXECUTE for this cache.

4. Keep PostgreSQLConfig existing aliases/flags and optional max_prepared_statements behavior compatible, while passing these capabilities through native configuration instead of ignoring them at pool construction. Avoid a new generic engine switch. Invalid prepared-name/schema errors invalidate cache but do not replay an executed DML; subsequent explicit calls can prepare afresh.

5. Retain cache only when the chosen reset policy preserves it; t46 DISCARD ALL invalidates it. Commit feat(postgres): cache prepared statements with pooler capabilities.

## Verification

Run the focused commands after introducing the public behavior tests to capture RED, then again after implementation to capture GREEN. New API absence may be the initial compile RED; existing-defect tasks require a behavioral reproduction. Use independent server fixtures or scripted peers, not implementation text scans.

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test statement_cache
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q
```

Run `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` and `git diff --check`. If Python tests load a changed extension, run `make dev` first, then `uv run --no-sync python -m mypy.stubtest vertebra._postgres` for changed public native surfaces. Record actual command/exit and assertion evidence; no command has run during planning. The parent acceptance task owns full native PG14–18, wheel, free-threaded, performance and aggregate acceptance; focused success does not close the charter.

## Acceptance Criteria

- [ ] R15: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Cache entries refer to real physical-session prepared statements and are invalidated safely.
- [ ] Modern transaction-mode PgBouncer can enable protocol caching; unsupported modes disable it.
- [ ] No automatic replay can duplicate DML side effects.
- [ ] Focused RED/GREEN evidence, relevant typing/lint and a clean task diff are recorded; exactly one signed local commit is linked.

## Notes & Discoveries

None.
