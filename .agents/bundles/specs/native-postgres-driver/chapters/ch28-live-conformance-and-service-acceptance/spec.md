---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch28-live-conformance-and-service-acceptance
title: "Chapter 28: Live PG14-18 Conformance, Logical Replication, pgvector, and Pooler Acceptance"
description: Verify the cut-over native PostgreSQL driver against live Docker PostgreSQL 14-18 servers, logical replication WAL streams, digest-pinned pgvector 0.8.1 containers, and SCRAM-authenticated PgBouncer transaction poolers.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch27-atomic-cutover-and-legacy-removal
replan_trigger: null
current_task: "native-postgres-driver:ch28:t01"
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:45:00Z"
    commit: dd432f18
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Clientless pytest-databases Docker harness fixtures for database integration tests
  - id: driver-conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix and pipeline suite
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL DSN, sslmode, pooler_mode, and direct_dsn configuration
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL LISTEN/NOTIFY source and publication helpers
  - id: pg-embeddings
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: PostgreSQL SQL embedding pushdown and pgvector similarity search
tags:
  - postgres
  - conformance
  - replication
  - pgvector
  - pgbouncer
  - integration
---

# Chapter 28: Live PG14–18 Conformance, Logical Replication, `pgvector`, and Pooler Acceptance

<!-- truth: start -->
- **Strict PG14–18 Conformance**: `ENGINE_SPECS` for `postgres` removes `pending_flow_id`, sets `supports_pipeline=True` with PostgreSQL-native `NUMERIC`/`BOOLEAN`/`BYTEA`/`TIMESTAMPTZ`/`JSONB` DDL, and passes strict `VERTEBRA_CONFORMANCE_DOCKER=1` conformance plus active-query cancellation (`57014` vs `asyncio.CancelledError`) across PostgreSQL 14, 15, 16, 17, and 18 with zero skips or xfails [^int-conftest] [^driver-conformance].
- **Live Logical Replication & Checkpoints**: `PostgresChangeSource` (`vertebra.db.adapters.postgres.change_stream`) streams `pgoutput` v1 changes from `wal_level=logical` PostgreSQL 14–18 containers, enforces durable checkpoint persistence before advancing `confirmed_flush_lsn`, replays at-least-once after reconnect, propagates consumer backpressure, and cleans up all test-owned slots and publications.
- **Pinned `pgvector` 0.8.1 & `PgBouncer` 1.26.0 Services**: Digest-pinned `pgvector/pgvector:0.8.1-pg14` and `0.8.1-pg18` containers plus `edoburu/pgbouncer:v1.26.0-p0` (`transaction` mode `max_prepared_statements=0` and `200` over Docker bridge IPs) validate binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, `PgVector`/`PgBitString`, `PostgresEmbeddingPushdown`, statement-cache safety, and `direct_dsn` event routing [^pg-config] [^pg-notify] [^pg-embeddings].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Prior to Chapter 28, `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` marks `postgres` with `pending_flow_id="native-postgres-driver"` and `supports_pipeline=False` using generic SQLite-oriented `_TYPES_DDL`, while `src/py/tests/integration/conftest.py` exposes only a single unversioned `vtb_postgres_config` without explicit `sslmode="disable"`, `wal_level=logical`, digest-pinned `pgvector` 0.8.1, or `PgBouncer` transaction-pooler containers [^int-conftest] [^driver-conformance].

This chapter proves end-to-end live server behavior across four vertical-slice acceptance tasks:
1. Strict PG14–18 conformance, native types DDL, single-`Sync` pipeline rollback, and live backend `CancelRequest` retirement (`ch28:t01`) [^driver-conformance].
2. Live PG14–18 `wal_level=logical` `pgoutput` v1 replication, durable checkpoint CAS before `confirmed_flush_lsn` feedback, reconnect replay, backpressure, and slot cleanup (`ch28:t02`).
3. Reusable digest-pinned `pgvector` 0.8.1 (PG14/PG18) and `edoburu/pgbouncer:v1.26.0-p0` (`max_prepared_statements=0` and `200`) Docker service fixtures with cold metadata and `VACUUM` transaction-pooler smoke controls (`ch28:t03`) [^int-conftest].
4. Full live feature acceptance across binary `COPY`, `VTBARROW` `ArrowCursor` / portal streaming, `LISTEN`/`NOTIFY` rollback suppression, `PgVector` / `PgBitString` / `PostgresEmbeddingPushdown`, and `PgBouncer` transaction pooling (`ch28:t04`) [^pg-config] [^pg-notify] [^pg-embeddings].

### 1.2 Non-Goals

- Mutating non-PostgreSQL engine entries in `ENGINE_SPECS` (`oracle`, `mysql`, `mssql`, `duckdb`, `sqlite`, `bigquery`, `spanner`).
- Live external Google Cloud SQL or AlloyDB network calls in local or CI test runs (cloud Admin/mTLS/IAM paths are verified via deterministic wire/TLS harnesses in Chapters 04–06).
- Packaging wheel isolation, GitHub Actions workflow edits, and Criterion / reference benchmarks (owned by [ch29-wheel-ci-and-benchmarks](../ch29-wheel-ci-and-benchmarks/spec.md)).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch27-atomic-cutover-and-legacy-removal](../ch27-atomic-cutover-and-legacy-removal/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md), [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), and `pytest-databases`. Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [benchmarks-and-free-threaded.md](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R25.1 (`ch28:t01`)**: Add `_POSTGRES_TYPES_DDL` (`NUMERIC(18, 6)`, `BOOLEAN`, `BYTEA`, `TIMESTAMPTZ`, `JSONB`), set `supports_pipeline=True` and `pending_flow_id=None` on the `postgres` `EngineConformanceSpec` in `test_driver_conformance.py`, update `vtb_postgres_config` and versioned PG14–18 fixtures in `integration/conftest.py` to pass explicit `sslmode="disable"`, parameterize PG14–18 strict Docker conformance (`VERTEBRA_CONFORMANCE_DOCKER=1` with zero skips/xfails), and verify live sync/async `SELECT pg_sleep(...)` cancellation (`driver.cancel()` -> `QueryCancelledError` `57014`; `task.cancel()` -> `asyncio.CancelledError` + background retirement + fresh `pg_backend_pid()`).
- **R25.2 (`ch28:t02`)**: Provision session-scoped logical PostgreSQL 14–18 containers in `integration/conftest.py` via `docker_service.run(..., command=["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"])` and verify `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource` `INSERT`/`UPDATE`/`DELETE`/TOAST/rollback ordering, durable checkpoint CAS before `confirmed_flush_lsn` feedback, reconnect replay, bounded queue backpressure, cancellation, and zero leaked slots in `pg_replication_slots`.
- **R25.3 (`ch28:t03`)**: Provision digest-pinned `pgvector` 0.8.1 PG14 (`sha256:2b8cdfaad8e380ef81202cb2df1db35a83482ca7c6f4d933d01d84c294ba64ff`) and PG18 (`sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c`) fixtures plus ephemeral `edoburu/pgbouncer:v1.26.0-p0` (`AUTH_TYPE=scram-sha-256`, `POOL_MODE=transaction`, `MAX_PREPARED_STATEMENTS=0` and `200`, session control, Docker bridge `DB_HOST`) in `integration/conftest.py`, verified by capability, cold-metadata, backend-rotation, `VACUUM`, and negative credential/cache smoke tests in `test_postgres_services.py`.
- **R25.4 (`ch28:t04`)**: Verify live binary `COPY` in/out, native `VTBARROW` `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow` (without Python row tuple materialization), portal early-close/cancel, `LISTEN`/`NOTIFY` delivery + rollback suppression, `PgVector`/`PgBitString`/`PostgresEmbeddingPushdown` over `pgvector` 0.8.1, and `PgBouncer` transaction-mode statement caching + `direct_dsn` event bypass in `test_postgres_features.py`.

### 2.2 Fixture & Collaborator Map

- `pytest_databases` `postgres_14_service`..`postgres_18_service` -> `integration/conftest.py` (`sslmode="disable"`) -> `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` / `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` (`test_driver_conformance.py`, `test_postgres_native.py`).
- `docker_service.run("postgres:<14..18>", wal_level=logical)` -> `integration/conftest.py` -> `PostgresChangeSource` + `PostgresCheckpointStore` (`vertebra.db.adapters.postgres.change_stream`, `test_postgres_replication.py`).
- `provide_postgres_service(pgvector@sha256:...)` + `docker_service.run("edoburu/pgbouncer:v1.26.0-p0")` -> `integration/conftest.py` -> `test_postgres_services.py` & `test_postgres_features.py`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Local Docker containers without TLS | PostgreSQL integration fixtures pass explicit `sslmode="disable"` without altering production `PostgreSQLConfig` (`prefer`) or raw client (`verify-full`) defaults. |
| `VERTEBRA_CONFORMANCE_DOCKER=1` with unreachable Docker or setup failure | Fails loud in strict mode; never skips or xfails PostgreSQL conformance, replication, `pgvector`, or `PgBouncer` tests. |
| Explicit `driver.cancel()` vs `asyncio.Task.cancel()` on active `pg_sleep` | `driver.cancel()` raises `QueryCancelledError` (`sqlstate == "57014"`); `task.cancel()` raises `asyncio.CancelledError` while independent Rust cleanup retires the backend PID and leaves concurrent pool borrowers untouched. |
| `PostgresChangeSource` checkpoint save failure or non-contiguous commit ack | Feedback LSN (`confirmed_flush_lsn`) does not advance; reconnect replays from last durable checkpoint LSN and signed-`int64` `next_event_id`. |
| `PgBouncer` `transaction` mode with `max_prepared_statements=0` vs `200` | `0` disables protocol statement cache even if an optimistic cache flag is tested; cold custom-type queries and `VACUUM` execute without implicit `BEGIN` or destroyed unnamed statements; `LISTEN` and `START_REPLICATION` require `direct_dsn`. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| `pgvector` container tags | Pin verified multi-arch OCI digests for `pgvector/pgvector:0.8.1-pg14` and `0.8.1-pg18` and assert `extversion == "0.8.1"` | Floating `:pg14` / `:pg18` tags — silently drift wire dimension limits or `halfvec` behavior across CI runs. |
| `PgBouncer` upstream target | Dial backing PostgreSQL container's Docker default-bridge IP (`NetworkSettings`) on `:5432` | Dialing `localhost` or host loopback from inside `PgBouncer` container — fails inside isolated bridge namespaces without `network_mode=host`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (live server capabilities and verification commands proven here are documented in [ch30-user-and-domain-docs](../ch30-user-and-domain-docs/spec.md) tasks `ch30:t01`–`ch30:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R12, R18, R25 | [t01](tasks/t01-pg14-18-strict-conformance.md) | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_postgres_native.py` |
| R22, R23, R25 | [t02](tasks/t02-live-logical-replication-acceptance.md) | `behavior_tdd` | `src/py/tests/integration/db/test_postgres_replication.py` |
| R12, R15, R25 | [t03](tasks/t03-pgvector-and-pooler-service-fixtures.md) | `behavior_tdd` | `src/py/tests/integration/db/test_postgres_services.py` |
| R15, R19, R20, R21, R25 | [t04](tasks/t04-live-copy-notify-vector-pooler-acceptance.md) | `behavior_tdd` | `src/py/tests/integration/db/test_postgres_features.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch28:t01 — Activate strict PostgreSQL 14-18 driver conformance and live active-query cancellation (tasks/t01-pg14-18-strict-conformance.md)
- [ ] native-postgres-driver:ch28:t02 — Verify live PG14-18 pgoutput logical replication, durable checkpoints, and slot cleanup (tasks/t02-live-logical-replication-acceptance.md)
- [ ] native-postgres-driver:ch28:t03 — Provision digest-pinned pgvector 0.8.1 and PgBouncer 1.26.0 acceptance service fixtures (tasks/t03-pgvector-and-pooler-service-fixtures.md)
- [ ] native-postgres-driver:ch28:t04 — Verify live binary COPY, VTBARROW, notifications, pgvector pushdown, and PgBouncer pooling (tasks/t04-live-copy-notify-vector-pooler-acceptance.md)

**Chapter Recovery Checkpoint:** `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py src/py/tests/integration/db/test_postgres_replication.py src/py/tests/integration/db/test_postgres_services.py src/py/tests/integration/db/test_postgres_features.py -q -rs && uv run --no-sync ruff check src/py/tests/integration` exits `0` with zero PostgreSQL skips or xfails.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `e8901de4`; `depends_on_chapters: [ch27-atomic-cutover-and-legacy-removal]`, `state: planned`, `plan_revision: 4`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Active Invariants**: All local Docker PostgreSQL fixtures pass `sslmode="disable"` explicitly; every test-created schema, database, replication slot, publication, and container is cleaned up deterministically in `finally` / fixture teardown.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Clientless `pytest-databases` Docker harness fixtures for database integration tests.
[^driver-conformance]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix and pipeline suite.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, `sslmode`, `pooler_mode`, and `direct_dsn` configuration.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL `LISTEN`/`NOTIFY` source and publication helpers.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — PostgreSQL SQL embedding pushdown and `pgvector` similarity search.
