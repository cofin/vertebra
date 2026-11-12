---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch28-live-conformance-and-service-acceptance
title: "Chapter 28: Live PG14-18 Conformance, Logical Replication, pgvector, and Pooler Acceptance"
description: Verify the cut-over native PostgreSQL driver against live Docker PostgreSQL 14-18 servers, logical replication WAL streams, multi-arch digest-pinned pgvector 0.8.1 containers, and SCRAM-authenticated PgBouncer 1.26.0 transaction poolers.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch27-atomic-cutover-and-legacy-removal
replan_trigger: null
current_task: "native-postgres-driver:ch28:t01"
plan_revision: 8
plan_commit: d90d4625
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T04:25:15Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Clientless pytest-databases Docker harness fixtures and strict zero-skip hooks
  - id: driver-conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix, _resolve_live_config, and pipeline suite
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
- **Gated Offline Safety & Strict Zero-Skip Conformance**: `_resolve_live_config()` and all PostgreSQL Docker fixtures skip cleanly when neither `VERTEBRA_CONFORMANCE_POSTGRES_DSN` nor `VERTEBRA_CONFORMANCE_DOCKER == "1"` is set (keeping offline `make py-test`/`coverage` green), while `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` fails the pytest session if any selected PostgreSQL test skips or xfails across PG14–18 [^int-conftest] [^driver-conformance].
- **Live Logical Replication & Parameterized Suites**: `PostgresChangeSource` streams `pgoutput` v1 WAL events across PG14–18 `wal_level=logical` containers with durable checkpoint CAS before `confirmed_flush_lsn` advances and zero leaked `pg_replication_slots`, using `@pytest.mark.parametrize` / `@pytest.fixture(params=...)` (zero test-body `for` loops over cases/backends).
- **Verified Multi-Arch `pgvector` 0.8.1 & SCRAM `PgBouncer` 1.26.0**: Multi-arch OCI index digests for `pgvector:0.8.1-pg14`/`0.8.1-pg18` and in-container `sh -c` `/tmp/pgbouncer.{users,ini}` startup using `pg_authid.rolpassword` (`SCRAM-SHA-256$...`) validate `COPY`, `/dev/shm`-clean `VTBARROW`, `LISTEN`/`NOTIFY`, `PgVector`/`PgBitString`, and `transaction` pooling (`max_prepared_statements=0` and `200`) [^pg-config] [^pg-notify] [^pg-embeddings].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Prior to Chapter 28, `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` marks `postgres` with `pending_flow_id="native-postgres-driver"` and `supports_pipeline=False` using generic SQLite-oriented `_TYPES_DDL`, while `src/py/tests/integration/conftest.py` exposes an unversioned `vtb_postgres_config` without explicit `VERTEBRA_CONFORMANCE_DOCKER == "1"` gating, zero-skip enforcement, `sslmode="disable"`, `wal_level=logical`, multi-arch `pgvector` 0.8.1, or SCRAM-verified `PgBouncer` 1.26.0 containers [^int-conftest] [^driver-conformance].

This chapter proves end-to-end live server behavior across four parameterized acceptance tasks (zero `for` loops over test cases or sync/async backends inside pytest functions):
1. Offline-safe `VERTEBRA_CONFORMANCE_DOCKER == "1"` gating + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` session enforcement, `_POSTGRES_TYPES_DDL`, PG14–18 conformance, single-`Sync` pipeline rollback, and live backend `CancelRequest` retirement (`ch28:t01`) [^driver-conformance].
2. Live PG14–18 `wal_level=logical` `pgoutput` v1 replication, durable checkpoint CAS before `confirmed_flush_lsn` feedback, reconnect replay, backpressure, and slot cleanup (`ch28:t02`).
3. Multi-arch OCI-index digest-pinned `pgvector` 0.8.1 (`PG14`/`PG18`) and `edoburu/pgbouncer:v1.26.0-p0` (`pg_authid.rolpassword` `SCRAM-SHA-256$` verifier written via in-container `sh -c` to `/tmp/pgbouncer.users` + `/tmp/pgbouncer.ini`, `max_prepared_statements=0` and `200`) service fixtures and smoke controls (`ch28:t03`) [^int-conftest].
4. Full live feature acceptance across binary `COPY`, `VTBARROW` `ArrowCursor` / portal early-close with `/dev/shm` segment hygiene assertions, `LISTEN`/`NOTIFY` rollback suppression, `PgVector` / `PgBitString` / `PostgresEmbeddingPushdown`, and `PgBouncer` transaction pooling (`ch28:t04`) [^pg-config] [^pg-notify] [^pg-embeddings].

### 1.2 Non-Goals

- Mutating non-PostgreSQL engine entries in `ENGINE_SPECS` (`oracle`, `mysql`, `mssql`, `duckdb`, `sqlite`, `bigquery`, `spanner`).
- Live external Google Cloud SQL or AlloyDB network calls in local or CI test runs (cloud Admin/mTLS/IAM paths are verified via deterministic wire/TLS harnesses in earlier chapters).
- Packaging wheel isolation, GitHub Actions workflow edits, and Criterion / reference benchmarks (owned by [ch29-wheel-ci-and-benchmarks](../ch29-wheel-ci-and-benchmarks/spec.md)).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch27-atomic-cutover-and-legacy-removal](../ch27-atomic-cutover-and-legacy-removal/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md), [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [review-tests](../../../../../skills/review-tests/SKILL.md), and `pytest-databases`. Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [benchmarks-and-free-threaded.md](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R25.1 (`ch28:t01`)**: Update `_resolve_live_config()` (`test_driver_conformance.py`) so `postgres` runs `_require_native_module(spec)` first and calls `pytest.skip(...)` when neither `VERTEBRA_CONFORMANCE_POSTGRES_DSN` nor `VERTEBRA_CONFORMANCE_DOCKER == "1"` is set; gate all live Docker PostgreSQL fixtures in `integration/conftest.py` lazily on `VERTEBRA_CONFORMANCE_DOCKER == "1"`; add a `pytest_runtest_logreport` / `pytest_sessionfinish` hook failing the run whenever `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS == "1"` sees any selected PostgreSQL test `skipped` or `xfailed` (or `0` executed); enable `_POSTGRES_TYPES_DDL`, `supports_pipeline=True`, and `pending_flow_id=None`; and verify PG14–18 conformance and live `pg_sleep` cancellation (`driver.cancel()` -> `QueryCancelledError` `57014`; `task.cancel()` -> `asyncio.CancelledError` + retired PID).
- **R25.2 (`ch28:t02`)**: Provision `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated session-scoped logical PostgreSQL 14–18 containers in `integration/conftest.py` (`wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`, `sslmode="disable"`) and verify parameterized `PostgresChangeSource` `INSERT`/`UPDATE`/`DELETE`/TOAST/rollback ordering, durable checkpoint CAS before `confirmed_flush_lsn` feedback, reconnect replay, bounded queue backpressure, cancellation, and zero leaked slots in `pg_replication_slots`.
- **R25.3 (`ch28:t03`)**: Provision `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated `pgvector` 0.8.1 fixtures pinned to verified multi-arch OCI manifest-index (`application/vnd.oci.image.index.v1+json`, `linux/amd64` + `linux/arm64`) digests (`0.8.1-pg14@sha256:2b8cdfaad8e380ef81202cb2df1db35a83482ca7c6f4d933d01d84c294ba64ff`, `0.8.1-pg18@sha256:508c5290cda481d4f5f846446a26e9c1b804766828a394a5861de1b348a18b4c`, asserting `extversion == "0.8.1"` and major `14`/`18`) plus ephemeral `edoburu/pgbouncer:v1.26.0-p0@sha256:b17551c776ef7e5769ef80b956d20f85e2fd25dd8912d31d58f782aad495b711` launched via in-container `sh -c` writing `/tmp/pgbouncer.users` (from `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, asserting `rolpassword.startswith("SCRAM-SHA-256$")`) and `/tmp/pgbouncer.ini` before `exec /usr/bin/pgbouncer /tmp/pgbouncer.ini`, verified in `test_postgres_services.py`.
- **R25.4 (`ch28:t04`)**: Verify parameterized live binary `COPY` in/out, native `VTBARROW` `select_to_arrow` / `stream_arrow_batches` / `load_from_arrow` with before/after `/dev/shm` zero-leak assertions across early close and cancellation, `LISTEN`/`NOTIFY` rollback suppression, `PgVector` / `PgBitString` / `PostgresEmbeddingPushdown` over `pgvector` 0.8.1, and `PgBouncer` transaction-mode statement caching + `direct_dsn` bypass in `test_postgres_features.py`.

### 2.2 Fixture & Collaborator Map

- Offline `make py-test` / `coverage` (`VERTEBRA_CONFORMANCE_DOCKER` unset) -> `_resolve_live_config` / `conftest.py` env guard -> clean `pytest.skip` before any `pytest-databases` Docker container starts.
- Live `make postgres-test` / CI (`VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`) -> lazy `request.getfixturevalue("postgres_14_service".."postgres_18_service")` / `docker_service.run(...)` -> `conftest.py` zero-skip terminal gate.
- Backing PG `SELECT rolpassword FROM pg_authid WHERE rolname = $1` -> `edoburu/pgbouncer:v1.26.0-p0` `entrypoint=["sh", "-c"]` `/tmp/pgbouncer.{users,ini}` -> `test_postgres_services.py` & `test_postgres_features.py`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Offline `make py-test` / `make coverage` / `ci.yml` `test-python` (`VERTEBRA_CONFORMANCE_DOCKER` & `VERTEBRA_CONFORMANCE_POSTGRES_DSN` unset) | Checks `_require_native_module` and skips cleanly before resolving `postgres_service` or calling Docker. |
| `VERTEBRA_CONFORMANCE_DOCKER=1` + `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` with any skipped/xfailed PostgreSQL test or Docker failure | Fails loud with non-zero pytest exit code; never passes silently with skipped PostgreSQL acceptance items. |
| `edoburu/pgbouncer:v1.26.0-p0` SCRAM authentication & host UID mismatch | Reads live `SCRAM-SHA-256$...` verifier from `pg_authid.rolpassword` and writes `/tmp/pgbouncer.users` + `/tmp/pgbouncer.ini` inside the container via `sh -c` before `exec /usr/bin/pgbouncer` (zero broken `/entrypoint.sh` plaintext userlists, zero host `tmp_path` UID permission errors). |
| `ArrowCursor` SHM materialization and early `stream_arrow_batches` `break` / `aclose()` / `Task.cancel()` | Pinned socket recovers or retires cleanly and `/dev/shm` diff asserts zero lingering test-owned `vtb*`/`vertebra*` SHM segments after cursor release. |
| `PgBouncer` `transaction` mode with `max_prepared_statements=0` vs `200` | `0` keeps protocol statement caching disabled; cold custom-type queries and `VACUUM` succeed without implicit `BEGIN`; `LISTEN` and `START_REPLICATION` require `direct_dsn`. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| `PgBouncer` 1.26 SCRAM config | Query `pg_authid.rolpassword` (`SCRAM-SHA-256$...`) and render `/tmp/pgbouncer.{users,ini}` inside `sh -c` before `exec /usr/bin/pgbouncer` | Relying on `edoburu/pgbouncer` `/entrypoint.sh` `DB_PASSWORD` (writes plaintext unsustainable for `scram-sha-256`) or host `tmp_path` bind mounts (fail on container non-root UID read permissions). |
| `pgvector` container tags | Pin verified multi-arch (`linux/amd64` + `linux/arm64`) OCI index digests from `docker buildx imagetools inspect` and assert `extversion == "0.8.1"` | Single-arch image digests or floating `:pg14`/`:pg18` tags, which break ARM64 hosts or silently drift wire codecs. |
| Conformance gating | Skip offline unless `VERTEBRA_CONFORMANCE_DOCKER=1` or `VERTEBRA_CONFORMANCE_POSTGRES_DSN` is set, paired with `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` hook | Unconditional Docker startup during `make py-test` (breaks offline unit/coverage lanes) or unchecked skips in live CI (hides regressions). |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (`[0/33 files documented]` across the `36`-chapter / `129`-task flow synchronized in `ch30-user-and-workflow-docs`, `ch31-data-and-testing-okf-sync`, and `ch32-ffi-okf-and-review-routing-sync` ahead of `ch33-final-review-and-release-gate`).

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

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres -q && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres src/py/tests/integration/db/test_postgres_native.py src/py/tests/integration/db/test_postgres_replication.py src/py/tests/integration/db/test_postgres_services.py src/py/tests/integration/db/test_postgres_features.py -q -rs && uv run --no-sync ruff check src/py/tests/integration` exits `0` with zero skipped or xfailed PostgreSQL items in the live pass.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 8`, `plan_commit: d90d4625`; `depends_on_chapters: [ch27-atomic-cutover-and-legacy-removal]`, `state: planned`, `planning_status: complete` (`36` chapters / `129` tasks; `ch01`–`ch21` and `ch22:t01`–`t02` (`80` tasks) are on `main` in `c8722c76`).
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Active Invariants**: Offline pytest runs never spawn Docker containers when `VERTEBRA_CONFORMANCE_DOCKER` and `VERTEBRA_CONFORMANCE_POSTGRES_DSN` are unset; live runs enforce `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, pass explicit `sslmode="disable"`, use `@pytest.mark.parametrize` / `@pytest.fixture(params=...)` exclusively, and leave zero leaked slots, containers, or `/dev/shm` segments.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Clientless `pytest-databases` Docker harness fixtures and strict zero-skip hooks.
[^driver-conformance]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix, `_resolve_live_config`, and pipeline suite.
[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, `sslmode`, `pooler_mode`, and `direct_dsn` configuration.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL `LISTEN`/`NOTIFY` source and publication helpers.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — PostgreSQL SQL embedding pushdown and `pgvector` similarity search.
