---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch05-atomic-cutover-and-legacy-removal
title: "Chapter 05: Atomic Consumer Cutover, Legacy Removal, and GCP Dialer Hardening"
description: "Switch SQLSpec, SQLAlchemy, driver.py, embeddings, queues, and Cloud SQL/AlloyDB dialers atomically to vertebra._postgres with zero psycopg/asyncpg/asyncmy code."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T00:25:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - python
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - dialer
sources:
  - id: pg-driver
    resource: "src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver"
    title: "Legacy PostgreSQL driver replaced by 12-line native barrel"
  - id: sqlspec-adp
    resource: "src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver"
    title: "SQLSpec PostgreSQL statement compilation seam"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "SQLAlchemy PostgreSQL dialect paramstyle"
  - id: pg-queue
    resource: "src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend"
    title: "PostgreSQL queue dependency guard"
  - id: cloudsql-py
    resource: "src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig"
    title: "Python Cloud SQL dialer"
  - id: alloydb-py
    resource: "src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig"
    title: "Python AlloyDB dialer"
---

# Chapter 05: Atomic Consumer Cutover, Legacy Removal, and GCP Dialer Hardening

<!-- truth: start -->
- `extensions/sqlspec/adapters.py` (`ParameterStyle.NUMERIC`) and `extensions/sqlalchemy/dialects.py` (`numeric_dollar`) cut over in one direct pass with zero staging flags [^sqlspec-adp] [^sa-dialects].
- `postgres/driver.py` becomes a clean native barrel over `_sync.py` and `_async.py` with zero `_Legacy*` classes, `embeddings.py` uses unconditional `$1`/`$2`, and `PostgreSQLQueueBackend` probes `vertebra._postgres` (`vertebra-core[postgres]`) [^pg-driver] [^pg-queue].
- `cloudsql.py` and `alloydb.py` delete `create_psycopg_connect_func`, `create_asyncpg_connect_func`, and `create_asyncmy_creator`, remove the fake RSA fallback and `ssl.CERT_NONE` default, propagate token-cache errors, and serialize async cert refreshes [^cloudsql-py] [^alloydb-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Performs the single atomic cutover from `psycopg`/`asyncpg` to `vertebra._postgres` and resolves all `G-09` Python GCP dialer defects while touching `cloudsql.py` and `alloydb.py` once.

## 2. Requirements, Concrete Contracts & Architecture

- `sqlspec/adapters.py` & `sqlalchemy/dialects.py`: `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` both compile to `ParameterStyle.NUMERIC` (`$1..$N`) preserving `StatementFilter`s and unwrapping `TypedParameter`; SQLAlchemy sync/async PostgreSQL dialects use `default_paramstyle = paramstyle = 'numeric_dollar'`.
- `postgres/{driver,__init__,embeddings}.py`: 12-line `driver.py` re-exporting `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver`, complete `__init__.py` lazy export map (`PostgresChangeSource`, `PostgresCheckpoint*`, `Pg*` types), and unconditional `$1`/`$2` pushdown SQL.
- `queues/postgres.py` + `dialer/{cloudsql,alloydb}.py`: `ensure_module('vertebra._postgres', package='vertebra-core[postgres]', extra='postgres')`; delete `create_psycopg_connect_func`, `create_asyncpg_connect_func`, and `create_asyncmy_creator`; require real `_vertebra` RSA keygen (zero fake PEM fallback), default `ssl.CERT_REQUIRED` (`create_default_context()` when `ca_pem=None`), validate `refresh_lead_seconds >= 0` and `ip_type`, use async non-blocking cert fetch + `anyio.Lock`, and remove `contextlib.suppress(Exception)` around token caches.

## 3. Documentation & OKF Knowledge Plan

`t04` updates `sqlspec-alchemy-and-dialects.md`, `distributed-queues-and-email.md`, and removes PostgreSQL split-state caveats from `database-driver-limitations.md` (`<= 200L` each).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R3` SQLSpec `NUMERIC` & SQLAlchemy `numeric_dollar` | `t01` | `sqlspec/adapters.py`, `sqlalchemy/dialects.py` | `pytest test_postgres_compiler_transport.py` |
| `R3` 12-line `driver.py` barrel & unit suite migration | `t02` | `driver.py`, `__init__.py`, `embeddings.py` | `pytest test_postgres_driver.py ...` |
| `R3` Queue guard + `cloudsql.py`/`alloydb.py` cleanup (`G-09`) | `t03` | `queues/postgres.py`, `cloudsql.py`, `alloydb.py` | `pytest test_distributed_queues.py test_gcp_adapters_and_sqlalchemy.py` |
| `R5` Extensions, queues & driver-limitations OKF sync | `t04` | 3 OKF chapters | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-sqlspec-numeric-and-sqlalchemy-dollar-cutover.md](tasks/t01-sqlspec-numeric-and-sqlalchemy-dollar-cutover.md) — Cut PostgreSQLSyncDriver/PostgreSQLAsyncDriver to ParameterStyle.NUMERIC and SQLAlchemy PostgreSQL dialects to numeric_dollar.
- [ ] [t02-activate-native-driver-barrel-and-migrate-unit-tests.md](tasks/t02-activate-native-driver-barrel-and-migrate-unit-tests.md) — Cut driver.py, __init__.py, and embeddings.py over to native PostgreSQL drivers and replace psycopg/SQLite test doubles with postgres_ports.py.
- [ ] [t03-queue-guard-and-gcp-dialer-cleanup.md](tasks/t03-queue-guard-and-gcp-dialer-cleanup.md) — Switch PostgreSQLQueueBackend to vertebra._postgres, delete legacy psycopg/asyncpg/asyncmy dialer helpers, and fix TLS/RSA/async refresh bugs in cloudsql.py and alloydb.py.
- [ ] [t04-cutover-extensions-and-limitations-okf-sync.md](tasks/t04-cutover-extensions-and-limitations-okf-sync.md) — Update sqlspec-alchemy-and-dialects.md, distributed-queues-and-email.md, and database-driver-limitations.md (<= 200L each) for native PostgreSQL.

## 6. Current State & Remaining Plan Snapshot

Planned (`4` tasks). Zero `psycopg`/`asyncpg`/`asyncmy` references remain in `src/py/vertebra` on completion.

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/framework/test_distributed_queues.py -q && test -z "$(git grep -n -E 'psycopg|asyncpg|asyncmy' -- src/py/vertebra)" && make review-check
```

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Legacy PostgreSQL driver replaced by 12-line native barrel.
[^sqlspec-adp]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec PostgreSQL statement compilation seam.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy PostgreSQL dialect paramstyle.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue dependency guard.
[^cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Python Cloud SQL dialer.
[^alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — Python AlloyDB dialer.
