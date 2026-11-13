---
type: Task
id: "native-postgres-driver:ch05-atomic-cutover-and-legacy-removal:t03"
flow_id: native-postgres-driver
chapter_id: ch05-atomic-cutover-and-legacy-removal
title: "Update PostgreSQLQueueBackend guard and clean up Cloud SQL and AlloyDB dialers"
description: "Switch PostgreSQLQueueBackend to vertebra._postgres, delete legacy psycopg/asyncpg/asyncmy dialer helpers, and fix TLS/RSA/async refresh bugs in cloudsql.py and alloydb.py."
status: stable
state: completed
commit: d635f818
last_verified_commit: d635f818
last_verified_at: "2026-11-13T15:43:00Z"
verification_evidence: "pytest test_distributed_queues.py test_cloud_dialers.py test_gcp_adapters_and_sqlalchemy.py + zero legacy grep matches"
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch05:t02"
files: 
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/vertebra/db/dialer/cloudsql.py
  - src/py/vertebra/db/dialer/alloydb.py
tests: 
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/vertebra/db/dialer/cloudsql.py
  - src/py/vertebra/db/dialer/alloydb.py
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
estimated_loc: 225
tags:
  - python
  - postgres
  - gcp
  - dialer
  - queues
sources:
  - id: pg-queue
    resource: "src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend"
    title: "PostgreSQLQueueBackend missing-executor guard"
  - id: cloudsql-py
    resource: "src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig"
    title: "Cloud SQL Python dialer"
  - id: alloydb-py
    resource: "src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig"
    title: "AlloyDB Python dialer"
---

# Task `t03`: Update PostgreSQLQueueBackend guard and clean up Cloud SQL and AlloyDB dialers

## Objective & Scope Boundary

Switch `PostgreSQLQueueBackend` to `vertebra._postgres`, delete legacy `create_psycopg_connect_func`/`create_asyncpg_connect_func`/`create_asyncmy_creator`, and remediate `G-09` TLS/keypair/refresh bugs in `cloudsql.py` and `alloydb.py` [^pg-queue] [^cloudsql-py] [^alloydb-py].

## Bounded Read Manifest

- `queues/postgres.py`, `cloudsql.py`, `alloydb.py`, and their unit test suites.

## Contracts & Edge Cases

- `PostgreSQLQueueBackend` probes `ensure_module('vertebra._postgres', package='vertebra-core[postgres]', extra='postgres')` before raising `ImproperConfigurationError` when `executor=` is absent.
- Delete `create_psycopg_connect_func`, `create_asyncpg_connect_func`, and `create_asyncmy_creator`; remove fake `MIIEpAIB...` RSA fallback; enforce `ssl.CERT_REQUIRED` when `ca_pem=None` via `ssl.create_default_context()`; validate `refresh_lead_seconds >= 0` and `ip_type`; serialize async cert refresh with `anyio.Lock` without `contextlib.suppress(Exception)` around token caches.

## Test Matrix

- Parameterized tests verifying `vertebra-core[postgres]` install hint, absence of legacy `create_*_connect_func` helpers, `CERT_REQUIRED` TLS verification, token error propagation, and concurrent async cert refresh locking.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && test -z "$(git grep -n -E 'psycopg|asyncpg|asyncmy' -- src/py/vertebra)"
```

## Acceptance Criteria

- [x] `git grep -E 'psycopg|asyncpg|asyncmy' -- src/py/vertebra` returns empty and dialer/queue unit suites pass.
- [x] Zero regressions in `ch05-atomic-cutover-and-legacy-removal` fast verification gate.

[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQLQueueBackend missing-executor guard.
[^cloudsql-py]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Cloud SQL Python dialer.
[^alloydb-py]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — AlloyDB Python dialer.
