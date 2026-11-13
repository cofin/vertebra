---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch06-live-conformance-and-service-acceptance
title: "Chapter 06: Live PostgreSQL 14-18 Conformance and Service Acceptance"
description: "Enable parameterized PG14-18 conformance with zero-skip gating plus live pgoutput replication and PGCOPY/VTBARROW/LISTEN/pgvector/PgBouncer suites."
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
  - conformance
  - testing
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py
    title: "Integration fixtures and pytest terminal hooks"
  - id: conformance-py
    resource: "src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS"
    title: "Cross-engine driver conformance matrix"
---

# Chapter 06: Live PostgreSQL 14-18 Conformance and Service Acceptance

<!-- truth: start -->
- `test_driver_conformance.py` sets `postgres` `supports_pipeline=True` and `pending_flow_id=None` across PostgreSQL `14`–`18`, enforced by `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` in `conftest.py` [^int-conftest] [^conformance-py].
- `test_postgres_replication_live.py` and `test_postgres_native_features_live.py` verify live `wal_level=logical` `PostgresChangeSource`, unchanged TOAST, `PGCOPY`, `VTBARROW`, `LISTEN/NOTIFY`, `pgvector`, and `PgBouncer` transaction/statement modes.
- The chapter gate stays `< 60s` offline (unit-testing the zero-skip hook + clean skip collection + Ruff/Mypy), reserving Docker container runs for `/flow:finish` and CI.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Adds live server conformance and service-level acceptance suites after the `ch05` native driver cutover.

## 2. Requirements, Concrete Contracts & Architecture

- `conftest.py` & `test_driver_conformance.py`: `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` fails `pytest_terminal_summary` if any `postgres` conformance item is skipped or deselected.
- `test_postgres_replication_live.py`: live `pgoutput` v1 transactions, `PG_UNCHANGED_TOAST`, invalid UTF-8 `bytea`, reconnect resume, and `55006` slot contention.
- `test_postgres_native_features_live.py`: self-contained `pgvector:0.8.1-pg18` and `pgbouncer:v1.26.0` fixtures exercising binary `COPY`, `VTBARROW`, `LISTEN/NOTIFY`, `vector`/`halfvec`/`bit`, and unnamed-statement pooler safety.

## 3. Documentation & OKF Knowledge Plan

Testing matrix and workflow OKF updates land in `ch07:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4` PG14–18 conformance & zero-skip hook | `t01` | `conftest.py`, `test_driver_conformance.py` | `pytest test_postgres_zero_skip_gate.py` |
| `R4` Live `pgoutput` replication suite | `t02` | `test_postgres_replication_live.py` | Offline collection + `/flow:finish` Docker |
| `R4` Live `PGCOPY`/`VTBARROW`/`pgvector`/`PgBouncer` suite | `t03` | `test_postgres_native_features_live.py` | Offline collection + `/flow:finish` Docker |

## 5. Implementation Plan

- [ ] [t01-pg14-18-conformance-and-zero-skip-gate.md](tasks/t01-pg14-18-conformance-and-zero-skip-gate.md) — Set supports_pipeline=True and pending_flow_id=None for postgres in test_driver_conformance.py and enforce zero-skip terminal gating.
- [ ] [t02-live-logical-replication-acceptance-suite.md](tasks/t02-live-logical-replication-acceptance-suite.md) — Add test_postgres_replication_live.py covering PostgresChangeSource, unchanged TOAST, non-UTF-8 bytea, reconnect, and slot contention.
- [ ] [t03-live-copy-arrow-notify-pgvector-pgbouncer-suite.md](tasks/t03-live-copy-arrow-notify-pgvector-pgbouncer-suite.md) — Add test_postgres_native_features_live.py covering binary COPY, Arrow SHM, notifications, pgvector, and PgBouncer transaction/statement modes.

## 6. Current State & Remaining Plan Snapshot

Planned (`3` tasks). Offline `< 30s` gate; full Docker PG14–18 runs in `/flow:finish` and `ci.yml`.

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_zero_skip_gate.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_replication_live.py src/py/tests/integration/db/test_postgres_native_features_live.py -q && uv run --no-sync ruff check src/py/tests/integration src/py/tests/unit/db/test_postgres_zero_skip_gate.py
```

[^int-conftest]: `src/py/tests/integration/conftest.py` — Integration fixtures and pytest terminal hooks.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance matrix.
