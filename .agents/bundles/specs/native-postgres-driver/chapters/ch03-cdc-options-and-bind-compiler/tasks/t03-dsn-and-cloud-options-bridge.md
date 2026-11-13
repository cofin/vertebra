---
type: Task
id: "native-postgres-driver:ch03-cdc-options-and-bind-compiler:t03"
flow_id: native-postgres-driver
chapter_id: ch03-cdc-options-and-bind-compiler
title: "Implement options.py DSN, socket, multi-host, SSL, pooler, and cloud bridge"
description: "Translate PostgreSQLConfig, libpq PG* env defaults, percent-decoded sockets, and Cloud SQL/AlloyDB settings into native client kwargs."
status: stable
state: completed
commit: a09818b9
last_verified_commit: a09818b9
last_verified_at: "2026-11-13T05:14:30Z"
verification_evidence: "ruff + mypy (15 files) + pytest (111 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files: 
  - src/py/vertebra/db/adapters/postgres/options.py
  - src/py/vertebra/db/adapters/postgres/config.py
tests: 
  - src/py/tests/unit/db/test_postgres_options_and_binds.py
read_set:
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/_postgres.pyi
  - .agents/bundles/specs/native-postgres-driver/contracts.md
estimated_loc: 215
tags:
  - python
  - postgres
  - config
sources:
  - id: pg-config
    resource: "src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig"
    title: "PostgreSQLConfig URL and cloud fields"
---

# Task `t03`: Implement options.py DSN, socket, multi-host, SSL, pooler, and cloud bridge

## Objective & Scope Boundary

Create `options.py` bridging `PostgreSQLConfig` to `vertebra._postgres` constructor kwargs per `../spec.md` §2 [^pg-config].

## Bounded Read Manifest

- `postgres/config.py`, `_postgres.pyi`, and `contracts.md`.

## Contracts & Edge Cases

- Percent-decode DSN host/socket paths via `urllib.parse.unquote` (preserving `+`), apply `sslmode=prefer` default for TCP and `disable` for implicit Unix sockets, and forward `PoolerMode` + Cloud SQL / AlloyDB options without mutating frozen configs.

## Test Matrix

- Parameterized tests for `postgresql://` multi-host, `%2Fvar%2Frun%2Fpostgresql` sockets, `+` paths, `PG*` env Precedence, and Cloud SQL/AlloyDB kwargs.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_options_and_binds.py -q
```

## Acceptance Criteria

- [x] All DSN, socket, SSL, pooler, and cloud option combinations normalize deterministically.
- [x] Zero regressions in `ch03-cdc-options-and-bind-compiler` fast verification gate.

[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQLConfig URL and cloud fields.
