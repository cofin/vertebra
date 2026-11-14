---
type: Task
id: sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t02
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: Implement Shared SQLAlchemy Dialect Mixins, ExecutionContext, and Native Types
description: Create dialects/_base.py and types.py handling parameter style compilation, server version introspection, and UUID/JSON/Vector/Numeric type bindings.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t01
files:
- src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
- src/py/vertebra/extensions/sqlalchemy/types.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/vertebra/extensions/sqlalchemy/types.py
- src/py/vertebra/utils/serializers.py
- src/py/vertebra/extensions/alchemy/broker.py
estimated_loc: 220
tags:
- sqlalchemy
- dialects
- types
sources:
- id: sa-types
  resource: src/py/vertebra/extensions/sqlalchemy/types.py
  title: SQLAlchemy custom vector and JSON type definitions
- id: sa-dialects-base
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Dialect pool selection and create_connect_args hooks
---

# Task `t02`: Implement Shared SQLAlchemy Dialect Mixins, ExecutionContext, and Native Types

## Objective & Scope Boundary

Create shared `_SyncDialectMixin` / `_AsyncDialectMixin` in `dialects/_base.py` and complete `extensions/sqlalchemy/types.py` per [`../spec.md`](../spec.md) [^sa-types] [^sa-dialects-base]. Out of scope: concrete per-engine dialect subclasses (`ch02`–`ch03`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — existing `import_dbapi`, `get_pool_class`, `create_connect_args`, and `is_disconnect` logic.
- `src/py/vertebra/extensions/sqlalchemy/types.py` — custom SQLAlchemy `TypeDecorator` and `UserDefinedType` classes.
- `src/py/vertebra/utils/serializers.py` — centralized `encode_json` / `decode_json` helpers.
- `src/py/vertebra/extensions/alchemy/broker.py` — broker transport pool hooks.

## Contracts & Edge Cases

- `_SyncDialectMixin` (`import_dbapi -> SyncDBAPIModule`) and `_AsyncDialectMixin` (`is_async = True`, `import_dbapi` wrapping `AsyncDBAPIModule` via `async_adapter`) pass the full rendered Vertebra URL through `create_connect_args(url)` and select `BrokerSyncPool` / `BrokerAsyncPool` when `broker` is present or native pool classes otherwise.
- Implement `PgVector`, `HalfVector`, `SparseVector`, `OracleVector` (`VECTOR(dim, FLOAT32)` + `array('f')`), `SpannerVector` (`ARRAY<FLOAT32>`), `BigQueryGeography` (`GEOGRAPHY` WKT), and `MsgspecJSON` (routing strictly through `encode_json` / `decode_json`).
- Enforce leading `Sync*` / `Async*` naming and zero `Vertebra*` / `*Vtb*` / `Native*` class names (`PY-07`).

## Test Matrix

- Parametrized `create_connect_args`, `get_pool_class` (embedded vs broker), and `is_disconnect` tests across sync and async mixins.
- Parametrized bind/result processor and distance comparator (`cosine_distance`, `l2_distance`, `max_inner_product`) tests for `PgVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py
```

## Acceptance Criteria

- [ ] `dialects/_base.py` and `types.py` pass Ruff, Mypy, and `PY-07` domain naming validation.
- [ ] `MsgspecJSON` routes all JSON serialization through centralized `vertebra.utils.serializers` helpers.
- [ ] Zero regressions in the fast verification gate.

[^sa-types]: `src/py/vertebra/extensions/sqlalchemy/types.py` — SQLAlchemy custom vector and JSON type definitions.
[^sa-dialects-base]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Dialect pool selection and create_connect_args hooks.
