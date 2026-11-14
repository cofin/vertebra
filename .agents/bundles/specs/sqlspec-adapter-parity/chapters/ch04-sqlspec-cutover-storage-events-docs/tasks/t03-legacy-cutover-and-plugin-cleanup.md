---
type: Task
id: sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t03
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-cutover-storage-events-docs
title: Delete Legacy SQLSpec Modules and Cut Over SQLSpecPlugin and Public Exports
description: Delete extensions/sqlspec/{adapters,bigquery,spanner}.py and rewire plugin.py and __init__.py onto per-engine packages.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 104fd6fa
depends_on:
- sqlspec-adapter-parity:ch04-sqlspec-cutover-storage-events-docs:t02
files:
- src/py/vertebra/extensions/sqlspec/adapters/__init__.py
- src/py/vertebra/extensions/sqlspec/__init__.py
- src/py/vertebra/extensions/sqlspec/plugin.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
- src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
read_set:
- src/py/vertebra/extensions/sqlspec/__init__.py
- src/py/vertebra/extensions/sqlspec/plugin.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/extensions/sqlspec/bigquery.py
- src/py/vertebra/extensions/sqlspec/spanner.py
estimated_loc: 185
tags:
- sqlspec
- debloat
- plugin
sources:
- id: sqlspec-init
  resource: src/py/vertebra/extensions/sqlspec/__init__.py
  title: Public SQLSpec extension barrel
- id: sqlspec-plugin
  resource: src/py/vertebra/extensions/sqlspec/plugin.py
  title: SQLSpecPlugin lifecycle and session DI
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Monolithic adapters module slated for deletion
---

# Task `t03`: Delete Legacy SQLSpec Modules and Cut Over SQLSpecPlugin and Public Exports

## Objective & Scope Boundary

Delete `extensions/sqlspec/{adapters,bigquery,spanner}.py`, clean `plugin.py`, and wire PEP 562 lazy barrels in `extensions/sqlspec/{adapters/__init__,__init__}.py` per [`../spec.md`](../spec.md) [^sqlspec-init] [^sqlspec-plugin] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/{adapters,bigquery,spanner}.py` — legacy monolithic files to remove via `git rm`.
- `src/py/vertebra/extensions/sqlspec/plugin.py` — remove `_inject_sqlspec_pool_defaults`, duplicate `resolve_*_config`, and standalone `_hydrate_*` helpers.

## Contracts & Edge Cases

- Delete `adapters.py`, `bigquery.py`, and `spanner.py` completely with zero re-export shims; confirm `_compat.py` does not exist.
- Export all `<Engine>SyncConfig` / `<Engine>AsyncConfig` and `<Engine>SyncDriver` / `<Engine>AsyncDriver` lazily via PEP 562 `__getattr__` in `extensions/sqlspec/adapters/__init__.py` and `extensions/sqlspec/__init__.py`.
- Ensure generic modules (`plugin.py`, `provider.py`, `proxy.py`, `storage.py`, `events.py`) contain zero vendor dispatch or engine SQL, and `spanner` imports zero `bigquery` modules.

## Test Matrix

- AST/import ownership checks proving generic SQLSpec modules contain zero engine SQL/vendor dispatch, `spanner` imports zero `bigquery` modules, `_compat.py` is absent, and PEP 562 lazy imports do not pull uninstalled optional wheels or `google.adk`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_data_storage_integrations.py
```

## Acceptance Criteria

- [ ] `adapters.py`, `bigquery.py`, and `spanner.py` are deleted and `_compat.py` does not exist.
- [ ] All 8 adapter packages resolve cleanly through `SQLSpecPlugin` and PEP 562 lazy barrels.
- [ ] Zero regressions in the fast verification gate.

[^sqlspec-init]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Public SQLSpec extension barrel.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py` — SQLSpecPlugin lifecycle and session DI.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Monolithic adapters module slated for deletion.
