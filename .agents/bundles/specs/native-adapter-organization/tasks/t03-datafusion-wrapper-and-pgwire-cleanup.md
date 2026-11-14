---
type: Task
id: "native-adapter-organization:t03"
flow_id: native-adapter-organization
title: "Remove unimplemented DataFusion pgwire_fdw fields and vendor registration wrappers"
description: "Delete mode='pgwire_fdw' and pgwire_* fields from DataFusionConfig and remove BigQuery/Spanner convenience wrappers from DataFusionEngine."
status: stable
state: completed
commit: 838c54d1
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-adapter-organization:t01"
files:
  - src/py/vertebra/db/adapters/datafusion/config.py
  - src/py/vertebra/db/adapters/datafusion/driver.py
  - src/py/vertebra/db/adapters/datafusion/__init__.py
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/integration/db/test_driver_conformance.py
read_set:
  - src/py/vertebra/db/adapters/datafusion/config.py
  - src/py/vertebra/db/adapters/datafusion/driver.py
  - src/py/vertebra/db/adapters/datafusion/__init__.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 165
tags:
  - database
  - datafusion
  - arrow
  - cleanup
sources:
  - id: df-config
    resource: src/py/vertebra/db/adapters/datafusion/config.py::DataFusionConfig
    title: "DataFusionConfig with unimplemented pgwire_fdw mode and pgwire_* fields"
  - id: df-driver
    resource: src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine
    title: "DataFusionEngine register_bigquery_* and register_spanner_* wrappers"
  - id: df-conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: "DataFusion URL parsing assertions in test_driver_conformance.py"
---

# Task `t03`: Remove Unimplemented DataFusion `pgwire_fdw` Fields and Vendor Registration Wrappers

## Objective & Scope Boundary

Implement `R3` in [`../spec.md` §2](../spec.md) by narrowing `DataFusionConfig` to implemented `mode="in_process"` execution and removing vendor-specific `register_bigquery_*` / `register_spanner_*` methods from `DataFusionEngine` [^df-config] [^df-driver] [^df-conformance]. Out of scope: adding a Rust `pgwire` server or modifying `ArrowCursor`.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/datafusion/{config,driver,__init__}.py` — `DataFusionMode`, `DataFusionConfig`, and `DataFusionEngine`.
- `src/py/tests/unit/db/{test_bigquery,test_spanner}.py` and `src/py/tests/integration/db/test_driver_conformance.py` — Existing callers of `register_bigquery_*`, `register_spanner_*`, and `pgwire_*` URL fields.

## Contracts & Edge Cases

- **`DataFusionConfig` (`datafusion/config.py`)**:
  - Narrow `type DataFusionMode = Literal["in_process"]` and delete `pgwire_host`, `pgwire_port`, and `require_mtls_for_pgwire` fields; update `pool_key()` to `("datafusion", self.mode, int(self.target_partitions), int(self.memory_limit_mb))`.
  - Validate in `__post_init__` and `from_url` that `mode == "in_process"`, `target_partitions >= 1` (reject `bool`), and `memory_limit_mb >= 1` (reject `bool`); raise `ImproperConfigurationError` when `datafusion://pgwire_fdw`, `host:port` netlocs, `mode=pgwire_fdw`, or `pgwire_*` query parameters are supplied.
- **`DataFusionEngine` (`datafusion/driver.py`, `__init__.py`)**:
  - Delete `register_bigquery_table`, `register_bigquery_query`, `register_spanner_table`, and `register_spanner_query` without aliases; callers compose `engine.register_arrow_table(name, driver.select_to_arrow(...))`.

## Test Matrix

- Parameterized tests in `test_pg_dataplane_and_datafusion.py` and `test_driver_conformance.py` asserting `DataFusionConfig.from_url("datafusion://in_process?target_partitions=8&memory_limit_mb=1024")` succeeds, `datafusion://pgwire_fdw` and `mode="pgwire_fdw"` raise `ImproperConfigurationError`, and `test_bigquery.py` / `test_spanner.py` federate via `driver.select_to_arrow(...)` + `engine.register_arrow_table(...)`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/integration/db/test_driver_conformance.py -k "datafusion or bigquery or spanner" -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/datafusion src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/integration/db/test_driver_conformance.py && uv run --no-sync mypy src/py/vertebra/db/adapters/datafusion
```

## Acceptance Criteria

- [x] Zero `pgwire_fdw`, `pgwire_host`, `pgwire_port`, `require_mtls_for_pgwire`, `register_bigquery_*`, or `register_spanner_*` symbols remain in `src/py/vertebra/db/adapters/datafusion/`.
- [x] Unsupported `DataFusionConfig` modes/URLs raise `ImproperConfigurationError`, and all DataFusion/BigQuery/Spanner tests pass.

[^df-config]: `src/py/vertebra/db/adapters/datafusion/config.py::DataFusionConfig` — DataFusionConfig with unimplemented pgwire_fdw mode and pgwire_* fields.
[^df-driver]: `src/py/vertebra/db/adapters/datafusion/driver.py::DataFusionEngine` — DataFusionEngine register_bigquery_* and register_spanner_* wrappers.
[^df-conformance]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — DataFusion URL parsing assertions in test_driver_conformance.py.
