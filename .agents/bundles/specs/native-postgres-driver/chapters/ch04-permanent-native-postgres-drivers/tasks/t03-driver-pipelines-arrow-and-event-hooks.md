---
type: Task
id: "native-postgres-driver:ch04-permanent-native-postgres-drivers:t03"
flow_id: native-postgres-driver
chapter_id: ch04-permanent-native-postgres-drivers
title: "Wire pipeline(), ArrowCursor/COPY import, and dedicated event hooks on both drivers"
description: "Connect pipeline.py, arrow.py, listen_notifications, open_replication, and EventSessionRegistry close hooks in _sync.py and _async.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch04:t02"
files: 
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
tests: 
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
  - src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
  - src/py/tests/unit/db/test_postgres_event_hooks.py
read_set:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/pipeline.py
  - src/py/vertebra/db/adapters/postgres/arrow.py
  - src/py/vertebra/db/adapters/postgres/events.py
estimated_loc: 245
tags:
  - python
  - postgres
  - pipeline
  - arrow
sources:
  - id: pg-pipeline
    resource: "src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline"
    title: "Native single-Sync pipeline wrappers"
  - id: pg-arrow
    resource: "src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync"
    title: "Native Arrow COPY import helpers"
---

# Task `t03`: Wire pipeline(), ArrowCursor/COPY import, and dedicated event hooks on both drivers

## Objective & Scope Boundary

Complete `_sync.py` and `_async.py` with `pipeline()`, `select_to_arrow`/`stream_arrow_batches`/`load_from_arrow`, and `EventSessionRegistry`-tracked `listen_notifications`/`open_replication` [^pg-pipeline] [^pg-arrow].

## Bounded Read Manifest

- `_sync.py`, `_async.py`, `pipeline.py`, `arrow.py`, and `events.py`.

## Contracts & Edge Cases

- Bind `pipeline()` to the pinned transaction connection when `in_transaction` is `True`, or lease one pooled session for the pipeline context.
- Register dedicated listener/replication sessions in `EventSessionRegistry` and close them deterministically during `driver.close()`.

## Test Matrix

- Unit suites for ambient-tx vs pooled `pipeline()`, `VTBARROW` `ArrowCursor` + `load_from_arrow`, and `EventSessionRegistry` teardown on `close()`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] Both drivers satisfy `SyncPipelineCapable`/`AsyncPipelineCapable`, Arrow methods, and dedicated event lifecycle contracts.
- [ ] Zero regressions in `ch04-permanent-native-postgres-drivers` fast verification gate.

[^pg-pipeline]: `src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline` — Native single-Sync pipeline wrappers.
[^pg-arrow]: `src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync` — Native Arrow COPY import helpers.
