---
type: Task
id: "native-oracle-driver:ch07:t01"
flow_id: native-oracle-driver
chapter_id: ch07-conformance-and-okf-synthesis
title: "Task 01: Wire Live Oracle 26ai Conformance Matrix and Native Integration Suite"
description: Add _ORACLE_ITEMS_DDL and _ORACLE_TYPES_DDL to the Oracle EngineConformanceSpec in ENGINE_SPECS (test_driver_conformance.py) with supports_pipeline=True and pending_flow_id=None, and implement live Oracle 26ai Free integration tests in test_oracle_native.py.
status: stable
state: pending
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch06:t03"
files:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/db/test_oracle_native.py
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/db/test_oracle_native.py
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
verification_strategy: behavior_tdd
estimated_loc: 240
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T01:28:30Z"
completed_at: null
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public contracts for Oracle 26ai codecs, Arrow SHM, pipelines, and VPD tenancy
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix
  - id: ora-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver, AsyncOracleDriver, SyncOraclePool, and AsyncOraclePool
tags:
  - oracle
  - integration
  - conformance
  - 26ai
---

# Task 01: Wire Live Oracle 26ai Conformance Matrix and Native Integration Suite

## Objective & Scope Boundary

Wire Oracle 26ai into the cross-engine driver conformance suite (`src/py/tests/integration/db/test_driver_conformance.py`) with Oracle-valid DDL (`_ORACLE_ITEMS_DDL`, `_ORACLE_TYPES_DDL`), `supports_pipeline=True`, and `pending_flow_id=None` on the `"oracle"` `EngineConformanceSpec` tuple entry in `ENGINE_SPECS`, and create `src/py/tests/integration/db/test_oracle_native.py` covering Oracle 26ai-specific features (`JSON`/`OSON`, `VECTOR`, `VTBARROW` `ArrowCursor`, `DML RETURNING INTO`, delayed-commit pipelines, VPD reset-or-evict tenancy, and out-of-band `ORA-01013` cancellation) against `127.0.0.1:5531/FREEPDB1` (`VERTEBRA_CONFORMANCE_ORACLE_DSN`) [^ora-contracts] [^conformance-py].

- **In Scope**:
  - Add `_ORACLE_ITEMS_DDL` (`vtb_conformance_items (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL, amount NUMBER(19))`) and `_ORACLE_TYPES_DDL` (`vtb_conformance_types` with `val_int NUMBER(19)`, `val_float BINARY_DOUBLE`, `val_decimal NUMBER(18, 6)`, `val_bool BOOLEAN`, `val_text CLOB`, `val_bytes RAW(2000)`, `val_ts TIMESTAMP WITH TIME ZONE`, `val_json JSON`) to `src/py/tests/integration/db/test_driver_conformance.py`.
  - Update the `"oracle"` `EngineConformanceSpec` entry in `ENGINE_SPECS` to set `items_ddl=_ORACLE_ITEMS_DDL`, `types_ddl=_ORACLE_TYPES_DDL`, `supports_pipeline=True`, and `pending_flow_id=None`.
  - Create `src/py/tests/integration/db/test_oracle_native.py` exercising live Oracle 26ai Free (`127.0.0.1:5531/FREEPDB1`, `VERTEBRA_CONFORMANCE_ORACLE_DSN`) end-to-end.
- **Out of Scope**:
  - Standalone wheel packaging and `test_freethreaded_gil.py` (owned by `native-oracle-driver:ch07:t02`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Complete native Oracle contract table.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch07-conformance-and-okf-synthesis/spec.md` — Section 2.1 conformance DDL and integration test matrix.
3. `src/py/tests/integration/db/test_driver_conformance.py` — `EngineConformanceSpec`, `ENGINE_SPECS`, `_ITEMS_DDL`, `_TYPES_DDL`, and `SCALAR_TYPE_CASES`.
4. `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver`, `AsyncOracleDriver`, `SyncOraclePool`, and `AsyncOraclePool`.
5. `src/py/vertebra/db/adapters/oracle/core.py` — `OracleCore` vector, OSON, and VPD SQL builders.

## Symbol & Signature Contracts

### `src/py/tests/integration/db/test_driver_conformance.py`

```python
_ORACLE_ITEMS_DDL = (
    "CREATE TABLE vtb_conformance_items (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL, amount NUMBER(19))"
)

_ORACLE_TYPES_DDL = (
    "CREATE TABLE vtb_conformance_types ("
    "id NUMBER(19) PRIMARY KEY, "
    "val_int NUMBER(19), "
    "val_float BINARY_DOUBLE, "
    "val_decimal NUMBER(18, 6), "
    "val_bool BOOLEAN, "
    "val_text CLOB, "
    "val_bytes RAW(2000), "
    "val_ts TIMESTAMP WITH TIME ZONE, "
    "val_json JSON"
    ")"
)
```

In `ENGINE_SPECS: tuple[EngineConformanceSpec, ...]`:

```python
    EngineConformanceSpec(
        engine="oracle",
        config_cls=OracleConfig,
        sync_driver_cls=SyncOracleDriver,
        async_driver_cls=AsyncOracleDriver,
        sample_url="oracle://system:secret@127.0.0.1:1521/FREEPDB1",
        probe_sql="SELECT 1 FROM DUAL",
        placeholder_style="colon",
        items_ddl=_ORACLE_ITEMS_DDL,
        types_ddl=_ORACLE_TYPES_DDL,
        supports_pipeline=True,
        native_extension_module="vertebra._oracle",
        pending_flow_id=None,
        env_dsn_vars=("VERTEBRA_CONFORMANCE_ORACLE_DSN",),
        container_fixture="vtb_oracle_config",
    ),
```

## Edge Cases & Error Semantics

- **Idempotent Table Cleanup (`DROP TABLE IF EXISTS ... PURGE`)**: Oracle 23ai/26ai supports `DROP TABLE IF EXISTS <table_name> PURGE`; ensure table teardown in `test_driver_conformance.py` and `test_oracle_native.py` drops scratch tables cleanly so test reruns never accumulate recyclebin segments or fail with `ORA-00955` (`name is already used by an existing object`).
- **Fail-Loud Conformance Execution (`pending_flow_id=None`)**: With `pending_flow_id=None`, `test_driver_conformance.py` must never `pytest.skip` or `pytest.xfail` Oracle conformance tests when `make oracle-test` runs against `127.0.0.1:5531/FREEPDB1` (`VERTEBRA_CONFORMANCE_ORACLE_DSN`).
- **Lowercase Column Projection Parity**: Conformance assertions access `row["id"]`, `row["label"]`, `row["amount"]`, `row["val_int"]`, `row["val_json"]`, and Arrow schema field names in lowercase (`enable_lowercase_column_names=True` default).

## Public-Seam Test Blueprint

In `src/py/tests/integration/db/test_driver_conformance.py` and `src/py/tests/integration/db/test_oracle_native.py`:
- Cross-engine conformance (`test_driver_conformance.py -k oracle`): sync and async CRUD on `vtb_conformance_items`, `SCALAR_TYPE_CASES` on `vtb_conformance_types`, positional (`:1..:n`) and named (`:name`) binds, `execute_many`, transactions (`commit`/`rollback`), nested savepoints, `select_to_arrow` (`ArrowCursor`), delayed-commit `pipeline()`, and `IntegrityError` (`ORA-00001` -> `sqlstate="23505"`).
- Native Oracle 26ai suite (`test_oracle_native.py`):
  - `test_oracle_26ai_oson_and_vector_roundtrip_and_similarity_search`: inserts and queries `JSON` (`OSON` decoded via `msgspec` to `dict`/`list`) and `VECTOR(3, FLOAT32)` / `VECTOR(3, INT8)` columns (`VectorView` / `list[float]`) and runs `OracleEmbeddingPushdown.similarity_search`.
  - `test_oracle_26ai_arrow_shm_cursor_above_threshold`: queries `>= ARROW_SHM_THRESHOLD_ROWS` rows via `select_to_arrow()` and verifies `VTBARROW` `ShmCapsule` zero-copy `ArrowCursor` consumption (`cursor.used_shm is True`).
  - `test_oracle_26ai_dml_returning_into_single_roundtrip`: executes `INSERT INTO ... VALUES (...) RETURNING id, label INTO :out_id, :out_label` and asserts returned values.
  - `test_oracle_26ai_pipeline_delayed_commit_rolls_back_on_caller_error`: runs `execute_stack` / `with driver.pipeline() as pipe:` where post-`results()` `one()` validation fails and verifies the earlier `INSERT` in the batch is rolled back on the database.
  - `test_oracle_26ai_vpd_client_identifier_reset_across_rollback`: sets `CLIENT_IDENTIFIER` on a pooled lease via `TenantConnectionScope`, triggers a transaction `ROLLBACK`, exits the tenant scope, and verifies the next borrower sees `SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') IS NULL`.
  - `test_oracle_26ai_out_of_band_cancel_aborts_sleep`: cancels an in-flight `DBMS_SESSION.SLEEP(5)` call and asserts `QueryCancelledError` (`sqlstate="57014"`, `vendor_code=1013`) followed by clean `SELECT 1 FROM DUAL` reuse.

## Verification

```bash
uv run pytest src/py/tests/integration/db/test_driver_conformance.py -k oracle
uv run pytest src/py/tests/integration/db/test_oracle_native.py
```

## Acceptance Criteria

- [ ] The `"oracle"` `EngineConformanceSpec` in `ENGINE_SPECS` (`src/py/tests/integration/db/test_driver_conformance.py`) uses `_ORACLE_ITEMS_DDL` (`vtb_conformance_items`) and `_ORACLE_TYPES_DDL` (`vtb_conformance_types`) with `supports_pipeline=True` and `pending_flow_id=None` [^conformance-py].
- [ ] `src/py/tests/integration/db/test_oracle_native.py` covers `JSON`/`OSON`, `VECTOR`, `VTBARROW` `ArrowCursor`, `DML RETURNING INTO`, delayed-commit pipelines, VPD reset-or-evict tenancy, and out-of-band `ORA-01013` cancellation [^ora-contracts].
- [ ] Both integration suites pass with zero skips or xfails against Oracle 26ai Free (`127.0.0.1:5531/FREEPDB1`).

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for Oracle 26ai codecs, Arrow SHM, pipelines, and VPD tenancy.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix.
[^ora-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver`, `AsyncOracleDriver`, `SyncOraclePool`, and `AsyncOraclePool`.
