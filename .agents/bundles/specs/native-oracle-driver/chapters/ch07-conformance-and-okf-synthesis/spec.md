---
type: Spec
flow_id: native-oracle-driver
parent_flow: native-oracle-driver
chapter_id: ch07-conformance-and-okf-synthesis
title: "Chapter 07: Live Oracle 26ai Free Conformance Suite, Wheel Isolation Gate, and OKF Synthesis"
description: Wire Oracle 26ai DDL and pipeline conformance into test_driver_conformance.py, add live Oracle 26ai Free integration tests, verify wheel isolation and disabled-GIL execution, and synchronize user docs and OKF v0.2 knowledge chapters.
status: stable
state: in_progress
planning_status: complete
chapter_order: 7
depends_on_chapters:
  - ch06-notifications-and-embeddings
replan_trigger: null
current_task: "native-oracle-driver:ch07:t04"
plan_revision: 3
plan_commit: 769302bc
state_revision: 3
last_verified_checkpoint: 549fdf73
last_reviewed_commit: null
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T18:31:49Z"
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
    title: Public contracts for Oracle 26ai codecs, Arrow SHM, pools, pipelines, and VPD tenancy
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix and pipeline test suite
  - id: freethreaded-py
    resource: src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false
    title: Free-threaded Python 3.14t/3.15t GIL_USED == False verification suite
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: Multi-wheel cibuildwheel and sdist release workflow
  - id: docs-db
    resource: docs/usage/databases.md
    title: User-facing database drivers and adapters guide
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF data domain chapter for database drivers and dialers
  - id: okf-pipeline
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF data domain chapter for query pipelines, notifications, and tenancy
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product tech-stack inventory of Rust workspace crates and optional wheels
tags:
  - oracle
  - conformance
  - 26ai
  - wheel
  - freethreaded
  - documentation
  - okf
---

# Chapter 07: Live Oracle 26ai Free Conformance Suite, Wheel Isolation Gate, and OKF Synthesis

<!-- truth: start -->
- **Zero-Skip Live Oracle 26ai Free Conformance (`127.0.0.1:5531/FREEPDB1`)**: The `"oracle"` `EngineConformanceSpec` entry in `ENGINE_SPECS` (`src/py/tests/integration/db/test_driver_conformance.py`) uses `_ORACLE_ITEMS_DDL` (`vtb_conformance_items`) and `_ORACLE_TYPES_DDL` (`vtb_conformance_types` with `NUMBER(19)`, `BINARY_DOUBLE`, `NUMBER(18, 6)`, `BOOLEAN`, `CLOB`, `RAW(2000)`, `TIMESTAMP WITH TIME ZONE`, `JSON`), `supports_pipeline=True`, and `pending_flow_id=None`, complemented by Oracle 26ai-specific integration suites in `src/py/tests/integration/db/test_oracle_native.py` [^ora-contracts] [^conformance-py].
- **Standalone Wheel Isolation & Free-Threaded Gate (`make oracle-wheel-check`)**: `tools/scripts/verify_oracle_wheel.py`, `.github/workflows/release.yaml` (`build_wheels`, `build_sdist`, and `test_wheels`), and `src/py/tests/unit/test_freethreaded_gil.py` verify that `vertebra-oracle` builds an isolated wheel containing `vertebra/_oracle*.so` and `vertebra/_oracle.pyi`, sibling wheels exclude `_oracle.pyi`, `vertebra._oracle.GIL_USED is False` under Python `3.14t`/`3.15t`, and `MissingDependencyError` is raised cleanly when uninstalled [^freethreaded-py] [^release-yaml].
- **User Documentation & OKF v0.2 Synthesis**: User guides (`docs/usage/databases.md`, `README.md`, `AGENTS.md`) and OKF knowledge chapters (`database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, `tech-stack.md`) document the shipped `vertebra-oracle` architecture within strict line budgets [^docs-db] [^okf-drivers] [^okf-pipeline] [^okf-tech-stack].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Chapter Scope & Upstream Dependencies

This chapter implements Requirements `R13` and `R14` from the [root specification](../../spec.md) and closes out the `native-oracle-driver` flow. It depends on [Chapter 06 (`ch06-notifications-and-embeddings`)](../ch06-notifications-and-embeddings/spec.md), when all Rust and Python driver, pool, pipeline, tenancy, notification, and embedding seams are complete.

Specifically, this chapter delivers:
1. Oracle-valid DDL (`_ORACLE_ITEMS_DDL` for `vtb_conformance_items`, `_ORACLE_TYPES_DDL` for `vtb_conformance_types`) and `supports_pipeline=True` (`pending_flow_id=None`) on the `"oracle"` `EngineConformanceSpec` in `src/py/tests/integration/db/test_driver_conformance.py`, plus live Oracle 26ai Free (`127.0.0.1:5531/FREEPDB1`, `VERTEBRA_CONFORMANCE_ORACLE_DSN`) integration coverage in `src/py/tests/integration/db/test_oracle_native.py` (`ch07:t01`).
2. Standalone wheel isolation verification in `tools/scripts/verify_oracle_wheel.py`, CI release matrix wiring across `build_wheels`, `build_sdist`, and `test_wheels` in `.github/workflows/release.yaml`, and `vertebra._oracle` disabled-GIL assertions in `src/py/tests/unit/test_freethreaded_gil.py` (`ch07:t02`).
3. User documentation updates in `docs/usage/databases.md`, `README.md`, and `AGENTS.md` (`ch07:t03`), followed by OKF v0.2 knowledge synchronization in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`, and `.agents/bundles/product/tech-stack.md` (`ch07:t04`).

### 1.2 Verified Code Seams

| Area | Existing Symbol (`path::Symbol`) | Verified Consequence |
| :--- | :--- | :--- |
| **Conformance Matrix** | [test_driver_conformance.py](../../../../../../src/py/tests/integration/db/test_driver_conformance.py)`::ENGINE_SPECS` [^conformance-py] | `_ITEMS_DDL` and `_TYPES_DDL` use `BIGINT` and `TEXT` (invalid on Oracle); the `"oracle"` `EngineConformanceSpec` tuple entry requires `_ORACLE_ITEMS_DDL`, `_ORACLE_TYPES_DDL`, `supports_pipeline=True`, and `pending_flow_id=None`. |
| **Free-Threaded Gate** | [test_freethreaded_gil.py](../../../../../../src/py/tests/unit/test_freethreaded_gil.py)`::test_native_modules_declare_gil_used_false` [^freethreaded-py] | Must include `vertebra._oracle` and its PyO3 exports (`SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, `OracleColumn`, `OracleQueryResult`, `OraclePipelineOutcome`, `OracleNotification`, `AsyncOracleListener`, `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED`). |
| **Release Workflow** | [release.yaml](../../../../../../.github/workflows/release.yaml) [^release-yaml] | Must add `oracle` to `jobs.build_wheels.strategy.matrix.package`, the `for package in ...` loop in `jobs.build_sdist`, and the `uv pip install` + export assertions in `jobs.test_wheels`. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Conformance DDL & Live Oracle 26ai Free Suite (`test_driver_conformance.py` & `test_oracle_native.py`)

In `src/py/tests/integration/db/test_driver_conformance.py`:

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

And the `"oracle"` entry in `ENGINE_SPECS: tuple[EngineConformanceSpec, ...]`:

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

`src/py/tests/integration/db/test_oracle_native.py` tests Oracle 26ai-specific features against `127.0.0.1:5531/FREEPDB1` (`VERTEBRA_CONFORMANCE_ORACLE_DSN` / `vtb_oracle_config`):
1. `JSON` (`OSON` binary wire format `0xFF 'J' 'Z' 0x01`) round-trip into Python `dict | list` via `msgspec` (`enable_json_deserialization=True`).
2. `VECTOR(3, FLOAT32)`, `VECTOR(3, FLOAT64)`, `VECTOR(3, INT8)`, and `VECTOR(8, BINARY)` (`0xDB 0x01` binary wire format) round-trip (`VectorView` / `list[float]` + `OracleEmbeddingPushdown.similarity_search`).
3. `select_to_arrow()` zero-copy `VTBARROW` `ArrowCursor` (`cursor.used_shm is True`) over `>= ARROW_SHM_THRESHOLD_ROWS` rows and `load_from_arrow()` batch ingestion.
4. `DML ... RETURNING id, label INTO :out_id, :out_label` single-round-trip out-binds.
5. Delayed-commit `SyncOraclePipeline` and `AsyncOraclePipeline` rollback on post-`results()` `one()` cardinality failure and nested `SAVEPOINT` rollback.
6. VPD `DBMS_SESSION.SET_IDENTIFIER` (`SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')`) and trusted context package setup, mandatory clear on lease exit after `ROLLBACK`, and physical session eviction on simulated reset failure.
7. Out-of-band `cancel()` / `asyncio.Task.cancel()` during `DBMS_SESSION.SLEEP(5)` raising `QueryCancelledError` (`sqlstate="57014"`, `vendor_code=1013`) and recovering the session for subsequent queries.

### 2.2 Standalone Wheel Isolation & Free-Threaded Gate (`verify_oracle_wheel.py`, `release.yaml`, `test_freethreaded_gil.py`)

`tools/scripts/verify_oracle_wheel.py` (`uv run python tools/scripts/verify_oracle_wheel.py`, wired to `make oracle-wheel-check`) verifies:
1. `uv build --wheel` at the repo root produces `vertebra_core-*.whl` that contains `vertebra/db/adapters/oracle/` and excludes `vertebra/_oracle.*` and `vertebra/_oracle.pyi`.
2. Building `src/rs/vertebra-oracle` produces `vertebra_oracle-*.whl` and `vertebra_oracle-*.tar.gz` containing `vertebra/_oracle*.so` (or `.pyd`/`.dylib`) and `vertebra/_oracle.pyi`, with zero Instant Client (`libclntsh` / `libnnz` / `libodpic`) dynamic library dependencies.
3. Installing `vertebra_core-*.whl` alone in an isolated temporary venv outside the repo checkout allows `import vertebra.db.adapters.oracle` cleanly and raises `MissingDependencyError` naming `vertebra-core[oracle]` on driver instantiation.
4. Installing `vertebra_oracle-*.whl` into that isolated venv allows `import vertebra._oracle`, verifies `vertebra._oracle.GIL_USED is False`, and instantiates `SyncOracleClient` / `AsyncOracleClient` / `SyncOraclePool` / `AsyncOraclePool` without I/O.

In `.github/workflows/release.yaml`:
- `jobs.build_wheels.strategy.matrix.package` adds:
  ```yaml
  - name: oracle
    path: src/rs/vertebra-oracle
    module: vertebra._oracle
    exports: SyncOracleClient AsyncOracleClient SyncOraclePool AsyncOraclePool
  ```
- `jobs.build_sdist` updates the sdist loop to `for package in bigquery bundler oracle spanner storage; do`.
- `jobs.test_wheels` adds `vertebra-oracle` and `"vertebra-core[bigquery,bundler,oracle,spanner,storage]==$version"`, checks `"oracle": ("SyncOracleClient", "AsyncOracleClient", "SyncOraclePool", "AsyncOraclePool")`, and asserts `not hasattr(_vertebra, "SyncOracleClient")`.

### 2.3 User Documentation & OKF v0.2 Knowledge Synchronization

- `docs/usage/databases.md`, `README.md`, and `AGENTS.md` (`<= 100` lines) are updated in `ch07:t03` (`<= 3` files).
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`<= 200` lines), `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`<= 200` lines), and `.agents/bundles/product/tech-stack.md` (`<= 200` lines) are updated in `ch07:t04` (`<= 3` files) with `sources:` and `[^id]:` provenance footnotes.

## 3. Documentation & OKF Knowledge Plan

Progress: `[3/6 files documented]` (owned by `ch07:t03` and `ch07:t04`).

- `docs/usage/databases.md` (`ch07:t03`) — Document `vertebra-core[oracle]` installation, `OracleConfig` descriptor precedence, Autonomous Wallet mTLS, 26ai `VECTOR` and `OSON` codecs, delayed-commit `pipeline()`, VPD trusted-package and `CLIENT_IDENTIFIER` tenancy, and `OracleNotificationSource`.
- `README.md` (`ch07:t03`) — Register `vertebra-oracle` (`_oracle`), the `[oracle]` optional extra, and `oracle-test` / `oracle-wheel-check` Make targets.
- `AGENTS.md` (`ch07:t03`) — Register `vertebra-oracle` (`_oracle`) in the workspace crate and verification command summary within `<= 100` lines.
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` (`ch07:t04`) — Document `vertebra-oracle` worker-thread completion bridge, `oracledb =26.0.0-beta.5` + crate-local `arrow 59` SHM integration, Autonomous Wallet mTLS, and DRCP pooling.
- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (`ch07:t04`) — Document Oracle delayed-commit sequential pipelines, VPD trusted-package + `SET_IDENTIFIER` reset-or-evict lifecycle, and `OracleNotificationSource`.
- `.agents/bundles/product/tech-stack.md` (`ch07:t04`) — Register `vertebra-oracle` (`oracledb =26.0.0-beta.5`, crate-local `arrow 59`) in the Rust workspace and optional wheel catalog.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Worksheet | Verification Strategy | Public Seam / Gate |
| :--- | :--- | :--- | :--- | :--- |
| R13 (Live Oracle 26ai Free Conformance Matrix) | `native-oracle-driver:ch07:t01` | [t01-live-oracle-26ai-conformance-matrix.md](tasks/t01-live-oracle-26ai-conformance-matrix.md) | `behavior_tdd` | `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_oracle_native.py` |
| R13 (Standalone Wheel Isolation & Free-Threaded Gate) | `native-oracle-driver:ch07:t02` | [t02-wheel-isolation-and-freethreaded-gate.md](tasks/t02-wheel-isolation-and-freethreaded-gate.md) | `regression_tdd` | `tools/scripts/verify_oracle_wheel.py`, `src/py/tests/unit/test_freethreaded_gil.py` |
| R14 (User Documentation & AGENTS.md Sync) | `native-oracle-driver:ch07:t03` | [t03-user-docs-and-okf-knowledge-sync.md](tasks/t03-user-docs-and-okf-knowledge-sync.md) | `documentation_validation` | `docs/usage/databases.md`, `README.md`, `AGENTS.md`, `make docs-check` |
| R14 (OKF Domain Knowledge & Tech-Stack Sync) | `native-oracle-driver:ch07:t04` | [t04-okf-domain-and-tech-stack-sync.md](tasks/t04-okf-domain-and-tech-stack-sync.md) | `documentation_validation` | `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `make review-check` |

## 5. Implementation Plan

- [x] [1e0fc5b5] native-oracle-driver:ch07:t01 — Wire Live Oracle 26ai Conformance Matrix and Native Integration Suite (tasks/t01-live-oracle-26ai-conformance-matrix.md)
- [x] [549fdf73] native-oracle-driver:ch07:t02 — Implement Standalone Wheel Isolation and Free-Threaded Gate (tasks/t02-wheel-isolation-and-freethreaded-gate.md)
- [x] native-oracle-driver:ch07:t03 — Synchronize User Documentation, README, and AGENTS.md (tasks/t03-user-docs-and-okf-knowledge-sync.md)
- [~] native-oracle-driver:ch07:t04 — Synchronize OKF Data Domain Chapters and Tech Stack (tasks/t04-okf-domain-and-tech-stack-sync.md)

**Chapter Recovery Checkpoint:** `make lint-all && make rs-test && make py-test && make oracle-test && make oracle-wheel-check && make docs-check && make review-check && make review-route` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `in_progress` (`3/4` tasks completed); `current_task: "native-oracle-driver:ch07:t04"`.
- **Upstream Prerequisite**: [Chapter 06 (`ch06-notifications-and-embeddings`)](../ch06-notifications-and-embeddings/spec.md) (`native-oracle-driver:ch06:t03`).
- **Downstream Consumer**: Flow closeout (`/flow:review`, `/flow:finish native-oracle-driver`, `/flow:archive native-oracle-driver`).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for Oracle 26ai codecs, Arrow SHM, pools, pipelines, and VPD tenancy.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix and pipeline test suite.
[^freethreaded-py]: `src/py/tests/unit/test_freethreaded_gil.py::test_native_modules_declare_gil_used_false` — Free-threaded Python `3.14t`/`3.15t` `GIL_USED == False` verification suite.
[^release-yaml]: `.github/workflows/release.yaml` — Multi-wheel `cibuildwheel` and `sdist` release workflow.
[^docs-db]: `docs/usage/databases.md` — User-facing database drivers and adapters guide.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF data domain chapter for database drivers and dialers.
[^okf-pipeline]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF data domain chapter for query pipelines, notifications, and tenancy.
[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product tech-stack inventory of Rust workspace crates and optional wheels.
