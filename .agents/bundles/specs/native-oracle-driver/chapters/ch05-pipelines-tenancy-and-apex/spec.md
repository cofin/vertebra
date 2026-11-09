---
type: Spec
parent_flow: native-oracle-driver
chapter_id: ch05-pipelines-tenancy-and-apex
title: "Chapter 05: Delayed-Commit Pipelines, VPD Trusted Tenancy, and APEX Session Bridge"
description: Implement delayed-commit SyncOraclePipeline and AsyncOraclePipeline with SAVEPOINT nesting, VPD trusted-package and DBMS_SESSION.SET_IDENTIFIER tenancy with mandatory reset-or-evict on pool release, SQLSpec ParameterStyle.NAMED_COLON adapters, and OracleSessionManager APEX session wiring.
status: stable
state: in_progress
planning_status: complete
depends_on_chapters:
  - ch04-pools-drcp-and-session-piggyback
replan_trigger: null
current_task: "native-oracle-driver:ch05:t02"
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-09T01:28:30Z"
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
    title: Public OraclePipelineOutcome, SyncOraclePipeline, VPD tenancy, and extension contracts
  - id: ora-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver and AsyncOracleDriver adapter surface
  - id: ora-config-py
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: OracleConfig and OracleSessionAttrs definitions
  - id: ora-core-py
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: OracleCore session context SQL builders and codecs
  - id: db-rls-py
    resource: src/py/vertebra/db/rls.py::TenantScopeConfig
    title: Engine-neutral TenantScopeConfig and TenantPredicate definitions
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver
    title: SQLSpec OracleSyncDriver, OracleAsyncDriver, and execute_stack pipeline integration
  - id: apex-session-py
    resource: src/py/vertebra/extensions/apex/session.py::OracleSessionManager
    title: Oracle APEX session attachment, VPD propagation, and OTel ECID tagging
tags:
  - oracle
  - pipeline
  - tenancy
  - vpd
  - sqlspec
  - apex
---

# Chapter 05: Delayed-Commit Pipelines, VPD Trusted Tenancy, and APEX Session Bridge

<!-- truth: start -->
- **Delayed-Commit Sequential Pipelines (`R9`)**: `SyncOraclePipeline` and `AsyncOraclePipeline` pin a pooled worker connection and hold an open transaction (or `SAVEPOINT _vtb_pipe_<n>` when already inside an outer transaction; Oracle uses `SAVEPOINT <name>` and `ROLLBACK TO SAVEPOINT <name>` with no `RELEASE SAVEPOINT`) across `results()`, committing only on clean `__exit__`/`__aexit__` (`exc_type is None`) so caller cardinality validation (`one`/`one_or_none`) and DTO hydration inside `with driver.pipeline() as pipe:` roll back the entire batch on error [^ora-contracts] [^ora-driver-py] [^sqlspec-py].
- **Trusted-Package + `SET_IDENTIFIER` VPD Tenancy & Mandatory Pool Reset (`R10`)**: `OracleConfig` and `OracleCore` support configurable trusted PL/SQL context package procedures (`context_package_proc`, `context_clear_proc`) alongside `DBMS_SESSION.SET_IDENTIFIER(:value)` / `DBMS_SESSION.CLEAR_IDENTIFIER` and TTC piggyback, while `TenantConnectionScope` / `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` and pool lease release always execute context reset and permanently evict/retire the physical session if reset fails [^ora-config-py] [^ora-core-py] [^db-rls-py].
- **SQLSpec `NAMED_COLON` & APEX Session Alignment (`R11`)**: `OracleSyncDriver` and `OracleAsyncDriver` in `extensions/sqlspec/adapters.py` set `_execution_parameter_style = ParameterStyle.NAMED_COLON` and enforce reset-or-evict tenant scopes, while `OracleSessionManager` in `extensions/apex/session.py` routes `build_attach_plsql` / `build_detach_plsql` and `acquire_scoped` through trusted context procedures, `SET_IDENTIFIER`, and TTC piggyback `set_session_attrs` (`ECID`) [^sqlspec-py] [^apex-session-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Upstream `oracledb = "=26.0.0-beta.5"` executes statements synchronously per TTC round-trip on a worker thread, and Oracle 26ai rejects direct `DBMS_SESSION.SET_CONTEXT` calls outside a trusted PL/SQL package (`ORA-01031: insufficient privileges`) while preserving session-scoped `SYS_CONTEXT` and `USERENV.CLIENT_IDENTIFIER` state across `ROLLBACK` [^ora-contracts] [^ora-core-py]. In addition, `OracleSyncDriver` and `OracleAsyncDriver` in `src/py/vertebra/extensions/sqlspec/adapters.py` currently omit `_execution_parameter_style = ParameterStyle.NAMED_COLON`, and `OracleSessionManager` in `src/py/vertebra/extensions/apex/session.py` hardcodes raw `DBMS_SESSION.SET_CONTEXT` calls without trusted-package support or failed-detach connection eviction [^sqlspec-py] [^apex-session-py].

Chapter 05 delivers three vertical slices:
1. Implement `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = False` and `OraclePipelineOutcome` in Rust (`src/rs/vertebra-oracle/src/client.rs` and `src/py/vertebra/_oracle.pyi`) and delayed-commit `SyncOraclePipeline` / `AsyncOraclePipeline` in `src/py/vertebra/db/adapters/oracle/driver.py`, holding the transaction or `SAVEPOINT _vtb_pipe_<n>` open across `results()` until clean context exit and adding a crate-capability tracking test (`ch05:t01`) [^ora-contracts] [^ora-driver-py].
2. Add `context_namespace`, `context_package_proc`, and `context_clear_proc` to `OracleConfig` and `OracleCore`, implement `TenantConnectionScope` and `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` with mandatory lease-exit reset and physical session eviction on reset failure (unit-tested against a protocol-compliant driver double), and cover both trusted-package and `DBMS_SESSION.SET_IDENTIFIER` modes (`ch05:t02`) [^ora-config-py] [^ora-core-py] [^db-rls-py].
3. Set `_execution_parameter_style = ParameterStyle.NAMED_COLON` on `OracleSyncDriver` and `OracleAsyncDriver`, wire `enter_tenant_scope` / `exit_tenant_scope` / `evict_connection` on `SyncOracleDriver` / `AsyncOracleDriver` and `apply_tenant_predicate` to the reset-or-evict contract, and align `OracleSessionManager` (`build_attach_plsql`, `build_detach_plsql`, `acquire_scoped`) with trusted context procedures, `SET_IDENTIFIER`, and TTC piggyback `set_session_attrs` (`ECID`) (`ch05:t03`) [^sqlspec-py] [^apex-session-py].

### 1.2 Non-Goals

- Implementing Oracle 26ai TxEventQ / `DBMS_AQ` notification streaming or `OracleEmbeddingPushdown` `VectorView` extraction (owned by Chapter 06).
- Running live Docker Oracle 26ai Free conformance or updating user/OKF documentation (owned by Chapter 07).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public contracts: [contracts.md](../../contracts.md). Depends on [ch04-pools-drcp-and-session-piggyback](../ch04-pools-drcp-and-session-piggyback/spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md) and [oracle-apex.md](../../../../knowledge/domains/extensions/oracle-apex.md).

| Existing Seam | Verified Consequence |
| :--- | :--- |
| `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver.execute_stack` [^sqlspec-py] | Executes `pipe.results()` and hydrates `StackResult` objects inside `with self.native_driver.pipeline() as pipe:`; committing inside `results()` would prevent rolling back when a post-`results()` hydration or `one()` check raises. |
| `src/py/vertebra/db/adapters/oracle/core.py::OracleCore.build_session_context_sql` [^ora-core-py] | Currently emits only `BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;`, which raises `ORA-01031` on Oracle 26ai unless called inside a `CREATE CONTEXT ... USING <pkg>` package; needs trusted-package and `DBMS_SESSION.SET_IDENTIFIER` branches plus matching clear builders. |
| `src/py/vertebra/extensions/apex/session.py::OracleSessionManager` [^apex-session-py] | `build_attach_plsql` and `build_detach_plsql` need optional `context_package_proc` / `context_clear_proc` parameters and `acquire_scoped` must evict/close the leased connection if `APEX_SESSION.DETACH` or context reset raises. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R9 (`ch05:t01`)**: Export `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = False` and `OraclePipelineOutcome` (`status: Literal["completed", "error", "skipped"]`, `result: OracleQueryResult | None`, `error: DatabaseError | None`) from `vertebra._oracle` and implement `SyncOraclePipeline` (`SyncPipeline`) and `AsyncOraclePipeline` (`AsyncPipeline`) on `SyncOracleDriver` / `AsyncOracleDriver` (`SyncPipelineCapable` / `AsyncPipelineCapable`). Hold the pipeline transaction or `SAVEPOINT _vtb_pipe_<n>` open across `results()` and commit only on clean `__exit__`/`__aexit__` (`exc_type is None`), rolling back on statement failure, post-`results()` caller exception, or unexecuted queued operations.
- **R10 (`ch05:t02`)**: Add `context_namespace: str = "USERENV"`, `context_package_proc: str | None = None`, and `context_clear_proc: str | None = None` to `OracleConfig`, extend `OracleCore.build_session_context_sql` and `OracleCore.build_clear_context_sql` to support trusted PL/SQL package procedures alongside `DBMS_SESSION.SET_IDENTIFIER(:value)` / `DBMS_SESSION.CLEAR_IDENTIFIER`, and implement `TenantConnectionScope` / `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` enforcing mandatory context reset on exit and physical connection eviction on reset failure.
- **R11 (`ch05:t03`)**: Set `_execution_parameter_style = ParameterStyle.NAMED_COLON` on `OracleSyncDriver` and `OracleAsyncDriver` in `src/py/vertebra/extensions/sqlspec/adapters.py`, wire `enter_tenant_scope` / `exit_tenant_scope` / `evict_connection` on `SyncOracleDriver` and `AsyncOracleDriver` and route `apply_tenant_predicate` through `OracleConfig`'s trusted-package / `SET_IDENTIFIER` configuration with reset-or-evict cleanup hooks, and update `OracleSessionManager` in `src/py/vertebra/extensions/apex/session.py` to support trusted VPD context procedures, `SET_IDENTIFIER`, TTC piggyback `set_session_attrs`, and connection eviction on detach failure.

### 2.2 Concrete Type & Signature Contracts

- Rust & Stub (`src/rs/vertebra-oracle/src/client.rs`, `src/py/vertebra/_oracle.pyi`):
  - `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = False`
  - `#[pyclass(module = "vertebra._oracle", frozen)] pub struct OraclePipelineOutcome` with `#[pyo3(get)] pub status: String`, `pub result: Option<Py<OracleQueryResult>>`, `pub error: Option<Py<PyAny>>`
  - `SyncOracleClient.execute_pipeline_batch(self, operations: Sequence[tuple[str, Sequence[Any] | Mapping[str, Any] | None, bool]], *, continue_on_error: bool = False) -> list[OraclePipelineOutcome]`
  - `AsyncOracleClient.execute_pipeline_batch(self, operations: Sequence[tuple[str, Sequence[Any] | Mapping[str, Any] | None, bool]], *, continue_on_error: bool = False) -> Awaitable[list[OraclePipelineOutcome]]`
- Python Pipeline (`src/py/vertebra/db/adapters/oracle/driver.py`):
  - `class SyncOraclePipeline(SyncPipeline)` and `class AsyncOraclePipeline(AsyncPipeline)`
  - `SyncOracleDriver.pipeline(self) -> SyncOraclePipeline` and `AsyncOracleDriver.pipeline(self) -> AsyncOraclePipeline`
- Python Tenancy (`src/py/vertebra/db/adapters/oracle/config.py`, `core.py`, `src/py/vertebra/db/rls.py`):
  - `OracleConfig.context_namespace: str = "USERENV"`
  - `OracleConfig.context_package_proc: str | None = None`
  - `OracleConfig.context_clear_proc: str | None = None`
  - `OracleCore.build_session_context_sql(tenant_value: str, config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE, *, namespace: str = "USERENV", context_package_proc: str | None = None) -> tuple[str, dict[str, str]]`
  - `OracleCore.build_clear_context_sql(config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE, *, namespace: str = "USERENV", context_clear_proc: str | None = None) -> tuple[str, dict[str, str]]`
  - `class TenantConnectionScope` and `class AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py`
- Extensions (`src/py/vertebra/extensions/sqlspec/adapters.py`, `src/py/vertebra/extensions/apex/session.py`):
  - `OracleSyncDriver._execution_parameter_style = ParameterStyle.NAMED_COLON`
  - `OracleAsyncDriver._execution_parameter_style = ParameterStyle.NAMED_COLON`
  - `OracleSessionManager.build_attach_plsql(context: ApexSessionContext, *, context_package_proc: str | None = None) -> tuple[str, dict[str, Any]]`
  - `OracleSessionManager.build_detach_plsql(context: ApexSessionContext | None = None, *, context_clear_proc: str | None = None) -> str | tuple[str, dict[str, Any]]`

### 2.3 Caller -> Seam -> Collaborator Map

```text
OracleSyncDriver.execute_stack / OracleAsyncDriver.execute_stack (extensions/sqlspec/adapters.py)
  -> SyncOracleDriver.pipeline() / AsyncOracleDriver.pipeline() (db/adapters/oracle/driver.py)
    -> SyncOraclePipeline / AsyncOraclePipeline (holds transaction or SAVEPOINT _vtb_pipe_<n> across results())
      -> SyncOracleClient / AsyncOracleClient.execute_pipeline_batch (src/rs/vertebra-oracle/src/client.rs)

TenantConnectionScope / OracleSyncDriver.apply_tenant_predicate / OracleSessionManager.acquire_scoped
  -> OracleCore.build_session_context_sql & build_clear_context_sql (db/adapters/oracle/core.py)
    -> Trusted PL/SQL package procedure OR DBMS_SESSION.SET_IDENTIFIER / CLEAR_IDENTIFIER
      -> SyncOraclePool / AsyncOraclePool release or evict_connection on reset failure
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Deterministic Behavior |
| :--- | :--- |
| Caller raises inside `with driver.pipeline() as pipe:` after `pipe.results()` returns | Pipeline `__exit__` / `__aexit__` sees `exc_type is not None` and executes `ROLLBACK` (or `ROLLBACK TO SAVEPOINT _vtb_pipe_<n>`) instead of committing. |
| Nested `with driver.pipeline():` inside an active `with driver.transaction():` | Issues `SAVEPOINT _vtb_pipe_<n>` on entry, rolls back via `ROLLBACK TO SAVEPOINT _vtb_pipe_<n>` on error, and leaves the outer transaction open on clean exit without emitting unsupported `RELEASE SAVEPOINT`. |
| `OracleCore.build_session_context_sql` with `namespace="USERENV"` and `attribute="CLIENT_IDENTIFIER"` (or default unprivileged mode) | Emits `BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;` and `{"value": tenant_value}` (paired with `BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;`), avoiding `ORA-01031` without custom DBA grants. |
| `OracleCore.build_session_context_sql` with `context_package_proc="APP_CTX_PKG.SET_ATTR"` | Validates dotted `<pkg>.<proc>` identifier via `validate_identifier(..., allow_dots=True)` and emits `BEGIN APP_CTX_PKG.SET_ATTR(:namespace, :attribute, :value); END;`. |
| Resetting tenant context or detaching APEX session raises `DatabaseError` on lease exit | `TenantConnectionScope` / `OracleSessionManager.acquire_scoped` marks the physical session broken/evicted (`evict_connection()`) before re-raising, guaranteeing the tainted connection is never handed to another borrower. |

### 2.5 Alternatives Comparison

| Seam | Chosen Design | Rejected Alternative & Rationale |
| :--- | :--- | :--- |
| Pipeline commit boundary | Commit in `__exit__` / `__aexit__` (`exc_type is None`) after caller block finishes | Committing eagerly inside `pipe.results()` — violates transactional atomicity when caller cardinality checks (`select_one`) or `msgspec` struct hydration fail after `results()`. |
| Oracle savepoint cleanup | Omit `RELEASE SAVEPOINT` on clean nested pipeline exit | Emitting `RELEASE SAVEPOINT _vtb_pipe_<n>` — Oracle SQL does not support `RELEASE SAVEPOINT` (`ORA-00900: invalid SQL statement`); savepoints release automatically when the outer transaction commits or rolls back. |
| VPD context reset failure | Evict/close physical session from pool when reset PL/SQL fails | Returning connection to pool with logged warning — Oracle `SYS_CONTEXT` and `CLIENT_IDENTIFIER` are session-scoped and survive `ROLLBACK`, causing cross-tenant data leakage. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (public documentation in `docs/usage/databases.md`, `README.md`, and `AGENTS.md` and OKF chapters `database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, and `tech-stack.md` are consolidated in [ch07-conformance-and-okf-synthesis](../ch07-conformance-and-okf-synthesis/spec.md) tasks `ch07:t03`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| :--- | :--- | :--- | :--- |
| R9 (Delayed-commit pipelines & savepoint nesting) | [t01](tasks/t01-delayed-commit-sync-and-async-pipelines.md) | `behavior_tdd` | `src/py/tests/unit/db/test_oracle_adapter.py` |
| R10 (VPD trusted-package + `SET_IDENTIFIER` & reset-or-evict in `rls.py`) | [t02](tasks/t02-vpd-trusted-tenancy-and-pool-reset.md) | `behavior_tdd` | `src/py/tests/unit/db/test_oracle_adapter.py` |
| R11 (SQLSpec `NAMED_COLON` & `OracleSessionManager`) | [t03](tasks/t03-sqlspec-named-colon-and-apex-session.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_oracle_apex.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |

## 5. Implementation Plan

- [x] native-oracle-driver:ch05:t01 — Implement OraclePipelineOutcome and delayed-commit SyncOraclePipeline and AsyncOraclePipeline (tasks/t01-delayed-commit-sync-and-async-pipelines.md)
- [~] native-oracle-driver:ch05:t02 — Implement VPD trusted-package and SET_IDENTIFIER tenancy with pool reset-or-evict (tasks/t02-vpd-trusted-tenancy-and-pool-reset.md)
- [ ] native-oracle-driver:ch05:t03 — Wire SQLSpec NAMED_COLON parameter style and APEX OracleSessionManager tenancy (tasks/t03-sqlspec-named-colon-and-apex-session.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked && uv run --no-sync python -m mypy.stubtest vertebra._oracle && uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/extensions/test_oracle_apex.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Plan Baseline**: `HEAD @ 769302bc`; `state: planned`, `planning_status: complete`, `plan_revision: 3`, `plan_commit: 769302bc`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks [ch06-notifications-and-embeddings](../ch06-notifications-and-embeddings/spec.md) (`ch06:t01` depends on `ch05:t03`).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public `OraclePipelineOutcome`, `SyncOraclePipeline`, VPD tenancy, and extension contracts.
[^ora-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver` and `AsyncOracleDriver` adapter surface.
[^ora-config-py]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — `OracleConfig` and `OracleSessionAttrs` definitions.
[^ora-core-py]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — `OracleCore` session context SQL builders and codecs.
[^db-rls-py]: `src/py/vertebra/db/rls.py::TenantScopeConfig` — Engine-neutral `TenantScopeConfig` and `TenantPredicate` definitions.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver` — SQLSpec `OracleSyncDriver`, `OracleAsyncDriver`, and `execute_stack` pipeline integration.
[^apex-session-py]: `src/py/vertebra/extensions/apex/session.py::OracleSessionManager` — Oracle APEX session attachment, VPD propagation, and OTel `ECID` tagging.
