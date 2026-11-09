---
type: Task
id: "native-oracle-driver:ch05:t03"
chapter_id: ch05-pipelines-tenancy-and-apex
title: Wire SQLSpec NAMED_COLON parameter style and APEX OracleSessionManager tenancy
description: Set _execution_parameter_style = ParameterStyle.NAMED_COLON on SQLSpec OracleSyncDriver and OracleAsyncDriver, wire driver tenant scope reset-or-evict hooks, and align OracleSessionManager with trusted VPD context procedures and TTC piggyback set_session_attrs.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch05:t02"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/apex/session.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/tests/unit/extensions/test_oracle_apex.py
files:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/apex/session.py
  - src/py/vertebra/db/adapters/oracle/driver.py
tests:
  - src/py/tests/unit/extensions/test_oracle_apex.py
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 210
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
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: SQLSpec NAMED_COLON, VPD tenancy, and APEX OracleSessionManager contracts
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver
    title: SQLSpec OracleSyncDriver and OracleAsyncDriver classes
  - id: apex-session
    resource: src/py/vertebra/extensions/apex/session.py::OracleSessionManager
    title: OracleSessionManager attach, detach, and acquire_scoped implementation
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver and AsyncOracleDriver tenant scope hooks
  - id: apex-tests
    resource: src/py/tests/unit/extensions/test_oracle_apex.py::test_oracle_session_manager_attach_vpd_and_guaranteed_detach
    title: Unit test suite for Oracle APEX session management
tags:
  - oracle
  - sqlspec
  - apex
  - tenancy
  - vpd
---

# Task t03: Wire SQLSpec `NAMED_COLON` Parameter Style and APEX `OracleSessionManager` Tenancy

## Objective & Scope Boundary

Set `_execution_parameter_style = ParameterStyle.NAMED_COLON` on `OracleSyncDriver` and `OracleAsyncDriver` in `src/py/vertebra/extensions/sqlspec/adapters.py`, implement `enter_tenant_scope` / `exit_tenant_scope` / `evict_connection` on `SyncOracleDriver` and `AsyncOracleDriver` (`src/py/vertebra/db/adapters/oracle/driver.py`) and wire `OracleSyncDriver.apply_tenant_predicate` / `OracleAsyncDriver.apply_tenant_predicate` to use the driver's configured `context_namespace`, `context_package_proc`, and `context_clear_proc`, and align `OracleSessionManager` (`src/py/vertebra/extensions/apex/session.py`) with trusted VPD context procedures, `SET_IDENTIFIER`, TTC piggyback `set_session_attrs` (`ECID`) propagation, and physical connection eviction when `build_detach_plsql` fails [^ora-contracts] [^sqlspec-adapters] [^apex-session] [^ora-driver]. Notification streaming and embeddings belong to Chapter 06 ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `.agents/bundles/specs/native-oracle-driver/contracts.md` — Section 5 SQLSpec `NAMED_COLON`, VPD tenancy, and `OracleSessionManager` contracts ([../../contracts.md](../../contracts.md)) [^ora-contracts].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `OracleSyncDriver`, `OracleAsyncDriver`, and `_native_statement_config` [^sqlspec-adapters].
- `src/py/vertebra/extensions/apex/session.py` — `OracleSessionManager`, `ApexSessionContext`, and `OracleConnectionLike` [^apex-session].
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver` [^ora-driver].
- `src/py/vertebra/db/adapters/oracle/core.py` — `OracleCore.build_session_context_sql` and `OracleCore.build_clear_context_sql`.
- `src/py/tests/unit/extensions/test_oracle_apex.py` — Existing APEX unit tests [^apex-tests].

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py`:
  - `OracleSyncDriver._execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_COLON`
  - `OracleAsyncDriver._execution_parameter_style: ClassVar[ParameterStyle | None] = ParameterStyle.NAMED_COLON`
  - `OracleSyncDriver.apply_tenant_predicate(self, predicate: TenantPredicate) -> str` and `OracleAsyncDriver.apply_tenant_predicate(self, predicate: TenantPredicate) -> str`:
    - Resolve `namespace`, `context_package_proc`, and `context_clear_proc` from `getattr(self.native_driver, "config", None)`, execute `OracleCore.build_session_context_sql(...)`, record active tenant state on `native_driver` for mandatory reset on session close/return, and return `predicate.to_sql_fragment(":tenant_id")`.
- `src/py/vertebra/db/adapters/oracle/driver.py`:
  - `SyncOracleDriver.enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig = TenantScopeConfig()) -> str`
  - `SyncOracleDriver.exit_tenant_scope(self, config: TenantScopeConfig = TenantScopeConfig()) -> None`
  - `SyncOracleDriver.evict_connection(self) -> None`
  - `AsyncOracleDriver.enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig = TenantScopeConfig()) -> str`
  - `AsyncOracleDriver.exit_tenant_scope(self, config: TenantScopeConfig = TenantScopeConfig()) -> None`
  - `AsyncOracleDriver.evict_connection(self) -> None`
- `src/py/vertebra/extensions/apex/session.py`:
  - `OracleSessionManager.build_attach_plsql(context: ApexSessionContext, *, context_package_proc: str | None = None) -> tuple[str, dict[str, Any]]`:
    - Validates `context_package_proc` via `validate_identifier(..., allow_dots=True)` when provided and emits `{context_package_proc}(:{ns_bind}, :{attr_bind}, :{val_bind});` instead of raw `DBMS_SESSION.SET_CONTEXT(...)`.
  - `OracleSessionManager.build_detach_plsql(context: ApexSessionContext | None = None, *, context_clear_proc: str | None = None) -> str | tuple[str, dict[str, Any]]`:
    - When `context is None and context_clear_proc is None`, returns `"BEGIN\n    APEX_SESSION.DETACH;\n    DBMS_SESSION.CLEAR_IDENTIFIER;\nEND;"` (preserving zero-arg call compatibility); when `context_clear_proc` and `context.vpd_context` are provided, appends validated `{context_clear_proc}(:{ns_bind}, :{attr_bind});` statements.
  - `OracleSessionManager.acquire_scoped(cls, pool: OraclePoolLike, context: ApexSessionContext, *, trace_id: str | None = None, span_id: str | None = None, module: str = "Vertebra", action: str = "apex.request", context_package_proc: str | None = None, context_clear_proc: str | None = None) -> AsyncIterator[OracleConnectionLike]`

## Edge Cases & Error Semantics

- **SQLSpec `NAMED_COLON` Compilation**: With `_execution_parameter_style = ParameterStyle.NAMED_COLON`, `OracleSyncDriver` and `OracleAsyncDriver` compile `?` and `:name` SQLSpec statements into `:name` / `:1` colon binds without falling back to qmark `?` placeholders.
- **APEX Detach Failure Eviction**: In `OracleSessionManager.acquire_scoped`, if executing the detach PL/SQL block in `finally` raises an exception, `acquire_scoped` invokes `evict_connection()` / `evict()` / `close()` on `conn` before clearing attributes and re-raising so a session with a still-attached APEX session or VPD context is never returned to the pool.
- **TTC Piggyback Compatibility**: `OracleSessionManager.acquire_scoped` supports both attribute assignment (`conn.module`, `conn.action`, `conn.client_identifier`, `conn.ecid`) and `conn.set_session_attrs(...)` when `conn` is a native `AsyncOracleClient` / `AsyncOracleDriver`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_oracle_apex.py` and `src/py/tests/unit/db/test_oracle_adapter.py`
- **Worked Test Matrix**:

| Test Name | Scenario & Input | Observable Assertion |
| :--- | :--- | :--- |
| `test_sqlspec_oracle_drivers_declare_named_colon_and_compile_stack` | Inspect `OracleSyncDriver._execution_parameter_style` and `OracleAsyncDriver._execution_parameter_style`, and prepare a `SQL("SELECT * FROM items WHERE id = ?", 7)` statement | Both drivers set `ParameterStyle.NAMED_COLON`; compiled SQL uses colon placeholder syntax and `execute_stack` routes multi-statement stacks through `driver.pipeline()`. |
| `test_sqlspec_oracle_apply_tenant_predicate_uses_trusted_package_or_set_identifier` | Call `apply_tenant_predicate(TenantPredicate(tenant_value="t-99"))` on `OracleSyncDriver` with default `OracleConfig()` and with `OracleConfig(context_namespace="APP_CTX", context_package_proc="SEC_PKG.SET_CTX")` | Default config executes `BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;`; trusted-package config executes `BEGIN SEC_PKG.SET_CTX(:namespace, :attribute, :value); END;`; both return `"tenant_id = :tenant_id"`. |
| `test_oracle_session_manager_trusted_package_and_evict_on_failed_detach` | Call `OracleSessionManager.acquire_scoped(..., context_package_proc="APEX_CTX_PKG.SET_VAL", context_clear_proc="APEX_CTX_PKG.CLEAR_VAL")` and simulate `execute` raising on detach | Attach block invokes `APEX_CTX_PKG.SET_VAL`; detach block invokes `APEX_CTX_PKG.CLEAR_VAL`; on detach error, `conn.evict_connection()` / `conn.close()` is called before the error propagates. |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_oracle_apex.py src/py/tests/unit/db/test_oracle_adapter.py -q
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/apex/session.py src/py/vertebra/db/adapters/oracle/driver.py src/py/tests/unit/extensions/test_oracle_apex.py
```

Commit as `feat(oracle): wire SQLSpec NAMED_COLON and APEX trusted VPD session management`.

## Acceptance Criteria

- [x] `OracleSyncDriver._execution_parameter_style` and `OracleAsyncDriver._execution_parameter_style` are set to `ParameterStyle.NAMED_COLON` in `src/py/vertebra/extensions/sqlspec/adapters.py` (R11).
- [x] `SyncOracleDriver`, `AsyncOracleDriver`, `OracleSyncDriver`, and `OracleAsyncDriver` wire tenant scope entry, exit reset, and failed-reset connection eviction (R10, R11).
- [x] `OracleSessionManager` supports `context_package_proc` and `context_clear_proc`, stages TTC `ECID` attributes via `set_session_attrs`, and evicts/closes the connection if detach fails (R11).

## Notes & Discoveries

- Preserving zero-argument `OracleSessionManager.build_detach_plsql()` return value (`str`) when `context_clear_proc is None` keeps all existing callers in `test_oracle_apex.py` backward-compatible while supporting bound clear procedures when configured.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — SQLSpec `NAMED_COLON`, VPD tenancy, and APEX `OracleSessionManager` contracts.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver` — SQLSpec `OracleSyncDriver` and `OracleAsyncDriver` classes.
[^apex-session]: `src/py/vertebra/extensions/apex/session.py::OracleSessionManager` — `OracleSessionManager` attach, detach, and `acquire_scoped` implementation.
[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver` and `AsyncOracleDriver` tenant scope hooks.
[^apex-tests]: `src/py/tests/unit/extensions/test_oracle_apex.py::test_oracle_session_manager_attach_vpd_and_guaranteed_detach` — Unit test suite for Oracle APEX session management.
