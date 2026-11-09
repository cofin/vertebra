---
type: Task
id: "native-oracle-driver:ch05:t02"
chapter_id: ch05-pipelines-tenancy-and-apex
title: Implement VPD trusted-package and SET_IDENTIFIER tenancy with pool reset-or-evict
description: Add context_package_proc and context_clear_proc to OracleConfig and OracleCore alongside DBMS_SESSION.SET_IDENTIFIER / CLEAR_IDENTIFIER and enforce mandatory reset or physical connection eviction in TenantConnectionScope / AsyncTenantConnectionScope in src/py/vertebra/db/rls.py.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch05:t01"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - src/py/vertebra/db/rls.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/driver.py
files:
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/rls.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 230
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
    title: VPD trusted context package, SET_IDENTIFIER, and pool reset-or-evict contracts
  - id: db-rls
    resource: src/py/vertebra/db/rls.py::TenantScopeConfig
    title: TenantScopeConfig and TenantPredicate definitions in vertebra.db.rls
  - id: ora-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: OracleConfig and OracleSessionAttrs configuration structs
  - id: ora-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: OracleCore.build_session_context_sql builder
  - id: okf-tenancy
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF domain rules for TenantPredicate and engine purity in vertebra.db
tags:
  - oracle
  - tenancy
  - vpd
  - rls
  - pool
---

# Task t02: Implement VPD Trusted-Package and `SET_IDENTIFIER` Tenancy with Pool Reset-or-Evict

## Objective & Scope Boundary

Add configurable VPD trusted context package procedure fields (`context_package_proc`, `context_clear_proc`, `context_namespace`) to `OracleConfig` (`src/py/vertebra/db/adapters/oracle/config.py`), extend `OracleCore.build_session_context_sql` and `OracleCore.build_clear_context_sql` (`src/py/vertebra/db/adapters/oracle/core.py`) to support both trusted PL/SQL context packages (`CREATE CONTEXT <ns> USING <pkg>`) and built-in `USERENV.CLIENT_IDENTIFIER` (`DBMS_SESSION.SET_IDENTIFIER(:value)` / `DBMS_SESSION.CLEAR_IDENTIFIER`), and implement engine-neutral `TenantConnectionScope` / `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` enforcing mandatory context reset on scope exit and physical session eviction (`evict_connection()` / `close()`) if reset fails [^ora-contracts] [^db-rls] [^ora-config] [^ora-core] [^okf-tenancy]. Keep `src/py/vertebra/db/rls.py` 100% engine-pure (zero adapter imports or Oracle SQL literals inside `db/rls.py`; it delegates to polymorphic driver hooks). This task (`ch05:t02`) unit-tests `TenantConnectionScope` and `AsyncTenantConnectionScope` with a protocol-compliant driver double, while `ch05:t03` wires `enter_tenant_scope` / `exit_tenant_scope` / `evict_connection` directly on `SyncOracleDriver` and `AsyncOracleDriver` alongside SQLSpec and APEX ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `.agents/bundles/specs/native-oracle-driver/contracts.md` — Section 5 VPD trusted-package, `SET_IDENTIFIER`, and mandatory pool reset-or-evict contracts ([../../contracts.md](../../contracts.md)) [^ora-contracts].
- `src/py/vertebra/db/rls.py` — `TenantScopeConfig` and `TenantPredicate` structs [^db-rls].
- `src/py/vertebra/db/adapters/oracle/config.py` — `OracleConfig` and `OracleSessionAttrs` [^ora-config].
- `src/py/vertebra/db/adapters/oracle/core.py` — `OracleCore.build_session_context_sql` and identifier validation [^ora-core].
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver` session lease hooks.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/oracle/config.py`:
  - Add fields on `OracleConfig`:
    - `context_namespace: str = "USERENV"`
    - `context_package_proc: str | None = None` (`"<SCHEMA>.<PKG>.<PROC>"` or `"<PKG>.<PROC>"` for `CREATE CONTEXT <ns> USING <pkg>`)
    - `context_clear_proc: str | None = None` (`"<SCHEMA>.<PKG>.<CLEAR_PROC>"` or `"<PKG>.<CLEAR_PROC>"`)
- `src/py/vertebra/db/adapters/oracle/core.py`:
  - `OracleCore.build_session_context_sql(tenant_value: str, config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE, *, namespace: str = "USERENV", context_package_proc: str | None = None) -> tuple[str, dict[str, str]]`:
    - When `context_package_proc` is provided: validates `context_package_proc` with `validate_identifier(..., allow_dots=True)`, `namespace`, and `attribute`, and returns `(f"BEGIN {context_package_proc}(:namespace, :attribute, :value); END;", {"namespace": namespace, "attribute": attribute, "value": tenant_value})`.
    - When `context_package_proc is None` and `namespace.upper() == "USERENV"`: returns `("BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;", {"value": tenant_value})`.
    - When `context_package_proc is None` and `namespace.upper() != "USERENV"`: returns `("BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;", {"namespace": namespace, "attribute": attribute, "value": tenant_value})`.
  - `OracleCore.build_clear_context_sql(config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE, *, namespace: str = "USERENV", context_clear_proc: str | None = None) -> tuple[str, dict[str, str]]`:
    - When `context_clear_proc` is provided: validates `context_clear_proc` with `validate_identifier(..., allow_dots=True)` and returns `(f"BEGIN {context_clear_proc}(:namespace, :attribute); END;", {"namespace": namespace, "attribute": attribute})`.
    - When `context_clear_proc is None` and `namespace.upper() == "USERENV"`: returns `("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", {})`.
    - When `context_clear_proc is None` and `namespace.upper() != "USERENV"`: returns `("BEGIN DBMS_SESSION.CLEAR_CONTEXT(:namespace, NULL, :attribute); END;", {"namespace": namespace, "attribute": attribute})`.
- `src/py/vertebra/db/rls.py`:
  - Export `TenantConnectionScope` and `AsyncTenantConnectionScope` in `__all__` alongside `TenantPredicate` and `TenantScopeConfig`:
  - `class TenantConnectionScope`:
    - `def __init__(self, driver: Any, tenant_value: str, config: TenantScopeConfig = TenantScopeConfig()) -> None`
    - `def __enter__(self) -> Any`
    - `def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None`
  - `class AsyncTenantConnectionScope`:
    - `def __init__(self, driver: Any, tenant_value: str, config: TenantScopeConfig = TenantScopeConfig()) -> None`
    - `async def __aenter__(self) -> Any`
    - `async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None`

## Edge Cases & Error Semantics

- **Engine Purity Gate (`src/py/vertebra/db/rls.py`)**: `src/py/vertebra/db/*.py` is scanned by the AST engine-purity gate (`test_native_db_drivers.py`) [^okf-tenancy]. `src/py/vertebra/db/rls.py` must not import any adapter module or contain engine-specific SQL strings; it invokes polymorphic methods (`driver.enter_tenant_scope(tenant_value, config)` / `driver.exit_tenant_scope(config)` and `driver.evict_connection()` / `driver.close()`).
- **Mandatory Reset Across `ROLLBACK`**: Oracle `SYS_CONTEXT` and `USERENV.CLIENT_IDENTIFIER` are session-level attributes that survive `ROLLBACK`. `TenantConnectionScope.__exit__` and `AsyncTenantConnectionScope.__aexit__` always execute the clear hook even when the body raised an exception or rolled back a transaction.
- **Evict-on-Failed-Reset**: If `exit_tenant_scope(config)` raises any `Exception` (e.g. broken socket `ORA-03113` or package invalidation `ORA-04068`), the scope calls `driver.evict_connection()` (falling back to `driver.close()`) to permanently retire the physical session so another borrower can never observe the previous tenant's context, and raises the reset failure (chaining any body exception as `__context__`).
- **PL/SQL Injection Guard**: `context_package_proc` and `context_clear_proc` are validated via `validate_identifier(..., allow_dots=True)`; any semicolon, space, comment, or parenthesis raises `ValueError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_oracle_adapter.py`
- **Worked Test Matrix**:

| Test Name | Scenario & Input | Observable Assertion |
| :--- | :--- | :--- |
| `test_oracle_core_build_session_and_clear_context_set_identifier_default` | `OracleCore.build_session_context_sql("acme-42")` and `OracleCore.build_clear_context_sql()` with default `namespace="USERENV"` | Returns `("BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;", {"value": "acme-42"})` and `("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", {})`. |
| `test_oracle_core_build_session_and_clear_context_trusted_package` | `context_package_proc="APP_SEC.CTX_PKG.SET_ATTR"`, `context_clear_proc="APP_SEC.CTX_PKG.CLEAR_ATTR"`, `namespace="VERTEBRA_CTX"`, `setting_name="app.tenant_id"` | Returns `("BEGIN APP_SEC.CTX_PKG.SET_ATTR(:namespace, :attribute, :value); END;", {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme-42"})` and `("BEGIN APP_SEC.CTX_PKG.CLEAR_ATTR(:namespace, :attribute); END;", {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id"})`. |
| `test_oracle_core_rejects_malicious_context_package_proc` | `context_package_proc="PKG.PROC; DROP USER system"` | Raises `ValueError` from `validate_identifier`. |
| `test_tenant_connection_scope_resets_on_rollback_and_evicts_on_reset_error` | Enter `TenantConnectionScope(driver_double, "tenant-a")` and `AsyncTenantConnectionScope(async_driver_double, "tenant-a")`, raise inside block, and simulate reset failure on second run | First run executes `exit_tenant_scope` (`CLEAR_IDENTIFIER`) despite body exception; second run where `exit_tenant_scope` raises `OperationalError` calls `evict_connection()` on the driver double and propagates `OperationalError`. |

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_native_db_drivers.py -k "tenancy or context or purity" -q
uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/config.py src/py/vertebra/db/adapters/oracle/core.py src/py/vertebra/db/rls.py src/py/tests/unit/db/test_oracle_adapter.py
```

Commit as `feat(oracle): add trusted-package VPD tenancy and pool reset-or-evict scope`.

## Acceptance Criteria

- [x] `OracleConfig` exposes `context_namespace`, `context_package_proc`, and `context_clear_proc` (R10).
- [x] `OracleCore.build_session_context_sql` and `OracleCore.build_clear_context_sql` support trusted package procedures, `DBMS_SESSION.SET_IDENTIFIER` / `CLEAR_IDENTIFIER`, and `DBMS_SESSION.SET_CONTEXT` / `CLEAR_CONTEXT` with strict identifier validation (R10).
- [x] `TenantConnectionScope` and `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` pass the generic `db/*.py` engine-purity gate, reset tenant state after normal or rolled-back execution, and evict the physical session when reset fails (R10).

## Notes & Discoveries

- Keeping `TenantConnectionScope` and `AsyncTenantConnectionScope` in `src/py/vertebra/db/rls.py` free of adapter imports satisfies `test_native_db_drivers.py`'s AST engine-purity scan across `src/py/vertebra/db/*.py`.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — VPD trusted context package, `SET_IDENTIFIER`, and pool reset-or-evict contracts.
[^db-rls]: `src/py/vertebra/db/rls.py::TenantScopeConfig` — `TenantScopeConfig` and `TenantPredicate` definitions in `vertebra.db.rls`.
[^ora-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — `OracleConfig` and `OracleSessionAttrs` configuration structs.
[^ora-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — `OracleCore.build_session_context_sql` builder.
[^okf-tenancy]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF domain rules for `TenantPredicate` and engine purity in `vertebra.db`.
