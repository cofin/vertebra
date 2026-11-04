---
type: Task
id: db-engine-primitives:t07
title: Make vertebra/db/rls.py dialect-agnostic, move GUC/context SQL builders to PostgreSQLCore and OracleCore, enforce generic vertebra/db engine purity, and update OKF chapters
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t05
  - db-engine-primitives:t06
files:
  - src/py/vertebra/db/rls.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
  - docs/usage/analytics-and-cdc.md
  - docs/reference/db.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_pg_gateway.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 4
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:49:00Z
updated_at: 2026-11-04T01:12:00Z
commit: null
tags: [db, rls, postgres, oracle, layering, okf]
---

## Objective

Refactor `src/py/vertebra/db/rls.py` (`TenantScopeConfig`, `TenantPredicate`) into a dialect-agnostic tenant-isolation module, move PostgreSQL `set_config` and `$N::<cast>` builders to `PostgreSQLCore` and Oracle `DBMS_SESSION.SET_CONTEXT` builders to `OracleCore`, remove dead `.apply` duck-typing branches in SQLSpec GCP drivers, enforce an AST/source test proving zero engine leaks remain in `src/py/vertebra/db/*.py` and `src/py/vertebra/db/driver/*.py`, and finalize the OKF data and embeddings knowledge chapters.

## Context

- **Verification strategy (`behavior_tdd`)**: ANSI SQL `TenantPredicate.to_sql_fragment` rendering, `PostgreSQLCore` GUC / `$N::<cast>` SQL builders, `OracleCore` VPD session-context builder, SQLSpec adapter `apply_tenant_predicate` delegation (`NotSupportedError` on unsupported dialects), and generic `vertebra/db` engine-purity AST checks are directly testable in unit tests.
- **Architectural invariants**:
  - `TenantScopeConfig` and `TenantPredicate` inherit `FrozenBaseStruct` (`frozen=True, gc=False, kw_only=True`) with inline PEP 257 field docstrings and contain zero PostgreSQL `set_config` or `::uuid` syntax.
  - Keep `<!-- truth: start -->` blocks in OKF knowledge chapters `<= 40` lines (`make review-check`).

## Steps

1. **Write failing dialect-agnostic RLS and generic `vertebra/db` engine-purity tests (RED)**:
   - In `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_pg_gateway.py`, and `src/py/tests/unit/db/test_enterprise_olap_drivers.py`:
     - Assert `TenantScopeConfig(setting_name="app.tenant_id", tenant_column="tenant_id", parameter_cast="UUID")` + `TenantPredicate(table_alias="o", tenant_value="t1", config=...)` renders ANSI `o.tenant_id = CAST(? AS UUID)` via `to_sql_fragment("?")` and `o.tenant_id = :tenant_id` when `parameter_cast=None` via `to_sql_fragment(":tenant_id")`, rejects legacy `guc_name=` with `TypeError`, and has no `build_session_guc_sql` method.
     - Assert `PostgreSQLCore.build_session_guc_sql("t1", scope, is_local=True)` returns `("SELECT set_config($1, $2, $3)", ("app.tenant_id", "t1", True))` and `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)` returns `"o.tenant_id = $1::UUID"` (and `"o.tenant_id = $1"` when `parameter_cast=None`).
     - Assert `OracleCore.build_session_context_sql("t1", scope, namespace="VERTEBRA_CTX")` extracts `attribute = scope.setting_name.rsplit(".", 1)[-1]`, validates `namespace` and `attribute` via `validate_identifier(..., allow_dots=False)`, and returns `("BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;", {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "t1"})`.
     - Update `test_sync_tenant_predicate_raises_when_guc_set_fails` and `test_async_tenant_predicate_raises_when_guc_set_fails` in `src/py/tests/unit/db/test_enterprise_olap_drivers.py` so calling `apply_tenant_predicate` on `SQLiteSyncConfig` / `SQLiteAsyncConfig` asserts `pytest.raises(NotSupportedError)` (instead of `sqlite3.OperationalError` on `set_config`), while `PostgreSQLSyncDriver`/`PostgreSQLAsyncDriver` execute `PostgreSQLCore.build_session_guc_sql(...)` and return `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)` and `OracleSyncDriver`/`OracleAsyncDriver` execute `OracleCore.build_session_context_sql(...)` and return `predicate.to_sql_fragment(":tenant_id")`.
   - In `src/py/tests/unit/db/test_native_db_drivers.py`:
     - Add an AST/module gate asserting that no file in `src/py/vertebra/db/*.py` or `src/py/vertebra/db/driver/*.py` (apart from `src/py/vertebra/db/registry.py`'s declarative `REGISTERED_CONFIG_TYPES` tuple) imports `vertebra.db.adapters.*` or embeds engine-specific SQL functions (`pg_notify`, `pg_logical_emit_message`, `set_config`, `DBMS_AQ`, `DBMS_SESSION`, `DBMS_VECTOR`, `ML.GENERATE_EMBEDDING`, `ML.PREDICT`).

2. **Refactor `rls.py`, `PostgreSQLCore`, `OracleCore`, `SyncOracleDriver`, `extensions/sqlspec/`, docs, and OKF chapters (GREEN)**:
   - Update `src/py/vertebra/db/rls.py` (`TenantScopeConfig` with `setting_name`, `tenant_column`, `parameter_cast: str | None = None`; `TenantPredicate` with `column_reference()` and ANSI `to_sql_fragment(placeholder: str = "?")`).
   - Add `build_session_guc_sql` and `build_tenant_predicate_sql` static methods on `PostgreSQLCore` (`src/py/vertebra/db/adapters/postgres/core.py`) and `build_session_context_sql` on `OracleCore` (`src/py/vertebra/db/adapters/oracle/core.py`), and handle `BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;` in `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver.execute` (recording `self._session_context[(namespace, attribute)] = value` and returning `0`) while the pre-Rust `sqlite3` stand-in remains prior to `native-oracle-driver`.
   - Update `apply_tenant_predicate` in `src/py/vertebra/extensions/sqlspec/adapters.py` (`BaseSyncDriver`/`BaseAsyncDriver` raising `NotSupportedError` by default; `PostgreSQLSyncDriver`/`PostgreSQLAsyncDriver` executing `PostgreSQLCore.build_session_guc_sql` and returning `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)`; `OracleSyncDriver`/`OracleAsyncDriver` executing `OracleCore.build_session_context_sql` and returning `predicate.to_sql_fragment(":tenant_id")`), and remove the dead `getattr(self._tenant_predicate, "apply", None)` branches in `src/py/vertebra/extensions/sqlspec/bigquery.py` and `src/py/vertebra/extensions/sqlspec/spanner.py`.
   - Update `docs/usage/analytics-and-cdc.md` and `docs/reference/db.md` for `PostgresNotifyConfig`/`PostgresNotificationSource`, `SyncSpannerChangeStreamSource`/`AsyncSpannerChangeStreamSource`, engine-neutral `CDCConfig`/`ChangeFeed`, and dialect-agnostic `TenantScopeConfig`/`TenantPredicate`.
   - Finalize `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`, `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (documenting `Base*Driver.apply_tenant_predicate` raising `NotSupportedError` and `PostgreSQL*Driver`/`Oracle*Driver` delegating to `PostgreSQLCore`/`OracleCore`), `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, and `.agents/bundles/product/product.md` (`sources:`, `<= 40`-line `<!-- truth: start -->` blocks, and prose).

3. **Run full verification gates**:
   - Run `make lint-all`, `make py-test`, `make docs`, and `make review-check`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py -k engine_purity` -> fails while `rls.py` contains `SELECT set_config($1, $2, $3)`.
- **Final (GREEN)**:
  - `make lint-all && make py-test && make docs && make review-check` -> exits `0`.

## Acceptance Criteria

- [ ] R7: `src/py/vertebra/db/rls.py` is dialect-agnostic, `PostgreSQLCore` and `OracleCore` own dialect-specific session setting SQL, dead `.apply` branches in `extensions/sqlspec/{bigquery,spanner}.py` are removed, the generic `vertebra/db` engine-purity AST gate passes, and `make lint-all`, `make py-test`, `make docs`, and `make review-check` exit `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
