---
type: Task
id: db-engine-primitives:t07
title: Make vertebra/db/rls.py dialect-agnostic, move GUC/context SQL builders to PostgreSQLCore and OracleCore, enforce generic vertebra/db engine purity, and update OKF chapters
description: "Refactor `src/py/vertebra/db/rls.py` (`TenantScopeConfig`, `TenantPredicate`) into a dialect-agnostic tenant-isolation module, move PostgreSQL `set_config` and `$N::<cast>` builders to `PostgreSQLCore` and Oracle `DBMS_SESSION.SET_CONTEXT` builders to `OracleCore`, remove dead `.apply` duck-typing branches in SQLSpec GCP drivers, enforce an AST/source test proving zero engine leaks remain in `src/py/vertebra/db/*.py` and `src/py/vertebra/db/driver/*.py`, and finalize the OKF data and embeddings knowledge chapters."
state: "in_progress"
priority: P2
verification_strategy: behavior_tdd
depends_on:
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
  - docs/reference/database-capabilities.md
  - docs/reference/index.md
  - zensical.toml
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_pg_gateway.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 10
plan_commit: "05bc06a5a53a1225d8c62495ac69162a15d935fc"
state_revision: 1
claimed_by: "/root/db_t03"
claimed_at: "2026-11-05T04:51:23Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:49:00Z
updated_at: "2026-11-05T05:05:25Z"
commit: null
tags: [db, rls, postgres, oracle, layering, okf]
---

## Objective

Refactor `src/py/vertebra/db/rls.py` (`TenantScopeConfig`, `TenantPredicate`) into a dialect-agnostic tenant-isolation module, move PostgreSQL `set_config` and `$N::<cast>` builders to `PostgreSQLCore` and Oracle `DBMS_SESSION.SET_CONTEXT` builders to `OracleCore`, remove dead `.apply` duck-typing branches in SQLSpec GCP drivers, enforce an AST/source test proving zero engine leaks remain in `src/py/vertebra/db/*.py` and `src/py/vertebra/db/driver/*.py`, and finalize the OKF data and embeddings knowledge chapters.

## Context

- **Independent prerequisite**: t06 is complete and db.mcp_tools deletion is already an ancestor at adeade96. Revised adapter-owned provider implementation is independent of RLS/generic-DB purity. t07 exclusively edits native-db test source and RLS/SQLSpec/PostgreSQL/Oracle sources; t05 runs the existing gate without editing that file. Root applies reviewed knowledge cache diffs.

- **Verification strategy (`behavior_tdd`)**: ANSI SQL `TenantPredicate.to_sql_fragment` rendering, `PostgreSQLCore` GUC / `$N::<cast>` SQL builders, `OracleCore` VPD session-context builder, SQLSpec adapter `apply_tenant_predicate` delegation (`NotSupportedError` on unsupported dialects), and generic `vertebra/db` engine-purity AST checks are directly testable in unit tests.
- **Architectural invariants**:
  - `TenantScopeConfig` and `TenantPredicate` inherit `FrozenBaseStruct` (`frozen=True, gc=False, kw_only=True`) with inline PEP 257 field docstrings and contain zero PostgreSQL `set_config` or `::uuid` syntax.
  - Scope engine purity to AST import rules, engine-specific SQL construction, and executable engine dispatch. Stable broker dialect IDs are protocol-registration metadata; engine names and identifiers used as metadata do not constitute engine-execution leakage. Do not use vocabulary or engine-name scans.
  - Preserve original session-setting failures through PostgreSQL/Oracle sync and async adapters; unsupported engines fail with `NotSupportedError`. Track adapter-import bindings and aliases in the gate, while preserving existing dialer branches and polymorphic `ensure_module(self.driver_module)` loading. Negative gate cases cover class identity, construction, aliases, and executable engine SQL while excluding docstrings and registration metadata.
  - All eight BigQuery/Spanner sync/async SQLSpec config and driver constructors reject non-None tenant_predicate with native NotSupportedError as their first executable statement, before base initialization, resolver/default-broker/native factory access, supplied-core access, or I/O. Ordinary None-predicate sessions preserve existing preparation; unsupported isolation sessions cannot exist, so script/Arrow/batch/snapshot entry points cannot bypass the policy. Preserve PostgreSQL session-setting preparation through completed t06 native sync %s/async $N profiles and original failure identity.
  - Keep `<!-- truth: start -->` blocks in OKF knowledge chapters `<= 40` lines (`make review-check`).

## Steps

1. **Write failing dialect-agnostic RLS and generic `vertebra/db` engine-purity tests (RED)**:
   - In `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_pg_gateway.py`, and `src/py/tests/unit/db/test_enterprise_olap_drivers.py`:
     - Assert `TenantScopeConfig(setting_name="app.tenant_id", tenant_column="tenant_id", parameter_cast="UUID")` + `TenantPredicate(table_alias="o", tenant_value="t1", config=...)` renders ANSI `o.tenant_id = CAST(? AS UUID)` via `to_sql_fragment("?")` and `o.tenant_id = :tenant_id` when `parameter_cast=None` via `to_sql_fragment(":tenant_id")`, rejects legacy `guc_name=` with `TypeError`, and has no `build_session_guc_sql` method.
     - Assert `PostgreSQLCore.build_session_guc_sql("t1", scope, is_local=True)` returns `("SELECT set_config($1, $2, $3)", ("app.tenant_id", "t1", True))` and `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)` returns `"o.tenant_id = $1::UUID"` (and `"o.tenant_id = $1"` when `parameter_cast=None`).
     - Assert `OracleCore.build_session_context_sql("t1", scope, namespace="VERTEBRA_CTX")` extracts `attribute = scope.setting_name.rsplit(".", 1)[-1]`, validates `namespace` and `attribute` via `validate_identifier(..., allow_dots=False)`, and returns `("BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;", {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "t1"})`.
     - Update `test_sync_tenant_predicate_raises_when_guc_set_fails` and `test_async_tenant_predicate_raises_when_guc_set_fails` in `src/py/tests/unit/db/test_enterprise_olap_drivers.py` so calling `apply_tenant_predicate` on `SQLiteSyncConfig` / `SQLiteAsyncConfig` asserts `pytest.raises(NotSupportedError)` (instead of `sqlite3.OperationalError` on `set_config`), while `PostgreSQLSyncDriver`/`PostgreSQLAsyncDriver` execute `PostgreSQLCore.build_session_guc_sql(...)` and return `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)` and `OracleSyncDriver`/`OracleAsyncDriver` execute `OracleCore.build_session_context_sql(...)` and return `predicate.to_sql_fragment(":tenant_id")`.
   - In `src/py/tests/unit/extensions/test_sqlspec_bigquery.py` and `src/py/tests/unit/extensions/test_sqlspec_spanner.py`, parameterize configured-predicate rejection across all eight constructors: BigQuerySyncDriver, BigQueryAsyncDriver, BigQuerySyncConfig, BigQueryAsyncConfig, SpannerSyncDriver, SpannerAsyncDriver, SpannerSyncConfig, and SpannerAsyncConfig. Assert native NotSupportedError before any supplied-core attribute/method access, base initialization, config resolver/default-broker/native factory access, or client/RPC side effect; no unsupported session is constructed. Preserve ordinary None-predicate query SQL/binds/results and retain existing script, Arrow, batch, and snapshot coverage. These files remain t07-owned; t05 does not edit them.
   - Add sync/async PostgreSQL and Oracle session-setting failure identity assertions to existing canonical RLS/enterprise tests, and verify actual PostgreSQL native request placeholders follow t06 sync %s/async $N preparation with the helper bind values retained.
   - In `src/py/tests/unit/db/test_native_db_drivers.py`:
     - Add an AST/module gate over `src/py/vertebra/db/*.py` and `src/py/vertebra/db/driver/*.py` that rejects runtime `vertebra.db.adapters.*` imports and engine-specific SQL functions (`pg_notify`, `pg_logical_emit_message`, `set_config`, `DBMS_AQ`, `DBMS_SESSION`, `DBMS_VECTOR`, `ML.GENERATE_EMBEDDING`, `ML.PREDICT`), with these narrow registry exceptions:
       - In `src/py/vertebra/db/registry.py`, permit module-level imports of configuration classes from `vertebra.db.adapters.<engine>.config` only for classes listed directly in `REGISTERED_CONFIG_TYPES`. Permit those imported names in that declarative tuple and annotation/type-alias expressions, and permit `SCHEME_REGISTRY` to derive URL schemes from the tuple. Polymorphic `config_cls.from_url(...)`, `config.build_sync_driver(...)`, and `config.build_async_driver(...)` dispatch remain valid.
       - In `registry.py`, permit driver imports nested under the `if TYPE_CHECKING:` guard only for annotation/type-alias use. Keep runtime adapter-driver imports forbidden.
       - Reject engine-specific executable dispatch using registered adapter configuration or driver classes, including `isinstance` branches, class-identity comparisons, and direct engine-driver construction. Do not exempt the entire registry module or its runtime function bodies.
     - Exercise the gate against the current registry as its positive case and parsed source snippets as negative cases: a runtime adapter-driver import, an adapter configuration import outside `registry.py`, a registry configuration class import absent from `REGISTERED_CONFIG_TYPES`, and an executable engine-specific class branch in the registry. Assert the rule rejects each negative case while retaining the existing `test_registry_parse_url_executes_zero_eager_driver_imports` coverage.

2. **Refactor `rls.py`, `PostgreSQLCore`, `OracleCore`, `SyncOracleDriver`, `extensions/sqlspec/`, docs, and OKF chapters (GREEN)**:
   - Update `src/py/vertebra/db/rls.py` (`TenantScopeConfig` with `setting_name`, `tenant_column`, `parameter_cast: str | None = None`; `TenantPredicate` with `column_reference()` and ANSI `to_sql_fragment(placeholder: str = "?")`).
   - Add `build_session_guc_sql` and `build_tenant_predicate_sql` static methods on `PostgreSQLCore` (`src/py/vertebra/db/adapters/postgres/core.py`) and `build_session_context_sql` on `OracleCore` (`src/py/vertebra/db/adapters/oracle/core.py`), and handle `BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;` in `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver.execute` (initializing `_session_context` and recording `self._session_context[(namespace, attribute)] = value` before returning `0`) while the pre-Rust `sqlite3` stand-in remains prior to `native-oracle-driver`.
   - Update `apply_tenant_predicate` in `src/py/vertebra/extensions/sqlspec/adapters.py` (`BaseSyncDriver`/`BaseAsyncDriver` raising `NotSupportedError` by default; `PostgreSQLSyncDriver`/`PostgreSQLAsyncDriver` executing `PostgreSQLCore.build_session_guc_sql` and returning `PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)`; `OracleSyncDriver`/`OracleAsyncDriver` executing `OracleCore.build_session_context_sql` and returning `predicate.to_sql_fragment(":tenant_id")`), and remove the dead `getattr(self._tenant_predicate, "apply", None)` branches in `src/py/vertebra/extensions/sqlspec/bigquery.py::_prepare_sql` and `src/py/vertebra/extensions/sqlspec/spanner.py::_prepare` while preserving existing unconfigured preparation. Import NotSupportedError from vertebra.db.driver and reject non-None tenant_predicate as the first executable statement in BigQuerySyncDriver, BigQueryAsyncDriver, BigQuerySyncConfig, BigQueryAsyncConfig, SpannerSyncDriver, SpannerAsyncDriver, SpannerSyncConfig, and SpannerAsyncConfig constructors. Each driver guard precedes super().__init__ and any supplied-core/config access; each config guard precedes field/config resolution and super().__init__, which may allocate the default broker. Unsupported isolation sessions cannot exist; do not add per-method guards or fail ordinary None-predicate sessions. Prepare PostgreSQL session-setting SQL through the existing t06 statement configuration to native sync %s/async $N, retain helper bind values, and propagate original driver exceptions unchanged.
   - Update `docs/usage/analytics-and-cdc.md` and `docs/reference/db.md` for `PostgresNotifyConfig`/`PostgresNotificationSource`, `SyncSpannerChangeStreamSource`/`AsyncSpannerChangeStreamSource`, engine-neutral `CDCConfig`/`ChangeFeed`, and dialect-agnostic `TenantScopeConfig`/`TenantPredicate`.
   - Update the tenant-isolation section of `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` for `setting_name`, ANSI placeholders, adapter-owned PostgreSQL/Oracle session builders, and the narrow declarative-registration / annotation-only registry exceptions. Preserve the CDC, Spanner relocation, and native pipelining documentation completed by prerequisite tasks. Finalize `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (documenting `Base*Driver.apply_tenant_predicate` raising `NotSupportedError` and `PostgreSQL*Driver`/`Oracle*Driver` delegating to `PostgreSQLCore`/`OracleCore`), `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, and `.agents/bundles/product/product.md` (`sources:`, `<= 40`-line `<!-- truth: start -->` blocks, and prose).

3. **Run focused and full verification gates**:
   - Run `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py`; then run `make lint-all`, `make py-test`, `make docs`, and `make review-check`. Root applies reviewed knowledge cache diffs before final gates.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py -k engine_purity` -> fails while `rls.py` contains `SELECT set_config($1, $2, $3)`.
- **Focused (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/docs/test_examples.py` -> exits `0`, including ordinary None-predicate success, all eight configured-predicate constructor refusals before core/resolver/broker/native effects, retained script/Arrow/batch/snapshot coverage, native session-bind preparation, and original failure propagation.
- **Final (GREEN)**:
  - `make lint-all && make py-test && make docs && make review-check` -> exits `0`.

## Acceptance Criteria

- [ ] R7: `src/py/vertebra/db/rls.py` is dialect-agnostic, `PostgreSQLCore` and `OracleCore` own dialect-specific session setting SQL, dead `.apply` branches in `extensions/sqlspec/{bigquery,spanner}.py` are removed, the generic `vertebra/db` engine-purity AST gate passes, and `make lint-all`, `make py-test`, `make docs`, and `make review-check` exit `0`.
- [ ] R7: All eight GCP SQLSpec config/driver constructors reject non-None tenant_predicate with native NotSupportedError before base initialization, supplied-core access, resolver/default-broker/native factory access, or I/O; unsupported sessions cannot exist. None-predicate sessions retain query/bind/result and script/Arrow/batch/snapshot behavior without scattered per-method guards. PostgreSQL session-setting binds use native sync %s/async $N through t06 preparation, Oracle uses named context binds, and original session-setting failures retain identity.
- [ ] R7: The engine-purity gate accepts registry configuration registration and annotation-only driver imports, rejects runtime adapter-driver imports and executable engine-specific class dispatch, and passes the existing registry zero-eager-driver-import regression.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->

- [2026-11-05] Root separates the native provider API into a linked database-capabilities reference page because the combined database reference exceeds the existing120000-byte artifact budget; the budget remains unchanged. Source and behavior acceptance are unaffected.

- [2026-11-05] RLS focused282 passed with real psycopg converter and scoped correctness review clean. Combined full make py-test2046 passed69 skipped170 xfailed; make lint-all, make docs and make review-check exit0. Documentation/metadata followup31 cases pass. Source is verified; mandatory whole-flow fresh correctness and quality review precede final task closure.
