---
type: Task
id: critical-correctness-fixes:t01
title: Tenant predicate raises, Spangres URL routing, and Spangres CDC positional params
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/db/_registry.py
  - src/py/vertebra/db/cdc.py
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
plan_revision: 3
plan_commit: 24e2931
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T16:33:00Z
last_verified_commit: 0cdf54c
verification_evidence: "RED: 5 failed, 4 passed (DID NOT RAISE OperationalError, ValueError: Unsupported database URL scheme 'spangres', dict vs list CDC params); GREEN: 27 passed in test_enterprise_olap_drivers.py, test_spanner.py, test_spanner_queue_and_cdc.py; ruff check, ruff format --check, mypy exit 0"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:33:00Z
commit: 0cdf54c
tags: [bugfix, db, tenancy, spanner, cdc]
---

# t01 — Tenant predicate raises, Spangres URL routing, and Spangres CDC positional params

## Objective

Make three data-layer paths fail loudly or bind correctly: the tenant GUC `SET` propagates its error, `DatabaseRegistry.parse_url` accepts `spangres://` and drops its dead `spanner+postgresql` branch, and Spanner change-stream polling binds positional parameters for Spangres.

## Context

- **Strategy**: `regression_tdd`. Each defect has a reproduction that fails at `142cc49`. `test_registry_rejects_spanner_postgresql_scheme` passes before and after the change; it pins the behavior that the dead-branch removal must keep.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver.apply_tenant_predicate` and `::BaseAsyncDriver.apply_tenant_predicate` wrap `self.execute(sql, params)` in `with suppress(Exception):`. On SQLite the `SELECT set_config($1, $2, $3)` call raises `sqlite3.OperationalError: no such function: set_config`, and the method still returns `"t.tenant_id = $1::uuid"`.
  - `src/py/vertebra/db/_registry.py::DatabaseRegistry.parse_url` routes only `base_scheme == "spanner"`. `parse_url("spangres://p/i/d")` raises `ValueError: Unsupported database URL scheme: 'spangres'`. Its `"+postgresql"` branch sets `overrides["dialect"] = "postgresql"`, but `SpannerConfig.from_dsn` rejects the `spanner+postgresql` scheme with `ImproperConfigurationError` before reading overrides, so the branch never takes effect.
  - `src/py/vertebra/db/cdc.py::SpannerChangeStreamSource.build_read_query` emits `spanner.read_json_<stream>($1, $2, $3, $4, NULL)` for Spangres, while `poll_partition_sync` / `poll_partition_async` always pass `{"start_ts", "end_ts", "token", "heartbeat_ms"}`. `src/py/vertebra/db/spanner/core.py` maps a sequence of parameters to `p1..pN`, which is what Spangres `$1..$4` needs.
- **Why**: a swallowed tenant `SET` runs the session without row-level isolation. Spangres users cannot use registry URLs, and Spangres CDC sends unbound parameters.
- **Canonical test modules**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (already exercises the sqlspec adapter sessions), `src/py/tests/unit/db/test_spanner.py` (holds `test_spanner_config_from_dsn_variants`), `src/py/tests/unit/db/test_spanner_queue_and_cdc.py` (holds `SpannerChangeStreamSource` tests).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.
- **Knowledge**: [query-pipeline-cdc-and-tenancy](../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [database-drivers-and-dialers](../../../knowledge/domains/data/database-drivers-and-dialers.md).

## Steps

### 1. Write failing tests (RED)

1. In `src/py/tests/unit/db/test_enterprise_olap_drivers.py`:
   - Add `import sqlite3` to the standard-library import group.
   - Add `TenantPredicate` to the existing `from vertebra.extensions.sqlspec.adapters import (...)` block (it is in that module's `__all__`).
   - Append these tests at the end of the module:

   ```python
   def test_sync_tenant_predicate_raises_when_guc_set_fails() -> None:
       """A failing tenant GUC SET propagates instead of returning an unenforced predicate."""
       config = SQLiteSyncConfig(path=":memory:")
       predicate = TenantPredicate(table_alias="t", tenant_value="acme")
       try:
           with config.provide_session() as session, pytest.raises(sqlite3.OperationalError, match="set_config"):
               session.apply_tenant_predicate(predicate)
       finally:
           config.close()


   @pytest.mark.anyio
   async def test_async_tenant_predicate_raises_when_guc_set_fails() -> None:
       """The async driver propagates a failing tenant GUC SET."""
       config = SQLiteAsyncConfig(path=":memory:")
       predicate = TenantPredicate(table_alias="t", tenant_value="acme")
       try:
           async with config.provide_session() as session:
               with pytest.raises(sqlite3.OperationalError, match="set_config"):
                   await session.apply_tenant_predicate(predicate)
       finally:
           await config.close()
   ```

   `AsyncAdapterConfig.close` is `async def`, so it is awaited (as `await duck_async.close()` is in `test_sqlspec_adapters_and_async_drivers`); `SyncAdapterConfig.close` is synchronous.

2. In `src/py/tests/unit/db/test_spanner.py`:
   - Add `from vertebra.typing import ImproperConfigurationError` next to the existing `from vertebra.typing import FrozenBaseStruct` (merge into one import line).
   - Append after `test_spanner_config_from_dsn_variants`:

   ```python
   @pytest.mark.parametrize("url", ["spangres://proj-pg/inst-pg/db-pg", "spanner+spangres://proj-pg/inst-pg/db-pg"])
   def test_registry_parses_spangres_scheme(url: str) -> None:
       """DatabaseRegistry.parse_url routes Spangres URLs to SpannerConfig with the spangres dialect."""
       config = DatabaseRegistry.parse_url(url)
       assert isinstance(config, SpannerConfig)
       assert config.dialect == "spangres"
       assert (config.project_id, config.instance_id, config.database_id) == ("proj-pg", "inst-pg", "db-pg")


   def test_registry_rejects_spanner_postgresql_scheme() -> None:
       """The unsupported spanner+postgresql scheme is rejected by SpannerConfig.from_dsn."""
       with pytest.raises(ImproperConfigurationError, match="spanner\\+postgresql"):
           DatabaseRegistry.parse_url("spanner+postgresql://proj/inst/db")
   ```

3. In `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`:
   - Add `from typing import Any` to the standard-library imports.
   - Add `SpannerChangeStreamSource` to the existing `from vertebra.db import (...)` block (it is in `vertebra.db.__all__`).
   - Append:

   ```python
   _START = "2026-11-02T00:00:00Z"
   _END = "2026-11-02T01:00:00Z"


   class _RecordingSpannerDriver:
       """Spanner driver stand-in that records select calls and returns no rows."""

       def __init__(self) -> None:
           self.calls: list[tuple[str, Any]] = []

       def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
           self.calls.append((sql, parameters))
           return []


   class _AsyncRecordingSpannerDriver:
       """Async Spanner driver stand-in that records select calls and returns no rows."""

       def __init__(self) -> None:
           self.calls: list[tuple[str, Any]] = []

       async def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
           self.calls.append((sql, parameters))
           return []


   _EXPECTED_READ_PARAMS: dict[str, Any] = {
       "spangres": [_START, _END, "tok-1", 10_000],
       "googlesql": {"start_ts": _START, "end_ts": _END, "token": "tok-1", "heartbeat_ms": 10_000},
   }


   @pytest.mark.anyio
   @pytest.mark.parametrize("dialect", ["spangres", "googlesql"])
   @pytest.mark.parametrize("mode", ["sync", "async"])
   async def test_spanner_change_stream_poll_binds_params_for_dialect(dialect: str, mode: str) -> None:
       """Spangres polls bind positional $1..$4 values; GoogleSQL polls bind named parameters."""
       driver: Any = _RecordingSpannerDriver() if mode == "sync" else _AsyncRecordingSpannerDriver()
       source = SpannerChangeStreamSource(driver, "orders_stream", dialect=dialect)
       if mode == "sync":
           events = source.poll_partition_sync("tok-1", start_timestamp=_START, end_timestamp=_END)
       else:
           events = await source.poll_partition_async("tok-1", start_timestamp=_START, end_timestamp=_END)
       assert events == []
       assert driver.calls == [(source.build_read_query(), _EXPECTED_READ_PARAMS[dialect])]
       assert "tok-1" in source.completed_partitions
   ```

4. Run RED (expected failures):

   ```bash
   uv run pytest "src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_sync_tenant_predicate_raises_when_guc_set_fails" "src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_async_tenant_predicate_raises_when_guc_set_fails" src/py/tests/unit/db/test_spanner.py::test_registry_parses_spangres_scheme src/py/tests/unit/db/test_spanner.py::test_registry_rejects_spanner_postgresql_scheme src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_change_stream_poll_binds_params_for_dialect -q
   ```

   Expected: both tenant tests fail with `Failed: DID NOT RAISE <class 'sqlite3.OperationalError'>`; `test_registry_parses_spangres_scheme[spangres://...]` fails with `ValueError: Unsupported database URL scheme: 'spangres'` (the `spanner+spangres` case passes); `test_registry_rejects_spanner_postgresql_scheme` passes; the two `spangres` cases of the CDC test fail with an assertion showing a dict where a list is expected; the two `googlesql` cases pass.

### 2. Implement (GREEN)

1. `src/py/vertebra/extensions/sqlspec/adapters.py`:
   - `BaseSyncDriver.apply_tenant_predicate`: replace the `with suppress(Exception):` block with a direct `self.execute(sql, params)` call. Update the docstring to: `"""Apply the transaction-local tenant GUC and return the SQL filter fragment.\n\n        Raises:\n            Exception: Any error raised by the driver while executing the GUC statement.\n        """`. Use the repository's Google docstring layout (summary line, blank line, `Raises:` section).
   - `BaseAsyncDriver.apply_tenant_predicate`: same change with `await self.execute(sql, params)`.
   - Keep the `suppress` import: `execute_stack` still uses it.
2. `src/py/vertebra/db/_registry.py::DatabaseRegistry.parse_url`: replace

   ```python
   if base_scheme == "spanner":
       if "+postgresql" in scheme and "dialect" not in overrides:
           overrides["dialect"] = "postgresql"
       return SpannerConfig.from_dsn(url, **overrides)
   ```

   with

   ```python
   if base_scheme in ("spanner", "spangres"):
       return SpannerConfig.from_dsn(url, **overrides)
   ```

3. `src/py/vertebra/db/cdc.py::SpannerChangeStreamSource`:
   - Add a private method at the end of the class (after `poll_partition_async`):

   ```python
   def _build_read_params(
       self,
       partition_token: str | None,
       start_timestamp: str,
       end_timestamp: str | None,
   ) -> dict[str, Any] | list[Any]:
       """Return change stream TVF bind parameters for the active dialect.

       Spangres binds the positional ``$1..$4`` placeholders, which the Spanner driver maps to
       ``p1..p4``. GoogleSQL binds the named ``@start_ts``, ``@end_ts``, ``@token``, and
       ``@heartbeat_ms`` placeholders.
       """
       if self._dialect == "spangres":
           return [start_timestamp, end_timestamp, partition_token, self._heartbeat_milliseconds]
       return {
           "start_ts": start_timestamp,
           "end_ts": end_timestamp,
           "token": partition_token,
           "heartbeat_ms": self._heartbeat_milliseconds,
       }
   ```

   - In `poll_partition_sync` and `poll_partition_async`, replace the inline `params = {...}` dict with `params = self._build_read_params(partition_token, start_timestamp, end_timestamp)`.
4. Knowledge:
   - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, the `parse_url(url, **overrides)` bullet: replace `` `spanner` → `SpannerConfig` `` with `` `spanner`/`spangres` → `SpannerConfig` (via `SpannerConfig.from_dsn`; `spanner+postgresql` raises `ImproperConfigurationError`) ``.
   - `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`:
     - In the `TenantPredicate.build_session_guc_sql` table row, append: `` The sqlspec adapter drivers run it in `apply_tenant_predicate` and propagate any driver error. ``
     - In the `SpannerChangeStreamSource` table row, append: `` Spangres polls bind `[start_timestamp, end_timestamp, partition_token, heartbeat_milliseconds]` positionally (`$1..$4` → `p1..p4`); GoogleSQL polls bind `@start_ts`, `@end_ts`, `@token`, and `@heartbeat_ms`. ``
5. Changelog `docs/changelog/index.md`, section `## 0.3.0` → `### Fixed`, append:

   ```markdown
   - `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` in `vertebra.extensions.sqlspec` now raise when the tenant GUC `SET` fails instead of returning an unenforced predicate.
   - `DatabaseRegistry.parse_url` accepts `spangres://` URLs, and `SpannerChangeStreamSource.poll_partition_sync` / `poll_partition_async` bind positional `$1..$4` parameters in Spangres mode.
   ```

### 3. Refactor & verify

1. Run the GREEN commands in `## Verification`.
2. Run `make lint` and fix anything it reports in the touched files.
3. Commit:

   ```bash
   git add src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/db/_registry.py src/py/vertebra/db/cdc.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md docs/changelog/index.md
   git commit -S -m "fix(db): raise on tenant GUC failure and bind Spangres URLs and CDC params"
   ```

## Verification

- **RED** (before the fix): the command in Step 1.4 exits `1` with the failures listed there.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q
  uv run ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/db/_registry.py src/py/vertebra/db/cdc.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py
  uv run ruff format --check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/db/_registry.py src/py/vertebra/db/cdc.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py
  uv run mypy src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/db/_registry.py src/py/vertebra/db/cdc.py
  ```

  All exit `0`.
- **Phase gate** (run at the Phase 1 checkpoint): `make lint-all` and `uv run pytest src/py/tests/unit/db src/py/tests/unit/agent src/py/tests/unit/extensions -q` exit `0`.

## Acceptance Criteria

- [x] R1: Sync and async `apply_tenant_predicate` propagate `sqlite3.OperationalError` on SQLite; no `suppress` around the GUC statement.
- [x] R2: `parse_url("spangres://...")` and `parse_url("spanner+spangres://...")` return `SpannerConfig(dialect="spangres")`; `spanner+postgresql://` raises `ImproperConfigurationError`; the `+postgresql` branch is gone.
- [x] R3: Spangres polls pass `[start, end, token, heartbeat]`; GoogleSQL polls pass the named mapping; both sync and async.
- [x] Knowledge chapters and changelog updated as specified.
- [x] Targeted tests, ruff, ruff format, and mypy pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning probe: `DatabaseRegistry.parse_url("spanner+postgresql://p/i/d")` raises `ImproperConfigurationError` at `142cc49`, so the removed branch was dead code.
- [2026-11-03 15:50] Positional-params check: `src/py/vertebra/db/spanner/core.py` (the parameter JSON encoder used by `SyncSpannerDriver.select` / `AsyncSpannerDriver.select`) maps any non-string `Sequence` to `{"p1": ..., "pN": ...}` through `_encode_param_entry`, independent of dialect, so the list `[start, end, token, heartbeat]` binds Spangres `$1..$4`; `None` entries go through the same encoder as named `None` values.
