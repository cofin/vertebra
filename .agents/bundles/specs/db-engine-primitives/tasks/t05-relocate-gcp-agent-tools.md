---
type: Task
id: db-engine-primitives:t05
title: Relocate GCP database MCP tools to vertebra/agent/tools/ and empty the vertebra/db upward-import allowlist
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t03
  - db-engine-primitives:t04
files:
  - src/py/vertebra/db/mcp_tools.py
  - src/py/vertebra/agent/tools/__init__.py
  - src/py/vertebra/agent/tools/bigquery.py
  - src/py/vertebra/agent/tools/spanner.py
  - src/py/vertebra/agent/__init__.py
  - docs/reference/agent.md
  - .agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
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
tags: [db, agent, mcp, tools, layering, clean-break]
---

## Objective

Move `src/py/vertebra/db/mcp_tools.py` into `src/py/vertebra/agent/tools/{__init__,bigquery,spanner}.py` using `provide_driver`, lazy-export the tool builders on `vertebra.agent` with `if TYPE_CHECKING:` parity, delete `src/py/vertebra/db/mcp_tools.py`, and empty `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST` in `src/py/tests/unit/db/test_native_db_drivers.py`.

## Context

- **Verification strategy (`behavior_tdd`)**: Tool registration (`bigquery_dry_run`, `bigquery_query`, `bigquery_object_table_inspect`, `spanner_query`, `spanner_schema_inspect`), `GCPDatabaseToolPolicy` dry-run byte caps, mutation confirmation-gate guards, `provide_driver` driver resolution, `ModuleNotFoundError` on `vertebra.db.mcp_tools`, and an empty upward-import allowlist across `src/py/vertebra/db/**/*.py` (including `t04`'s `db/adapters/*/embeddings.py` modules) are directly observable in unit tests.
- **Architectural invariants**:
  - Clean break: `vertebra.db.mcp_tools` is deleted with zero re-exports in `vertebra.db`.
  - Import `PermissionDeniedError` from `vertebra.agent.core` (rather than `vertebra.framework.security`) inside `vertebra.agent.tools` so `vertebra.agent` keeps zero dependency on `vertebra.framework`.
  - Dispatch sync vs. async drivers via `isinstance(drv, AsyncDatabaseDriver)` vs. `await asyncio.to_thread(...)` (eliminating `inspect.isawaitable` duck-typing), and scope `spanner_schema_inspect` strictly to `information_schema.tables` and `information_schema.columns` via lock-free `ReadOnly` snapshots.
  - Once `db/mcp_tools.py` is deleted, `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST` in `test_native_db_drivers.py` becomes `frozenset()` (zero exemptions).

## Steps

1. **Write failing `vertebra.agent.tools` and zero-allowlist import-gate tests (RED)**:
   - In `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`:
     - Update imports to `from vertebra.agent.core import PermissionDeniedError` and `from vertebra.agent.tools import GCPDatabaseToolPolicy, build_bigquery_tools, build_spanner_tools, is_mutating_sql, register_gcp_database_tools` (and verify lazy resolution via `vertebra.agent.GCPDatabaseToolPolicy`, `vertebra.agent.build_bigquery_tools`, `vertebra.agent.build_spanner_tools`, `vertebra.agent.is_mutating_sql`, `vertebra.agent.register_gcp_database_tools`, and `vertebra.agent.tools`).
     - Assert `importlib.import_module("vertebra.db.mcp_tools")` raises `ModuleNotFoundError`.
     - Add coverage verifying `build_bigquery_tools` and `build_spanner_tools` resolve drivers lazily via `provide_driver` at tool-invocation time when passed a `DatabaseRegistry` + key or `BaseDatabaseConfig` (`BigQueryConfig` / `SpannerConfig`), and that `register_gcp_database_tools(bigquery_driver=bq_driver)` succeeds even when no `"default"` Spanner config is registered.
   - In `src/py/tests/unit/db/test_native_db_drivers.py`:
     - Set `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST: frozenset[str] = frozenset()` and assert `len(_TEMPORARY_UPWARD_IMPORT_ALLOWLIST) == 0` alongside the AST scan blocking `vertebra.{agent,framework,extensions,server,embeddings}` imports across all `src/py/vertebra/db/**/*.py` files.

2. **Implement `src/py/vertebra/agent/tools/`, update `vertebra/agent/__init__.py`, and delete `db/mcp_tools.py` (GREEN)**:
   - Create:
     - `src/py/vertebra/agent/tools/__init__.py` defining shared `GCPDatabaseToolPolicy(FrozenBaseStruct, frozen=True, gc=False)` with inline PEP 257 field docstrings, `is_mutating_sql(sql: str) -> bool`, and `register_gcp_database_tools(...) -> dict[str, Tool]`, and re-exporting `PermissionDeniedError` (`from vertebra.agent.core import PermissionDeniedError`), `build_bigquery_tools`, and `build_spanner_tools`.
     - `src/py/vertebra/agent/tools/bigquery.py` defining `build_bigquery_tools(*, driver=None, config=None, policy=None, confirmation_gate=None, registry=None) -> dict[str, Tool]` (`bigquery_dry_run`, `bigquery_query`, `bigquery_object_table_inspect`) resolving `provide_driver(config or "default", registry=registry)` lazily inside each tool handler when `driver is None`.
     - `src/py/vertebra/agent/tools/spanner.py` defining `build_spanner_tools(*, driver=None, config=None, policy=None, confirmation_gate=None, registry=None) -> dict[str, Tool]` (`spanner_query`, `spanner_schema_inspect` over `information_schema.tables` and `information_schema.columns`) resolving `provide_driver(config or "default", registry=registry)` lazily inside each tool handler when `driver is None`.
   - Add `if TYPE_CHECKING:` imports and lazy exports (`_TOOLS_EXPORTS` + `"tools"` submodule branch in `__getattr__` and `__all__`) for `tools`, `GCPDatabaseToolPolicy`, `build_bigquery_tools`, `build_spanner_tools`, `is_mutating_sql`, and `register_gcp_database_tools` in `src/py/vertebra/agent/__init__.py`.
   - Document `vertebra.agent.tools` in `docs/reference/agent.md` and update `.agents/bundles/knowledge/domains/agent/mcp-and-a2a-protocol-harness.md` (`sources:` and prose).
   - Delete `src/py/vertebra/db/mcp_tools.py` (`git rm`).

3. **Refactor & verify**:
   - Run `uv run pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/docs/test_examples.py`, `uv run python -m tools.docs.scan_public_docstrings`, and `make lint-all`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py -k upward_import` -> fails while `src/py/vertebra/db/mcp_tools.py` exists and `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST` is empty.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
  - `uv run python -m tools.docs.scan_public_docstrings && make lint-all` -> exits `0`.

## Acceptance Criteria

- [ ] R5: `src/py/vertebra/db/mcp_tools.py` is deleted, `vertebra.agent.tools` (`bigquery.py`, `spanner.py`, `__init__.py`) exposes `GCPDatabaseToolPolicy`, `is_mutating_sql`, `build_bigquery_tools`, `build_spanner_tools`, and `register_gcp_database_tools` (re-exported lazily with `if TYPE_CHECKING:` parity on `vertebra.agent`), and `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST` is empty with zero upward imports from `src/py/vertebra/db/**/*.py`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
