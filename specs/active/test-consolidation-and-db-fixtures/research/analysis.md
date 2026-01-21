# Analysis: Test Consolidation + Custom DB Fixtures

## Scope Summary
This PRD targets two related problems:
1) **Vertebra test sprawl**: Python tests in `src/py/tests` are flat and lack the unit/integration organization seen in other Litestar ecosystem repos (litestar-vite, sqlspec). This makes ownership, fixture layering, and test selection harder.
2) **Custom database fixtures vs pytest-databases**: The accelerator repo (`/home/cody/code/g/dma/accelerator`) still depends on `pytest-databases`, but there is a custom container-managed database fixture system already implemented. It is partially wired and needs completion so we can remove the pytest-databases dependency.

## Key Findings (Vertebra)
- **Flat test layout**: All tests live in `src/py/tests` with no unit/integration split. Files like `test_sqlspec_integration.py` and `test_ipc_integration.py` are integration-style but not tagged or separated.
- **Markers configured but unused**: `pyproject.toml` defines pytest markers (`unit`, `integration`, `postgres`, `mysql`, `redis`, etc.), yet most tests do not declare markers. The `conftest.py` integration skip logic is therefore largely ineffective.
- **Custom DB fixtures exist but are not wired**:
  - Real fixtures live in `src/py/tests/database.py` and use `tools/lib/container.py` + database helpers.
  - `conftest.py` declares `pytest_plugins = ["tests.database"]`, but `tests/database.py` is only a stub. This suggests a partially implemented plugin handoff.
- **Existing quality PRD references test cleanup**: `specs/active/quality/research/plan.md` already discusses test deadlocks, shared memory cleanup, and helper modules. This new PRD must align with that effort to avoid conflict.

## Key Findings (Litestar-Vite)
- **Structured test taxonomy**:
  - `src/py/tests/unit`, `src/py/tests/integration`, `src/py/tests/e2e`.
  - `src/py/tests/fixtures` and `src/py/tests/test_app` for concrete app assets.
- **Clear fixture isolation**: `conftest.py` resets Vite-related environment variables and uses typed fixtures for a test app path and Vite config.
- **Test style spec**: The archived `test-style-migration` PRD shows strict function-based test conventions and explicit grouping to keep files readable.

## Key Findings (SQLSpec)
- **Unit/integration split**: `tests/unit` and `tests/integration`, with a `tests/fixtures` package and a central `tests/conftest.py`.
- **pytest-databases usage**: SQLSpec uses `pytest_databases.docker.*` plugins in `tests/conftest.py` and integrates DB services at the plugin layer.
- **Fixture layering**: `tests/fixtures/sql_utils.py` centralizes SQL formatting helpers; session-scoped fixtures are used to reduce container churn.

## Key Findings (Accelerator)
- **Unit/integration split already exists**: `src/py/tests/unit` and `src/py/tests/integration` with clear domain groupings, plus `src/py/tests/fixtures`.
- **Custom container-managed DB fixtures**: `src/py/tests/database.py` implements a `SessionContainerManager`, dynamic port allocation, dataset loaders, and db-specific fixtures (`postgres_collector_db`, `mysql_collector_db`, etc.).
- **Partial wiring**: pytest still depends on `pytest-databases` in `pyproject.toml` even though custom fixtures exist. This indicates dependency cleanup and fixture wiring remain.
- **Integration conftest**: `src/py/tests/integration/conftest.py` relies on `app_db` and dynamic ports from the custom fixtures, so removing pytest-databases is feasible once the custom fixtures are the official path.

## Implications
- Vertebra needs a **test taxonomy** (unit/integration/system) to cut sprawl and align with litestar-vite/sqlspec conventions.
- The **custom DB fixtures** should be promoted to first-class, and any pytest-databases shims removed (both in Vertebra and Accelerator).
- Consolidation must avoid breaking the ongoing **quality PRD** work (shared memory cleanup and test deadlock mitigation).

## Proposed Direction
1. Define a unified test structure for Vertebra (unit/integration/system) modeled after litestar-vite/sqlspec.
2. Move/rename tests into appropriate buckets, and enforce marker usage.
3. Replace the `tests.database` stub in Vertebra with the real `src/py/tests/database.py` fixture module (or move it to a real plugin module and import it correctly).
4. In Accelerator, remove pytest-databases from dependencies, document the custom container fixture system, and ensure CI/dev workflows use it.
5. Update test docs and PRDs to reflect the new structure and shared fixture strategy.

