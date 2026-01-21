# Research Plan: Test Consolidation + Custom DB Fixtures

**PRD Target**: Vertebra test reorganization and accelerator DB fixture cleanup
**Date**: 2026-01-20
**Scope**: Vertebra repo (`/home/cody/code/c/vertebra`) + accelerator repo (`/home/cody/code/g/dma/accelerator`), informed by patterns from litestar-vite and sqlspec.

---

## 1. Background and Context

Vertebra’s Python test suite currently lives in a single flat directory (`src/py/tests`). This was workable early on, but as the project adds IPC, broker, ASGI worker, SQLSpec integration, and storage layers, the test suite now spans multiple subsystems. The flat structure hides differences in test cost and dependencies (unit vs integration), and it makes it harder to apply fixtures or markers consistently. We also have integration-only fixtures (Docker/Podman database services) and shared-memory cleanup logic that should be used by a specific subset of tests, but the current layout makes those boundaries ambiguous.

Two additional inputs shape this work:

1. **Litestar-vite** uses a clean test taxonomy under `src/py/tests` with `unit/`, `integration/`, and `e2e/` directories, with `fixtures/` and `test_app/` for sample assets. It also enforces function-based tests and documents the reasoning in an archived PRD. This repo provides a strong structural template for Python projects in the Litestar ecosystem.

2. **SQLSpec** uses a similar taxonomy under `tests/`, plus a fixtures package and a centralized `tests/conftest.py` that wires database services using `pytest_databases`. While SQLSpec’s exact dependencies might not be transferable, the organizational practices and marker usage provide solid reference points.

The second half of this effort is driven by the accelerator repo. Accelerator already has a unit/integration split and has a custom container-based database fixture system (`src/py/tests/database.py`) with dynamic port allocation, dataset seeding, and per-dialect fixtures. However, it still depends on `pytest-databases` in `pyproject.toml`, indicating that dependency removal and fixture wiring are incomplete. The goal is to finish the custom fixture path so the dependency can be removed cleanly and tests can run consistently without relying on the pytest-databases plugin.

This research plan consolidates evidence from the current codebase and external reference repositories, then identifies a safe consolidation strategy that aligns with existing PRDs (especially the active quality spec) and avoids breaking test stability.

---

## 2. Repository Observations

### 2.1 Vertebra (current repository)

**Current test layout**:
- `src/py/tests/` contains all test modules at the root (e.g., `test_ipc.py`, `test_asgi_worker.py`, `test_sqlspec_integration.py`, `test_postgres_driver_async.py`).
- `src/py/tests/helpers/` exists for shared utilities (currently only `helpers/shm.py`).
- There is a `src/py/tests/database.py` module implementing container-based database fixtures (Postgres, MySQL, Redis) via `tools/lib/container.py` and `tools/*/database.py`.

**Pytest configuration**:
- `pyproject.toml` defines markers for `unit`, `integration`, `postgres`, `mysql`, `redis`, and others, but most tests do not use them.
- `conftest.py` implements `--integration` filtering: it will skip tests marked with `@pytest.mark.integration` unless the flag is passed, but few tests actually carry this marker.
- `conftest.py` also attempts to group tests by database fixtures (`postgres_service`, `mysql_service`, `redis_service`) for xdist.

**Potential inconsistency**:
- `conftest.py` declares `pytest_plugins = ["tests.database"]`, which points to a top-level `tests/database.py` module. That top-level file is a stub (`"""Stub database plugin for pytest-databases."""`) and does not provide fixtures.
- The actual fixture implementation lives in `src/py/tests/database.py` and is not referenced by pytest via plugin registration.
- This indicates a partial or broken handoff: the custom DB fixture system exists but is not effectively used by pytest.

**Related active spec**:
- The `specs/active/quality` PRD already addresses test stability issues like shared memory cleanup, test hangs, and helper modules. It includes specific notes about stale shared memory, hardcoded names, and timeouts. This indicates that test structure and fixtures are already an active quality focus, and any new PRD must align with it to prevent conflicting guidance.

### 2.2 Litestar-Vite (external reference)

**Structure**:
- `src/py/tests/unit/` for isolated tests
- `src/py/tests/integration/` for tests that rely on real filesystem or application layering
- `src/py/tests/e2e/` for end-to-end flows
- `src/py/tests/fixtures/` for reusable fixtures
- `src/py/tests/test_app/` for embedded web app sample assets

**Fixture design**:
- `conftest.py` clears Vite-related environment variables before each test to prevent test order dependencies.
- A typed fixture stack provides the test app path and Vite config objects.

**Testing conventions**:
- The archived PRD `specs/archive/test-style-migration/prd.md` codifies function-based tests and explicitly disallows class-based tests.
- The same PRD shows a naming strategy to preserve grouping without classes, plus module-level grouping comments for readability.

**Implications for Vertebra**:
- Litestar-vite shows how to keep unit/integration/e2e boundaries explicit while still supporting fixture sharing.
- The presence of a `test_app` directory demonstrates a pattern for embedding integration fixtures that are more complex than simple dataset files.
- The project enforces function-based tests and reorganizes when conventions drift, making it a good model for clarity and discipline.

### 2.3 SQLSpec (external reference)

**Structure**:
- `tests/unit/` for unit tests
- `tests/integration/` for integration tests
- `tests/fixtures/` and helpers like `tests/fixtures/sql_utils.py`
- `tests/conftest.py` for global fixtures and pytest plugin registration

**Fixture wiring**:
- `tests/conftest.py` uses `pytest_plugins` to load database services from the `pytest_databases.docker.*` namespace.
- Test grouping is done by markers and by path, and there are dedicated integration adapter directories like `tests/integration/adapters/`.

**Key pattern**:
- SQLSpec embraces *explicit integration boundaries* and uses a dedicated fixtures layer to reduce duplication.
- The presence of multiple adapter subdirectories indicates that integration tests can still be partitioned by domain (adapter type, framework, etc.) without losing the integration boundary.

**Implications for Vertebra and Accelerator**:
- SQLSpec provides a concrete example of how to keep DB infrastructure in fixtures and provide session-scoped services to avoid unnecessary churn.
- While we are removing pytest-databases in accelerator, SQLSpec’s plugin usage shows how a central conftest can still manage DB lifecycles cleanly.

### 2.4 Accelerator (external reference on server)

**Structure**:
- `src/py/tests/unit/` and `src/py/tests/integration/` already exist with domain subdivisions.
- `src/py/tests/fixtures/` holds dataset definitions and test resources.
- `src/py/tests/conftest.py` configures markers and xdist groupings for database types.

**Custom DB fixture system**:
- `src/py/tests/database.py` defines a `SessionContainerManager` using `tools/lib/container.py` for Docker/Podman abstraction.
- The module contains:
  - App database fixture (`app_db`) using AlloyDB Omni container.
  - Collector database fixtures for Postgres, MySQL, Oracle, SQL Server using `params` and dynamic port allocation.
  - Dataset seeding helpers, with schema and data file resolution under `src/py/tests/fixtures/datasets`.
  - `pytest_sessionfinish` cleanup to remove containers and volumes.

**Partial wiring**:
- The repo’s `pyproject.toml` still lists `pytest-databases>=0.10.0`.
- `uv.lock` includes pytest-databases artifacts.
- There is no evidence that pytest-databases is actively used in the accelerator tests, implying this dependency is leftover and can be removed once the custom fixture system is formalized.

**Implications**:
- Accelerator already has the structure Vertebra needs; its fixture system can serve as the “working example” to finish in accelerator and adapt for Vertebra.
- Removing pytest-databases will require confirming that custom fixtures cover all current database usage, plus documenting any additional setup steps (runtime availability, dataset files, environment variables).

---

## 3. Comparative Patterns

### 3.1 Test Taxonomy Patterns

**Litestar-vite**:
- unit/integration/e2e split
- fixtures and test_app directories

**SQLSpec**:
- unit/integration split
- fixtures directory
- heavy use of markers and fixture layers

**Accelerator**:
- unit/integration split
- fixtures for data and datasets
- strong fixture stack for DB integration

**Vertebra**:
- flat tests with no explicit taxonomy
- helpers folder but no test-level organization

**Conclusion**:
The cross-repo patterns show that a unit/integration split is standard for Python projects in this ecosystem. Litestar-vite adds e2e to provide a third boundary for full-stack coverage, and SQLSpec divides its integration tests by adapter domains. Vertebra should adopt a similar taxonomy to make test cost and infrastructure requirements explicit.

### 3.2 Fixture Layering Patterns

**Litestar-vite**:
- `conftest.py` to clear environment variables and provide typed config fixtures.

**SQLSpec**:
- `tests/fixtures/` provides SQL formatting helpers and other reusable functions.
- `tests/conftest.py` wires database services (currently via pytest-databases).

**Accelerator**:
- `src/py/tests/database.py` handles container lifecycle and dataset seeding.
- `src/py/tests/integration/conftest.py` configures application environment using `app_db` and dynamic ports.

**Vertebra**:
- Has `src/py/tests/helpers/` but the custom database fixtures live in `src/py/tests/database.py` and are not wired correctly.

**Conclusion**:
Vertebra’s fixture layering should resemble accelerator’s approach: a dedicated database fixture module, a shared conftest to configure markers, and clear test-level usage of fixtures. The existing helper folder can be extended to house common IPC and shared-memory utilities. A dedicated `fixtures/` package can contain per-domain test data and shared setup helpers.

### 3.3 Test Style Conventions

Litestar-vite uses function-based tests and codifies the convention in a PRD. SQLSpec and accelerator also use function-based tests. Vertebra currently uses function-based tests but has no explicit spec. The consolidation should preserve function-based tests and adopt the simple conventions from the reference repos: descriptive function names, module-level grouping comments for readability, and fixtures for shared setup instead of classes.

---

## 4. Key Gaps and Risks

### 4.1 Vertebra Gaps

1. **No explicit unit/integration boundary**
   - Tests with real IPC / broker flows look like unit tests by file location but are integration-like in practice.
   - The current structure makes it harder to run quick unit-only passes.

2. **Marker configuration is unused**
   - The `--integration` flag and markers do not control test selection because tests are not marked.
   - The xdist grouping for database fixtures is effectively unused since fixtures are not in use.

3. **Database fixture plugin mismatch**
   - `pytest_plugins` points to a stub module, so the custom database fixtures are not being registered.

### 4.2 Accelerator Gaps

1. **pytest-databases still in dependencies**
   - The custom fixture system is in place, but the dependency remains.
   - This can create confusion and conflicting expectations for contributors.

2. **Documentation mismatch**
   - `specs/guides/testing.md` references `tools/local-infra.sh` rather than the new custom container approach. This may require updates if the custom fixtures are the new standard.

### 4.3 Cross-Repo Risks

- **Breaking test import paths**: Moving tests into subdirectories will require updating imports and pytest discovery paths.
- **CI assumptions**: If CI expects tests in a flat directory or uses specific globs, those need updating.
- **Fixture scope conflicts**: Introducing new fixtures in subdirectory conftests can accidentally shadow or conflict with shared fixtures.
- **Alignment with existing PRDs**: The active `quality` PRD in Vertebra already addresses test stability. This PRD should reference and align with that work, not replace it.

---

## 5. Recommended Consolidation Strategy (Research Outcome)

### 5.1 Target Test Structure for Vertebra

Adopt a unit/integration/system taxonomy mirroring litestar-vite and sqlspec:

```
src/py/tests/
  conftest.py
  fixtures/
  helpers/
  unit/
    ipc/
    broker/
    drivers/
    types/
    storage/
  integration/
    ipc/
    broker/
    sqlspec/
    server/
  system/ (optional, for multi-process end-to-end flows)
```

Key points:
- **Unit**: Pure in-memory tests with mocked dependencies.
- **Integration**: Real IPC, broker, and database fixture usage.
- **System**: End-to-end flows that orchestrate multiple processes (optional; could be a future phase).

### 5.2 Marker Enforcement

- Require `@pytest.mark.unit` for unit tests and `@pytest.mark.integration` for integration tests.
- Keep `postgres`, `mysql`, `redis` markers for DB-specific tests.
- Provide `pytest.ini` guidance in docs or the PRD to ensure `--integration` works as intended.

### 5.3 Fixture Rewiring

- Replace the `tests.database` stub in Vertebra with real plugin exposure. Options:
  1. Move `src/py/tests/database.py` to `tests/database.py` and import via `pytest_plugins`.
  2. Keep `src/py/tests/database.py` and update `pytest_plugins = ["src.py.tests.database"]` or `pytest_plugins = ["tests.database"]` with a real shim that imports from `src/py/tests/database.py`.
- Add a short `docs` or `specs` note to explain how database services are managed.

### 5.4 Accelerator Dependency Cleanup

- Remove `pytest-databases` from `pyproject.toml` and regenerate lockfile.
- Ensure pytest still discovers and uses `src/py/tests/database.py` for integration fixtures.
- Confirm any tests that previously relied on pytest-databases now use the custom fixtures (e.g., `postgres_collector_db`, `app_db`).
- Update `specs/guides/testing.md` or create a separate `docs/` note to clarify runtime requirements (Docker/Podman) and dataset expectations.

### 5.5 Documentation Updates

- Vertebra: update `docs/arch.md` or a relevant spec to document the new test structure.
- Accelerator: update `specs/guides/testing.md` to align with custom DB fixtures and remove pytest-databases references.
- Consider a shared “testing conventions” note for ecosystem consistency (optional, depending on scope).

---

## 6. Open Questions (to resolve before implementation)

1. **Should Vertebra adopt an `e2e` or `system` directory immediately?**
   - Litestar-vite has an explicit `e2e` directory; SQLSpec does not. Vertebra could start with `integration` only and reserve `system` for future.

2. **How should the `tests.database` stub be handled?**
   - Is it a deliberate placeholder for pytest-databases integration, or a mistake? The PRD should clarify the intended plugin shape.

3. **Should pytest-databases be removed from Vertebra as well?**
   - It is listed in `pyproject.toml` but not clearly used. If it is not used, removing it might be safe, but this should be explicitly approved in the PRD.

4. **How should we coordinate with the existing `quality` PRD?**
   - The test cleanup and naming work in `specs/active/quality` may overlap. The new PRD must reference those findings and either extend them or explicitly state boundaries.

---

## 7. Research Sources and Artifacts

### Vertebra Files Consulted
- `src/py/tests/conftest.py`
- `src/py/tests/database.py`
- `src/py/tests/helpers/shm.py`
- `pyproject.toml` (pytest configuration, dependencies)
- `specs/active/quality/index.md`
- `specs/active/quality/research/plan.md`
- `tools/lib/container.py`, `tools/postgres/database.py` (container runtime and DB helper patterns)

### Litestar-Vite Files Consulted
- `src/py/tests/conftest.py`
- `src/py/tests/` directory layout
- `specs/archive/test-style-migration/prd.md`

### SQLSpec Files Consulted
- `tests/` directory layout
- `tests/conftest.py`
- `tests/fixtures/sql_utils.py`

### Accelerator Files Consulted
- `src/py/tests/conftest.py`
- `src/py/tests/database.py`
- `src/py/tests/integration/conftest.py`
- `specs/guides/testing.md`

---

## 8. Preliminary Recommendation

Based on the observations above, the PRD should recommend a two-track implementation:

1. **Vertebra test reorganization**
   - Introduce a structured test taxonomy (unit/integration/system).
   - Move existing tests accordingly and enforce markers.
   - Fix the plugin wiring for database fixtures so integration tests can rely on the custom container framework.
   - Align with the `quality` PRD’s shared memory and test stability guidance.

2. **Accelerator DB fixture completion**
   - Remove pytest-databases dependency.
   - Finish wiring the custom fixtures as the official integration test path.
   - Update testing guides to reflect the new approach.

This dual path keeps the work scoped to test organization and database fixture stabilization, while borrowing proven structural patterns from litestar-vite and sqlspec.

---

## 9. Next Steps

- Finalize PRD with clear acceptance criteria for test reorg and pytest-databases removal.
- Define migration steps for test file moves, fixture updates, and dependency cleanup.
- Confirm any needed coordination with the `quality` PRD and update it only if necessary.
- Execute test and lint commands to validate the new structure once implementation begins.
