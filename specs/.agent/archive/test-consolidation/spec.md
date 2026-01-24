# PRD: Test Consolidation + Custom DB Fixture Completion

## 1. Intelligence Context

### Complexity Assessment
**Complexity**: Complex (multi-repo coordination, cross-cutting test migration, fixture plumbing, dependency cleanup, documentation updates).
**Checkpoint Target**: 10+ checkpoints.

### Similar Implementations Found
1. `src/py/tests/conftest.py` (Vertebra) - Current pytest configuration, integration flag, and xdist grouping logic.
2. `src/py/tests/database.py` (Vertebra) - Custom container-based database fixtures that are currently not wired into pytest.
3. `tools/lib/container.py` (Vertebra) - Docker/Podman runtime abstraction used by custom DB fixtures.
4. `/home/cody/code/litestar/litestar-vite/src/py/tests/` - Unit/integration/e2e structure + `fixtures/` and `test_app/` layout.
5. `/home/cody/code/litestar/sqlspec/tests/` - Unit/integration split with central fixture layer and plugin wiring.
6. `/home/cody/code/g/dma/accelerator/src/py/tests/database.py` - Fully specified container-based database fixtures, dynamic ports, dataset seeding.

### Patterns to Follow
- **Test taxonomy**: Unit vs Integration (and optional System/E2E) directories as in litestar-vite and sqlspec.
- **Fixture layering**: Dedicated fixtures modules + central conftest for shared configuration.
- **Function-based tests only**: Litestar-vite’s test-style PRD shows consistent, readable function-based tests.
- **Session-scoped DB services**: Accelerator’s custom fixture pattern reduces container churn and is better aligned with xdist grouping.

### Detected Stack and Commands
- **Stack**: Python 3.12, Bun/TypeScript (Bun tests), Rust (Cargo tests).
- **Primary test commands (Vertebra)**:
  - `make test` (Python tests)
  - `make test-all` (Rust → TS → Python)
  - `make js-test` (Bun tests)
  - `make rs-test` (Rust tests)
  - Integration tests: `uv run pytest src/py/tests -m integration --integration`
- **Accelerator test commands**:
  - `uv run pytest src/py/tests`
  - `make test` (per accelerator Makefile)

---

## 2. Problem Statement

### User Problem
Vertebra’s Python tests are currently **flat and sprawling**, which makes it difficult to identify which tests are unit-level (fast, isolated) versus integration-level (expensive, external dependencies, IPC). This leads to three concrete problems:

1. **Developer inefficiency**: Engineers cannot quickly run fast unit-only tests without remembering which files are safe to execute, which discourages short feedback loops.
2. **Fixture ambiguity**: The custom database fixture system exists in `src/py/tests/database.py` but is not wired into pytest. As a result, database fixtures are not consistently discoverable, and the integration/test selection logic in `conftest.py` is mostly unused.
3. **Inconsistent standards**: Litestar-vite and SQLSpec, which are closely related in ecosystem conventions, already use an explicit unit/integration split and clear fixture layering. Vertebra does not, creating friction for contributors used to those patterns.

The accelerator repository has the opposite problem: it already has a **unit/integration split** and a robust **custom database fixture system**, but it still depends on `pytest-databases`. This dependency is now redundant and creates confusion. The test infrastructure needs to be cleaned up so the custom fixtures are the sole supported path.

### Business Value
- **Faster iteration**: Developers can run unit tests quickly and avoid running full integration suites unless needed.
- **Reduced CI flakiness**: Clear integration boundaries and properly wired fixtures decrease timeouts and stale resource issues.
- **Consistency across ecosystem**: Aligning Vertebra with litestar-vite and SQLSpec reduces cognitive overhead and improves contributor onboarding.
- **Future scalability**: A structured test taxonomy and fixture layering makes it easier to expand tests as new driver integrations and IPC features are added.

### Success Criteria
- All Python tests are categorized and discoverable via markers and paths.
- Custom DB fixtures are wired and documented in both Vertebra and accelerator.
- pytest-databases is removed from accelerator and no longer required for test execution.
- Documentation reflects the new structure and is consistent across repos.

---

## 3. Acceptance Criteria

### Structure and Organization
- [ ] Vertebra Python tests are reorganized into explicit directories (at minimum `unit/` and `integration/`; optionally `system/` or `e2e/` if needed).
- [ ] Each test module is placed in the correct directory based on dependency level (pure in-memory vs external services / IPC / broker processes).
- [ ] Test directories include a `fixtures/` package for shared test data and a `helpers/` package for utility helpers.

### Marker and Selection Behavior
- [ ] All tests in the unit folder are explicitly marked `@pytest.mark.unit`.
- [ ] All tests in the integration folder are explicitly marked `@pytest.mark.integration`.
- [ ] The `--integration` flag in Vertebra correctly filters integration tests.
- [ ] xdist grouping markers for `postgres`, `mysql`, `redis` are preserved and applied to tests that use those fixtures.

### Database Fixture Wiring (Vertebra)
- [ ] `src/py/tests/database.py` fixtures are correctly registered with pytest (no stub plugin).
- [ ] Database fixtures can be imported and used without referencing a stub module.
- [ ] If pytest-databases is unused, it is removed from Vertebra dependencies and docs (optional if explicitly approved).

### Scope Guardrail (Accelerator)
- [ ] Accelerator remains a **reference only**. No code or documentation changes are made in `/home/cody/code/g/dma/accelerator`.
### Database Fixture Cleanup (Vertebra)
- [ ] `pytest-databases` is removed from Vertebra dependencies and lockfile.
- [ ] Vertebra test suite no longer references `pytest_databases` or stub plugins.

### Documentation & Alignment
- [ ] Vertebra docs/specs describe the new test taxonomy and how to run unit vs integration tests.
- [ ] No conflicts with the `specs/active/quality` PRD (explicit alignment or cross-linking).

---

## 4. Technical Approach

### 4.1 Target Test Taxonomy (Vertebra)

**Proposed Structure** (modeled after litestar-vite and sqlspec, simplified for Vertebra’s needs):

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
```

**Notes**:
- **Unit**: tests that only use in-memory constructs, local helpers, and do not spawn processes or use external services.
- **Integration**: tests that use IPC, shared memory, broker processes, Docker/Podman database services, or ASGI worker behavior.
- **System/E2E**: optional and not required for initial migration unless we identify clear multi-process workflows that need a separate boundary.

### 4.2 Test Migration Strategy

1. **Inventory and categorization**:
   - Map each existing `src/py/tests/test_*.py` file to a target directory based on dependency usage.
   - Example mapping:
     - `test_ipc.py`, `test_ipc_async.py`, `test_ipc_channel.py` → `integration/ipc/` (if shared memory resources are used), otherwise `unit/ipc/`.
     - `test_sqlspec_integration.py` → `integration/sqlspec/`.
     - `test_driver_types.py` → `unit/drivers/`.

2. **Relocation and import fixes**:
   - Move files into new directories.
   - Update imports that rely on relative paths or module locations.
   - Update any pytest discovery assumptions (e.g., test paths or explicit references in docs).

3. **Marker enforcement**:
   - Add `@pytest.mark.unit` or `@pytest.mark.integration` at module level or on individual tests.
   - Ensure the `--integration` flag behaves as intended.

4. **Fixture and helper consolidation**:
   - Place shared fixtures in `src/py/tests/fixtures/`.
   - Keep `src/py/tests/helpers/` for shared IPC and shared-memory helper logic (aligning with the existing `helpers/shm.py` pattern).

### 4.3 Database Fixture Wiring (Vertebra)

**Current state**: Fixtures implemented in `src/py/tests/database.py`, but pytest loads a stub `tests.database` module.

**Resolution options**:
1. **Move fixtures to the expected module**:
   - Move `src/py/tests/database.py` to `tests/database.py` and update imports accordingly.
2. **Update pytest_plugins**:
   - Change `pytest_plugins = ["tests.database"]` to reference the real module path (e.g., `pytest_plugins = ["src.py.tests.database"]` if importable).
3. **Shim module**:
   - Keep `tests/database.py` but make it import and re-export fixtures from `src/py/tests/database.py`.

**Preferred approach**: The shim module is least disruptive. It preserves the existing plugin path while ensuring the real fixtures are registered.

### 4.4 Accelerator Reference Only

**Goal**: Use accelerator as a reference pattern only. Do not modify accelerator code or documentation.

**Notes**:
1. Accelerator’s custom fixtures inform Vertebra’s approach, but changes stay within this repo.
2. Any differences between accelerator and Vertebra are documented here, not changed there.

### 4.5 Documentation Updates

**Vertebra**:
- Update `docs/arch.md` or a dedicated spec to document test taxonomy and fixture usage.
- Cross-link or reference `specs/active/quality` to avoid parallel standards.

**Accelerator**:
- No changes. Accelerator remains reference-only.

**Skills**:
- Update the `vertebra` skill to mention the test taxonomy and fixture conventions.
- If relevant, update the `testing` skill with project-specific fixture patterns (optional).

### 4.6 Database Changes
No schema or migration changes are required for this PRD. The focus is on test infrastructure and fixture wiring.

### 4.7 API Endpoints / Frontend Changes
No API endpoints or frontend changes are required. This is a test-organization and infrastructure effort only.

### 4.8 Detailed File Mapping (Initial Draft)

The following mapping is a first-pass categorization. The final map should be validated during implementation and adjusted where test dependencies differ from expectations.

**IPC and Shared Memory**
- `test_ipc.py` → `unit/ipc/` if purely in-memory channel usage, otherwise `integration/ipc/` if shared-memory endpoints are used.
- `test_ipc_async.py` → `integration/ipc/` (uses async IPC flows; likely integration).
- `test_ipc_channel.py` → `integration/ipc/` (shared-memory channel behavior).
- `test_ipc_integration.py` → `integration/ipc/` (already integration-style).

**Broker and Client**
- `test_broker_client.py` → `integration/broker/` (interacts with broker IPC behavior).
- `test_broker_pool.py` → `integration/broker/` (pool coordination; integration).
- `test_broker_multi_client.py` → `integration/broker/`.
- `test_broker_client.py` and `test_broker_multi_client.py` are also candidates for system tests if they require multi-process coordination.

**ASGI Worker / Server**
- `test_asgi_worker.py` → `integration/server/` (full-stack worker logic).
- `test_supervisor.py` → `integration/server/` (process supervision).

**Drivers**
- `test_driver_types.py` → `unit/drivers/` (pure encoding/decoding functions).
- `test_driver_exceptions.py` → `unit/drivers/`.
- `test_driver_serialization.py` → `unit/drivers/`.
- `test_postgres_driver_async.py` → `integration/drivers/` (requires real driver context).
- `test_postgres_transaction.py` → `integration/drivers/` (requires actual driver behavior).
- `test_sqlite_driver_async.py` → `integration/drivers/` (async driver behavior).

**SQLSpec Integration**
- `test_sqlspec_integration.py` → `integration/sqlspec/`.
- `test_provider_registration.py` → `unit/sqlspec/` or `integration/sqlspec/` depending on how the provider registry is used (if it only validates configuration and naming, keep unit).

**Type System**
- `test_types.py` → `unit/types/`.
- `test_type_bridge.py` → `unit/types/` (unless it depends on external I/O).

**Storage**
- `test_storage_store.py` → `unit/storage/` if in-memory storage; `integration/storage/` if it hits disk or real storage backends.

**Procedures**
- `test_procedures.py` → `unit/procedures/` (procedure registry and serialization validation).

This mapping is intentionally explicit to guide migration and avoid guesswork during implementation.

### 4.9 Fixture Layering and Conftest Strategy

**Principles**:
1. **Single root conftest**: All global fixtures should live in `src/py/tests/conftest.py` to avoid unpredictable fixture resolution.
2. **Dedicated fixture modules**: Domain-specific fixtures should live in `src/py/tests/fixtures/` and be imported by tests or re-exported via conftest.
3. **Helpers for shared IPC**: Helper functions for shared memory cleanup, unique name generation, and IPC orchestration should remain in `src/py/tests/helpers/`.
4. **No fixture duplication**: If a fixture is needed in multiple folders, define it once and import it rather than re-implementing.

**Fixture naming and scope**:
- Session-scoped for expensive resources (database containers, broker processes).
- Function-scoped for object-level setup (temporary directories, config overrides).

**Database fixture registration**:
- Use a single plugin registration path (no stub modules). The plugin must expose the `postgres_service`, `mysql_service`, and `redis_service` fixtures.
- Ensure that any fixtures that use Docker/Podman are marked `@pytest.mark.integration` at call sites or through module-level markers.

**Marker-to-fixture alignment**:
- Tests that use DB fixtures should be auto-marked with `postgres`, `mysql`, or `redis` markers via `pytest_collection_modifyitems`.
- This ensures xdist grouping remains consistent even after the new folder structure.

### 4.10 Integration Test Runtime Requirements

Integration tests should declare their infrastructure needs clearly in docs and fixtures:
- **Container runtime**: Docker or Podman is required. If not available, tests should skip gracefully.
- **Dynamic ports**: Fixtures should allocate dynamic ports and expose connection URLs (as already done in `src/py/tests/database.py`).
- **Environment variables**: Any test environment toggles should be set in `conftest.py` before imports, matching the current pattern.
- **Cleanup discipline**: The shared-memory cleanup logic should remain in the root conftest to avoid stale IPC segments.

### 4.11 Cross-Repo Harmonization Guidelines

Because Vertebra and accelerator live in the same ecosystem and share many contributors, the PRD should align conventions where possible:
- **Directory taxonomy**: Keep `unit/` and `integration/` directory names consistent across both repos.
- **Markers**: Use `unit`, `integration`, and DB markers (`postgres`, `mysql`, `redis`, `oracle`, `sqlserver`) consistently.
- **Fixture naming**: Use descriptive fixture names like `app_db`, `postgres_service`, `mysql_collector_db` to mirror accelerator’s schema.
- **Dataset seeding**: Mirror accelerator’s dataset naming and SQL file layout for any Vertebra datasets that are introduced later.

### 4.12 Documentation and Skills Update Detail

**Vertebra docs**:
- Add a short section in `docs/arch.md` describing the test taxonomy and how to run unit vs integration tests.
- Add a note to `specs/active/quality` that the reorganization is covered by this PRD, with references to the shared memory cleanup guidance already documented.

**Accelerator docs**:
- Update `specs/guides/testing.md` to replace references to `tools/local-infra.sh` with the custom container fixtures.
- Document dataset loader usage (schema-only vs data-only vs legacy SQL file) so integration tests are consistent.

**Skills**:
- Update the `vertebra` skill to include a short “testing conventions” subsection once the reorg is implemented.
- Optionally update the `testing` skill with a project-specific note about container fixtures and marker enforcement.

### 4.13 Non-Goals (Explicit)
- This PRD does **not** change runtime functionality in the IPC, broker, or driver layers.
- This PRD does **not** introduce new database features or SQL migrations.
- This PRD does **not** modify JS/TS or Rust tests beyond documentation alignment.

---

## 5. Testing Strategy

### Unit Tests
- **Goal**: 90%+ coverage for touched test modules.
- **Execution**: `make test` in Vertebra; `uv run pytest src/py/tests` in accelerator.
- **Focus**: Ensure unit tests do not require external services or Docker/Podman.

### Integration Tests
- **Goal**: All integration tests pass when the `--integration` flag is enabled.
- **Execution (Vertebra)**:
  - `uv run pytest src/py/tests -m integration --integration`
- **Execution (Accelerator)**:
  - `uv run pytest src/py/tests -m integration` (or full suite if integration-only selection is preferred)

### Full Suite
- **Vertebra**:
  - `make test-all` (Rust → TS → Python) after major moves.
- **Accelerator**:
  - `make test-all` (per accelerator Makefile).

### Additional Validation
- **pytest-xdist grouping**: ensure DB-specific tests still run in grouped workers.
- **No stale shared memory files**: verify with `specs/active/quality` guidance.
- **Cross-platform**: Ensure the custom DB fixtures behave correctly for Docker and Podman.

---

## 6. Implementation Notes

### 6.1 Migration Sequencing

1. **Preparation**
   - Inventory current Vertebra test files and categorize them.
   - Verify all integration-style tests are clearly identified.

2. **Restructure Tests**
   - Create `unit/` and `integration/` directories under `src/py/tests`.
   - Move test files into appropriate subdirectories.
   - Update import paths and ensure pytest discovery still works.

3. **Add Markers and Update Conftest**
   - Apply `@pytest.mark.unit` or `@pytest.mark.integration` at module level.
   - Ensure `pytest_collection_modifyitems` still works as expected.

4. **Fix Database Fixture Wiring**
   - Replace the stub `tests.database` plugin with a functional shim or real module.
   - Verify that `postgres_service`, `mysql_service`, `redis_service` fixtures are discoverable.

5. **Accelerator Cleanup**
   - Remove pytest-databases dependency and update lockfile.
   - Confirm custom fixtures and dataset loaders work for integration tests.
   - Update accelerator testing documentation.

6. **Documentation Alignment**
   - Update Vertebra docs/specs to reflect new test structure.
   - Cross-link to the `quality` PRD for shared-memory cleanup and test stability patterns.

### 6.2 Dependencies and Coordination

- **Coordination with `specs/active/quality`**: This PRD should not duplicate or overwrite the existing quality spec; instead, it should reference that spec for shared-memory and timeout guidance.
- **Cross-repo awareness**: Changes in accelerator should be referenced in Vertebra PRD as a parallel workstream; they may not share code but should share conventions.

### 6.3 Risk Management

**Risk**: Breaking pytest discovery.
- **Mitigation**: Keep `testpaths` in `pyproject.toml` as `src/py/tests` so pytest still finds tests. Verify with `pytest --collect-only`.

**Risk**: Misclassification of tests.
- **Mitigation**: Use a review checklist to confirm any test using Docker, IPC, or broker processes is integration.

**Risk**: Fixture conflicts due to multiple conftest scopes.
- **Mitigation**: Keep a single top-level conftest and avoid subdirectory conftests unless necessary.

**Risk**: Accelerator tests rely on pytest-databases implicitly.
- **Mitigation**: Search for any `pytest_databases` import; ensure tests use custom fixtures only.

### 6.4 Rollback Plan

- If reorganization causes failures, revert file moves and fixture wiring changes, preserving the previous flat test layout.
- Keep a small incremental approach: move a subset of tests, validate, then continue.
- Use git checkpoints after each migration stage.

### 6.5 Phased Delivery Plan

This work is safest when delivered in phases with verifiable checkpoints between each stage. A recommended phase breakdown:

**Phase A: Inventory and Classification**
- Produce a checklist of every existing test file in Vertebra and mark it as unit or integration.
- Record which tests use shared memory, broker IPC, or containerized services.
- Confirm whether any tests implicitly rely on pytest-databases or stubbed fixtures.

**Phase B: Structural Migration**
- Create the new folder hierarchy under `src/py/tests/` and move a small batch of files (e.g., driver tests and type tests) as the first migration set.
- Apply markers and update imports for the first batch.
- Run `pytest --collect-only` to confirm discovery and marker tagging.

**Phase C: Fixture Wiring**
- Replace the stub plugin with a real fixture module.
- Ensure `postgres_service`, `mysql_service`, and `redis_service` are available and auto-tagging works.
- Verify `pytest_collection_modifyitems` uses the new marker model as intended.

**Phase D: Accelerator Cleanup**
- Remove pytest-databases from dependency lists.
- Confirm the custom database fixture system still passes all integration tests.
- Update accelerator docs and scripts as needed.

**Phase E: Documentation and Quality Alignment**
- Update Vertebra docs/specs to describe the new test taxonomy.
- Add cross-links to the active `quality` PRD to avoid parallel standards.
- Ensure any knowledge is reflected in the `vertebra` skill file for future sessions.

### 6.6 CI and Tooling Implications

Test reorganization can affect CI in subtle ways even if testpaths remain the same. The PRD must explicitly cover the following:

- **Test selection commands**: Confirm that any CI or contributor scripts using explicit globs (e.g., `src/py/tests/test_*.py`) are updated to include subdirectories.
- **Coverage configuration**: The existing coverage configuration omits `tests/*`; ensure this remains valid after the move and that coverage runs still include relevant modules.
- **Parallel test runs**: xdist grouping is currently set by fixture usage. If integration tests are moved, verify that grouping still prevents database collisions.
- **Integration flags**: `--integration` should remain required for tests marked `integration`, and the CI pipeline should explicitly set or not set this flag depending on the stage.

### 6.7 Assumptions and Constraints

- Docker or Podman remains the required runtime for integration tests that use database fixtures. This PRD does not introduce alternative mock-based replacements.
- The container runtime abstraction (`tools/lib/container.py`) is the supported interface; no new container orchestrator is introduced.
- Test files remain function-based; no class-based tests should be added during migration.
- The PRD does not attempt to change external repo code beyond the explicit accelerator cleanup scope.

---

## 7. Acceptance Matrix (Checklist View)

| Area | Requirement | Status (Target) |
|------|-------------|----------------|
| Structure | Unit/integration split for Vertebra | ✅ Planned |
| Markers | Unit/integration markers applied | ✅ Planned |
| Fixtures | DB fixtures wired correctly | ✅ Planned |
| Dependency | pytest-databases removed from Vertebra | ✅ Planned |
| Docs | Vertebra docs/specs updated | ✅ Planned |
| Stability | Alignment with quality PRD | ✅ Planned |

---

## 8. Summary

This PRD defines a test consolidation and database fixture cleanup initiative that aligns Vertebra and accelerator with established patterns in the Litestar ecosystem (litestar-vite and SQLSpec). The goal is to reduce test sprawl, make unit vs integration boundaries explicit, and remove lingering dependencies on pytest-databases by promoting custom fixture systems that already exist. The outcome should be a cleaner, faster, and more maintainable testing workflow, with consistent documentation and predictable execution across both repositories.

Success is measured not only by passing tests but by contributors being able to identify the correct test tier at a glance and run it without extra tribal knowledge.

Accelerator remains a reference-only baseline for fixture patterns. All implementation changes live in Vertebra, keeping scope tight while preserving the fixture and taxonomy conventions proven elsewhere in the ecosystem.
