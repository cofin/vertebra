# Implementation Plan: Refactor Serialization

## Phase 1: Structure & Implementation

### Task Group 1.1: Create New Package
- [ ] **Task 1.1.1**: Create `src/py/vertebra/serializers/` directory.
- [ ] **Task 1.1.2**: Create `__init__.py` exporting public API.
- [ ] **Task 1.1.3**: Implement `msgpack.py` (migrating from `_internal` + new features).
- [ ] **Task 1.1.4**: Implement `json.py` (migrating from `_internal`).
- [ ] **Task 1.1.5**: Implement `types.py` for shared type definitions.

### Task Group 1.2: Consolidate & Clean Up
- [ ] **Task 1.2.1**: Update `src/py/vertebra/_internal/serialization.py` to re-export from new package (deprecation shim).
- [ ] **Task 1.2.2**: Search and replace all internal imports to use `vertebra.serializers`.
- [ ] **Task 1.2.3**: Remove `src/py/vertebra/_internal/serialization.py` once unused.

### Task Group 1.3: Update Dependent PRDs
- [ ] **Task 1.3.1**: Update `database-orm-integration` plan.md to reference `vertebra.serializers`.
- [ ] **Task 1.3.2**: Update `database-orm-integration` spec.md to reference `vertebra.serializers`.

## Phase 2: Verification
- [ ] **Task 2.1.1**: Run unit tests for serialization.
- [ ] **Task 2.1.2**: Check for circular imports.
- [ ] **Task 2.1.3**: Checkpoint - Verify Refactor Complete.
- [ ] **Task 2.1.4**: Flow - User Manual Verification 'Phase 2: Verification' (Protocol in workflow.md).
