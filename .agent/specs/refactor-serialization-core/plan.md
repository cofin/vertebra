# Implementation Plan: Refactor Serialization

## Phase 1: Structure & Implementation

### Task Group 1.1: Create New Package
- [x] **Task 1.1.1**: Create `src/py/vertebra/serializers/` directory.
- [x] **Task 1.1.2**: Create `__init__.py` exporting public API.
- [x] **Task 1.1.3**: Implement `msgpack.py` (migrating from `_internal` + new features).
- [x] **Task 1.1.4**: Implement `json.py` (migrating from `_internal`).
- [x] **Task 1.1.5**: Implement `types.py` for shared type definitions.

### Task Group 1.2: Consolidate & Clean Up
- [x] **Task 1.2.1**: Update `src/py/vertebra/_internal/serialization.py` to re-export from new package (deprecation shim).
- [x] **Task 1.2.2**: Search and replace all internal imports to use `vertebra.serializers`.
- [x] **Task 1.2.3**: Remove `src/py/vertebra/_internal/serialization.py` once unused.
- [x] **Task 1.2.4**: Update `src/py/vertebra/procedures/serialization.py` to use `vertebra.serializers` and enhance `JSONCodec`.

### Task Group 1.3: Update Dependent PRDs
- [x] **Task 1.3.1**: Update `database-orm-integration` plan.md to reference `vertebra.serializers`.
- [x] **Task 1.3.2**: Update `database-orm-integration` spec.md to reference `vertebra.serializers`.

## Phase 2: Verification
- [x] **Task 2.1.1**: Run unit tests for serialization.
- [x] **Task 2.1.2**: Check for circular imports.
- [x] **Task 2.1.3**: Checkpoint - Verify Refactor Complete. [checkpoint: 1e8bbbd]
- [x] **Task 2.1.4**: Flow - User Manual Verification 'Phase 2: Verification' (Protocol in workflow.md).
