# Implementation Plan: Refactor Serialization

## Status: COMPLETED

## Phase 1: Structure & Implementation

### Task Group 1.1: Create New Package
- [x] **Task 1.1.1**: Create `src/py/vertebra/serializers/` directory.
- [x] **Task 1.1.2**: Create `__init__.py` exporting public API.
- [x] **Task 1.1.3**: Merged `msgpack.py` and `json.py` into unified `codecs.py`.
- [x] **Task 1.1.4**: Moved type definitions to `vertebra.protocols`.

### Task Group 1.2: Consolidate & Clean Up
- [x] **Task 1.2.1**: Removed entire `vertebra/_internal/` directory.
- [x] **Task 1.2.2**: Created `vertebra.utils/` with `module_loader.py`, `dependencies.py`, `encoders.py`.
- [x] **Task 1.2.3**: Created `vertebra.protocols` with 21 consolidated protocols.
- [x] **Task 1.2.4**: Created `vertebra.typing` as public typing facade.
- [x] **Task 1.2.5**: Created `vertebra._typing` for internal native stubs.
- [x] **Task 1.2.6**: Created `vertebra.exceptions` with `MissingDependencyError`.
- [x] **Task 1.2.7**: Moved `async_transport.py` to `vertebra.ipc/`.
- [x] **Task 1.2.8**: Updated all imports across codebase.

### Task Group 1.3: Update Dependent PRDs
- [x] **Task 1.3.1**: Update `database-orm-integration` plan.md to reference `vertebra.serializers`.
- [x] **Task 1.3.2**: Update `database-orm-integration` spec.md to reference `vertebra.serializers`.

## Phase 2: Verification
- [x] **Task 2.1.1**: Run unit tests for serialization.
- [x] **Task 2.1.2**: Check for circular imports.
- [x] **Task 2.1.3**: Checkpoint - Verify Refactor Complete. [checkpoint: 1e8bbbd]
- [x] **Task 2.1.4**: Flow - User Manual Verification 'Phase 2: Verification' (Protocol in workflow.md).
