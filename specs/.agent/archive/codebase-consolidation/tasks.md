# Codebase Consolidation Tasks

**PRD Reference:** `specs/active/codebase-consolidation/prd.md`
**Status:** Ready for Implementation

---

## Phase 1: Quick Wins (Low Risk, High Impact)

### Task 1.1: Delete Python Filesystem Wrapper Classes

**Status:** [x] Complete

**Description:** Remove the 6 wrapper classes from `filesystem/__init__.py` that add no value beyond forwarding to `FileSystemClient`.

**Files:**
- `src/py/vertebra/filesystem/__init__.py` - Remove classes, keep only imports

**Steps:**
1. [x] Search for imports of wrapper classes: `grep -r "from vertebra.filesystem import" src/py/`
2. [x] Update any imports found to use `FileSystemClient` directly
3. [x] Delete the 6 wrapper class definitions (lines 14-324)
4. [x] Keep re-exports of `FileSystemClient` and `AsyncFileSystemClient`
5. [x] Run `make test` to verify no regressions
6. [x] Run `make mypy pyright` to verify type safety

**Acceptance Criteria:**
- [x] AC-PY-1 from PRD is satisfied
- [x] All tests pass
- [x] Type checking passes

---

### Task 1.2: Merge TypeScript Metrics Files

**Status:** [x] Complete

**Description:** Merge `server/unified-metrics.ts` into `server/metrics.ts` and remove duplicate type definitions.

**Files:**
- `src/ts/src/server/unified-metrics.ts` - DELETED
- `src/ts/src/server/metrics.ts` - Added `MetricsRegistry` class
- `src/ts/src/server/index.ts` - Updated imports

**Steps:**
1. [x] Copy `UnifiedMetricsRegistry` class from `unified-metrics.ts` to `metrics.ts`
2. [x] Update imports in `MetricsRegistry` to use local types
3. [x] Delete duplicate type definitions (`BrokerPoolStats`, etc.)
4. [x] Import `PoolStats`, `BrokerStats` from `../broker/client`
5. [x] Update `server/index.ts` to import from `./metrics` only
6. [x] Delete `unified-metrics.ts`
7. [x] Run `bun test` to verify no regressions
8. [x] Type checking passes for modified files

**Acceptance Criteria:**
- [x] AC-TS-1, AC-TS-2 from PRD are satisfied
- [x] All tests pass
- [x] Type checking passes for modified files

---

### Task 1.3: Standardize Python MessagePack Usage

**Status:** [x] Complete

**Description:** Replace raw encoder/decoder instantiation with `MessagePackCodec` from `_internal/serialization.py`.

**Files:**
- `src/py/vertebra/resources/ipc.py` - Uses MessagePackCodec
- `src/py/vertebra/resources/provider.py` - Uses MessagePackCodec
- `src/py/vertebra/server/worker.py` - Uses MessagePackCodec

**Steps:**
1. [x] Add `from .._internal.serialization import MessagePackCodec` to each file
2. [x] Replace `self._encoder = msgspec.msgpack.Encoder()` with codec instantiation
3. [x] Replace decode calls to use codec's type-cached decoder
4. [x] Remove direct `msgspec.msgpack` imports where no longer needed
5. [x] Run tests to verify no regressions
6. [x] Type safety verified for changes (pre-existing errors are unrelated)

**Acceptance Criteria:**
- [x] AC-PY-3 from PRD is satisfied
- [x] All MessagePack operations use `MessagePackCodec`
- [x] All tests pass

---

### Task 1.4: Unify TypeScript Type Definitions

**Status:** [x] Complete

**Description:** Remove `Broker` prefix from stats types in `metrics.ts` and use canonical types from `broker/client.ts`.

**Files:**
- `src/ts/src/server/metrics.ts` - Removed duplicates, imports from broker
- `src/ts/src/broker/client.ts` - Source of truth
- `src/ts/src/broker/index.ts` - Added type exports

**Steps:**
1. [x] Delete `BrokerPoolStats`, `BrokerSchedulerClientStats`, `BrokerSchedulerPoolStats` from metrics.ts
2. [x] Add `import { PoolStats, SchedulerClientStats, SchedulerPoolStats, BrokerStats } from '../broker'`
3. [x] Update `updateBrokerMetrics` function to use imported types
4. [x] Export scheduler types from broker/index.ts
5. [x] Run tests to verify

**Acceptance Criteria:**
- [x] Single source of truth for pool stats types
- [x] No duplicate type definitions
- [x] All tests pass

---

## Phase 2: Core Consolidation (Medium Risk, High Impact)

### Task 2.1: Merge Storage and Filesystem Modules

**Status:** [x] Complete

**Description:** Consolidate `filesystem/` and `storage/` into a unified `storage/` module.

**Files:**
- `src/py/vertebra/storage/client.py` - NEW (from filesystem/client.py)
- `src/py/vertebra/storage/fsspec.py` - NEW (from filesystem/fsspec.py)
- `src/py/vertebra/filesystem/__init__.py` - Converted to backwards-compat shim
- `src/py/vertebra/storage/s3.py` - Updated imports
- `src/py/vertebra/storage/gcs.py` - Updated imports
- `src/py/vertebra/storage/__init__.py` - Updated with all exports
- `src/py/vertebra/storage/store.py` - Updated imports

**Naming Changes:**
- `FileSystemClient` → `SyncFileSystemClient`
- `VertebraFileSystem` → `SyncVertebraFileSystem`

**Steps:**
1. [x] Create `storage/client.py` with content from `filesystem/client.py`
2. [x] Create `storage/fsspec.py` with content from `filesystem/fsspec.py`
3. [x] Update `storage/__init__.py` with all exports
4. [x] Update all internal imports to use `storage.client`
5. [x] Update test imports
6. [x] Convert `filesystem/__init__.py` to backwards-compat shim
7. [x] Run tests and mypy - all pass

**Acceptance Criteria:**
- [x] AC-PY-2 from PRD is satisfied
- [x] `filesystem/` contains only backwards-compat shim
- [x] All internal imports use `vertebra.storage`
- [x] All tests pass
- [x] mypy passes with minimal type: ignore comments

---

### Task 2.2: Create TypeScript BasePoolAdapter

**Status:** [x] Complete

**Description:** Create abstract base class for pool adapters to eliminate duplicate stats tracking.

**Files:**
- `src/ts/src/broker/base-pool.ts` - Created (112 lines)
- `src/ts/src/broker/serve.ts` - All pools refactored
- `src/ts/src/broker/index.ts` - Exports BasePoolAdapter

**Steps:**
1. [x] Create `broker/base-pool.ts` with `BasePoolAdapter` abstract class
2. [x] Implement shared stats tracking (acquires, releases, errors)
3. [x] Define abstract methods: `inUse()`, `acquire()`, `release()`, `close()`
4. [x] Add protected helpers: `recordAcquire()`, `recordRelease()`, `recordError()`
5. [x] Refactor `SqlPool` to extend `BasePoolAdapter`
6. [x] Refactor `SqlitePool` to extend `BasePoolAdapter`
7. [x] Refactor `RedisPool` to extend `BasePoolAdapter`
8. [x] Refactor `StoragePool` to extend `BasePoolAdapter`
9. [x] Refactor `ExternalProviderPool` to extend `BasePoolAdapter`
10. [x] Export from `broker/index.ts`
11. [x] Run `make js-test js-type-check`

**Acceptance Criteria:**
- [x] AC-TS-3, AC-TS-4 from PRD are satisfied
- [x] All 5 pools extend `BasePoolAdapter`
- [x] No duplicate stats tracking code
- [x] All tests pass

---

### Task 2.3: Rename Python resources/ipc.py

**Status:** [x] Complete

**Description:** Rename `resources/ipc.py` to `resources/broker.py` for clarity.

**Files:**
- `src/py/vertebra/resources/broker.py` - Renamed from ipc.py
- `src/py/vertebra/resources/__init__.py` - Updated imports
- All files importing from `resources.ipc` - Updated

**Steps:**
1. [x] Rename file: `mv src/py/vertebra/resources/ipc.py src/py/vertebra/resources/broker.py`
2. [x] Update `resources/__init__.py` to import from `.broker`
3. [x] Search and update all imports: `grep -r "from vertebra.resources.ipc" src/py/`
4. [x] Search and update relative imports: `grep -r "from .ipc" src/py/vertebra/resources/`
5. [x] Run `make test` and `make lint-all`

**Acceptance Criteria:**
- [x] AC-PY-4 from PRD is satisfied
- [x] File is renamed to `broker.py`
- [x] All imports updated
- [x] All tests pass

---

## Phase 3: Rust Feature Parity

### Task 3.1: Add ChannelHandle to vertebra-js

**Status:** [x] Complete

**Description:** Implement `ChannelHandle` in JavaScript bindings with API parity to `PyChannel`.

**Files:**
- `src/rs/vertebra-js/src/lib.rs` - Complete implementation (lines 271-545)

**Steps:**
1. [x] Create `ChannelHandle` struct with request/response SpscRings
2. [x] Implement `#[napi(constructor)] pub fn new(buffer_size, capacity)`
3. [x] Implement `send()` (renamed from send_request)
4. [x] Implement `try_send()` non-blocking variant
5. [x] Implement `recv()` blocking
6. [x] Implement `try_recv()` non-blocking
7. [x] Implement timeout variants (`send_timeout`, `recv_timeout`)
8. [x] Add `#[cfg(unix)]` named channel factories (`create_named`, `open_named`)
9. [x] Implement `ChannelRole` enum (Client/Server)
10. [x] Implement `createChannelPair()` factory function
11. [x] Add `BufferChannelHandle` for buffer-based channels
12. [x] Add `name()` getter and `unlink()` cleanup for named channels
13. [x] Write tests for all methods
14. [x] Run `make rs-test rs-lint`

**Acceptance Criteria:**
- [x] AC-RS-1, AC-RS-2, AC-RS-3 from PRD are satisfied
- [x] `ChannelHandle` exported from vertebra-js
- [x] API parity with `PyChannel` (and enhanced with additional features)
- [x] All tests pass

---

## Phase 4: Naming and Documentation

### Task 4.1: Rename TypeScript Worker Files

**Status:** [ ] Not Started

**Description:** Rename TypeScript files for clarity about their purpose.

**Files:**
- `src/ts/src/server/worker-pool.ts` - RENAME to `worker-connections.ts`
- `src/ts/src/server/worker-manager.ts` - RENAME to `process-manager.ts`
- `src/ts/src/server/index.ts` - UPDATE imports

**Steps:**
1. [ ] Rename `worker-pool.ts` to `worker-connections.ts`
2. [ ] Rename `worker-manager.ts` to `process-manager.ts`
3. [ ] Update imports in `server/index.ts`
4. [ ] Search and update all external imports
5. [ ] Run `make js-test js-type-check`

**Acceptance Criteria:**
- [ ] AC-TS-5, AC-TS-6 from PRD are satisfied
- [ ] Files renamed
- [ ] All imports updated
- [ ] All tests pass

---

### Task 4.2: Update CLAUDE.md

**Status:** [ ] Not Started

**Description:** Update project documentation to reflect new module structure.

**Files:**
- `CLAUDE.md` - UPDATE Directory Layout and architecture sections

**Steps:**
1. [ ] Update Directory Layout section with new structure
2. [ ] Document storage module (merged from filesystem + storage)
3. [ ] Document resources/broker.py purpose
4. [ ] Document TypeScript pool naming conventions
5. [ ] Add note about Rust binding feature parity

**Acceptance Criteria:**
- [ ] AC-DOC-1 from PRD is satisfied
- [ ] Documentation reflects actual structure
- [ ] Clear explanation of module purposes

---

### Task 4.3: Add Architecture Documentation

**Status:** [ ] Not Started

**Description:** Add docstrings explaining module boundaries and design decisions.

**Files:**
- `src/py/vertebra/storage/__init__.py` - ADD module docstring
- `src/py/vertebra/resources/__init__.py` - ADD module docstring
- `src/ts/src/broker/index.ts` - ADD JSDoc comments
- `src/ts/src/server/index.ts` - ADD JSDoc comments

**Steps:**
1. [ ] Add module docstring to `storage/__init__.py` explaining unified storage abstraction
2. [ ] Add module docstring to `resources/__init__.py` explaining broker communication
3. [ ] Add JSDoc to `broker/index.ts` explaining pool types
4. [ ] Add JSDoc to `server/index.ts` explaining server architecture
5. [ ] Document the three "pool" concepts in TypeScript

**Acceptance Criteria:**
- [ ] AC-DOC-2, AC-DOC-3 from PRD are satisfied
- [ ] Module purposes are documented
- [ ] Naming conventions are explained

---

## Verification Checklist

**Status as of 2026-01-19:**

- [x] `make test` passes (Python)
- [x] `make js-test` passes (TypeScript)
- [x] `make rs-test` passes (Rust)
- [x] `make lint-all` passes (all linting)
- [ ] `make coverage` shows no regression (verify)
- [x] Net line reduction achieved (filesystem wrappers, metrics merge)
- [x] At least 3 files eliminated or merged:
  - `unified-metrics.ts` deleted
  - Filesystem wrapper classes deleted
  - `resources/ipc.py` renamed to `broker.py`
- [x] No source files with old import paths remain (filesystem imports to storage)

**Remaining Work:**
- [ ] Rename `worker-pool.ts` to `worker-connections.ts` (Task 4.1)
- [ ] Rename `worker-manager.ts` to `process-manager.ts` (Task 4.1)
- [ ] Update CLAUDE.md documentation (Task 4.2)
- [ ] Add architecture docstrings (Task 4.3)

---

## Dependencies

```
Task 1.1 (filesystem wrappers) → Task 2.1 (storage merge)
Task 1.2 (metrics merge) → Task 2.2 (BasePoolAdapter)
Task 1.3 (msgpack) → None (independent)
Task 1.4 (type unification) → Task 1.2 (metrics merge)
Task 2.1 (storage merge) → Task 4.2 (docs)
Task 2.2 (BasePoolAdapter) → Task 4.3 (docs)
Task 2.3 (rename ipc) → Task 4.2 (docs)
Task 3.1 (Channel) → None (independent)
Task 4.1 (rename workers) → Task 4.2 (docs)
```

Execute Phase 1 tasks first (in any order), then Phase 2 tasks, then Phase 3 (independent), then Phase 4.
