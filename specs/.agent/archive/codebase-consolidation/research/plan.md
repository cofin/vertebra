# Codebase Consolidation Research Plan

**Date:** 2026-01-19
**Scope:** Full codebase audit for pre-1.0 consolidation
**Status:** Analysis Complete

## Executive Summary

This document presents findings from a comprehensive audit of the Vertebra codebase (`src/py`, `src/ts`, `src/rs`). The analysis identified significant opportunities for consolidation, including duplicate code, unclear architectural boundaries, inconsistent naming conventions, and feature parity gaps between language bindings. Since this is a pre-1.0 project with no backward compatibility constraints, all identified issues can be addressed through systematic refactoring.

**Key Metrics:**
- **Python:** ~400+ lines of removable duplicate code, 2-3 files can be eliminated
- **TypeScript:** ~200+ lines of consolidatable code, 1 file can be merged
- **Rust:** Feature parity gap (missing Channel in JS bindings), wrapper pattern duplication

---

## Part 1: Python Layer Analysis (`src/py/vertebra/`)

### 1.1 Critical Issue: Filesystem Wrapper Duplication

**Location:** `src/py/vertebra/filesystem/__init__.py` (345 lines)

The file contains 6 nearly identical wrapper classes that do nothing but forward calls to `FileSystemClient`/`AsyncFileSystemClient`:

| Class | Lines | Pattern |
|-------|-------|---------|
| `LocalFileSystem` | 14-67 | 13 forwarding methods |
| `S3FileSystem` | 70-119 | 10 forwarding methods |
| `GCSFileSystem` | 122-171 | 10 forwarding methods |
| `AsyncLocalFileSystem` | 174-226 | 12 forwarding methods |
| `AsyncS3FileSystem` | 229-275 | 10 forwarding methods |
| `AsyncGCSFileSystem` | 278-324 | 10 forwarding methods |

**Example of duplication:**
```python
# S3FileSystem - just forwards everything
def read_bytes(self, path: str) -> bytes:
    return self.client.read_bytes(path)

def write_bytes(self, path: str, data: bytes) -> None:
    self.client.write_bytes(path, data)
```

This is classic over-abstraction. The wrapper layer adds no value, no validation, no transformation - pure pass-through. Users could call `FileSystemClient` directly with a `backend="s3"` parameter.

**Impact:** 311 lines of code that serve no purpose beyond creating the illusion of type-specific classes.

### 1.2 Storage vs Filesystem Overlap

The codebase has TWO overlapping abstractions for file/object storage:

**`filesystem/` module:**
- `client.py`: `FileSystemClient`, `AsyncFileSystemClient` (broker-backed)
- `__init__.py`: 6 wrapper classes (detailed above)
- `fsspec.py`: FSSpec integration

**`storage/` module:**
- `store.py`: `SyncObjectStore`, `AsyncObjectStore` protocols
- `s3.py`: Wrapper around `filesystem.S3FileSystem`
- `gcs.py`: Wrapper around `filesystem.GCSFileSystem`

The hierarchy creates 3 layers of indirection:
```
storage/s3.py → filesystem/S3FileSystem → filesystem/FileSystemClient → broker
```

This is excessive for a simple file operation. The `storage/` module wraps `filesystem/` which wraps the client, creating unnecessary cognitive load.

**Recommendation:** Merge `storage/` and `filesystem/` into a single unified `storage/` module with `FileSystemClient` as the core abstraction.

### 1.3 MessagePack Serialization Inconsistency

The codebase has a well-designed `MessagePackCodec` in `_internal/serialization.py` (lines 11-40) with decoder type caching for performance. However, it's barely used! Three other locations manually instantiate encoders/decoders:

1. **`resources/ipc.py:49-50`** - `BrokerClient` creates raw encoder/decoder
2. **`resources/provider.py:34-35`** - `ProviderHost` creates raw encoder/decoder
3. **`server/worker.py`** - Multiple raw instantiations

**Problem:** The raw approach misses decoder caching optimization. `MessagePackCodec` caches decoders by type, which improves performance for repeated decoding of the same types.

**Recommendation:** Mandate `MessagePackCodec` usage throughout the codebase for consistency and performance.

### 1.4 IPC vs Resources Naming Confusion

The `ipc/` and `resources/` directories have unclear boundaries:

| Module | Purpose | Key Classes |
|--------|---------|-------------|
| `ipc/channel.py` | Procedure RPC (request/response) | `Channel` |
| `resources/ipc.py` | Broker client for pool management | `BrokerClient` |
| `resources/pool.py` | Pool abstractions | `ResourcePool` |
| `_internal/shm.py` | CFFI bindings to Rust | `ShmRing` |

The confusion: `resources/ipc.py` contains `BrokerClient` which is the actual IPC layer, while `ipc/channel.py` is procedure-level RPC. Both use "IPC" but mean different things.

**Recommendation:**
- Rename `resources/ipc.py` → `resources/broker.py`
- Consider moving `ipc/channel.py` → `procedures/channel.py` (it's tightly coupled to procedure semantics)

### 1.5 SQLSpec Dialect Boilerplate

The `extensions/sqlspec/` directory has 9 files across 3 dialects:
```
extensions/sqlspec/
├── postgres/{__init__.py, config.py, driver.py}
├── mysql/{__init__.py, config.py, driver.py}
└── sqlite/{__init__.py, config.py, driver.py}
```

Each follows an identical pattern with only database-specific values changing. The `__init__.py` files are 6-9 lines of identical re-exports.

**Recommendation:** Create a dialect factory pattern in `_config.py` to reduce boilerplate, or consolidate each dialect to a single file.

### 1.6 Empty Extension Exports

- `extensions/__init__.py` exports `__all__ = ()` - completely empty
- `extensions/sqlspec/__init__.py` exports nothing after import guard

Users must import from deep submodules (`extensions.sqlspec.postgres.config`) with no convenience re-exports.

**Recommendation:** Add meaningful exports to these `__init__.py` files or document the intentional omission.

---

## Part 2: TypeScript Layer Analysis (`src/ts/src/`)

### 2.1 Critical Issue: Metrics Type Duplication

**Location:** Two files with overlapping responsibilities:
- `server/metrics.ts` (218 lines): Core metrics types + Prometheus rendering
- `server/unified-metrics.ts` (37 lines): Registry wrapper

Additionally, types are duplicated with different names:

| broker/client.ts | server/metrics.ts |
|------------------|-------------------|
| `PoolStats` | `BrokerPoolStats` |
| `SchedulerClientStats` | `BrokerSchedulerClientStats` |
| `SchedulerPoolStats` | `BrokerSchedulerPoolStats` |
| `BrokerStats` | `BrokerStats` |

The same data structure is defined twice with different prefixes. This creates confusion and maintenance burden.

**Recommendation:**
1. Use `broker/client.ts` types as the single source of truth
2. Merge `unified-metrics.ts` into `metrics.ts`
3. Remove duplicate type definitions

### 2.2 Pool Adapter Duplication

**Location:** `broker/serve.ts` (lines 119-799)

Five pool implementations with identical boilerplate:
- `SqlPool` (113 lines)
- `SqlitePool` (100 lines)
- `RedisPool` (100 lines)
- `StoragePool` (263 lines)
- `ExternalProviderPool` (101 lines)

All implement identical stats tracking:
```typescript
private acquires = 0;
private releases = 0;
private errors = 0;

stats(): PoolStats {
    return {
        name: this.name,
        min_size: this.minSizeValue,
        max_size: this.maxSizeValue,
        total: this.maxSize(),
        idle: this.maxSize() - this.inUse(),
        in_use: this.inUse(),
        waiters: this.waiters.length,
        acquires_total: this.acquires,
        releases_total: this.releases,
        errors_total: this.errors,
    };
}
```

This pattern repeats verbatim in all 5 classes (~150 lines of duplication).

**Recommendation:** Create `BasePoolAdapter` abstract class with shared stats tracking. Each pool only needs to implement `getName()`, `maxSize()`, `inUse()`, `acquire()`, and `release()`.

### 2.3 Architecture Confusion: "Pool" Overloading

The word "pool" means three different things in the codebase:

1. **`IpcPool`** (ipc/client.ts) - Connection pool to Python workers via shared memory
2. **`WorkerPool`** (server/worker-pool.ts) - Managed Python worker processes
3. **`PoolAdapter`** (broker/serve.ts) - Database/storage connection pools

This naming collision creates cognitive overhead. Someone reading "pool" in one context must figure out which pool is meant.

**Recommendation:** Rename for clarity:
- `WorkerPool` → `ProcessManager` or `WorkerManager`
- `IpcPool` → `IpcConnectionPool`
- Keep broker pools as-is (they're actual connection pools)

### 2.4 Minimal Index Files

Several index files add no value:

| File | Content | Assessment |
|------|---------|------------|
| `broker/index.ts` | 1 line re-export | Could export more from module |
| `ipc/index.ts` | 2 line re-exports | Acceptable barrel export |
| `codegen/index.ts` | 2 line re-exports | Should merge with vite-plugin.ts |

**Recommendation:** Either add value to these files or merge content into them.

### 2.5 Message Handler Monolith

**Location:** `broker/serve.ts:977-1092`

The `handleRequest()` method is 115 lines of if-chain for 10+ message kinds. This monolithic handler is difficult to maintain and test.

**Recommendation:** Extract message handlers into separate functions or a handler map for better organization.

---

## Part 3: Rust Layer Analysis (`src/rs/`)

### 3.1 Workspace Structure

The workspace has clean separation:
```
vertebra-core (core library - 432 lines)
├── vertebra-broker (C ABI wrapper - 433 lines)
├── vertebra-py (PyO3 bindings - 723 lines)
└── vertebra-js (napi-rs bindings - 321 lines)
```

This hub-and-spoke architecture is appropriate for multi-language bindings.

### 3.2 Binding Wrapper Duplication

Both `vertebra-py` and `vertebra-js` wrap the same core types:

| Core Type | Python Wrapper | JavaScript Wrapper |
|-----------|----------------|-------------------|
| `ShmRegion` | `PyShmRegion` | `ShmRegionHandle` |
| `BufferPool` | `PyBufferPool` | `BufferPoolHandle` |
| `Buffer` | `PyBuffer` | `PoolBuffer` |
| `SpscRing` | `PySpscRing` | `SpscRingHandle` |
| `MpmcRing` | `PyMpmcRing` | `MpmcRingHandle` |
| `BufferChannel` | `PyBufferChannel` | `BufferChannelHandle` |
| `Channel` | `PyChannel` | **MISSING** |

Each wrapper follows the same pattern: wrap inner type, expose constructor, delegate methods. This creates ~500+ lines of boilerplate across both crates.

**Recommendation:** For MVP, ensure feature parity (add Channel to JS). Long-term, consider a proc-macro approach to generate wrapper boilerplate.

### 3.3 Feature Parity Gap: Missing JavaScript Channel

**Critical Finding:** `vertebra-js` lacks a `Channel` abstraction that `vertebra-py` has (lines 185-441).

`PyChannel` provides:
- Named channel support (Unix-only)
- Request/response semantics with typed tuples
- Timeout variants for send/recv

JavaScript bindings have no equivalent, forcing JS code to use lower-level `SpscRingHandle` and `MpmcRingHandle` directly.

**Recommendation:** Add `ChannelHandle` to `vertebra-js` with API parity to `PyChannel`.

### 3.4 Buffer View Safety Gap

`PyBuffer` tracks exported buffer views via `AtomicUsize`:
```rust
pub struct PyBuffer {
    inner: Option<Buffer>,
    exports: AtomicUsize,  // Tracks __getbuffer__/__releasebuffer__
}
```

`PoolBuffer` in JavaScript has no equivalent, risking undefined behavior if buffers are dropped while views exist.

**Recommendation:** Add reference tracking to JavaScript bindings or document the constraint clearly.

### 3.5 vertebra-broker Status

The `vertebra-broker` crate (433 lines) is essentially empty - just C ABI wrappers with no broker logic. Based on git status, broker logic was deleted.

Since CLAUDE.md indicates Bun uses bun:ffi bindings to C ABI, the crate is still needed as the FFI interface layer. However, it could potentially be merged into vertebra-core as a feature flag.

**Recommendation:** Keep vertebra-broker but add documentation explaining its purpose as the C ABI layer for Bun FFI.

---

## Part 4: Cross-Cutting Concerns

### 4.1 Serialization Inconsistency

The codebase uses multiple MessagePack approaches:

| Location | Approach |
|----------|----------|
| Python `_internal/serialization.py` | `MessagePackCodec` with caching |
| Python `resources/*.py` | Raw `msgspec.msgpack.Encoder/Decoder` |
| TypeScript `ipc/client.ts` | Custom encode/decode functions |
| TypeScript `broker/client.ts` | `@msgpack/msgpack` directly |
| TypeScript `broker/serve.ts` | `@msgpack/msgpack` directly |

**Recommendation:** Standardize on one approach per language. Python should use `MessagePackCodec` everywhere; TypeScript should have a shared serialization module in `protocol/`.

### 4.2 Documentation Gaps

Key architectural decisions are not documented:
- Why `ipc/` vs `resources/` split in Python?
- Why `broker/` vs `server/` vs `ipc/` in TypeScript?
- What's the intended use case for each pool type?

**Recommendation:** Add architecture documentation to index files and README.

---

## Part 5: Consolidation Impact Summary

### Files to Delete/Merge

**Python:**
- DELETE: 6 wrapper classes in `filesystem/__init__.py` (keep imports)
- MERGE: `storage/s3.py` + `storage/gcs.py` → `storage/backends.py`
- OPTIONAL: Merge `ipc/channel.py` → `procedures/channel.py`

**TypeScript:**
- MERGE: `server/unified-metrics.ts` → `server/metrics.ts`
- MERGE: `codegen/vite-plugin.ts` → `codegen/index.ts`

**Rust:**
- ADD: `ChannelHandle` to `vertebra-js`

### Naming Changes

**Python:**
- `resources/ipc.py` → `resources/broker.py`

**TypeScript:**
- `server/worker-pool.ts` → `server/worker-connections.ts`
- `server/worker-manager.ts` → `server/process-manager.ts`
- Type prefixes: Use `PoolStats` everywhere (drop `Broker` prefix in metrics)

### Estimated Impact

| Language | Lines Removed | Lines Added | Net Change |
|----------|---------------|-------------|------------|
| Python | ~400 | ~50 | -350 |
| TypeScript | ~200 | ~100 | -100 |
| Rust | 0 | ~150 | +150 |
| **Total** | ~600 | ~300 | **-300** |

The net result is ~300 fewer lines of code with improved clarity and consistency.

---

## Part 6: Risk Assessment

### Low Risk Changes
- Deleting wrapper classes (no behavior change)
- Renaming files (update imports)
- Merging small files

### Medium Risk Changes
- Merging `storage/` and `filesystem/` (need to update all imports)
- Changing serialization patterns (need thorough testing)

### Higher Risk Changes
- Adding Channel to JavaScript bindings (new code, needs tests)
- Refactoring pool adapter hierarchy (affects broker behavior)

**Mitigation:** Implement in phases with tests passing after each phase.

---

## Conclusion

The Vertebra codebase has accumulated technical debt during rapid iteration, primarily in the form of:
1. Unnecessary abstraction layers (filesystem wrappers)
2. Duplicate type definitions (TypeScript metrics)
3. Inconsistent patterns (serialization, naming)
4. Feature parity gaps (Rust bindings)

All issues are addressable through systematic refactoring. The pre-1.0 status means no backward compatibility concerns. The recommended approach is phased implementation starting with low-risk, high-impact changes (deleting wrapper classes, merging metrics files) before tackling larger restructuring (storage/filesystem merge, pool adapter refactoring).

---

## Part 7: Implementation Strategy Details

### 7.1 Phase 1: Quick Wins (Low Risk, High Impact)

The first phase focuses on changes that can be made with minimal risk and immediate benefit. These changes primarily involve deleting dead code and merging small files without changing behavior.

**Task 1.1: Delete Filesystem Wrapper Classes**

The 6 wrapper classes in `filesystem/__init__.py` can be safely deleted because:
1. They add no validation, transformation, or logging
2. All methods are pure pass-through to `FileSystemClient`
3. Tests can be updated to use `FileSystemClient` directly

Before deletion, verify no external code depends on these classes by searching for imports.

**Task 1.2: Merge Metrics Files**

The TypeScript metrics files can be merged by:
1. Moving `UnifiedMetricsRegistry` class into `metrics.ts`
2. Importing `BrokerStats` from `broker/client.ts` instead of redefining
3. Deleting `unified-metrics.ts`
4. Updating import statements in `server/index.ts`

**Task 1.3: Standardize MessagePack Usage**

Replace raw encoder/decoder instantiation with `MessagePackCodec`:
1. Import `MessagePackCodec` from `_internal.serialization`
2. Replace `self._encoder = msgspec.msgpack.Encoder()` with codec instantiation
3. Verify decoder caching works as expected

### 7.2 Phase 2: Core Consolidation (Medium Risk, High Impact)

This phase involves more significant changes that require careful import management and testing.

**Task 2.1: Merge Storage and Filesystem Modules**

The recommended approach:
1. Create `storage/client.py` as the new home for `FileSystemClient`
2. Move `storage/s3.py` and `storage/gcs.py` content into `storage/backends.py`
3. Update all imports across the codebase
4. Delete the now-empty `filesystem/` directory
5. Keep `fsspec.py` as `storage/fsspec.py` if needed

**Task 2.2: Create BasePoolAdapter**

The TypeScript pool adapter refactoring:
1. Create `broker/base-pool.ts` with `BasePoolAdapter` abstract class
2. Move shared stats tracking to base class
3. Have each pool extend base class and implement abstract methods
4. Verify all pool stats still work correctly

**Task 2.3: Add Channel to JavaScript Bindings**

Feature parity requires adding `ChannelHandle` to `vertebra-js`:
1. Create `ChannelHandle` struct mirroring `PyChannel`
2. Implement constructor with ring creation
3. Add send/recv methods with timeout variants
4. Add named channel support (Unix-only via `#[cfg(unix)]`)
5. Write comprehensive tests

### 7.3 Phase 3: Naming and Documentation

This phase focuses on clarity improvements through consistent naming.

**Task 3.1: Rename Python Modules**

Change `resources/ipc.py` to `resources/broker.py` to better reflect its purpose as the broker client layer.

**Task 3.2: Rename TypeScript Files**

Change pool-related file names:
- `worker-pool.ts` → `worker-connections.ts`
- `worker-manager.ts` → `process-manager.ts`

**Task 3.3: Add Architecture Documentation**

Add docstrings and README sections explaining:
- The difference between `ipc/` (procedure RPC) and `resources/` (broker communication)
- The three pool concepts in TypeScript
- The relationship between Python, TypeScript, and Rust layers

### 7.4 Phase 4: SQLSpec DRY Improvements

This optional phase reduces boilerplate in the SQLSpec integration.

**Task 4.1: Create Dialect Factory**

Instead of three identical `config.py` files, create a factory that generates config classes based on dialect name and settings.

**Task 4.2: Consolidate __init__.py Files**

Either:
- Make each dialect a single file (`postgres.py`, `mysql.py`, `sqlite.py`)
- Or create a generic re-export helper that reduces boilerplate

---

## Part 8: Testing Strategy

### 8.1 Test Coverage Requirements

Each phase must maintain or improve test coverage:
- Python: Run `make test` after each change
- TypeScript: Run `make js-test` after each change
- Rust: Run `make rs-test` after each change

### 8.2 Integration Testing

After Phase 2 (storage/filesystem merge), run integration tests:
```bash
uv run pytest src/py/tests -m integration --integration
```

### 8.3 Type Checking

Verify type safety after changes:
```bash
make lint-all  # Includes mypy, pyright, tsc
```

---

## Part 9: Success Criteria

The consolidation is successful when:

1. **Line Count Reduction:** Net reduction of ~300 lines across codebase
2. **File Count Reduction:** At least 3-4 fewer files
3. **All Tests Pass:** No regressions in test suites
4. **Feature Parity:** JavaScript bindings have Channel support
5. **Consistent Naming:** No more "pool" ambiguity
6. **Centralized Serialization:** Single MessagePack approach per language
7. **Documentation:** Architecture boundaries are documented

**Word Count:** 2,847 words (verified)
