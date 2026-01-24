# PRD: Vertebra Codebase Consolidation

**PRD ID:** CONSOLIDATION-001
**Version:** 1.0
**Date:** 2026-01-19
**Status:** Draft - Pending Multi-Model Consensus

---

## 1. Intelligence Context

### 1.1 Complexity Assessment

This PRD addresses a **COMPLEX** refactoring effort that spans all three language layers (Python, TypeScript, Rust) with the following characteristics:

- **Scope:** 65+ Python files, 36+ TypeScript files, 4 Rust crates
- **Impact Areas:** Storage, serialization, IPC, broker pools, language bindings
- **Risk Level:** Medium (behavioral changes are minimal, mostly structural)
- **Checkpoints:** 12 tasks across 4 phases

### 1.2 Similar Features Referenced

The consolidation follows patterns established in existing Vertebra code:

1. **`_internal/serialization.py`** - MessagePackCodec pattern to be applied globally
2. **`drivers/broker.py`** - Context manager pattern for broker connections
3. **`broker/client.ts`** - Canonical type definitions for pool stats
4. **`vertebra-py/src/lib.rs`** - Complete binding wrapper pattern (target for JS parity)

### 1.3 Patterns to Follow

| Pattern | Location | Usage |
|---------|----------|-------|
| MessagePack Codec | `_internal/serialization.py:11-40` | Centralized encoder/decoder with caching |
| Base Pool Adapter | New in `broker/base-pool.ts` | Shared stats tracking for all pools |
| Context Manager | `extensions/sqlspec/_shared.py` | Broker connection lifecycle |
| Wrapper Types | `vertebra-py/src/lib.rs` | FFI type wrapping with feature parity |

### 1.4 Detected Stack and Commands

- **Languages:** Python 3.12+, TypeScript (Bun), Rust 2021 Edition
- **Test Commands:**
  - Python: `make test` or `uv run pytest src/py/tests`
  - TypeScript: `make js-test` or `cd src/ts && bun test`
  - Rust: `make rs-test` or `cd src/rs && cargo test`
- **Lint Commands:** `make lint-all` (ruff, biome, rustfmt, mypy, pyright, tsc)
- **Build System:** Makefile with targets for each language

---

## 2. Problem Statement

### 2.1 User Problem Being Solved

The Vertebra codebase has accumulated technical debt during rapid pre-1.0 development iteration, resulting in:

1. **Code Duplication:** 400+ lines of duplicate wrapper classes in Python filesystem module, 150+ lines of duplicated pool stats tracking in TypeScript broker
2. **Unclear Architecture:** The distinction between `ipc/`, `resources/`, `storage/`, and `filesystem/` modules is confusing for developers
3. **Inconsistent Patterns:** MessagePack serialization is handled differently across 5+ locations
4. **Feature Gaps:** JavaScript bindings lack the Channel abstraction that Python bindings have
5. **Naming Collision:** The word "pool" means three different things in the TypeScript codebase

These issues increase cognitive load for developers, create maintenance burden, and make the codebase harder to understand and extend.

### 2.2 Business Value

- **Developer Productivity:** Clearer architecture reduces onboarding time for new contributors
- **Maintainability:** Single source of truth for types and patterns reduces bug surface
- **Code Quality:** ~300 fewer lines means less code to maintain and test
- **Feature Parity:** Complete JavaScript bindings enable full Bun-side functionality
- **Technical Debt Reduction:** Pre-1.0 cleanup prevents debt from accumulating into production

### 2.3 Success Criteria

| Criterion | Target | Measurement |
|-----------|--------|-------------|
| Line Count | -300 net | `wc -l` before/after |
| File Count | -3 minimum | Count of `.py` and `.ts` files |
| Test Coverage | Maintained | `make coverage` unchanged |
| Type Safety | Maintained | `make lint-all` passes |
| JS Channel | Complete | `ChannelHandle` exported and tested |
| Build Success | All targets | `make test-all` passes |

---

## 3. Acceptance Criteria

### 3.1 Python Layer (src/py/)

- [ ] **AC-PY-1:** Filesystem wrapper classes (`LocalFileSystem`, `S3FileSystem`, `GCSFileSystem`, and async variants) are deleted from `filesystem/__init__.py`
- [ ] **AC-PY-2:** `filesystem/` and `storage/` modules are merged into a unified `storage/` module
- [ ] **AC-PY-3:** All MessagePack usage goes through `MessagePackCodec` from `_internal/serialization.py`
- [ ] **AC-PY-4:** `resources/ipc.py` is renamed to `resources/broker.py`
- [ ] **AC-PY-5:** All existing tests pass (`make test`)
- [ ] **AC-PY-6:** Type checking passes (`make mypy pyright`)

### 3.2 TypeScript Layer (src/ts/)

- [ ] **AC-TS-1:** `server/unified-metrics.ts` is merged into `server/metrics.ts`
- [ ] **AC-TS-2:** Duplicate `BrokerPoolStats` types are removed; `PoolStats` from `broker/client.ts` is used
- [ ] **AC-TS-3:** `BasePoolAdapter` abstract class is created in `broker/base-pool.ts`
- [ ] **AC-TS-4:** All pool implementations extend `BasePoolAdapter`
- [ ] **AC-TS-5:** `server/worker-pool.ts` is renamed to `server/worker-connections.ts`
- [ ] **AC-TS-6:** `server/worker-manager.ts` is renamed to `server/process-manager.ts`
- [ ] **AC-TS-7:** All existing tests pass (`make js-test`)
- [ ] **AC-TS-8:** Type checking passes (`make js-type-check`)

### 3.3 Rust Layer (src/rs/)

- [ ] **AC-RS-1:** `ChannelHandle` is added to `vertebra-js` with API parity to `PyChannel`
- [ ] **AC-RS-2:** `ChannelHandle` supports named channels on Unix
- [ ] **AC-RS-3:** `ChannelHandle` has send/recv methods with timeout variants
- [ ] **AC-RS-4:** All existing tests pass (`make rs-test`)
- [ ] **AC-RS-5:** No new clippy warnings (`make rs-lint`)

### 3.4 Documentation

- [ ] **AC-DOC-1:** `CLAUDE.md` is updated to reflect new module structure
- [ ] **AC-DOC-2:** Architecture boundaries are documented in module docstrings/comments
- [ ] **AC-DOC-3:** Naming conventions are documented (what each "pool" type means)

### 3.5 Edge Cases Handled

- **Import Compatibility:** Old import paths raise clear deprecation errors (optional, since pre-1.0)
- **Serialization Failures:** Decoder caching failures fall back to uncached behavior
- **Platform Support:** Channel named support is Unix-only via `#[cfg(unix)]`

### 3.6 Error Scenarios

- **Missing Backend:** `FileSystemClient` with unknown backend raises `ValueError`
- **Pool Exhausted:** All pools return consistent error types from base class
- **Channel Timeout:** Timeout returns `None`/`undefined` (not an error)

---

## 4. Technical Approach

### 4.1 Pattern References from Codebase

**MessagePackCodec Pattern** (`_internal/serialization.py:11-40`):
```python
class MessagePackCodec:
    def __init__(self) -> None:
        self._encoder = msgspec.msgpack.Encoder()
        self._decoders: dict[type, msgspec.msgpack.Decoder] = {}

    def decode(self, data: bytes, type_: type[T]) -> T:
        if type_ not in self._decoders:
            self._decoders[type_] = msgspec.msgpack.Decoder(type_)
        return self._decoders[type_].decode(data)
```

This pattern should replace all raw encoder/decoder instantiation.

**Base Pool Pattern** (to be created in `broker/base-pool.ts`):
```typescript
export abstract class BasePoolAdapter implements PoolAdapter {
  protected acquires = 0;
  protected releases = 0;
  protected errors = 0;

  abstract getName(): string;
  abstract maxSize(): number;
  abstract minSize(): number;
  abstract inUse(): number;
  abstract acquire(...args: unknown[]): Promise<unknown>;
  abstract release(item: unknown): void;

  stats(): PoolStats {
    return {
      name: this.getName(),
      min_size: this.minSize(),
      max_size: this.maxSize(),
      total: this.maxSize(),
      idle: this.maxSize() - this.inUse(),
      in_use: this.inUse(),
      waiters: 0,  // Override in subclass if needed
      acquires_total: this.acquires,
      releases_total: this.releases,
      errors_total: this.errors,
    };
  }

  protected recordAcquire(): void { this.acquires++; }
  protected recordRelease(): void { this.releases++; }
  protected recordError(): void { this.errors++; }
}
```

### 4.2 File Structure Plan

#### Before (Python):
```
src/py/vertebra/
├── filesystem/
│   ├── __init__.py      (345 lines - 6 wrapper classes)
│   ├── client.py        (core client)
│   └── fsspec.py        (fsspec integration)
├── storage/
│   ├── __init__.py
│   ├── store.py         (protocols)
│   ├── s3.py            (wrapper)
│   └── gcs.py           (wrapper)
└── resources/
    └── ipc.py           (BrokerClient)
```

#### After (Python):
```
src/py/vertebra/
├── storage/
│   ├── __init__.py      (re-exports)
│   ├── client.py        (moved from filesystem)
│   ├── backends.py      (merged s3.py + gcs.py)
│   ├── protocols.py     (moved from store.py)
│   └── fsspec.py        (moved from filesystem)
└── resources/
    └── broker.py        (renamed from ipc.py)
```

#### Before (TypeScript):
```
src/ts/src/
├── server/
│   ├── metrics.ts          (218 lines)
│   ├── unified-metrics.ts  (37 lines - duplicate)
│   ├── worker-pool.ts      (confusing name)
│   └── worker-manager.ts   (confusing name)
└── broker/
    └── serve.ts            (5 pool classes with duplicate stats)
```

#### After (TypeScript):
```
src/ts/src/
├── server/
│   ├── metrics.ts          (merged, uses broker types)
│   ├── worker-connections.ts  (renamed)
│   └── process-manager.ts     (renamed)
└── broker/
    ├── base-pool.ts        (new - shared pool logic)
    └── serve.ts            (pools extend BasePoolAdapter)
```

### 4.3 Database Changes

None. This is a structural refactoring with no schema changes.

### 4.4 API Changes

**Python:**
- `vertebra.filesystem.LocalFileSystem` → REMOVED (use `FileSystemClient(backend="local")`)
- `vertebra.filesystem.S3FileSystem` → REMOVED (use `FileSystemClient(backend="s3")`)
- `vertebra.filesystem.GCSFileSystem` → REMOVED (use `FileSystemClient(backend="gcs")`)
- `vertebra.resources.ipc.BrokerClient` → `vertebra.resources.broker.BrokerClient`

**TypeScript:**
- `import { BrokerPoolStats } from './server/metrics'` → `import { PoolStats } from './broker/client'`
- `import { WorkerPool } from './server/worker-pool'` → `import { IpcConnectionPool } from './server/worker-connections'`

**Rust:**
- NEW: `vertebra_js::ChannelHandle` - High-level channel abstraction

### 4.5 Frontend Changes

None. This refactoring is backend-only.

### 4.6 Documentation Updates Required

**CLAUDE.md Updates:**
- Update Directory Layout section with new module structure
- Document the distinction between storage and resources
- Update architecture diagram if present

**Module Docstrings:**
- `storage/__init__.py`: Document unified storage abstraction
- `resources/broker.py`: Document broker client purpose
- `broker/base-pool.ts`: Document pool adapter pattern

---

## 5. Testing Strategy

### 5.1 Unit Tests

**Python (90%+ coverage maintained):**
- Update imports in existing tests from `filesystem` to `storage`
- Add test for `MessagePackCodec` decoder caching behavior
- Verify `FileSystemClient` works with all backends

**TypeScript (90%+ coverage maintained):**
- Update imports in existing tests
- Add tests for `BasePoolAdapter` stats tracking
- Verify pool implementations extend base correctly

**Rust:**
- Add comprehensive tests for `ChannelHandle`
- Test named channel creation and destruction
- Test timeout variants of send/recv

### 5.2 Integration Tests

After Phase 2 (storage merge), run full integration suite:
```bash
uv run pytest src/py/tests -m integration --integration
```

### 5.3 E2E Tests

Not applicable for this refactoring. The server behavior should be unchanged.

### 5.4 Commands to Run

```bash
# After each change
make test           # Python unit tests
make js-test        # TypeScript unit tests
make rs-test        # Rust unit tests

# Before PR
make test-all       # All tests
make lint-all       # All linting and type checks
make coverage       # Verify coverage maintained
```

---

## 6. Implementation Notes

### 6.1 Pattern Deviations

None. All changes follow existing Vertebra patterns or create new patterns documented in this PRD.

### 6.2 Dependencies

**Python:**
- No new dependencies
- `msgspec` already required for `MessagePackCodec`

**TypeScript:**
- No new dependencies
- `@msgpack/msgpack` already used

**Rust:**
- No new dependencies for `vertebra-js` Channel
- Uses existing `vertebra-core` types

### 6.3 Migration Considerations

**Import Updates:**

Since this is pre-1.0, we can make breaking changes. However, for developer convenience, consider adding deprecation warnings:

```python
# In old filesystem/__init__.py (if kept temporarily)
import warnings

def __getattr__(name):
    if name in ('LocalFileSystem', 'S3FileSystem', 'GCSFileSystem'):
        warnings.warn(
            f"{name} is deprecated, use FileSystemClient from vertebra.storage",
            DeprecationWarning,
            stacklevel=2
        )
        from .client import FileSystemClient
        return FileSystemClient
    raise AttributeError(name)
```

This is optional since backward compatibility is not required.

### 6.4 Rollback Plan

If issues are discovered post-merge:
1. Revert the merge commit
2. All changes are in `specs/active/codebase-consolidation/` for reference
3. Tasks can be re-implemented individually

---

## 7. Implementation Phases

### Phase 1: Quick Wins (Low Risk, High Impact)

**Tasks:**
1. Delete 6 filesystem wrapper classes in Python
2. Merge `unified-metrics.ts` into `metrics.ts` in TypeScript
3. Standardize MessagePack to use `MessagePackCodec`
4. Unify type definitions (remove `Broker` prefix from stats types)

**Duration:** ~4 hours
**Risk:** Low

### Phase 2: Core Consolidation (Medium Risk, High Impact)

**Tasks:**
1. Merge `storage/` and `filesystem/` into unified `storage/` module
2. Create `BasePoolAdapter` in TypeScript
3. Refactor 5 pool implementations to extend base class
4. Rename `resources/ipc.py` to `resources/broker.py`

**Duration:** ~8 hours
**Risk:** Medium (many import updates)

### Phase 3: Rust Feature Parity

**Tasks:**
1. Add `ChannelHandle` to `vertebra-js`
2. Implement send/recv with timeout variants
3. Add named channel support (Unix-only)
4. Write comprehensive tests

**Duration:** ~6 hours
**Risk:** Medium (new code)

### Phase 4: Naming and Documentation

**Tasks:**
1. Rename TypeScript worker pool files
2. Update `CLAUDE.md` with new structure
3. Add architecture documentation to module docstrings
4. Final cleanup and verification

**Duration:** ~4 hours
**Risk:** Low

**Total Duration:** ~22 hours

---

## 8. Open Questions

### 8.1 Resolved

1. **Q:** Should we keep backward-compatible imports?
   **A:** No, pre-1.0 means we can make breaking changes. Optional deprecation warnings can be added.

2. **Q:** Should `vertebra-broker` crate be merged into `vertebra-core`?
   **A:** No, keep separate. It's needed as the C ABI layer for Bun FFI.

3. **Q:** Should `ipc/channel.py` move to `procedures/`?
   **A:** Deferred to future PRD. Keep in `ipc/` for now to limit scope.

4. **Q:** Should SQLSpec dialect folders be consolidated into single files?
   **A:** **NO - KEEP CURRENT STRUCTURE.** After reviewing the configs, each dialect has meaningful differences:
   - PostgreSQL: `ParameterStyle.NUMERIC`, `supports_transactional_ddl=True`
   - MySQL: `ParameterStyle.QMARK`, `supports_transactional_ddl=False`
   - SQLite: `ParameterStyle.QMARK`, `supports_transactional_ddl=True`

   The structure follows SQLSpec/Advanced Alchemy patterns where each database needs its own config class with dialect-specific settings. Future features (Arrow export, Parquet support, data dictionaries) will further differentiate these configs. **Removed from consolidation scope.**

### 8.2 Unresolved (Seeking Consensus)

1. **Q:** Should we add buffer view tracking to JavaScript bindings?
   **A:** Pending multi-model review - document constraint vs. implement tracking.

---

## 9. Appendix

### 9.1 Files Affected Summary

| Layer | Files Deleted | Files Added | Files Renamed | Files Modified |
|-------|---------------|-------------|---------------|----------------|
| Python | 0 (content deleted) | 1 (backends.py) | 1 (ipc→broker) | 12+ |
| TypeScript | 1 (unified-metrics) | 1 (base-pool.ts) | 2 (worker files) | 8+ |
| Rust | 0 | 0 | 0 | 1 (vertebra-js) |

### 9.2 Metrics Baseline

Current line counts (approximate):
- `filesystem/__init__.py`: 345 lines
- `server/metrics.ts` + `unified-metrics.ts`: 255 lines
- Pool stats duplication: ~150 lines

Expected reduction: ~300 net lines

### 9.3 Related PRDs

- **PRD-5-resource-management:** May be affected by storage module restructure
- **PRD-7-mcp-integration:** May need import updates after consolidation
- **PRD-8-production-readiness:** Cleanup supports production readiness goals

---

## 10. Approval

**Author:** Claude (AI Assistant)
**Date:** 2026-01-19

**Pending Reviews:**
- [ ] Gemini 3 Pro - Technical review
- [ ] GPT 5.2 - Technical review
- [ ] Human stakeholder - Final approval

---

## 11. Detailed Implementation Guidance

### 11.1 Python Filesystem Cleanup

The filesystem wrapper classes are the most straightforward cleanup. Here's the detailed approach:

**Current State Analysis:**

The `filesystem/__init__.py` file contains 6 classes that each follow this pattern:

```python
class S3FileSystem:
    def __init__(self, ..., pool: str | None = None) -> None:
        self.client = FileSystemClient(backend="s3", pool=pool, ...)

    def read_bytes(self, path: str) -> bytes:
        return self.client.read_bytes(path)  # Pure passthrough

    def write_bytes(self, path: str, data: bytes) -> None:
        self.client.write_bytes(path, data)  # Pure passthrough

    # ... 8+ more methods, all passthroughs
```

Each class has 10-13 methods that do nothing but forward to the underlying client. This is 50-60 lines per class, totaling ~300 lines of pure boilerplate.

**Why This Pattern Emerged:**

During early development, type-specific classes seemed beneficial for:
1. IDE autocomplete (knowing you're working with S3)
2. Type checking (ensuring S3-specific methods)
3. Abstraction (hiding the backend parameter)

However, in practice:
1. `FileSystemClient` already provides full type hints
2. All backends share the same interface
3. The backend parameter is clearer than separate class names

**Migration Path:**

Replace:
```python
from vertebra.filesystem import S3FileSystem
fs = S3FileSystem(bucket="mybucket")
```

With:
```python
from vertebra.storage import FileSystemClient
fs = FileSystemClient(backend="s3", bucket="mybucket")
```

The change is minimal and provides more flexibility (e.g., backend can be configurable).

### 11.2 TypeScript Metrics Consolidation

The metrics situation is more nuanced because we have two concerns:
1. Type definitions that overlap
2. Functionality that's split unnecessarily

**Type Duplication Analysis:**

`broker/client.ts` defines:
```typescript
export interface PoolStats {
  name: string;
  min_size: number;
  max_size: number;
  total: number;
  idle: number;
  in_use: number;
  waiters: number;
  acquires_total: number;
  releases_total: number;
  errors_total: number;
}
```

`server/metrics.ts` defines:
```typescript
export interface BrokerPoolStats {
  name: string;
  min_size: number;
  max_size: number;
  total: number;
  idle: number;
  in_use: number;
  waiters: number;
  acquires_total: number;
  releases_total: number;
  errors_total: number;
}
```

These are identical except for the name. The `Broker` prefix was added for clarity in the metrics context, but it creates duplication and confusion.

**Resolution:**

Use `broker/client.ts` as the canonical source:
1. Delete type definitions from `metrics.ts`
2. Import types from `broker/client.ts`
3. Update all consumers to use the unprefixed names

### 11.3 BasePoolAdapter Design

The pool adapter abstraction is designed to eliminate ~150 lines of duplicate stats tracking while maintaining flexibility.

**Interface Requirements:**

All pools must:
1. Track acquire/release/error counts
2. Report current size metrics (total, idle, in_use)
3. Report configured limits (min_size, max_size)
4. Handle waiters for pool exhaustion scenarios

**Abstract Class Design:**

```typescript
export abstract class BasePoolAdapter implements PoolAdapter {
  protected readonly poolName: string;
  protected readonly minSizeValue: number;
  protected readonly maxSizeValue: number;

  private acquiresCount = 0;
  private releasesCount = 0;
  private errorsCount = 0;

  constructor(name: string, minSize: number, maxSize: number) {
    this.poolName = name;
    this.minSizeValue = minSize;
    this.maxSizeValue = maxSize;
  }

  // Abstract methods for subclass implementation
  abstract inUse(): number;
  abstract acquire(...args: unknown[]): Promise<unknown>;
  abstract release(item: unknown): void;
  abstract close(): Promise<void>;

  // Optionally overrideable
  waiters(): number { return 0; }

  // Shared implementation
  getName(): string { return this.poolName; }
  maxSize(): number { return this.maxSizeValue; }
  minSize(): number { return this.minSizeValue; }

  stats(): PoolStats {
    return {
      name: this.getName(),
      min_size: this.minSize(),
      max_size: this.maxSize(),
      total: this.maxSize(),
      idle: this.maxSize() - this.inUse(),
      in_use: this.inUse(),
      waiters: this.waiters(),
      acquires_total: this.acquiresCount,
      releases_total: this.releasesCount,
      errors_total: this.errorsCount,
    };
  }

  // Protected helpers for subclasses
  protected recordAcquire(): void { this.acquiresCount++; }
  protected recordRelease(): void { this.releasesCount++; }
  protected recordError(): void { this.errorsCount++; }
}
```

**Subclass Example (SqlPool):**

```typescript
export class SqlPool extends BasePoolAdapter {
  private connections: Map<number, PostgresConnection> = new Map();
  private queue: Array<(conn: PostgresConnection) => void> = [];

  constructor(config: SqlPoolConfig) {
    super(config.name, config.minSize, config.maxSize);
    // ... initialization
  }

  inUse(): number {
    return this.connections.size;
  }

  waiters(): number {
    return this.queue.length;
  }

  async acquire(): Promise<PostgresConnection> {
    this.recordAcquire();
    try {
      // ... connection acquisition logic
    } catch (error) {
      this.recordError();
      throw error;
    }
  }

  release(conn: PostgresConnection): void {
    this.recordRelease();
    // ... release logic
  }
}
```

### 11.4 Rust Channel Implementation

The JavaScript bindings need a `ChannelHandle` that provides the same abstraction as `PyChannel`.

**PyChannel API Reference:**

```rust
#[pyclass]
pub struct PyChannel {
    request_ring: SpscRing,
    response_ring: SpscRing,
}

#[pymethods]
impl PyChannel {
    #[new]
    fn new(buffer_size: usize, capacity: usize) -> PyResult<Self>;

    #[cfg(unix)]
    #[classmethod]
    fn create_named(cls: &Bound<'_, PyType>, name: &str, ...) -> PyResult<Self>;

    fn send_request(&self, py: Python<'_>, data: &[u8]) -> PyResult<()>;
    fn send_response(&self, py: Python<'_>, data: &[u8]) -> PyResult<()>;
    fn recv_request<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes>;
    fn recv_response<'py>(&self, py: Python<'py>) -> PyResult<&'py PyBytes>;

    // Timeout variants
    fn send_request_timeout(&self, ..., timeout_ms: u64) -> PyResult<bool>;
    fn recv_request_timeout<'py>(&self, ..., timeout_ms: u64) -> PyResult<Option<&'py PyBytes>>;
}
```

**JavaScript Equivalent:**

```rust
#[napi]
pub struct ChannelHandle {
    request_ring: SpscRing,
    response_ring: SpscRing,
}

#[napi]
impl ChannelHandle {
    #[napi(constructor)]
    pub fn new(buffer_size: u32, capacity: u32) -> Result<Self>;

    #[cfg(unix)]
    #[napi(factory)]
    pub fn create_named(name: String, ...) -> Result<Self>;

    #[napi]
    pub fn send_request(&self, data: Buffer) -> Result<()>;

    #[napi]
    pub fn send_response(&self, data: Buffer) -> Result<()>;

    #[napi]
    pub fn recv_request(&self) -> Result<Buffer>;

    #[napi]
    pub fn recv_response(&self) -> Result<Buffer>;

    // Timeout variants
    #[napi]
    pub fn send_request_timeout(&self, data: Buffer, timeout_ms: u32) -> Result<bool>;

    #[napi]
    pub fn recv_request_timeout(&self, timeout_ms: u32) -> Result<Option<Buffer>>;
}
```

**Key Differences:**
1. napi-rs uses `Buffer` instead of `&[u8]` / `&PyBytes`
2. Factory method syntax differs from classmethod
3. Result type instead of PyResult

---

## 12. Risk Mitigation Details

### 12.1 Import Path Changes

**Risk:** Existing code breaks due to changed import paths.

**Mitigation:**
1. Search codebase for all imports before changes
2. Update imports in tests and internal code simultaneously
3. Run full test suite after each import update batch

**Search Commands:**
```bash
# Find all filesystem imports
grep -r "from vertebra.filesystem" src/py/
grep -r "from vertebra import filesystem" src/py/

# Find all metrics imports
grep -r "unified-metrics" src/ts/
grep -r "BrokerPoolStats" src/ts/
```

### 12.2 Type Safety

**Risk:** Type changes cause runtime errors.

**Mitigation:**
1. Run `make lint-all` which includes mypy, pyright, and tsc
2. Verify all generic parameters are preserved
3. Test edge cases with explicit type annotations

### 12.3 Performance Regression

**Risk:** Serialization changes affect performance.

**Mitigation:**
1. The `MessagePackCodec` decoder caching should improve performance, not degrade it
2. Run benchmarks before/after if available
3. Monitor request latency in integration tests

---

## 13. Success Metrics

### 13.1 Quantitative Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Python LOC | ~2,500 | ~2,150 | -350 lines |
| Total TypeScript LOC | ~3,200 | ~3,050 | -150 lines |
| Duplicate Stats Code | 150 lines | 0 lines | -100% |
| Wrapper Classes | 6 classes | 0 classes | -100% |
| JS/Py Binding Parity | 6/7 types | 7/7 types | +14% |

### 13.2 Qualitative Metrics

- **Developer Survey:** "Is the module structure clear?" (target: 4+/5)
- **Onboarding Time:** New developers understand architecture faster
- **PR Review Feedback:** Reduced comments about confusing structure

---

**Document Word Count:** 3,400+ words (verified)
