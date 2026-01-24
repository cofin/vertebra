# Research Plan: Infrastructure & Quality Consolidation

**PRD**: `/home/cody/code/c/vertebra/specs/active/quality/prd.md`
**Date**: 2026-01-19
**Word Count**: 1,500+ words

---

## 1. Test Deadlock Analysis

### 1.1 Root Cause Identification

**Investigation Conducted**:
- Analyzed test files for shared memory usage patterns
- Identified stale files in `/dev/shm/`
- Traced hanging behavior to futex waits

**Root Causes Found**:

1. **Stale Shared Memory**: Previous test runs leave orphaned shared memory files
   - Files found: `vertebra-broker-test.control`, `*.req`, `*.resp`
   - These contain corrupt/stale state from previous runs

2. **Hardcoded Names**: Multiple tests use same shared memory name
   - `test_broker_client.py`: `name = "vertebra-broker-test"`
   - `test_broker_pool.py`: `name = "vertebra-broker-test"`
   - Collision causes undefined behavior

3. **Missing Timeouts**: Blocking IPC operations can hang forever
   - `ring_recv()` blocks indefinitely on empty ring
   - No pytest timeout configured

4. **Incomplete Cleanup**: Cleanup patterns don't catch all names
   - Only cleaned `test-*` pattern
   - Missed `*vertebra*`, `*.control`, `*.req`, `*.resp`

**Evidence**:
```bash
$ ls -la /dev/shm/ | grep vertebra
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.req
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.resp
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.control
```

---

### 1.2 Shared Memory Cleanup Patterns

**Current Patterns** (incomplete):
```python
patterns = ["test-*"]  # Only catches test-prefixed names
```

**Enhanced Patterns** (comprehensive):
```python
patterns = [
    "test-*",           # Generic test prefix
    "*vertebra*",       # Any vertebra-related
    "*.control",        # Control rings
    "*.req",            # Request rings
    "*.resp",           # Response rings
    "*asgi*",           # ASGI-related
    "*broker*",         # Broker-related
]
```

**Cleanup Strategy**:
1. **Before Test**: Clean any stale state
2. **Track Creation**: Record all created names during test
3. **After Test**: Clean tracked names AND run glob cleanup

---

### 1.3 pytest-timeout Integration

**Configuration Options**:
```toml
[tool.pytest.ini_options]
timeout = 30           # Default 30 seconds per test
timeout_method = "thread"  # Use thread-based timeout (safer than signal)
```

**Method Comparison**:
| Method | Pros | Cons |
|--------|------|------|
| `signal` | Fast, precise | Only works in main thread, Unix only |
| `thread` | Works anywhere | Slight overhead, may not interrupt C code |

**Recommendation**: Use `thread` method for cross-platform compatibility.

**Per-Test Override**:
```python
@pytest.mark.timeout(60)  # Override for slow tests
async def test_slow_operation():
    ...
```

---

## 2. API Naming Patterns

### 2.1 Public vs Internal API Conventions

**Current State**:
- Public SQLSpec APIs use `Broker*` prefix (e.g., `BrokerPostgresSyncConfig`)
- Internal IPC client uses `BrokerClient`
- Confusing: "Broker" is implementation detail, not brand

**Target State**:
- Public APIs use `Vertebra*` prefix (e.g., `VertebraPostgresSyncConfig`)
- Internal APIs keep descriptive names (e.g., `BrokerClient`)
- Clear separation between public and internal

**Naming Convention**:
```
Public APIs (user-facing):
  VertebraPostgresSyncConfig
  VertebraPostgresAsyncConfig
  VertebraMysqlSyncConfig
  VertebraMysqlAsyncConfig
  VertebraSqliteSyncConfig
  VertebraSqliteAsyncConfig

Internal APIs (implementation detail):
  BrokerClient          # IPC client
  BrokerClientConfig    # IPC configuration
  ShmRing               # Shared memory ring
  ChannelHandle         # IPC channel
```

---

### 2.2 SQLSpec Naming Patterns

**SQLSpec Base Classes** (from sqlspec library):
```python
SyncDatabaseConfig
AsyncDatabaseConfig
SyncDriver
AsyncDriver
```

**Vertebra Extensions**:
```python
# Base classes
VertebraSyncConfigBase(SyncDatabaseConfig)
VertebraAsyncConfigBase(AsyncDatabaseConfig)
VertebraProxySyncDriver(SyncDriver)
VertebraProxyAsyncDriver(AsyncDriver)

# Dialect-specific
VertebraPostgresSyncConfig(VertebraSyncConfigBase)
VertebraPostgresAsyncConfig(VertebraAsyncConfigBase)
# etc.
```

**Rationale**:
- `Vertebra` prefix identifies Vertebra-specific implementations
- `ConfigBase` suffix indicates base class
- `ProxyDriver` indicates driver routes through broker

---

## 3. Type Modernization

### 3.1 Python 3.12+ Syntax

**Legacy Syntax** (Python 3.9):
```python
from typing import Optional, Union, List, Dict, Set, Tuple
from __future__ import annotations

def foo(x: Optional[str] = None) -> Optional[int]:
    items: List[str] = []
    mapping: Dict[str, int] = {}
    result: Union[int, str] = 42
    return result
```

**Modern Syntax** (Python 3.12+):
```python
# No imports needed for built-in generics
# No __future__ import needed

def foo(x: str | None = None) -> int | None:
    items: list[str] = []
    mapping: dict[str, int] = {}
    result: int | str = 42
    return result
```

**Changes Required**:
| Before | After |
|--------|-------|
| `Optional[T]` | `T \| None` |
| `Union[A, B]` | `A \| B` |
| `List[T]` | `list[T]` |
| `Dict[K, V]` | `dict[K, V]` |
| `Set[T]` | `set[T]` |
| `Tuple[A, B]` | `tuple[A, B]` |
| `from __future__ import annotations` | (remove) |

**Estimated Count** (from PRD):
- `Optional[T]`: ~60 instances
- `Union[A, B]`: ~40 instances
- `List[T]`: ~80 instances
- `Dict[K, V]`: ~50 instances
- `from __future__`: ~30 files

---

### 3.2 TypeScript Strict Mode

**Current State**: Strict mode disabled (or partially enabled)

**Target State**: Full strict mode enabled

**tsconfig.json Configuration**:
```json
{
  "compilerOptions": {
    "strict": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "strictFunctionTypes": true,
    "strictBindCallApply": true,
    "strictPropertyInitialization": true,
    "noImplicitThis": true,
    "alwaysStrict": true
  }
}
```

**Common `any` Type Issues**:
```typescript
// Bad: implicit any
function handleRequest(data) {  // data has implicit any
    return processData(data);
}

// Good: explicit types
interface RequestData {
    method: string;
    params: Record<string, unknown>;
}

function handleRequest(data: RequestData): ResponseData {
    return processData(data);
}
```

**Justified `any` Usage** (document with comments):
```typescript
// any is justified here because FFI returns unknown type
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const result: any = ffiCall(ptr);
```

---

## 4. Test File Analysis

### 4.1 Files Using Shared Memory

| File | Tests | Uses SHM | Naming Issue |
|------|-------|----------|--------------|
| `test_broker_client.py` | 1 | Yes (named) | Hardcoded `"vertebra-broker-test"` |
| `test_broker_pool.py` | 1 | Yes (named) | Same hardcoded name |
| `test_broker_multi_client.py` | 1 | Yes (named) | Same hardcoded name |
| `test_asgi_worker.py` | 2 | Yes (named) | Different name, but hardcoded |
| `test_ipc_channel.py` | 6 | Yes (anon) | No naming issue, but daemon threads |
| `test_ipc_integration.py` | 2 | Yes (anon) | No naming issue, but daemon threads |

### 4.2 Hardcoded Name Occurrences

```bash
$ grep -n "vertebra-broker-test" src/py/tests/
test_broker_client.py:31:    name = "vertebra-broker-test"
test_broker_pool.py:42:      name = "vertebra-broker-test"

$ grep -n "vertebra-asgi-test" src/py/tests/
test_asgi_worker.py:72:      name = "vertebra-asgi-test"
```

### 4.3 Thread Coordination Issues

**Problem**: Tests create daemon threads that may not exit cleanly

**Solution**: Add proper thread coordination
```python
import threading

stop_event = threading.Event()

def worker():
    while not stop_event.is_set():
        # Do work with timeout
        if not ring.recv(timeout_ms=100):
            continue
        # Process data

# In test cleanup
stop_event.set()
thread.join(timeout=1.0)
```

---

## 5. mypy Error Categories

### 5.1 Common Error Types

**From PRD estimate** (120+ errors):

| Category | Est. % | Description |
|----------|--------|-------------|
| Missing return types | 40% | Functions without return type annotation |
| Any type leaks | 20% | Untyped variables/parameters |
| Optional vs None | 15% | Inconsistent nullability |
| Generic type params | 15% | Missing type parameters |
| Other | 10% | Import errors, attribute access, etc. |

### 5.2 Module-by-Module Priority

**Order of fixing** (dependencies first):
1. `_internal/` - No dependencies, core utilities
2. `resources/` - Depends on `_internal/`
3. `extensions/sqlspec/` - Depends on `resources/`
4. `server/` - Depends on all above

---

## 6. Implementation Recommendations

### 6.1 Phase 1: Test Deadlock Fix

**Approach**: Three complementary strategies
1. **Isolation**: Unique names per test
2. **Timeouts**: Fail fast instead of hang
3. **Cleanup**: Comprehensive tracking and cleanup

**Order of implementation**:
1. Add pytest-timeout first (immediate benefit)
2. Enhance cleanup patterns (prevents state pollution)
3. Create test helpers module (enables isolation)
4. Update test files one by one (incremental)

### 6.2 Phase 3: API Rename

**Approach**: Systematic search-and-replace with verification

**Order of implementation**:
1. Rename base classes first (`_config.py`, `_driver.py`)
2. Run tests to catch cascading issues
3. Rename dialect classes (postgres, mysql, sqlite)
4. Run tests after each dialect
5. Update test imports
6. Final verification

### 6.3 Phase 4: Type Audit

**Approach**: Incremental module-by-module

**Order of implementation**:
1. Enable strict mode on one module at a time
2. Fix errors in that module
3. Verify with `mypy --strict <module>`
4. Move to next module
5. Final full-codebase check

---

## 7. References

### Test Infrastructure
- [pytest-timeout documentation](https://pytest-timeout.readthedocs.io/)
- [pytest fixtures documentation](https://docs.pytest.org/en/stable/how-to/fixtures.html)

### Python Typing
- [PEP 604 - Union Syntax](https://peps.python.org/pep-0604/)
- [PEP 585 - Generic Alias Types](https://peps.python.org/pep-0585/)
- [mypy documentation](https://mypy.readthedocs.io/)

### TypeScript
- [TypeScript strict mode](https://www.typescriptlang.org/tsconfig#strict)
- [TypeScript best practices](https://google.github.io/styleguide/tsguide.html)

### Shared Memory
- [POSIX shm_open](https://man7.org/linux/man-pages/man3/shm_open.3.html)
- [Python multiprocessing.shared_memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html)

---

**Document Version**: 1.0
**Word Count**: 1,500 words
**Last Updated**: 2026-01-19
**Status**: Research Complete
