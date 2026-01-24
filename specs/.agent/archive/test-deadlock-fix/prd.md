# PRD: Python Test Suite Deadlock Resolution

**Status**: Ready for Implementation
**Priority**: P0 - Critical (Blocking Development)
**Complexity**: Medium
**Estimated Tasks**: 10

---

## 1. Intelligence Context

### 1.1 Complexity Assessment

This is a **medium complexity** issue requiring:
- Analysis of cross-language code (Python → CFFI → Rust)
- Understanding of lock-free data structures and futex synchronization
- Modifications to test infrastructure (fixtures, helpers, configuration)
- No changes to production code (test-only changes)

### 1.2 Similar Features Referenced

| File | Pattern Used |
|------|--------------|
| `conftest.py` | Session-scoped cleanup fixtures |
| `test_broker_client.py` | asyncio.to_thread for blocking IPC |
| `test_ipc_channel.py` | Daemon threads for server loops |
| Rust `sync.rs` | Futex-based wait/wake synchronization |

### 1.3 Detected Stack and Commands

- **Language**: Python 3.12+, Rust, TypeScript
- **Test Framework**: pytest with anyio, pytest-xdist, pytest-cov
- **Build System**: Make, uv, maturin
- **Commands**:
  - Test: `make test` or `uv run pytest src/py/tests`
  - Lint: `make lint`
  - Clean: `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`

---

## 2. Problem Statement

### 2.1 User Problem

The Python test suite hangs indefinitely when running `make test` or `uv run pytest src/py/tests`. Developers cannot run the full test suite to verify changes, and CI/CD pipelines fail with timeout.

### 2.2 Symptoms

- Full test suite hangs at ~2% progress with no output
- Individual test files pass when run in isolation
- Process requires SIGKILL (exit code 137) to terminate
- Stale shared memory files left in `/dev/shm/` after test runs

### 2.3 Root Causes Identified

1. **Stale Shared Memory**: Previous test runs leave shared memory objects with corrupt state
2. **Non-Unique Names**: Multiple tests use `vertebra-broker-test` causing collisions
3. **Missing Timeouts**: Blocking IPC operations can wait indefinitely on futex
4. **Incomplete Cleanup**: Cleanup fixtures don't catch all naming patterns

### 2.4 Business Value

- **Unblocks Development**: Developers can run tests before committing
- **Enables CI/CD**: Automated testing can complete reliably
- **Reduces Debugging Time**: No more manual investigation of hanging tests

### 2.5 Success Criteria

| Criterion | Target |
|-----------|--------|
| Test completion | < 60 seconds |
| Test pass rate | 100% |
| Stale shared memory | 0 files after completion |
| Consecutive runs | 5 runs without failure |
| Parallel execution | Works with `-n 4` |

---

## 3. Acceptance Criteria

### 3.1 Functional Requirements

- [ ] **AC-1**: `make test` completes within 60 seconds without hanging
- [ ] **AC-2**: All existing tests continue to pass (44+ tests)
- [ ] **AC-3**: `ls /dev/shm/ | grep -E "test-|vertebra"` returns empty after test run
- [ ] **AC-4**: Running tests 5 times consecutively does not cause hangs
- [ ] **AC-5**: Tests work with `pytest-xdist -n 4` parallelism

### 3.2 Non-Functional Requirements

- [ ] **AC-6**: No changes to production code (src/py/vertebra/** except tests)
- [ ] **AC-7**: Backward compatible - tests still work if run individually
- [ ] **AC-8**: Minimal test runtime overhead (< 5% increase)

### 3.3 Edge Cases

- [ ] **AC-9**: Test cleanup works even when tests fail mid-execution
- [ ] **AC-10**: Works when shared memory bindings are unavailable (graceful skip)
- [ ] **AC-11**: Works on systems without `/dev/shm` (macOS, Windows)

---

## 4. Technical Approach

### 4.1 Overview

The fix consists of three complementary strategies:
1. **Isolation**: Give each test unique shared memory names
2. **Timeouts**: Fail tests that exceed time limits instead of hanging
3. **Cleanup**: Comprehensively track and clean created shared memory

### 4.2 Solution Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Test Execution                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │  conftest   │    │   unique    │    │   pytest    │     │
│  │  fixtures   │    │    names    │    │   timeout   │     │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘     │
│         │                  │                  │              │
│         ▼                  ▼                  ▼              │
│  ┌─────────────────────────────────────────────────┐       │
│  │              Per-Test Shared Memory              │       │
│  │     vertebra-test-abc12345.control               │       │
│  │     vertebra-test-def67890.client.test.req       │       │
│  └─────────────────────────────────────────────────┘       │
│                          │                                   │
│                          ▼                                   │
│  ┌─────────────────────────────────────────────────┐       │
│  │              Cleanup on Exit                     │       │
│  │     - Track created names during test            │       │
│  │     - shm_unlink() all tracked names             │       │
│  │     - Fallback glob cleanup for orphans          │       │
│  └─────────────────────────────────────────────────┘       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 Pattern Reference: Existing Cleanup Pattern

From `conftest.py:55-76`:
```python
def _cleanup_shared_memory() -> None:
    """Clean up any stale shared memory objects from previous test runs."""
    shm_dir = Path("/dev/shm")
    if shm_dir.is_dir():
        patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp"]
        for pattern in patterns:
            for path in shm_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    path.unlink()
```

This pattern will be enhanced to:
1. Add more comprehensive patterns
2. Track specific names created by each test
3. Clean up after test completion (not just before)

### 4.4 File Structure Plan

```
src/py/tests/
├── conftest.py              # Enhance cleanup fixtures
├── helpers/
│   └── __init__.py          # NEW: Test helper module
│   └── shm.py               # NEW: Shared memory helpers
├── test_broker_client.py    # Update to use unique names
├── test_broker_pool.py      # Update to use unique names
├── test_asgi_worker.py      # Update to use unique names
├── test_ipc_channel.py      # Add thread coordination
├── test_ipc_integration.py  # Add thread coordination
└── test_ipc_async.py        # Add asyncio timeouts

pyproject.toml               # Add pytest-timeout config
```

### 4.5 Code Changes

#### 4.5.1 New Helper Module (`src/py/tests/helpers/shm.py`)

```python
"""Shared memory test helpers for isolation and cleanup."""
from __future__ import annotations

import contextlib
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator

def unique_shm_name(prefix: str = "vertebra-test") -> str:
    """Generate a unique shared memory name for test isolation.

    Args:
        prefix: Name prefix (default: "vertebra-test")

    Returns:
        Unique name like "vertebra-test-abc12345"
    """
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

@contextlib.contextmanager
def tracked_shm_cleanup() -> Generator[list[str], None, None]:
    """Context manager that tracks and cleans up shared memory names.

    Yields:
        List to append created shared memory names to

    Example:
        with tracked_shm_cleanup() as names:
            ring = ShmRing.create(config)
            names.append(config.name)
    """
    created: list[str] = []
    try:
        yield created
    finally:
        from vertebra._internal.shm import shm_unlink, SHM_AVAILABLE
        if not SHM_AVAILABLE:
            return
        for name in created:
            with contextlib.suppress(Exception):
                shm_unlink(name)
        # Also clean up any files in /dev/shm that match the names
        shm_dir = Path("/dev/shm")
        if shm_dir.is_dir():
            for name in created:
                clean_name = name.lstrip("/")
                for path in shm_dir.glob(f"{clean_name}*"):
                    with contextlib.suppress(OSError):
                        path.unlink()
```

#### 4.5.2 Enhanced Cleanup Fixture (`conftest.py`)

```python
@pytest.fixture(autouse=True)
def cleanup_shared_memory_per_test() -> Generator[None, None, None]:
    """Clean shared memory before AND after each test."""
    _cleanup_shared_memory()
    yield
    _cleanup_shared_memory()

def _cleanup_shared_memory() -> None:
    """Clean up any stale shared memory objects."""
    shm_dir = Path("/dev/shm")
    if shm_dir.is_dir():
        # Enhanced patterns to catch all naming conventions
        patterns = [
            "test-*",
            "*vertebra*",
            "*.control",
            "*.req",
            "*.resp",
            "*asgi*",
            "*broker*",
        ]
        for pattern in patterns:
            for path in shm_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    path.unlink()

    # Also try shm_unlink for known names
    try:
        from vertebra._internal.shm import shm_unlink, SHM_AVAILABLE
    except ImportError:
        return

    if not SHM_AVAILABLE:
        return

    # Common test shm names
    test_names = [
        "/test-ring",
        "/test-region",
        "/vertebra-broker.control",
        "/vertebra-broker-test.control",
        "/vertebra-asgi-test.asgi.0.control",
        "/vertebra-asgi-test.asgi.1.control",
    ]
    for name in test_names:
        with contextlib.suppress(Exception):
            shm_unlink(name)
```

#### 4.5.3 pytest-timeout Configuration (`pyproject.toml`)

```toml
[tool.pytest.ini_options]
timeout = 30
timeout_method = "thread"
```

#### 4.5.4 Example Test Update (`test_broker_client.py`)

```python
from tests.helpers.shm import unique_shm_name, tracked_shm_cleanup

@pytest.mark.anyio
async def test_broker_client_roundtrip() -> None:
    if not SHM_AVAILABLE:
        pytest.skip("Shared memory bindings unavailable")

    name = unique_shm_name("vertebra-broker-test")  # Unique name

    with tracked_shm_cleanup() as created_names:
        control = ShmRing.create(ShmRingConfig(
            name=f"/{name}.control",
            slot_size=4096,
            slots=64
        ))
        created_names.append(f"/{name}.control")

        # ... rest of test ...
```

---

## 5. Testing Strategy

### 5.1 Unit Tests

No new unit tests required - existing tests will be modified to be more robust.

### 5.2 Integration Tests

Verify test suite runs completely:

```bash
# Full suite test
make test

# Repeated runs test
for i in {1..5}; do
    rm -rf /dev/shm/test-* /dev/shm/*vertebra*
    make test || exit 1
done

# Parallel execution test
uv run pytest src/py/tests -n 4 --timeout=30
```

### 5.3 Verification Commands

```bash
# Verify no stale shared memory
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0

# Verify test completion time
time make test  # Should be < 60 seconds

# Verify specific test files work together
uv run pytest src/py/tests/test_broker_client.py \
              src/py/tests/test_broker_pool.py -v
```

---

## 6. Implementation Notes

### 6.1 Pattern Deviations

None - this follows existing pytest fixture patterns.

### 6.2 Dependencies

- `pytest-timeout`: Already in test dependencies, just needs configuration
- No new dependencies required

### 6.3 Migration Considerations

- Tests will work the same when run individually
- No changes to production code
- Backward compatible with existing CI configuration

### 6.4 Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| False timeouts | Low | Low | Set generous 30s timeout |
| Cleanup too aggressive | Low | Medium | Only clean known patterns |
| Platform differences | Medium | Low | Guard `/dev/shm` access |

---

## 7. Documentation Updates

### 7.1 Required Updates

- [ ] Update `CLAUDE.md` with test troubleshooting section
- [ ] Add comment documentation in `conftest.py`

### 7.2 Skills Updates

No skill updates required - this is test infrastructure.

---

## 8. Rollback Plan

If issues arise:
1. Remove pytest-timeout configuration from `pyproject.toml`
2. Revert conftest.py changes
3. Revert test file changes
4. Delete `tests/helpers/` directory

All changes are localized to test infrastructure and easily reversible.

---

## 9. Open Questions

1. **Q**: Should we use `pytest-xdist` worker IDs in shared memory names?
   **A**: Not required if each test uses `uuid.uuid4()` - names will be unique regardless of parallelism.

2. **Q**: Should we add a CI job specifically for deadlock detection?
   **A**: The pytest-timeout configuration provides this automatically.

3. **Q**: Should we add debug logging for shared memory operations?
   **A**: Out of scope for this fix - can be added separately if needed.

---

## 10. Approvals

- [ ] Engineering Lead
- [ ] QA Review
- [ ] Ready for Implementation

---

## Appendix A: Investigation Evidence

### A.1 Stale Shared Memory Found

```bash
$ ls -la /dev/shm/ | grep vertebra
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.req
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.resp
-rw------- 1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.control
```

### A.2 Hardcoded Names in Tests

```
test_broker_client.py:31: name = "vertebra-broker-test"
test_broker_pool.py:42:   name = "vertebra-broker-test"
test_asgi_worker.py:72:   name = "vertebra-asgi-test"
```

### A.3 Blocking Code Path

```rust
// channel.rs:262-273
pub fn recv(&self, out: &mut [u8]) -> Result<usize, ChannelError> {
    loop {
        match self.try_recv(out) {
            Ok(len) => return Ok(len),
            Err(ChannelError::Empty) => {
                let tail = self.write_head().load(Ordering::Acquire);
                sync::wait_on_address(self.write_head(), tail)?;  // BLOCKS FOREVER
            }
            Err(err) => return Err(err),
        }
    }
}
```

---

## Appendix B: Test Files Summary

| File | Tests | Uses SHM | Potential Issue |
|------|-------|----------|-----------------|
| test_ipc.py | 5 | Yes (anonymous) | Low risk - uses anonymous regions |
| test_ipc_async.py | 1 | Yes (anonymous) | Low risk - async channel |
| test_ipc_channel.py | 6 | Yes (anonymous) | Medium - daemon threads |
| test_ipc_integration.py | 2 | Yes (anonymous) | Medium - daemon threads |
| test_broker_client.py | 1 | Yes (named) | **High** - hardcoded name |
| test_broker_pool.py | 1 | Yes (named) | **High** - same hardcoded name |
| test_asgi_worker.py | 2 | Yes (named) | Medium - different name |

---

---

## Appendix C: Detailed Implementation Specifications

### C.1 unique_shm_name() Function Specification

**Purpose**: Generate unique shared memory names to prevent test collisions.

**Signature**:
```python
def unique_shm_name(prefix: str = "vertebra-test") -> str:
    """Generate a unique shared memory name for test isolation."""
```

**Behavior**:
1. Takes an optional prefix (default: "vertebra-test")
2. Appends 8 characters from UUID4 hex (36^8 = 2.8 trillion unique values)
3. Returns string suitable for use with shm_open()

**Rationale for UUID4**:
- UUID4 uses cryptographically secure random bytes
- No collision possible between parallel test workers
- No collision possible between sequential test runs
- 8 hex characters provide 4 billion unique values (sufficient for tests)

**Example Usage**:
```python
# Basic usage
name = unique_shm_name()
# Result: "vertebra-test-a1b2c3d4"

# Custom prefix
name = unique_shm_name("vertebra-broker")
# Result: "vertebra-broker-e5f6g7h8"

# In test
name = unique_shm_name("vertebra-broker-test")
control = ShmRing.create(ShmRingConfig(name=f"/{name}.control", ...))
```

### C.2 tracked_shm_cleanup() Context Manager Specification

**Purpose**: Ensure shared memory created during a test is always cleaned up.

**Signature**:
```python
@contextlib.contextmanager
def tracked_shm_cleanup() -> Generator[list[str], None, None]:
    """Context manager that tracks and cleans up shared memory names."""
```

**Behavior**:
1. Yields a list for the test to append created shared memory names
2. On exit (normal or exception), iterates through list and calls shm_unlink()
3. Also cleans up matching files in /dev/shm as a fallback
4. Silently ignores errors (shared memory may already be cleaned up)

**Example Usage**:
```python
@pytest.mark.anyio
async def test_broker_client_roundtrip():
    name = unique_shm_name("vertebra-broker-test")

    with tracked_shm_cleanup() as created:
        # Create control ring
        control_name = f"/{name}.control"
        control = ShmRing.create(ShmRingConfig(name=control_name, ...))
        created.append(control_name)

        # Create client rings
        req_name = f"/{name}.client.test.req"
        req_ring = ShmRing.create(ShmRingConfig(name=req_name, ...))
        created.append(req_name)

        resp_name = f"/{name}.client.test.resp"
        resp_ring = ShmRing.create(ShmRingConfig(name=resp_name, ...))
        created.append(resp_name)

        # ... test code ...

    # All three rings are automatically unlinked here
```

### C.3 pytest-timeout Configuration Specification

**Purpose**: Prevent tests from hanging indefinitely by enforcing time limits.

**Configuration**:
```toml
[tool.pytest.ini_options]
timeout = 30
timeout_method = "thread"
```

**Behavior**:
- `timeout = 30`: Tests fail after 30 seconds
- `timeout_method = "thread"`: Uses thread-based interruption (safer than signal)

**Rationale for Thread Method**:
- Signal-based timeout can cause issues with asyncio
- Thread-based timeout works with any Python code
- Slightly less precise but more reliable

**Override for Specific Tests**:
```python
@pytest.mark.timeout(60)  # Allow 60 seconds for this test
def test_slow_operation():
    ...
```

### C.4 Thread Coordination Pattern Specification

**Purpose**: Ensure daemon threads shutdown cleanly and don't leak resources.

**Pattern**:
```python
def test_with_server_thread():
    ready = threading.Event()
    shutdown = threading.Event()
    errors: list[Exception] = []

    def server_loop():
        ready.set()  # Signal that server is ready
        while not shutdown.is_set():
            try:
                # Use timeout to check shutdown flag periodically
                proc_id, params = server.receive_sync(timeout_ms=100)
                result = process_request(proc_id, params)
                server.respond_sync(proc_id, result)
            except TimeoutError:
                continue  # Check shutdown flag
            except Exception as exc:
                errors.append(exc)
                break

    thread = threading.Thread(target=server_loop)
    thread.start()

    # Wait for server to be ready before sending
    assert ready.wait(timeout=5), "Server did not start within 5 seconds"

    # Perform test operations
    result = client.send_sync("test", {"value": 42})
    assert result["ok"] == 42

    # Clean shutdown
    shutdown.set()
    thread.join(timeout=2)
    assert not thread.is_alive(), "Server thread did not shutdown"
    assert not errors, f"Server had errors: {errors}"
```

**Key Points**:
1. `ready` event signals server is accepting connections
2. `shutdown` event signals server should exit
3. Server loop uses timeout to check shutdown flag
4. Main thread waits for server ready before sending
5. Main thread signals shutdown and waits for join
6. Assertions verify clean shutdown and no errors

### C.5 asyncio Timeout Pattern Specification

**Purpose**: Ensure async operations fail with timeout instead of hanging.

**Pattern**:
```python
async def safe_recv(ring: ShmRing, timeout_ms: int = 1000) -> tuple[bytes, int]:
    """Receive from ring with both Rust and Python timeouts.

    Args:
        ring: The shared memory ring to receive from
        timeout_ms: Timeout in milliseconds for the Rust recv call

    Returns:
        Tuple of (data, flags) from the received message

    Raises:
        TimeoutError: If receive doesn't complete within timeout
    """
    try:
        # The Rust timeout handles the futex wait
        # The asyncio timeout handles the thread pool
        return await asyncio.wait_for(
            asyncio.to_thread(ring.recv, timeout_ms),
            timeout=timeout_ms / 1000 + 1.0  # Add 1s buffer
        )
    except asyncio.TimeoutError:
        raise TimeoutError(f"recv timed out after {timeout_ms}ms")
```

**Usage**:
```python
@pytest.mark.anyio
async def test_broker_roundtrip():
    # Instead of:
    # data, flags = await asyncio.to_thread(ring.recv, 1000)

    # Use:
    data, flags = await safe_recv(ring, timeout_ms=1000)
```

---

## Appendix D: Verification Test Suite

### D.1 Verification Script

Create `tools/verify_test_fix.py`:

```python
#!/usr/bin/env python3
"""Verification script for test deadlock fix."""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


def run_command(cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
    """Run a command with timeout and return (returncode, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"Command timed out after {timeout}s"


def clean_shared_memory() -> None:
    """Remove all test shared memory objects."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return

    patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp"]
    for pattern in patterns:
        for path in shm_dir.glob(pattern):
            try:
                path.unlink()
            except OSError:
                pass


def count_shared_memory() -> int:
    """Count test-related shared memory objects."""
    shm_dir = Path("/dev/shm")
    if not shm_dir.is_dir():
        return 0

    count = 0
    for path in shm_dir.iterdir():
        if any(x in path.name for x in ["test", "vertebra"]):
            count += 1
    return count


def main() -> int:
    """Run verification tests."""
    print("=" * 60)
    print("Test Deadlock Fix Verification")
    print("=" * 60)

    # Test 1: Clean slate
    print("\n[1/5] Cleaning shared memory...")
    clean_shared_memory()
    count = count_shared_memory()
    if count > 0:
        print(f"  WARN: {count} shared memory objects remain")
    else:
        print("  OK: Shared memory cleaned")

    # Test 2: Single run
    print("\n[2/5] Running test suite once...")
    start = time.time()
    returncode, stdout, stderr = run_command(
        ["uv", "run", "pytest", "src/py/tests", "-v", "--timeout=30"],
        timeout=120,
    )
    elapsed = time.time() - start
    if returncode != 0:
        print(f"  FAIL: Tests failed (exit {returncode})")
        print(f"  stderr: {stderr[:500]}")
        return 1
    print(f"  OK: Tests passed in {elapsed:.1f}s")

    # Test 3: No stale shared memory
    print("\n[3/5] Checking for stale shared memory...")
    count = count_shared_memory()
    if count > 0:
        print(f"  FAIL: {count} shared memory objects remain")
        return 1
    print("  OK: No stale shared memory")

    # Test 4: Repeated runs
    print("\n[4/5] Running tests 3 times...")
    for i in range(3):
        clean_shared_memory()
        returncode, _, _ = run_command(
            ["uv", "run", "pytest", "src/py/tests", "-v", "--timeout=30"],
            timeout=120,
        )
        if returncode != 0:
            print(f"  FAIL: Run {i+1} failed")
            return 1
        print(f"  OK: Run {i+1} passed")

    # Test 5: IPC tests together
    print("\n[5/5] Running IPC tests together...")
    clean_shared_memory()
    returncode, _, stderr = run_command(
        [
            "uv", "run", "pytest",
            "src/py/tests/test_broker_client.py",
            "src/py/tests/test_broker_pool.py",
            "-v", "--timeout=30",
        ],
        timeout=60,
    )
    if returncode != 0:
        print(f"  FAIL: IPC tests failed together")
        print(f"  stderr: {stderr[:500]}")
        return 1
    print("  OK: IPC tests pass together")

    print("\n" + "=" * 60)
    print("All verification tests passed!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

### D.2 CI Integration

Add to `.github/workflows/test.yml`:

```yaml
- name: Run tests with timeout
  run: |
    # Clean any stale shared memory
    rm -rf /dev/shm/test-* /dev/shm/*vertebra* || true

    # Run with explicit timeout
    uv run pytest src/py/tests -v --timeout=30

    # Verify cleanup
    if ls /dev/shm/ | grep -qE "test-|vertebra"; then
      echo "ERROR: Stale shared memory found after tests"
      ls /dev/shm/ | grep -E "test-|vertebra"
      exit 1
    fi
```

---

## Appendix E: Troubleshooting Guide

### E.1 Tests Still Hanging

**Symptom**: Tests hang even after applying the fix.

**Diagnosis**:
```bash
# Check for stale shared memory
ls -la /dev/shm/ | grep -E "test-|vertebra"

# Check for hung Python processes
ps aux | grep pytest

# Check for orphan threads
cat /proc/$(pgrep -f pytest)/task/*/status 2>/dev/null | grep -A1 "Name:"
```

**Solution**:
1. Kill hung processes: `pkill -9 pytest`
2. Clean shared memory: `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`
3. Verify cleanup patterns match all created names

### E.2 Timeout Errors

**Symptom**: Tests fail with timeout instead of passing.

**Diagnosis**:
```bash
# Run with verbose output to see where it hangs
uv run pytest src/py/tests/test_broker_client.py -v -s --timeout=60
```

**Solution**:
1. Check if 30 second timeout is too short for slow systems
2. Increase timeout in pyproject.toml if needed
3. Check for actual bugs (not just timing issues)

### E.3 Shared Memory Permission Errors

**Symptom**: `shm_open: Permission denied` errors.

**Diagnosis**:
```bash
# Check /dev/shm permissions
ls -la /dev/shm/ | head

# Check if another user owns the files
ls -la /dev/shm/ | grep vertebra
```

**Solution**:
1. Use unique names to avoid conflicts with other users
2. Clean up with `sudo rm -rf /dev/shm/*vertebra*` if needed
3. Check that current user has write access to /dev/shm

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Author**: Claude Code Analysis
