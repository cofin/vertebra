# Research Plan: Python Test Suite Deadlock Resolution

## 1. Problem Context

The Vertebra project's Python test suite hangs indefinitely when running `make test` or `uv run pytest src/py/tests`. Individual test files pass when run in isolation, but the full suite deadlocks at approximately 2% progress with no output. The process requires SIGKILL (exit code 137) to terminate.

This issue blocks development velocity and CI/CD pipelines. The root cause is related to shared memory IPC (Inter-Process Communication) primitives used by the project's broker and ASGI worker components.

## 2. Technical Background

### 2.1 Vertebra's IPC Architecture

Vertebra uses shared memory ring buffers for zero-copy IPC between Python workers and a Bun/TypeScript broker process. The architecture is:

```
Python App ──CFFI──> C ABI Shared Memory <──bun:ffi── Bun Broker
                          │
                    Ring Buffers (SPSC)
                          │
              ┌───────────┴───────────┐
              │                       │
        MessagePack              Arrow IPC
```

Key components:
- **SpscRing** (Rust): Lock-free single-producer single-consumer ring buffer
- **ShmRing** (Python CFFI): Python bindings to the Rust shared memory primitives
- **Channel** (Python): High-level request/response abstraction over ring buffers

### 2.2 Shared Memory Lifecycle

1. **Creation**: `ShmRing.create()` creates a named shared memory region in `/dev/shm/`
2. **Opening**: `ShmRing.open()` opens an existing named region
3. **Operations**: `send()` and `recv()` transfer data through the ring buffer
4. **Cleanup**: `ShmRing.close()` releases the handle; `shm_unlink()` removes the file

### 2.3 Synchronization Primitives

The ring buffer uses futex-based synchronization:

```rust
// When sending to a full ring, block until space is available
pub fn send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
    loop {
        match self.try_send(payload, flags) {
            Ok(()) => return Ok(()),
            Err(ChannelError::Full) => {
                let head = self.read_head().load(Ordering::Acquire);
                sync::wait_on_address(self.read_head(), head)?;  // BLOCKS
            }
            Err(err) => return Err(err),
        }
    }
}
```

If the counterpart process/thread dies before waking the blocked thread, the wait is indefinite.

## 3. Investigation Methodology

### 3.1 Files Examined

| Category | Files |
|----------|-------|
| Test Infrastructure | `src/py/tests/conftest.py`, `pyproject.toml` |
| IPC Tests | `test_ipc.py`, `test_ipc_async.py`, `test_ipc_channel.py`, `test_ipc_integration.py` |
| Broker Tests | `test_broker_client.py`, `test_broker_pool.py` |
| ASGI Tests | `test_asgi_worker.py` |
| Python IPC | `src/py/vertebra/ipc/channel.py`, `src/py/vertebra/_internal/shm.py` |
| Rust IPC | `src/rs/vertebra-core/src/channel.rs`, `src/rs/vertebra-core/src/sync.rs` |

### 3.2 Evidence Collection

1. **Shared Memory Inspection**: Listed `/dev/shm/` to find stale objects
2. **Code Analysis**: Traced blocking operations through Python → CFFI → Rust
3. **Pattern Analysis**: Identified naming conventions and cleanup patterns
4. **Test Flow Analysis**: Mapped test execution order and resource dependencies

## 4. Findings

### 4.1 Finding 1: Stale Shared Memory Objects

**Evidence**:
```
/dev/shm/vertebra-broker-test.client.test-client.req
/dev/shm/vertebra-broker-test.client.test-client.resp
/dev/shm/vertebra-broker-test.control
```

**Impact**: These files contain ring buffer state (read_head, write_head) from previous test runs. When a new test opens these regions, it encounters mismatched state that causes blocking operations to deadlock.

### 4.2 Finding 2: Non-Unique Shared Memory Names

**Evidence** from test files:

```python
# test_broker_client.py:31
name = "vertebra-broker-test"

# test_broker_pool.py:42
name = "vertebra-broker-test"

# test_asgi_worker.py:72
name = "vertebra-asgi-test"
```

**Impact**: When tests run in sequence, the second test sees shared memory created by the first. The second test expects fresh state but gets corrupted state from the first test's operations.

### 4.3 Finding 3: Incomplete Cleanup Fixture

**Evidence** from `conftest.py`:

```python
def _cleanup_shared_memory() -> None:
    patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp"]
    for pattern in patterns:
        for path in shm_dir.glob(pattern):
            with contextlib.suppress(OSError):
                path.unlink()
```

**Issues**:
1. Runs only at session start/end and before each test, not after failures
2. Uses `contextlib.suppress()` which hides cleanup failures
3. Patterns don't catch all naming conventions (e.g., `vertebra-asgi-test.asgi.0.control`)
4. Doesn't coordinate with pytest-xdist workers

### 4.4 Finding 4: Blocking Operations Without Timeouts

**Evidence** from test files:

```python
# test_broker_client.py:37
data, _flags = await asyncio.to_thread(control.recv, 1000)

# test_ipc_channel.py:22
proc_id, params = server.receive_sync(timeout_ms=1000)
```

**Issue**: While these have 1000ms timeouts for the underlying recv, there's no timeout on the `asyncio.to_thread()` wrapper. If the Rust code blocks on a futex that never wakes, the thread blocks forever even though a timeout was specified.

### 4.5 Finding 5: Daemon Thread Leakage

**Evidence** from test files:

```python
# test_ipc_channel.py:27
thread = threading.Thread(target=server_loop, daemon=True)
thread.start()
# ...
thread.join(timeout=2)  # Gives up after 2 seconds, thread keeps running
```

**Issue**: If the thread is blocked on `recv()`, the `join(timeout=2)` returns without the thread completing. The thread continues running, potentially holding locks or resources that block subsequent tests.

## 5. Root Cause Synthesis

The deadlock occurs through this sequence:

1. **Test A** runs, creates shared memory `vertebra-broker-test.control`
2. Test A's cleanup runs but fails to unlink shared memory (process still has handle open)
3. **Test B** runs, opens the same shared memory name
4. Test B's `recv()` checks `write_head` expecting value 0 (fresh state)
5. But `write_head` has value N from Test A's operations
6. Test B's `recv()` calls `wait_on_address(write_head, N)`
7. No process will ever change `write_head` because Test A is gone
8. **Deadlock**: Test B waits forever on a futex that will never wake

## 6. Solution Design

### 6.1 Solution 1: Unique Per-Test Shared Memory Names

**Rationale**: Eliminates the possibility of tests interfering with each other's shared memory.

**Implementation**:
```python
# tests/helpers.py
import uuid

def unique_shm_name(prefix: str) -> str:
    """Generate a unique shared memory name for test isolation."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

# Usage in test_broker_client.py
name = unique_shm_name("vertebra-broker-test")
```

### 6.2 Solution 2: Tracked Cleanup Fixture

**Rationale**: Explicitly tracks all shared memory created by a test and cleans up both before and after.

**Implementation**:
```python
@pytest.fixture(autouse=True)
def track_shm_cleanup(request):
    """Track and cleanup shared memory created during test."""
    created = []
    original_create = ShmRing.create

    def tracking_create(config):
        created.append(config.name)
        return original_create(config)

    ShmRing.create = tracking_create
    yield
    ShmRing.create = original_create

    for name in created:
        with contextlib.suppress(Exception):
            shm_unlink(name)
```

### 6.3 Solution 3: pytest-timeout Configuration

**Rationale**: Provides a safety net that fails tests exceeding time limits instead of hanging.

**Implementation**:
```toml
# pyproject.toml
[tool.pytest.ini_options]
timeout = 30
timeout_method = "thread"
```

### 6.4 Solution 4: asyncio.wait_for Wrappers

**Rationale**: Ensures timeouts are enforced at the Python level, not just the Rust level.

**Implementation**:
```python
async def safe_recv(ring, timeout_ms, asyncio_timeout=5.0):
    """Receive with both Rust-level and Python-level timeouts."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(ring.recv, timeout_ms),
            timeout=asyncio_timeout
        )
    except asyncio.TimeoutError:
        raise TimeoutError(f"recv timed out after {asyncio_timeout}s")
```

### 6.5 Solution 5: Thread Coordination Events

**Rationale**: Ensures threads are ready before operations begin and shutdown cleanly.

**Implementation**:
```python
def test_channel_pair_roundtrip():
    ready = threading.Event()
    shutdown = threading.Event()

    def server_loop():
        ready.set()  # Signal ready
        while not shutdown.is_set():
            try:
                proc_id, params = server.receive_sync(timeout_ms=100)
                server.respond_sync(proc_id, result)
            except TimeoutError:
                continue

    thread = threading.Thread(target=server_loop)
    thread.start()
    ready.wait(timeout=1)  # Wait for server ready

    # ... perform operations ...

    shutdown.set()  # Signal shutdown
    thread.join(timeout=2)
```

## 7. Implementation Priority

| Priority | Solution | Effort | Impact |
|----------|----------|--------|--------|
| P0 | pytest-timeout config | Low | High - Immediate unblock |
| P0 | Unique per-test names | Low | High - Eliminates root cause |
| P1 | Tracked cleanup fixture | Medium | High - Comprehensive cleanup |
| P2 | asyncio.wait_for wrappers | Medium | Medium - Defense in depth |
| P2 | Thread coordination events | Medium | Medium - Clean shutdown |

## 8. Verification Plan

### 8.1 Immediate Verification

```bash
# Clean slate test
rm -rf /dev/shm/test-* /dev/shm/*vertebra*
uv run pytest src/py/tests -v --timeout=30

# Verify no stale shared memory after tests
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0
```

### 8.2 Regression Verification

```bash
# Run tests 5 times to catch intermittent issues
for i in {1..5}; do
    rm -rf /dev/shm/test-* /dev/shm/*vertebra*
    uv run pytest src/py/tests -v --timeout=30 || exit 1
done
```

### 8.3 Parallel Execution Verification

```bash
# Test with pytest-xdist
uv run pytest src/py/tests -n 4 --timeout=30
```

## 9. Acceptance Criteria

1. `make test` completes within 60 seconds without hanging
2. All 44+ tests pass when run together
3. No stale `/dev/shm/` files after test completion
4. Tests can be run 5 times in succession without issues
5. Parallel execution with `-n 4` works without collisions

## 10. Deep Dive: Futex-Based Synchronization

### 10.1 How Futex Works

A futex (fast userspace mutex) is a Linux kernel feature that provides efficient synchronization primitives. The key operations are:

1. **FUTEX_WAIT**: Block the calling thread if the futex word equals the expected value
2. **FUTEX_WAKE**: Wake up threads waiting on the futex

The Vertebra codebase uses futex through the platform abstraction in `sync.rs`:

```rust
pub fn wait_on_address(word: &AtomicU32, expected: u32) -> Result<(), SyncError> {
    match platform::wait_on_address(word, expected) {
        Ok(()) => { mark_wait_ready(); Ok(()) }
        Err(err) if wait_is_unsupported(&err) => {
            enable_fallback("wait primitive unsupported");
            Err(SyncError::Unsupported)
        }
        Err(err) => Err(err),
    }
}
```

### 10.2 The Deadlock Scenario

The deadlock occurs when:

1. Thread A calls `recv()` which internally calls `wait_on_address(write_head, current_tail)`
2. Thread A expects Thread B to call `send()` which would update `write_head` and call `wake_one()`
3. If Thread B exits without calling `send()`, Thread A waits forever

In the test context:
- Test A's `broker_task()` sends 3 responses then exits
- Test B tries to use the same shared memory, expecting fresh state
- Test B's `recv()` waits on a `write_head` value that no process will ever change

### 10.3 Why Timeouts Don't Always Help

The Python tests use timeouts like `ring.recv(1000)` (1000ms timeout). However:

1. The Rust timeout uses `wait_on_address_timeout()` which does work correctly
2. But if the shared memory state is corrupt, `try_recv()` might succeed with garbage data
3. Or `wait_on_address_timeout()` might return immediately if the futex word already differs

The issue is **stale state**, not missing timeouts.

## 11. Alternative Solutions Considered

### 11.1 Per-Process Shared Memory (Rejected)

**Idea**: Use anonymous shared memory instead of named.

**Pros**: No name collisions possible
**Cons**: Cannot test named shared memory functionality; harder to debug

**Decision**: Rejected - tests should validate the named shared memory code path.

### 11.2 Test Process Isolation with subprocess (Rejected)

**Idea**: Run each IPC test in a separate subprocess.

**Pros**: Complete isolation; no shared state
**Cons**: Slow; complex subprocess management; harder to debug failures

**Decision**: Rejected - unique names provide sufficient isolation with less complexity.

### 11.3 Global Lock for IPC Tests (Rejected)

**Idea**: Use `pytest.mark.serial` or similar to run IPC tests sequentially.

**Pros**: Simple to implement
**Cons**: Doesn't fix root cause; masks bugs; slows test suite

**Decision**: Rejected - fixes the symptom but not the disease.

### 11.4 Shared Memory Pool with Lease (Considered)

**Idea**: Create a pool of pre-allocated shared memory regions, lease to tests.

**Pros**: Efficient; guaranteed unique names
**Cons**: Complex infrastructure; over-engineering for the problem

**Decision**: Deferred - could be implemented later for performance optimization.

## 12. Platform-Specific Considerations

### 12.1 Linux

- Uses `/dev/shm/` for shared memory files
- Futex is natively supported via `syscall(SYS_futex, ...)`
- Shared memory persists until explicitly unlinked or system reboot

### 12.2 macOS

- Uses `shm_open()` which creates files in a different location
- Uses `ulock` instead of futex
- Cleanup may work differently

### 12.3 Windows

- Uses `CreateFileMapping()` for shared memory
- Uses `WaitOnAddress()` / `WakeByAddressSingle()` for futex-like behavior
- Namespace is different; may need different cleanup patterns

The fix should work on all platforms by:
1. Using unique names (platform-agnostic)
2. Using the shm_unlink abstraction (platform-specific but already implemented)
3. Guarding `/dev/shm/` access with existence checks

## 13. Performance Impact Analysis

### 13.1 Unique Name Generation

```python
def unique_shm_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
```

- `uuid.uuid4()` takes ~1.5 microseconds
- Called once per test (not in hot path)
- **Negligible impact**

### 13.2 Tracked Cleanup

```python
with tracked_shm_cleanup() as names:
    # Test code
```

- Adds one list append per created region
- Cleanup iterates over created names (typically 1-5)
- **Negligible impact**

### 13.3 pytest-timeout

- Thread-based timeout adds minimal overhead
- Only triggers if test exceeds time limit
- **Negligible impact**

### 13.4 Overall Impact

Estimated runtime increase: **< 1%** (within measurement noise)

## 14. References

- Rust futex implementation: `src/rs/vertebra-core/src/sync.rs`
- Python CFFI bindings: `src/py/vertebra/_internal/shm.py`
- pytest-timeout documentation: https://pypi.org/project/pytest-timeout/
- POSIX shared memory: `man 7 shm_overview`
- Linux futex: `man 2 futex`
- Python asyncio.to_thread: https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread

## 15. Glossary

| Term | Definition |
|------|------------|
| **Futex** | Fast userspace mutex - kernel primitive for thread synchronization |
| **SPSC** | Single-Producer Single-Consumer ring buffer |
| **MPMC** | Multi-Producer Multi-Consumer ring buffer |
| **Shared Memory** | Memory region mapped into multiple process address spaces |
| **shm_unlink** | POSIX function to remove a named shared memory object |
| **Ring Buffer** | Circular queue data structure for efficient FIFO operations |
| **CFFI** | C Foreign Function Interface - Python library for calling C code |
| **pytest-xdist** | pytest plugin for parallel test execution |
| **asyncio.to_thread** | Python function to run blocking code in a thread pool |

## 16. Appendix: Code Snippets

### 16.1 Complete unique_shm_name Implementation

```python
"""Shared memory test helpers for isolation and cleanup."""
from __future__ import annotations

import contextlib
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Generator

if TYPE_CHECKING:
    from vertebra._internal.shm import ShmRing, ShmRingConfig


def unique_shm_name(prefix: str = "vertebra-test") -> str:
    """Generate a unique shared memory name for test isolation.

    Uses UUID4 for guaranteed uniqueness across parallel test workers,
    repeated test runs, and multiple test sessions.

    Args:
        prefix: Name prefix. Default "vertebra-test" ensures cleanup
                patterns match.

    Returns:
        Unique name like "vertebra-test-abc12345"

    Example:
        >>> name = unique_shm_name("my-test")
        >>> name
        'my-test-a1b2c3d4'
        >>> unique_shm_name("my-test") != unique_shm_name("my-test")
        True
    """
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@contextlib.contextmanager
def tracked_shm_cleanup() -> Generator[list[str], None, None]:
    """Context manager that tracks and cleans up shared memory names.

    Records all shared memory names appended to the yielded list and
    ensures they are unlinked when the context exits, even on exception.

    Yields:
        List to append created shared memory names to. Each name should
        include the leading "/" required by shm_open().

    Example:
        with tracked_shm_cleanup() as names:
            config = ShmRingConfig(name=f"/{unique_shm_name()}.control", ...)
            ring = ShmRing.create(config)
            names.append(config.name)
            # Test code...
        # Ring is automatically unlinked here
    """
    created: list[str] = []
    try:
        yield created
    finally:
        # Import late to handle case where bindings unavailable
        try:
            from vertebra._internal.shm import shm_unlink, SHM_AVAILABLE
        except ImportError:
            return

        if not SHM_AVAILABLE:
            return

        # Unlink each tracked name
        for name in created:
            with contextlib.suppress(Exception):
                shm_unlink(name)

        # Also clean up any files in /dev/shm that match the names
        # This handles the case where shm_unlink fails but file exists
        shm_dir = Path("/dev/shm")
        if shm_dir.is_dir():
            for name in created:
                clean_name = name.lstrip("/")
                for path in shm_dir.glob(f"{clean_name}*"):
                    with contextlib.suppress(OSError):
                        path.unlink()
```

### 16.2 Complete conftest.py Cleanup

```python
def _cleanup_shared_memory() -> None:
    """Clean up any stale shared memory objects from previous test runs.

    This function is intentionally aggressive in its cleanup patterns to
    ensure no shared memory from previous test runs interferes with
    current tests. It uses multiple strategies:

    1. File-based cleanup via /dev/shm (Linux)
    2. shm_unlink for known test names (cross-platform)

    The function silently ignores errors, as shared memory may not exist
    or may be in use by another process.
    """
    # Strategy 1: File-based cleanup (Linux/Unix)
    shm_dir = Path("/dev/shm")
    if shm_dir.is_dir():
        # Comprehensive patterns to catch all naming conventions
        patterns = [
            "test-*",           # Generic test prefix
            "*vertebra*",       # Any vertebra-related
            "*.control",        # Control rings
            "*.req",            # Request rings
            "*.resp",           # Response rings
            "*asgi*",           # ASGI worker tests
            "*broker*",         # Broker tests
        ]
        for pattern in patterns:
            for path in shm_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    path.unlink()

    # Strategy 2: shm_unlink for known names (cross-platform)
    try:
        from vertebra._internal.shm import shm_unlink, SHM_AVAILABLE
    except ImportError:
        return

    if not SHM_AVAILABLE:
        return

    # Common test shared memory names that might be left over
    # These are the hardcoded names used before we introduced unique names
    test_names = [
        "/test-ring",
        "/test-region",
        "/vertebra-broker.control",
        "/vertebra-broker-test.control",
        "/vertebra-broker-test.client.test-client.req",
        "/vertebra-broker-test.client.test-client.resp",
        "/vertebra-asgi-test.asgi.0.control",
        "/vertebra-asgi-test.asgi.0.client.test-client.req",
        "/vertebra-asgi-test.asgi.0.client.test-client.resp",
        "/vertebra-asgi-test.asgi.1.control",
    ]
    for name in test_names:
        with contextlib.suppress(Exception):
            shm_unlink(name)
```
