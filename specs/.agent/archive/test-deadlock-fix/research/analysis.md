# Test Deadlock Analysis

## Executive Summary

The Python test suite hangs when running `make test` or `uv run pytest src/py/tests`. This analysis identifies the root causes as: (1) stale shared memory objects persisting between test runs, (2) non-unique shared memory names causing test collisions, and (3) blocking IPC operations without proper timeouts.

## Evidence Collected

### 1. Stale Shared Memory Objects Found

```bash
$ ls -la /dev/shm/ | grep -E "test-|vertebra"
-rw-------  1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.req
-rw-------  1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.client.test-client.resp
-rw-------  1 cody cody 266240 Jan 19 20:29 vertebra-broker-test.control
```

These files persist from previous test runs and contain stale state (incorrect head/tail positions) that causes subsequent tests to deadlock.

### 2. Hardcoded Shared Memory Names

Multiple test files use the same hardcoded names:

| Test File | Shared Memory Name |
|-----------|-------------------|
| test_broker_client.py | `vertebra-broker-test` |
| test_broker_pool.py | `vertebra-broker-test` |
| test_asgi_worker.py | `vertebra-asgi-test` |

When tests run in sequence, the second test encounters shared memory from the first test with mismatched state.

### 3. Blocking Operations Without Timeouts

The Rust `SpscRing.recv()` method blocks indefinitely:

```rust
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

Python tests use this through `asyncio.to_thread()`, which means a thread pool worker becomes permanently blocked.

### 4. Incomplete Cleanup Patterns

The `conftest.py` cleanup has gaps:

```python
patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp"]
```

This doesn't catch patterns like `vertebra-asgi-test.asgi.0.control` (nested naming convention).

## Root Cause Analysis

### Primary Cause: Test Isolation Failure

Tests share the same shared memory namespace (`vertebra-broker-test`) without proper isolation. When:

1. Test A creates shared memory and completes partially
2. Test B tries to use the same shared memory name
3. Test B encounters stale state (wrong head/tail positions)
4. Test B's `recv()` blocks waiting for data that will never arrive

### Secondary Cause: Thread Leakage

Tests create daemon threads that block on IPC operations:

```python
thread = threading.Thread(target=server_loop, daemon=True)
thread.start()
# If test finishes before thread completes, thread blocks on recv()
# Thread persists in thread pool, consuming resources
```

### Tertiary Cause: No Test-Level Timeouts

No mechanism exists to fail a test that exceeds a reasonable time limit. A single deadlocked test causes the entire suite to hang.

## Impact Assessment

- **Severity**: High - Tests cannot complete, blocking CI/CD
- **Frequency**: Consistent when running full test suite
- **Workaround**: Run individual test files in isolation

## Recommended Fixes

1. **Unique per-test shared memory names** - Eliminate collisions entirely
2. **Comprehensive cleanup fixtures** - Track and clean all created shared memory
3. **pytest-timeout configuration** - Fail tests that exceed time limits
4. **Replace blocking calls with timeout variants** - Prevent indefinite waits
