# Implementation Tasks: Test Deadlock Fix

## Overview

**Complexity**: Medium
**Total Tasks**: 10
**Estimated Phases**: 3

---

## Phase 1: Immediate Unblock (P0)

### Task 1: Add pytest-timeout configuration

**File**: `pyproject.toml`
**Description**: Add timeout configuration to fail tests that exceed time limits.

- [ ] Add `timeout = 30` to `[tool.pytest.ini_options]`
- [ ] Add `timeout_method = "thread"` for thread-based interruption
- [ ] Verify with `uv run pytest --co` that config is recognized

**Acceptance**: `uv run pytest src/py/tests --timeout=30 -v` works without config errors

---

### Task 2: Clean stale shared memory manually

**File**: N/A (manual cleanup)
**Description**: Remove existing stale shared memory to enable immediate testing.

- [ ] Run `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`
- [ ] Verify with `ls /dev/shm/ | grep -E "test-|vertebra" | wc -l` returns 0

**Acceptance**: No stale shared memory files exist

---

### Task 3: Enhance cleanup patterns in conftest.py

**File**: `src/py/tests/conftest.py`
**Description**: Add missing patterns to catch all shared memory naming conventions.

- [ ] Add `"*asgi*"`, `"*broker*"` to cleanup patterns
- [ ] Add common test names to shm_unlink list
- [ ] Add cleanup AFTER test (not just before)

**Acceptance**: Cleanup covers all shared memory patterns used in tests

---

## Phase 2: Test Isolation (P1)

### Task 4: Create test helpers module

**File**: `src/py/tests/helpers/__init__.py`, `src/py/tests/helpers/shm.py`
**Description**: Create helper module with unique name generation.

- [ ] Create `tests/helpers/` directory
- [ ] Create `__init__.py` with exports
- [ ] Create `shm.py` with `unique_shm_name()` function
- [ ] Create `tracked_shm_cleanup()` context manager

**Acceptance**: `from tests.helpers.shm import unique_shm_name` works

---

### Task 5: Update test_broker_client.py

**File**: `src/py/tests/test_broker_client.py`
**Description**: Use unique shared memory names instead of hardcoded.

- [ ] Import `unique_shm_name` from helpers
- [ ] Replace `name = "vertebra-broker-test"` with `name = unique_shm_name(...)`
- [ ] Update cleanup to use tracked names

**Acceptance**: Test passes when run with other broker tests

---

### Task 6: Update test_broker_pool.py

**File**: `src/py/tests/test_broker_pool.py`
**Description**: Use unique shared memory names instead of hardcoded.

- [ ] Import `unique_shm_name` from helpers
- [ ] Replace `name = "vertebra-broker-test"` with `name = unique_shm_name(...)`
- [ ] Update cleanup to use tracked names

**Acceptance**: Test passes when run with other broker tests

---

### Task 7: Update test_asgi_worker.py

**File**: `src/py/tests/test_asgi_worker.py`
**Description**: Use unique shared memory names.

- [ ] Import `unique_shm_name` from helpers
- [ ] Replace `name = "vertebra-asgi-test"` with `name = unique_shm_name(...)`
- [ ] Update `_cleanup_asgi_shm` to accept dynamic name

**Acceptance**: Test passes when run with other tests

---

## Phase 3: Robustness (P2)

### Task 8: Add thread coordination to test_ipc_channel.py

**File**: `src/py/tests/test_ipc_channel.py`
**Description**: Add threading.Event for proper thread coordination.

- [ ] Add `ready = threading.Event()` to signal server readiness
- [ ] Add `shutdown = threading.Event()` to signal clean shutdown
- [ ] Server loop checks shutdown flag with timeout
- [ ] Main thread waits for ready before sending

**Acceptance**: No daemon threads left running after test

---

### Task 9: Add thread coordination to test_ipc_integration.py

**File**: `src/py/tests/test_ipc_integration.py`
**Description**: Add threading.Event for proper thread coordination.

- [ ] Add `ready = threading.Event()` for server readiness
- [ ] Add `shutdown = threading.Event()` for clean shutdown
- [ ] Ensure server loop exits cleanly

**Acceptance**: No daemon threads left running after test

---

### Task 10: Add asyncio timeouts to async tests

**File**: Multiple test files
**Description**: Wrap `asyncio.to_thread` calls with `asyncio.wait_for`.

- [ ] Update `test_broker_client.py` async recv calls
- [ ] Update `test_broker_pool.py` async recv calls
- [ ] Update `test_asgi_worker.py` async recv calls

**Acceptance**: Async operations fail with TimeoutError instead of hanging

---

## Verification Checklist

After all tasks complete:

- [ ] `make test` completes within 60 seconds
- [ ] All tests pass (44+ tests)
- [ ] No stale `/dev/shm/` files after completion
- [ ] Running tests 5 times consecutively passes
- [ ] `uv run pytest src/py/tests -n 4` works (if xdist enabled)

---

## Rollback Instructions

If issues arise:

1. Remove pytest-timeout config from `pyproject.toml`
2. Revert `conftest.py` to previous version
3. Revert test file changes
4. Delete `tests/helpers/` directory
5. Run `git checkout src/py/tests/`

---

## Notes

- Tasks 1-3 provide immediate unblock
- Tasks 4-7 fix the root cause
- Tasks 8-10 add defense in depth
- All changes are test-only (no production code changes)
