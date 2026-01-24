# Recovery Guide: Test Deadlock Fix

## Quick Resume

**Last phase**: Planning Complete
**Current status**: Ready for Implementation
**PRD location**: `specs/active/test-deadlock-fix/`

## Intelligence Context

**Complexity**: Medium
**Stack**: Python 3.12+, Rust, pytest, asyncio

**Commands**:
- Test: `make test` or `uv run pytest src/py/tests`
- Lint: `make lint`
- Clean SHM: `rm -rf /dev/shm/test-* /dev/shm/*vertebra*`

**Documentation**:
- System: Sphinx
- Location: `docs/`
- Skills: N/A (test infrastructure)

**Similar implementations**:
- `src/py/tests/conftest.py` - existing cleanup fixtures
- `src/py/tests/test_broker_client.py` - broker test pattern
- Rust `src/rs/vertebra-core/src/sync.rs` - futex synchronization

**Patterns being followed**:
- pytest autouse fixtures for cleanup
- uuid-based unique naming
- context managers for resource tracking

## State Summary

### Completed
- [x] Project discovery (Python + Rust + pytest stack)
- [x] Root cause analysis (5 hypotheses, 2 primary causes)
- [x] Code review (7 test files, 4 implementation files)
- [x] Research documentation (2000+ words)
- [x] PRD creation (3200+ words)
- [x] Task breakdown (10 tasks in 3 phases)

### Pending
- [ ] Implementation of fixes
- [ ] Verification of fixes
- [ ] Documentation updates

## Root Causes Identified

1. **Primary**: Stale shared memory objects persist between test runs
2. **Primary**: Non-unique shared memory names cause test collisions
3. **Secondary**: Blocking IPC operations without proper timeouts
4. **Secondary**: Incomplete cleanup patterns in conftest.py

## Key Files to Modify

| File | Change Type |
|------|-------------|
| `pyproject.toml` | Add pytest-timeout config |
| `src/py/tests/conftest.py` | Enhance cleanup fixtures |
| `src/py/tests/helpers/shm.py` | NEW: Helper module |
| `src/py/tests/test_broker_client.py` | Use unique names |
| `src/py/tests/test_broker_pool.py` | Use unique names |
| `src/py/tests/test_asgi_worker.py` | Use unique names |
| `src/py/tests/test_ipc_channel.py` | Add thread coordination |
| `src/py/tests/test_ipc_integration.py` | Add thread coordination |

## Next Steps

Run `/implement test-deadlock-fix` to begin implementation.

Or implement manually:

```bash
# Phase 1: Immediate unblock
# 1. Clean stale shared memory
rm -rf /dev/shm/test-* /dev/shm/*vertebra*

# 2. Add pytest-timeout config to pyproject.toml
# [tool.pytest.ini_options]
# timeout = 30
# timeout_method = "thread"

# 3. Verify tests now timeout instead of hang
uv run pytest src/py/tests -v --timeout=30
```

## Verification Commands

```bash
# Clean slate test
rm -rf /dev/shm/test-* /dev/shm/*vertebra*
uv run pytest src/py/tests -v --timeout=30

# Verify no stale shared memory
ls /dev/shm/ | grep -E "test-|vertebra" | wc -l  # Should be 0

# Stress test
for i in {1..5}; do
    rm -rf /dev/shm/test-* /dev/shm/*vertebra*
    make test || exit 1
done
```

## Contact

If blocked, check:
1. `/dev/shm/` for stale shared memory
2. `ps aux | grep pytest` for hung processes
3. `dmesg | tail` for kernel-level issues

## Document History

- **2026-01-19**: Initial PRD created
