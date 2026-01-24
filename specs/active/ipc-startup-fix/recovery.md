# IPC Startup Fix - Recovery State

## Current State

**Phase**: Planning Complete
**Last Updated**: 2025-01-24
**Blocker**: None

## Implementation Progress

| Phase | Status | Notes |
|-------|--------|-------|
| 1. Shared Memory Cleanup | Not Started | |
| 2. Process Management Consolidation | Not Started | |
| 3. Health Validation | Not Started | |
| 4. Broker Status IPC | Not Started | |
| 5. Cleanup | Not Started | |

## Files Modified

_None yet - implementation not started_

## Key Decisions Made

1. **Rust supervisor is source of truth** for process management
2. **Unlink-before-create** is the primary shm cleanup strategy
3. **O_EXCL** provides race condition safety
4. **IPC ping** validates actual connectivity before reporting ready
5. **httpx removal** via broker IPC status query

## Environment Setup

Before implementing, ensure:

```bash
# Clean any stale shared memory
rm -rf /dev/shm/*vertebra*

# Rebuild everything
make install

# Verify baseline
make test-all
```

## Quick Validation Commands

```bash
# Test server startup
uv run vertebra serve benchmarks.servers.app.asgi:app --port 9999

# Test health endpoint
curl http://127.0.0.1:9999/health

# Test benchmarks
make bench-vertebra
```

## Rollback Plan

If issues arise:
1. Revert commits in this feature branch
2. Keep `VERTEBRA_SKIP_PROCESS_MANAGER` check but don't set it
3. Keep httpx as fallback for CLI status

## Notes

- `cli-broker-status-ipc` spec will be archived after completion
- `ipc-performance-optimization` spec partially merged (FD inheritance, process consolidation)
- Remaining latency optimization items stay in original spec
