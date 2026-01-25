# IPC Startup Fix - Research & Analysis

## Investigation Summary

This document captures the findings from investigating the benchmark failure message:

```
Server: Vertebra
Status: ❌ Not ready
Issue: --app arg fixed, but IPC/Bun layer not responding
```

## Root Cause Analysis

### Issue 1: Dual Process Spawning Architecture Conflict

**Finding**: There are two independent systems that can spawn Python workers:

1. **Rust `NativeSupervisor`** (used by `vertebra serve` CLI):
   - Location: `src/rs/vertebra-py/src/supervisor.rs`
   - Spawns both Bun engine AND Python workers
   - Passes `VERTEBRA_NOTIFY_FD` environment variable for IPC coordination
   - Uses `Command::new(&python_executable)` with sys.executable from Python

2. **Bun `ProcessManager`** (used when running `serve.ts` directly):
   - Location: `src/ts/src/server/process-manager.ts`
   - Spawns Python workers via `Bun.spawn`
   - Defaults to `Bun.availableParallelism` workers (12+ on test system)
   - Uses hardcoded `"python"` if no venv specified

**Impact**: When `vertebra serve` runs:
1. Rust supervisor starts Bun (which creates ProcessManager)
2. Rust supervisor ALSO starts Python workers
3. Bun ProcessManager ALSO tries to start Python workers
4. Result: Duplicate workers, race conditions, port conflicts

### Issue 2: Python Binary Not Found

**Finding**: Bun's `ProcessManager` looks for `"python"` in PATH:

```typescript
// process-manager.ts:195
private resolvePythonExecutable(python: PythonConfig): string {
  if (python.venv) {
    // ...venv path resolution
  }
  return "python";  // ← Hardcoded fallback
}
```

On typical Linux systems:
- `/usr/bin/python3` ✓ exists
- `python` symlink ✗ often missing

Error observed:
```
error: Executable not found in $PATH: "python"
  path: "python",
 errno: -2,
  code: "ENOENT"
```

### Issue 3: Shared Memory Cleanup (O_EXCL)

**Finding**: The Rust ring buffer creation uses `O_CREAT | O_RDWR` but doesn't properly handle existing files:

```rust
// vertebra-core shm creation (conceptual)
shm_open(name, O_CREAT | O_RDWR, 0o600)
```

When a previous server run crashes or is killed:
- `/dev/shm/vertebra-asgi.*` files remain
- New `SpscRing.create_named()` fails with:
  ```
  RuntimeError: system call failed: File exists (os error 17)
  ```

**Observed files after crash**:
```
/dev/shm/vertebra-asgi.asgi.0.control
/dev/shm/vertebra-asgi.asgi.0.client.*.req
/dev/shm/vertebra-asgi.asgi.0.client.*.resp
... (up to 12 workers)
/dev/shm/vertebra-broker.control
```

### Issue 4: Health Check Doesn't Validate IPC Readiness

**Finding**: The `/health` endpoint implementation always reports success if objects exist:

```typescript
// status.ts:70-85
const status: ServerStatus = {
  bun: {
    status: "ready",  // Always ready once Bun starts
    version: Bun.version,
    uptime_ms: uptimeMs,
  },
  broker: {
    status: broker ? "ready" : "starting",  // Object exists = ready
    clients: 0,
    pools: {},
  },
  ipc: {
    status: pool ? "ready" : "starting",  // Pool created = ready
    channels: pool ? pool.size : 0,
  },
};
```

**Problems**:
- Doesn't verify Python workers actually started
- Doesn't test IPC round-trip communication
- Doesn't check if shared memory rings are connected
- Benchmark health check sees 502 Bad Gateway (IPC timeout)

### Issue 5: httpx Dependency in CLI

**Finding**: The CLI banner status collection uses `httpx` to poll `/internal/status`:

```python
# collector.py (current)
import httpx
response = httpx.get(f"http://{host}:{port}/internal/status", timeout=2.0)
```

This introduces:
- Unnecessary HTTP dependency
- Side-channel communication outside IPC architecture
- Circular dependency (CLI → HTTP → Bun → Python)

## Architecture Decision

**Recommendation**: Consolidate on Rust supervisor for all process management.

| Approach | Pros | Cons |
|----------|------|------|
| Rust supervisor only | Single source of truth, inherits FDs properly, GIL-free | Bun ProcessManager becomes unused |
| Bun ProcessManager only | All-JS stack | Can't inherit FDs, Python path issues |
| Both (current) | Flexibility | Race conditions, duplicate workers |

**Decision**: Keep Rust `NativeSupervisor`, remove Bun `ProcessManager` from production path.

The Bun ProcessManager should only be used for:
- Testing (with explicit configuration)
- Standalone Bun development

## Files Requiring Changes

### Rust (vertebra-core)
- `src/rs/vertebra-core/src/ring.rs` - Add O_EXCL handling, cleanup on create
- `src/rs/vertebra-core/src/shm.rs` - Add unlink-before-create option

### TypeScript/Bun
- `src/ts/src/server/index.ts` - Conditionally skip ProcessManager
- `src/ts/src/server/config.ts` - Add env var to disable ProcessManager
- `src/ts/src/server/status.ts` - Add real IPC health ping
- `src/ts/src/broker/server.ts` - Add status IPC handler

### Python
- `src/py/vertebra/cli/banner/collector.py` - Remove httpx, use IPC
- `src/py/vertebra/resources/broker.py` - Add lightweight status client

### Benchmarks
- `benchmarks/servers/utils.py` - Update health check to handle IPC status

## Validation Commands

```bash
# Clean shared memory
rm -rf /dev/shm/*vertebra* 2>/dev/null

# Run tests
make test
make js-test
make rs-test

# Test server startup
uv run vertebra serve benchmarks.servers.app.asgi:app --port 9999

# Test health endpoint
curl http://127.0.0.1:9999/health

# Run benchmarks
make bench-vertebra
```

## References

- CLAUDE.md architecture documentation
- specs/active/cli-broker-status-ipc (existing PRD to merge)
- specs/active/shared-memory-broker (ring buffer design)
