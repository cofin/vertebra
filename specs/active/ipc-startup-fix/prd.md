# PRD: IPC Startup Fix & Process Consolidation

**Status**: Active
**Priority**: Critical (blocking benchmarks)
**Consolidates**: `cli-broker-status-ipc`, `ipc-performance-optimization` (partial)

## Executive Summary

The Vertebra benchmark system fails because the IPC/Bun layer doesn't respond to health checks. This PRD consolidates three areas of work into a single coherent fix:

1. **Process Management Consolidation** - Single supervisor (Rust) for all process spawning
2. **Shared Memory Cleanup** - Proper O_EXCL handling and stale file cleanup
3. **Health Validation** - Real IPC connectivity testing before reporting "ready"
4. **CLI Status via IPC** - Remove httpx dependency, query broker directly via shared memory

## Problem Statement

When running `make bench-vertebra` or `vertebra serve`, the server fails health checks:

```
Server: Vertebra
Status: ❌ Not ready
Issue: --app arg fixed, but IPC/Bun layer not responding
```

### Root Causes

| Issue | Impact | Severity |
|-------|--------|----------|
| Dual process spawning (Rust + Bun both spawn workers) | Race conditions, duplicate workers | Critical |
| Stale `/dev/shm/vertebra-*` files not cleaned | "File exists" errors on restart | High |
| Health check doesn't validate IPC connectivity | False positives, 502 responses | High |
| Bun ProcessManager hardcodes `"python"` | Process spawn fails on many systems | High |
| httpx dependency for CLI status | Unnecessary HTTP round-trip | Medium |

## Architecture

### Current Flow (Broken)

```
vertebra serve app:app
    │
    ├── Python CLI (serve.py)
    │       │
    │       └── Rust NativeSupervisor
    │               │
    │               ├── Spawns Bun engine ──────────────────────┐
    │               │                                           │
    │               └── Spawns Python workers (N)               │
    │                       │                                   │
    │                       └── SpscRing.create_named()         │
    │                               └── FAILS if shm exists     │
    │                                                           │
    └── Bun Engine ─────────────────────────────────────────────┘
            │
            ├── ProcessManager.start() ──── ALSO spawns workers!
            │       │
            │       └── Bun.spawn("python", ...) ──── "python" not found
            │
            └── /health ──── Returns 502 (no IPC response)
```

**Problems:**
1. Two systems spawn workers → race conditions, duplicate processes
2. Bun uses `"python"` (doesn't exist) instead of `sys.executable`
3. Shared memory files from crashed runs block new runs
4. Health endpoint reports "ready" before IPC is functional

### Target Flow (Fixed)

```
vertebra serve app:app
    │
    ├── Python CLI (serve.py)
    │       │
    │       ├── Sets VERTEBRA_SUPERVISOR_MODE=rust
    │       │
    │       └── Rust NativeSupervisor
    │               │
    │               ├── Cleans up stale /dev/shm/vertebra-* files
    │               ├── Creates Notifier (eventfd/pipe)
    │               │
    │               ├── Spawns Bun engine
    │               │       └── env: VERTEBRA_SKIP_PROCESS_MANAGER=1
    │               │
    │               └── Spawns Python workers (N)
    │                       │
    │                       └── SpscRing.create_named()
    │                               └── unlink first, then O_CREAT|O_EXCL
    │
    └── Bun Engine
            │
            ├── ProcessManager ──── SKIPPED (env var)
            │
            ├── IpcConnectionPool ──── Connects to supervisor-spawned workers
            │
            ├── BrokerServer ──── Handles status IPC requests
            │
            └── /health ──── Validates IPC ping, returns 200 or 503
                    │
                    └── IPC Status ──── Python CLI queries via shared memory
```

## Detailed Design

### 1. Shared Memory Cleanup Strategy

**Approach**: Unlink-before-create with O_EXCL for atomicity.

```rust
// In SpscRing::create_named()
pub fn create_named(name: &str, slot_size: usize, slots: usize) -> Result<Self> {
    // Always try to unlink first (ignore ENOENT)
    let _ = shm_unlink(name);

    // Create with O_EXCL to ensure we're the only creator
    let fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0o600)?;
    // ... rest of creation
}
```

**Defensive cleanup in supervisor:**

```rust
// In NativeSupervisor::start()
fn cleanup_stale_shm(prefix: &str) -> Result<()> {
    let shm_dir = Path::new("/dev/shm");
    for entry in std::fs::read_dir(shm_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy();
        if name.starts_with(prefix) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}
```

### 2. Disable Bun ProcessManager When Rust Supervisor Active

**TypeScript changes:**

```typescript
// src/ts/src/server/index.ts
export async function createAsgiServer(
  config: ServerConfig = {},
  pool?: IpcConnectionPool,
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);

  // Skip ProcessManager if Rust supervisor is managing workers
  const skipProcessManager = process.env.VERTEBRA_SKIP_PROCESS_MANAGER === "1";

  const manager = !skipProcessManager && resolved.python.app
    ? new ProcessManager({
        count: resolved.workers.count,
        ipc: resolved.ipc,
        python: resolved.python,
      })
    : null;

  manager?.start();
  // ... rest unchanged
}
```

**Rust supervisor sets the env var:**

```rust
// src/rs/vertebra-py/src/supervisor.rs
cmd.args(&engine_args)
    .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string())
    .env("VERTEBRA_SKIP_PROCESS_MANAGER", "1")  // ← Add this
    .env("VERTEBRA_SPIN_US", spin_us.to_string());
```

### 3. Real IPC Health Validation

Replace superficial status check with actual IPC ping:

```typescript
// src/ts/src/server/status.ts

export function createStatusHandler(
  broker?: BrokerServer,
  pool?: IpcConnectionPool,
): (req: Request) => Promise<Response> {
  return async () => {
    const uptimeMs = Math.round(performance.now() - serverStartTime);

    // Actually test IPC connectivity
    let ipcStatus: StatusLevel = "starting";
    let ipcChannels = 0;
    let ipcLatencyUs: number | undefined;

    if (pool) {
      try {
        const start = performance.now();
        // Send lightweight ping to any available worker
        const pingResult = await Promise.race([
          pingWorkerViaIpc(pool),
          sleep(2000).then(() => { throw new Error("timeout"); }),
        ]);
        if (pingResult.ok) {
          ipcStatus = "ready";
          ipcChannels = pool.size;
          ipcLatencyUs = Math.round((performance.now() - start) * 1000);
        }
      } catch {
        ipcStatus = "error";
      }
    }

    const status: ServerStatus = {
      bun: { status: "ready", version: Bun.version, uptime_ms: uptimeMs },
      broker: {
        status: broker?.isReady() ? "ready" : "starting",
        clients: broker?.clientCount ?? 0,
        pools: broker?.poolStats ?? {},
      },
      ipc: { status: ipcStatus, channels: ipcChannels, latency_p99_us: ipcLatencyUs },
    };

    const httpStatus = ipcStatus === "ready" ? 200 : 503;
    return new Response(JSON.stringify(status), {
      status: httpStatus,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
      },
    });
  };
}

async function pingWorkerViaIpc(pool: IpcConnectionPool): Promise<{ ok: boolean }> {
  // Acquire any channel and send a no-op request
  const channel = await pool.acquire();
  try {
    // Send minimal ASGI-like scope that workers handle as ping
    const scope = { type: "vertebra.ping" };
    await channel.send(msgpack.encode(scope));
    const response = await channel.recv(1000); // 1s timeout
    return { ok: response !== null };
  } finally {
    pool.release(channel);
  }
}
```

### 4. Broker IPC Status Handler (from cli-broker-status-ipc)

Add status request handling to the broker for CLI queries:

```typescript
// src/ts/src/broker/server.ts

interface StatusRequest {
  kind: "status";
}

interface StatusResponse {
  ok: boolean;
  status: {
    bun: { status: string; version: string; uptime_ms: number };
    broker: { status: string; clients: number; pools: Record<string, PoolStats> };
    ipc: { status: string; channels: number; latency_p99_us?: number };
  };
}

// In BrokerServer.handleMessage()
private async handleMessage(clientId: string, message: BrokerMessage): Promise<void> {
  switch (message.kind) {
    case "status":
      await this.handleStatus(clientId);
      break;
    // ... other cases
  }
}

private async handleStatus(clientId: string): Promise<void> {
  const uptimeMs = Math.round(performance.now() - this.startTime);

  const response: StatusResponse = {
    ok: true,
    status: {
      bun: { status: "ready", version: Bun.version, uptime_ms: uptimeMs },
      broker: {
        status: "ready",
        clients: this.clients.size,
        pools: this.getPoolStats(),
      },
      ipc: { status: "ready", channels: this.workerPoolSize },
    },
  };

  await this.sendResponse(clientId, response);
}
```

### 5. Python IPC Status Client

Lightweight client for querying broker status without HTTP:

```python
# src/py/vertebra/resources/status_client.py

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from vertebra._vertebra import SpscRing


class StatusClient:
    """Lightweight IPC client for querying broker status."""

    def __init__(
        self,
        control_name: str = "/vertebra-broker.control",
        slot_size: int = 65536,
        slots: int = 1024,
        timeout_ms: int = 2000,
    ) -> None:
        self._control_name = control_name
        self._slot_size = slot_size
        self._slots = slots
        self._timeout_ms = timeout_ms

    def query_status(self) -> dict | None:
        """Query broker status via IPC. Returns None on any failure."""
        try:
            return self._do_query()
        except Exception:
            return None

    def _do_query(self) -> dict:
        from vertebra._vertebra import SpscRing

        client_id = uuid.uuid4().hex

        # Open control ring (broker created it)
        control = SpscRing.open_named(self._control_name, self._slot_size)

        # Create client rings
        req_name = f"{self._control_name}.client.{client_id}.req"
        resp_name = f"{self._control_name}.client.{client_id}.resp"

        try:
            request_ring = SpscRing.create_named(req_name, self._slot_size, self._slots)
            response_ring = SpscRing.create_named(resp_name, self._slot_size, self._slots)

            # Register with broker
            register_msg = msgspec.msgpack.encode({
                "kind": "register_client",
                "client_id": client_id,
            })
            control.send(register_msg, timeout_ms=self._timeout_ms)

            # Wait for ack
            ack = control.recv(timeout_ms=self._timeout_ms)
            ack_msg = msgspec.msgpack.decode(ack)
            if not ack_msg.get("ok"):
                raise RuntimeError("Registration failed")

            # Send status request
            request = msgspec.msgpack.encode({"kind": "status"})
            request_ring.send(request, timeout_ms=self._timeout_ms)

            # Receive response
            response_bytes = response_ring.recv(timeout_ms=self._timeout_ms)
            return msgspec.msgpack.decode(response_bytes)

        finally:
            # Cleanup rings (unlink)
            SpscRing.unlink_named(req_name)
            SpscRing.unlink_named(resp_name)
```

### 6. CLI Banner Update (Remove httpx)

Replace HTTP polling with IPC:

```python
# src/py/vertebra/cli/banner/collector.py

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ServerStatus:
    bun: dict | None = None
    broker: dict | None = None
    ipc: dict | None = None


class StatusCollector:
    """Collects server status via broker IPC."""

    def __init__(self, timeout_ms: int = 2000) -> None:
        self._timeout_ms = timeout_ms

    def collect(self) -> ServerStatus:
        """Collect status from broker via IPC. Returns empty status on failure."""
        try:
            from vertebra.resources.status_client import StatusClient

            client = StatusClient(timeout_ms=self._timeout_ms)
            result = client.query_status()

            if result and result.get("ok"):
                status = result.get("status", {})
                return ServerStatus(
                    bun=status.get("bun"),
                    broker=status.get("broker"),
                    ipc=status.get("ipc"),
                )
        except Exception:
            pass

        return ServerStatus()  # Empty status on any failure
```

## Testing Strategy

### Unit Tests

| Component | Test Cases |
|-----------|------------|
| Rust ring buffer | create_named with existing file succeeds after unlink; O_EXCL race handling |
| Bun status handler | Response shape; IPC ping timeout; HTTP 200 vs 503 codes |
| Python status client | Connection/registration; request/response; timeout returns None |
| CLI banner collector | Successful collection; graceful failure (no crash) |

### Integration Tests

1. **Full server startup**: Start `vertebra serve`, verify health returns 200
2. **Restart resilience**: Stop/start server multiple times, no "File exists" errors
3. **Benchmark completion**: `make bench-vertebra` completes successfully

### Commands

```bash
# Rust tests
make rs-test

# TypeScript tests
make js-test

# Python tests
make test

# Full integration
make bench-vertebra
```

## Implementation Phases

### Phase 1: Shared Memory Cleanup (Day 1)
- [ ] Implement unlink-before-create in Rust ring buffer
- [ ] Add supervisor-level cleanup as defensive measure
- [ ] Test: Restart server multiple times, verify no "File exists" errors

### Phase 2: Consolidate Process Management (Day 1-2)
- [ ] Add `VERTEBRA_SKIP_PROCESS_MANAGER` env var check in Bun
- [ ] Update Rust supervisor to set it
- [ ] Test: Verify only N workers spawn (not 2N)

### Phase 3: Real Health Validation (Day 2)
- [ ] Implement IPC ping in status handler
- [ ] Return 503 until IPC is ready
- [ ] Add `vertebra.ping` scope handling in Python workers
- [ ] Test: Health endpoint returns 200 only when IPC works

### Phase 4: Broker Status IPC (Day 2-3)
- [ ] Add status handler to BrokerServer
- [ ] Implement Python StatusClient
- [ ] Update CLI banner collector
- [ ] Test: CLI banner shows correct status via IPC

### Phase 5: Cleanup (Day 3)
- [ ] Remove httpx from dependencies
- [ ] Remove unused ProcessManager code paths when supervisor active
- [ ] Update documentation
- [ ] Archive merged specs (`cli-broker-status-ipc`, partial `ipc-performance-optimization`)

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| IPC status client fails silently | Banner shows "unknown" | Acceptable degradation; add logging |
| Shared memory cleanup races | Startup fails occasionally | O_EXCL provides atomic guarantee |
| Health check too slow | Benchmark times out | Tune timeout; add fast-path ping |
| Removing httpx breaks something | Import errors | Search codebase; run full test suite |

## Dependencies

- msgspec (already present)
- No new dependencies required
- httpx to be removed from core requirements

## Success Criteria

1. ✅ `make bench-vertebra` completes successfully
2. ✅ `vertebra serve` starts without "File exists" errors
3. ✅ Health endpoint returns 200 only when IPC is functional
4. ✅ CLI banner displays status without httpx
5. ✅ No duplicate worker processes on startup

## Consolidated Specs

This PRD consolidates work from:

- **`cli-broker-status-ipc`**: Fully merged (IPC status query, httpx removal)
- **`ipc-performance-optimization`**: Partially merged (FD inheritance, process consolidation)
  - Remaining items (buffer pooling, sub-10μs latency targets) stay in original spec

## Appendix: Files to Modify

### Rust
- `src/rs/vertebra-core/src/ring.rs` - Add unlink-before-create
- `src/rs/vertebra-py/src/supervisor.rs` - Add stale shm cleanup, set skip env var

### TypeScript
- `src/ts/src/server/index.ts` - Skip ProcessManager when env var set
- `src/ts/src/server/status.ts` - Add real IPC ping validation
- `src/ts/src/broker/server.ts` - Add status IPC handler

### Python
- `src/py/vertebra/resources/status_client.py` - New IPC status client
- `src/py/vertebra/cli/banner/collector.py` - Remove httpx, use IPC
- `src/py/vertebra/server/worker.py` - Handle `vertebra.ping` scope
- `pyproject.toml` - Remove httpx from dependencies
