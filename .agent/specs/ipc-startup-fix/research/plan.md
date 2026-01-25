# IPC Startup Fix - Research & Implementation Plan

## Executive Summary

The Vertebra benchmark system fails because the IPC/Bun layer doesn't respond to health checks. This document outlines a comprehensive fix that consolidates process management on the Rust supervisor, implements proper shared memory cleanup, adds real IPC health validation, and removes the httpx dependency from the CLI by using native IPC for status collection.

## Problem Statement

When running `make bench-vertebra` or `vertebra serve`, the server fails to pass health checks with:

```
Server: Vertebra
Status: ❌ Not ready
Issue: --app arg fixed, but IPC/Bun layer not responding
```

The root causes are:
1. **Dual process spawning** - Both Rust supervisor and Bun ProcessManager try to spawn workers
2. **Shared memory files not cleaned up** - Previous runs leave stale `/dev/shm/vertebra-*` files
3. **Health check is superficial** - Reports "ready" without validating actual IPC connectivity
4. **Python binary hardcoded** - Bun ProcessManager uses `"python"` which doesn't exist on many systems
5. **HTTP dependency in CLI** - Banner status uses httpx instead of native IPC

## Current Architecture

### Process Flow (Current - Broken)

```
vertebra serve app:app
    │
    ├── Python CLI (serve.py)
    │       │
    │       ├── Sets VERTEBRA_* env vars
    │       ├── Calls run_supervisor()
    │       │
    │       └── Rust NativeSupervisor
    │               │
    │               ├── Creates Notifier (eventfd/pipe)
    │               ├── Spawns Bun engine ──────────────────┐
    │               │                                       │
    │               └── Spawns Python workers (N)           │
    │                       │                               │
    │                       └── vertebra.server.worker      │
    │                               │                       │
    │                               └── SpscRing.create_named() ─── FAILS if exists
    │                                                       │
    └── Bun Engine ─────────────────────────────────────────┘
            │
            ├── createAsgiServer()
            │       │
            │       ├── ProcessManager.start() ──── ALSO spawns workers!
            │       │       │
            │       │       └── Bun.spawn("python", ...) ──── "python" not found
            │       │
            │       └── IpcConnectionPool ──── Creates ring clients
            │
            └── Bun.serve() ──── HTTP server starts
                    │
                    └── /health ──── Returns 502 (no workers responding)
```

### Process Flow (Target - Fixed)

```
vertebra serve app:app
    │
    ├── Python CLI (serve.py)
    │       │
    │       ├── Sets VERTEBRA_* env vars
    │       ├── Sets VERTEBRA_SUPERVISOR_MODE=rust
    │       ├── Calls run_supervisor()
    │       │
    │       └── Rust NativeSupervisor
    │               │
    │               ├── Creates Notifier (eventfd/pipe)
    │               ├── Cleans up stale /dev/shm/vertebra-* files
    │               ├── Spawns Bun engine (with VERTEBRA_SKIP_PROCESS_MANAGER=1)
    │               │
    │               └── Spawns Python workers (N)
    │                       │
    │                       └── vertebra.server.worker
    │                               │
    │                               └── SpscRing.create_named() ─── O_EXCL, unlink first
    │
    └── Bun Engine
            │
            ├── createAsgiServer()
            │       │
            │       ├── ProcessManager ──── SKIPPED (env var)
            │       │
            │       ├── IpcConnectionPool ──── Connects to supervisor-spawned workers
            │       │
            │       └── BrokerServer ──── Handles status IPC requests
            │
            └── Bun.serve() ──── HTTP server starts
                    │
                    ├── /health ──── Validates IPC round-trip, returns 200
                    │
                    └── IPC status ──── Python CLI queries via shared memory
```

## Detailed Design

### 1. Shared Memory Cleanup Strategy

#### Option A: Unlink-Before-Create (Recommended)

```rust
// In SpscRing::create_named()
pub fn create_named(name: &str, slot_size: usize, slots: usize) -> Result<Self> {
    // Always try to unlink first (ignore errors if doesn't exist)
    let _ = shm_unlink(name);

    // Now create with O_EXCL to ensure we're the only creator
    let fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0o600)?;
    // ... rest of creation
}
```

**Pros**: Simple, always works
**Cons**: Could race with another process (mitigated by O_EXCL)

#### Option B: Supervisor Cleanup at Startup

```rust
// In NativeSupervisor::start()
fn cleanup_stale_shm(&self, name_prefix: &str) -> Result<()> {
    let shm_dir = Path::new("/dev/shm");
    for entry in std::fs::read_dir(shm_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy();
        if name.starts_with(name_prefix) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
```

**Pros**: Central cleanup point
**Cons**: Platform-specific path, may need admin rights

**Decision**: Use Option A (unlink-before-create) in the ring buffer creation, with Option B as a defensive measure in the supervisor.

### 2. Disable Bun ProcessManager When Rust Supervisor Active

Add environment variable check in `createAsgiServer()`:

```typescript
// src/ts/src/server/index.ts
export async function createAsgiServer(
  config: ServerConfig = {},
  pool?: IpcConnectionPool,
): Promise<BunServer> {
  const resolved = await resolveServerConfig(config);

  // Skip ProcessManager if Rust supervisor is managing workers
  const skipProcessManager = process.env.VERTEBRA_SKIP_PROCESS_MANAGER === "1";

  const workerPool = pool ?? createIpcConnectionPool({
    count: resolved.workers.count,
    ipc: resolved.ipc
  });

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

Update Rust supervisor to set this env var:

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
    let ipcLatencyP99: number | undefined;

    if (pool) {
      try {
        const start = performance.now();
        // Send a lightweight ping to any worker
        const pingResult = await Promise.race([
          pingWorkerViaIpc(pool),
          sleep(2000).then(() => { throw new Error("timeout"); }),
        ]);
        if (pingResult.ok) {
          ipcStatus = "ready";
          ipcChannels = pool.size;
          ipcLatencyP99 = Math.round((performance.now() - start) * 1000); // microseconds
        }
      } catch {
        ipcStatus = "error";
      }
    }

    const status: ServerStatus = {
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: uptimeMs,
      },
      broker: {
        status: broker?.isReady() ? "ready" : "starting",
        clients: broker?.clientCount ?? 0,
        pools: broker?.poolStats ?? {},
      },
      ipc: {
        status: ipcStatus,
        channels: ipcChannels,
        latency_p99_us: ipcLatencyP99,
      },
    };

    // Return appropriate HTTP status based on component health
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
```

### 4. Broker IPC Status Handler

Add status request handler to the broker (per cli-broker-status-ipc spec):

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
      bun: {
        status: "ready",
        version: Bun.version,
        uptime_ms: uptimeMs,
      },
      broker: {
        status: "ready",
        clients: this.clients.size,
        pools: this.getPoolStats(),
      },
      ipc: {
        status: "ready",
        channels: this.workerPoolSize,
      },
    },
  };

  await this.sendResponse(clientId, response);
}
```

### 5. Python IPC Status Client

Add a lightweight Python client that matches the broker protocol:

```python
# src/py/vertebra/resources/status_client.py

from __future__ import annotations

import msgspec
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
        self._client_id: str | None = None
        self._request_ring: SpscRing | None = None
        self._response_ring: SpscRing | None = None

    def query_status(self) -> dict | None:
        """Query broker status via IPC. Returns None on any failure."""
        try:
            self._connect()
            return self._request_status()
        except Exception:
            return None
        finally:
            self._disconnect()

    def _connect(self) -> None:
        import uuid
        self._client_id = str(uuid.uuid4()).replace("-", "")

        # Open control ring (broker created it)
        self._control = SpscRing.open_named(self._control_name, self._slot_size)

        # Create client rings
        req_name = f"{self._control_name}.client.{self._client_id}.req"
        resp_name = f"{self._control_name}.client.{self._client_id}.resp"

        self._request_ring = SpscRing.create_named(req_name, self._slot_size, self._slots)
        self._response_ring = SpscRing.create_named(resp_name, self._slot_size, self._slots)

        # Send registration
        register_msg = msgspec.msgpack.encode({
            "kind": "register_client",
            "client_id": self._client_id,
        })
        self._control.send(register_msg, timeout_ms=self._timeout_ms)

        # Wait for ack
        ack = self._control.recv(timeout_ms=self._timeout_ms)
        ack_msg = msgspec.msgpack.decode(ack)
        if not ack_msg.get("ok"):
            raise RuntimeError("Registration failed")

    def _request_status(self) -> dict:
        # Send status request
        request = msgspec.msgpack.encode({"kind": "status"})
        self._request_ring.send(request, timeout_ms=self._timeout_ms)

        # Receive response
        response_bytes = self._response_ring.recv(timeout_ms=self._timeout_ms)
        return msgspec.msgpack.decode(response_bytes)

    def _disconnect(self) -> None:
        # Cleanup rings
        if self._request_ring:
            self._request_ring.close()
        if self._response_ring:
            self._response_ring.close()
        self._client_id = None
```

### 6. CLI Banner Update (Remove httpx)

Update the status collector to use IPC:

```python
# src/py/vertebra/cli/banner/collector.py

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertebra.resources.status_client import StatusClient

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

1. **Rust ring buffer cleanup**
   - Test create_named with existing file (should succeed after unlink)
   - Test O_EXCL race condition handling

2. **Bun status handler**
   - Test status response shape
   - Test IPC ping timeout handling
   - Test HTTP status codes (200 vs 503)

3. **Python status client**
   - Test connection/registration
   - Test status request/response
   - Test timeout handling (returns None)

4. **CLI banner collector**
   - Test successful status collection
   - Test graceful failure (no crash on IPC error)

### Integration Tests

1. **Full server startup**
   - Start `vertebra serve`, verify health returns 200
   - Verify shared memory cleanup between restarts

2. **Benchmark compatibility**
   - Run `make bench-vertebra`, verify it completes

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

## Rollout Plan

### Phase 1: Shared Memory Cleanup
- Implement unlink-before-create in Rust ring buffer
- Add supervisor-level cleanup as defensive measure
- Test: Restart server multiple times, verify no "File exists" errors

### Phase 2: Consolidate Process Management
- Add VERTEBRA_SKIP_PROCESS_MANAGER env var
- Update Rust supervisor to set it
- Update Bun createAsgiServer to respect it
- Test: Verify only N workers spawn (not 2N)

### Phase 3: Real Health Validation
- Implement IPC ping in status handler
- Return 503 until IPC is ready
- Update benchmark health check timeout if needed
- Test: Health endpoint returns 200 only when IPC works

### Phase 4: Broker Status IPC (from cli-broker-status-ipc)
- Add status handler to BrokerServer
- Implement Python StatusClient
- Update CLI banner collector
- Remove httpx from dependencies
- Test: CLI banner shows correct status via IPC

### Phase 5: Validation & Cleanup
- Run full benchmark suite
- Remove unused ProcessManager code paths
- Update documentation
- Archive cli-broker-status-ipc spec (merged)

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| IPC status client fails silently | Banner shows "unknown" | Acceptable degradation, add logging |
| Shared memory cleanup races | Startup fails occasionally | O_EXCL provides atomic guarantee |
| Health check too slow | Benchmark times out | Tune timeout, add fast-path ping |
| Removing httpx breaks something else | Import errors | Search codebase, run full test suite |

## Dependencies

- msgspec (already present for msgpack)
- No new dependencies required
- httpx to be removed from core requirements

## Success Criteria

1. `make bench-vertebra` completes successfully
2. `vertebra serve` starts without "File exists" errors
3. Health endpoint returns 200 only when IPC is functional
4. CLI banner displays status without httpx
5. No duplicate worker processes on startup
