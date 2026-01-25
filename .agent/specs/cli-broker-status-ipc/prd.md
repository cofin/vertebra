# PRD: CLI Banner Status via Broker IPC (No HTTP)

## Summary
Replace the CLI banner's HTTP polling (`/internal/status` + `httpx`) with a native broker IPC status request. The CLI should query the Bun broker over shared memory to render component status, removing HTTP from the CLI path and aligning with the IPC-first architecture.

## Background / Problem
- The CLI banner currently fetches status from `http://{host}:{port}/internal/status` via `httpx`.
- This introduces an HTTP side-channel and a runtime dependency that conflicts with the broker-first IPC model.
- The existing status endpoint is a placeholder and does not expose broker/client stats without additional APIs.

## Goals
- Eliminate `httpx` usage in the CLI banner status path.
- Provide an IPC-native status payload sourced directly from the Bun broker.
- Expose useful status details (bun version/uptime, broker pools/clients, IPC channel count).

## Non-Goals
- Replace or remove the `/internal/status` HTTP endpoint (optional follow-up).
- Build a live TUI; banner remains static at startup.
- Implement new telemetry exporters beyond what exists today.

## User Stories
- As a developer running `vertebra serve`, I see accurate component status without HTTP polling.
- As a maintainer, I can remove `httpx` from core dependencies without losing banner status.
- As a system architect, I know the CLI uses only shared-memory IPC for broker status.

## Requirements

### Functional
1. The Bun broker exposes a status/stats IPC response containing:
   - Bun version
   - Uptime (ms)
   - Broker client count
   - Pool stats (name, size, in-use, idle, health)
   - IPC channel count (worker pool size)
   - Optional: p99 latency if already available
2. Python CLI can request status via shared memory without HTTP.
3. `src/py/vertebra/cli/banner/collector.py` uses IPC status and no `httpx`.
4. If broker IPC is unavailable, banner renders with `unknown/starting` statuses (no crash).

### Non-Functional
- No new network calls in the CLI status path.
- Keep latency low (single IPC round trip).
- No side-channel communication outside the IPC bridge.

## Design Overview

### Protocol Alignment
There are two broker IPC protocols in the repo today:
- **Python `AsyncBrokerClient`** uses framed IPC with `encode_request("broker.request", ...)`.
- **Bun `BrokerServer`** expects msgpack payloads directly over ring buffers with `{kind: ...}`.

To avoid protocol mismatch, introduce a *lightweight Python status client* that matches the Bun broker protocol directly (msgpack payloads, register + request/response over rings).

### Status Payload
Define a new payload kind, for example `kind: "status"` or extend existing `kind: "stats"`.
Suggested response shape:

```
{
  ok: true,
  status: {
    bun: { status: "ready", version: "<Bun.version>", uptime_ms: <number> },
    broker: { status: "ready", clients: <number>, pools: { [name]: { total, in_use, idle, errors } } },
    ipc: { status: "ready", channels: <number>, latency_p99_us?: <number> }
  }
}
```

Bun broker can compute:
- `bun.version` and uptime via `performance.now()`.
- `broker.clients` from the broker server client map.
- `broker.pools` from existing pool adapters or metrics registry.
- `ipc.channels` from worker pool size (pass in or attach to broker status handler).

### Python IPC Client
Add a minimal client in Python that:
- Opens control ring and per-client rings using the Bun broker naming convention.
- Sends `register_client` and waits for ack.
- Sends a `status` (or `stats`) request and waits for response.
- Uses msgpack for encoding/decoding (already available via `msgspec`).

### CLI Integration
- Update `StatusCollector` to call the IPC client.
- Remove `httpx` dependency from the collector entirely.
- Keep failure handling: return `None` on any IPC error.

## Acceptance Criteria
- CLI banner renders status without `httpx` installed.
- `pyproject.toml` does not include `httpx` as a dependency.
- New IPC status request returns populated fields for bun/broker/ipc.
- Tests cover IPC status request and banner collector fallback behavior.

## Testing
- Unit tests for IPC status client (happy path and timeout).
- Integration test that spins a broker and queries status over IPC.
- CLI banner unit test to ensure status collection works without `httpx`.

**Suggested commands:**
- `make test` (Python)
- `make rs-test` (if Rust changes)
- `make js-test` (if Bun/TS changes)

## Risks & Mitigations
- **Protocol mismatch**: Use a separate client matching the Bun broker protocol explicitly.
- **Broker status fields unavailable**: Start with minimal fields and expand as metrics become available.
- **IPC availability**: Return `None` and keep banner rendering graceful.

## Milestones
1. Define broker-side status response schema.
2. Implement Bun broker handler for `status`.
3. Add Python IPC status client.
4. Update CLI banner collector and tests.

