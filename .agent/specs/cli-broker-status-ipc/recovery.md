# Recovery: CLI Banner Status via Broker IPC

## Goal
Remove HTTP polling from the CLI banner and fetch status via broker IPC.

## Current State (pre-implementation)
- `StatusCollector` uses `httpx` to call `/internal/status`.
- Bun server exposes `/internal/status` with placeholder broker stats.
- Bun broker already supports a msgpack IPC request loop and a `stats` stub.

## Key Files
- Bun broker: `src/ts/src/broker/server.ts`, `src/ts/src/broker/client.ts`
- Status endpoint: `src/ts/src/server/status.ts`, `src/ts/src/server/index.ts`
- CLI banner: `src/py/vertebra/cli/banner/collector.py`
- Broker client (Python): `src/py/vertebra/resources/broker.py`

## Plan Snapshot
1. Add broker IPC status response (prefer `kind: "status"`).
2. Implement Python IPC status client using Bun broker protocol (msgpack on rings).
3. Replace `httpx` usage in `StatusCollector` with IPC client.
4. Update tests.

## Validation Commands
- `make js-test`
- `make test`

## Notes
- Avoid reusing Python `AsyncBrokerClient` (protocol mismatch with Bun broker).
- Keep banner rendering resilient on IPC failure.

