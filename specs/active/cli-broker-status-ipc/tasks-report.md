# Task Checklist Report: cli-broker-status-ipc

- Generated: 2026-01-24
- Source: `specs/active/cli-broker-status-ipc/tasks.md`
- Total tasks: 16
- Done: 0
- Pending: 16

## Checklist

# Tasks: CLI Banner Status via Broker IPC

## Phase 1: Protocol & Payload
- [ ] Audit Bun broker IPC protocol and confirm msgpack payload shape (`src/ts/src/broker/server.ts`, `src/ts/src/broker/client.ts`).
- [ ] Decide on `kind: "status"` vs extending `kind: "stats"` and document response schema.

## Phase 2: Bun Broker Status Handler
- [ ] Implement broker handler to return status payload.
- [ ] Add pool stats aggregation (use existing pool adapters/metrics).
- [ ] Include bun version and uptime.
- [ ] Include broker client count.
- [ ] Wire IPC channel count (pass from server or store in broker state).

## Phase 3: Python IPC Status Client
- [ ] Add Python IPC status client matching Bun broker protocol (msgpack, shared memory rings).
- [ ] Add timeout/error handling that returns `None`.
- [ ] Unit tests for client handshake and response parsing.

## Phase 4: CLI Banner Integration
- [ ] Update `StatusCollector` to use IPC client only.
- [ ] Remove `httpx` import and related tests for httpx patching.
- [ ] Update/adjust banner tests to new IPC path.

## Phase 5: Validation
- [ ] Integration test: broker running + IPC status query.
- [ ] Run `make test` and `make js-test`.
- [ ] Update recovery notes and spec status if needed.
