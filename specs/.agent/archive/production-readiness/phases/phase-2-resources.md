# Phase 2: Resource Management

**Goal**: Centralized, fair, and observable resource pooling.
**Status**: Complete (broker core, plugin registration, observability shipped).

## Tasks

### Shared Memory Transport (Rust C ABI)
- [x] **P2.1**: Implement shared-memory ring C ABI (create/open/send/recv).
- [x] **P2.2**: Add buffer pool + Arrow IPC helpers.
- [x] **P2.3**: Expose stats + timeout-aware waits.

### Bun Broker Core
- [x] **P2.4**: Implement broker request loop + fairness scheduler.
- [x] **P2.5**: Bun SQL pools for Postgres/MySQL/SQLite.
- [x] **P2.6**: Redis pool inside broker.
- [x] **P2.7**: Provider registry + `REGISTER_PROVIDER` routing.

### Python Integration
- [x] **P2.8**: Python CFFI client for shared memory broker.
- [x] **P2.9**: SQLSpec broker adapters + provider host.
- [x] **P2.10**: Generic `register_plugin()` API.

### Observability
- [x] **P2.11**: Broker metrics via shared memory stats.
- [x] **P2.12**: Track pool utilization + queue wait times.

## Verification
- Start Broker.
- Run Python script with 50 concurrent workers (simulated via asyncio).
- Verify fairness (requests per worker).
- Verify metrics endpoint shows utilization.
