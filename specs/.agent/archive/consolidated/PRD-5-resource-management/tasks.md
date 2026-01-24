# PRD-5 Implementation Tasks

**Document Version**: 2.0
**Last Updated**: 2026-01-19
**Purpose**: Task breakdown aligned to the shared-memory broker program.

---

## Notes

- Shared-memory C ABI is the only transport (no sockets/UDS/TCP).
- Detailed task tracking lives in `specs/active/shared-memory-broker/tasks.md`.
- Production readiness sequencing lives in `specs/active/production-readiness/phases/`.

---

## Phase 1: C ABI Shared Memory Library (Rust)

- [x] Define C ABI for shm ring creation/open/close.
- [x] Expose send/recv APIs with timeouts + stats.
- [x] Add Arrow IPC helpers with buffer pooling.
- [x] Add ffi-safe error codes + diagnostics.

## Phase 2: Bun Broker (Central Authority)

- [x] Bun broker process uses bun:ffi to shared-memory library.
- [x] Implement request loop + fairness scheduler.
- [x] Implement Bun SQL pools (Postgres/MySQL/SQLite).
- [x] Implement Redis pool (native in broker).
- [x] Implement storage adapter registry + routing.
- [x] Implement provider registry + routing.

## Phase 3: Python CFFI Client + SQLSpec

- [x] CFFI bindings to shm library (client + provider host).
- [x] SQLSpec broker adapters + proxy configs.
- [x] Plugin registration via `Vertebra(plugins=[...])` / `register_plugin(...)`.
- [x] Provider host for non-Bun adapters.
- [x] Storage layer integration (fsspec bridge + brokered ops).

## Phase 4: Tests + Benchmarks

- [ ] Shared-memory ring tests (multi-client).
- [ ] Arrow threshold tests (msgspec vs Arrow).
- [ ] SQLSpec integration tests (postgres/mysql/sqlite/redis).
- [ ] Provider registration tests.
- [ ] Storage integration tests (local backend + brokered access).
