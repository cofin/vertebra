# Tasks: Shared Memory Broker

**Last Updated:** 2026-01-19

## Phase 0: Spec Alignment

- [x] Add shared-memory broker spec (this file set).
- [x] Update PRDs and active specs to remove socket/TCP driver protocol.
- [x] Update docs/protocol + arch to shared-memory transport.

## Phase 1: C ABI Shared Memory Library (Rust)

**Location:** `src/rs/vertebra-core/` and `src/rs/vertebra-broker/`

- [x] Define C ABI for shm ring creation/open/close.
  - `vtb_region_create`, `vtb_region_open` in vertebra-broker
  - `vtb_ring_create`, `vtb_ring_open` for ring channels
- [x] Expose send/recv APIs with timeouts + stats.
  - `vtb_ring_send`, `vtb_ring_recv` with timeout_ms parameter
  - `SpscRing` and `MpmcRing` implementations in vertebra-core
- [x] Add Arrow IPC helpers (write/read) with buffer pool.
  - `write_arrow_ipc()`, `read_arrow_ipc()` functions
  - `BufferPool` with acquire/release
- [x] Add ffi-safe error codes + diagnostics.
  - `VtbStatus` enum with OK, TIMEOUT, EMPTY, FULL, etc.
  - `vtb_last_error()` for error messages

## Phase 2: Bun Broker (Central Authority)

**Location:** `src/ts/src/broker/`

- [x] Bun broker process uses bun:ffi to shm library.
  - `shm.ts` with `loadShmLib()` and FFI definitions
- [x] Implement request loop + fairness scheduler.
  - `FairScheduler` in `scheduler.ts` with deficit round-robin
  - Starvation detection and compensation
- [x] Implement Bun SQL pools (Postgres/MySQL/SQLite).
  - `SqlPool`, `SqlitePool` in `serve.ts`
  - All extend `BasePoolAdapter`
- [x] Implement Redis pool (native in broker).
  - `RedisPool` in `serve.ts`
- [x] Implement storage adapter registry (fsspec-compatible backends / object store).
  - `StoragePool` in `serve.ts`
  - S3, GCS, local filesystem drivers
- [x] Implement provider registry + routing.
  - External provider pool support

## Phase 3: Python CFFI Client + SQLSpec

**Location:** `src/py/vertebra/_internal/shm.py`, `src/py/vertebra/extensions/sqlspec/`

- [x] CFFI bindings to shm library (client + provider host).
  - `ShmRing` and `ShmRegion` classes in `shm.py`
  - Cross-platform library discovery
- [x] SQLSpec broker adapters + proxy configs.
  - `BrokerProxySyncConfig`, `BrokerProxyAsyncConfig`
  - `BrokerProxySyncDriver`, `BrokerProxyAsyncDriver`
  - Dialect configs in postgres/, mysql/, sqlite/
- [x] Plugin registration via `Vertebra(plugins=[...])` / `register_plugin(...)`.
- [x] Provider host for non-Bun adapters.
  - `SqlSpecProvider` in `provider.py`
  - Handles full query lifecycle
- [x] Storage layer integration (fsspec bridge + broker-backed storage ops).
  - `SyncFileSystemClient`, `AsyncFileSystemClient` in `storage/client.py`
  - Routes all ops through broker

## Phase 4: Tests + Benchmarks

**Status:** COMPLETE (2026-01-19)

- [x] Shared-memory ring tests (SPSC roundtrip, MPMC, buffer pool exhaustion).
- [x] Arrow IPC serialization tests.
- [x] Multi-client concurrent access tests.
  - `test_broker_multi_client.py`: Ring isolation, concurrent operations, capacity limits, message ordering
- [x] Arrow threshold tests (msgspec vs Arrow selection).
  - `test_arrow_threshold.py`: Threshold boundary tests, config tests, fallback behavior
- [x] SQLSpec integration tests (config + proxy wrappers).
  - `test_sqlspec_integration.py`: Config classes, proxy wrappers, provider config (skips when sqlspec not installed)
  - Note: Full database integration tests require Docker/pytest-databases
- [x] Provider registration tests.
  - `test_provider_registration.py`: BrokerClientConfig, RingConfig, BrokerClient, ProviderHost
- [x] Storage integration tests (local backend + brokered access).
  - `test_storage_store.py`: LocalStore, AsyncLocalStore operations (existing)
