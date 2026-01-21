# Implementation Plan: Comprehensive Review & Optimization

## Phase 1: Rust IPC & Supervisor (High Priority)
- [x] Task 1.1: Implement Shared-Memory `MpmcRing` in `vertebra-core` [3aef62b].
    - [x] Add `ShmRing` support to `MpmcRing`.
    - [x] Implement sequence-based atomic sync for shared memory.
- [x] Task 1.2: Implement native `Supervisor` in `vertebra-py` [20e1f56].
    - [x] Process group management.
    - [x] Signal propagation.
    - [x] GIL-free execution.
- [x] Task 1.3: Update Python `supervisor.py` to use the native implementation [26ea3f1].

## Phase 2: Data Plane & Drivers
- [x] Task 2.1: Implement Arrow Support in Bun Broker [d30239b].
    - [x] Update `postgres.ts` / `sqlite.ts` to return Arrow RecordBatches for large results.
    - [x] Update broker to use `ShmRegion` for Arrow data.
- [x] Task 2.2: Implement `SyncBrokerClient` in Python [6287539].
    - [x] Expose blocking `send`/`recv` from Rust to Python.
    - [x] Create `vertebra.resources.broker.SyncBrokerClient`.
- [x] Task 2.3: Update Python Drivers [6287539].
    - [x] Use `SyncBrokerClient` in `SyncDriver`.
    - [x] Use `AsyncBrokerClient` in `AsyncDriver`.
    - [x] Add `VertebraArrowCursor` for zero-copy result access.

## Phase 3: Observability & DX
- [ ] Task 3.1: Add Metrics to Bun Broker.
    - [ ] Implement `/__metrics__` endpoint.
    - [ ] Export scheduler stats (pending, wait time, starvation).
- [ ] Task 3.2: Type Bridge HMR.
    - [ ] Implement polling/websocket notify in `vertebra-types` endpoint.
    - [ ] Update Vite plugin to support HMR.

## Phase 4: Cleanup & Quality
- [ ] Task 4.1: Audit all Python/Rust/TS types.
- [ ] Task 4.2: Comprehensive Benchmarking (Arrow vs MessagePack).
- [ ] Task 4.3: Documentation updates for new features.