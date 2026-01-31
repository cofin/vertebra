# Plan: Vertebra IPC Implementation

## Phase 1: The Ring (Core IPC)
**Goal:** Robust, shared-memory circular buffers.

### 1.1 Scaffold & Primitives
- [ ] **Task 1.1.1:** Crate Setup.
    -   `Cargo.toml`: `nix`, `libc`, `thiserror`, `memmap2`, `pyo3`.
    -   `lib.rs`: Define modules `shm`, `ring`, `arrow`.
- [ ] **Task 1.1.2:** Memory Layout.
    -   Implement `ShmHeader` (repr(C), align(64)).
    -   Implement `SlotHeader` (len: u32, flags: u32).
    -   **Validation:** Ensure alignment matches Python `struct` expectations.

### 1.2 Ring Implementation
- [ ] **Task 1.2.1:** `ShmRing` Struct.
    -   AtomicU64 head/tail.
    -   `write_slot(data: &[u8])` with wrap-around logic.
    -   `read_slot() -> &[u8]`.
- [ ] **Task 1.2.2:** Notification (`eventfd`).
    -   Implement `Notifier` struct wrapping `eventfd`.
    -   Expose `raw_fd()` for Python `loop.add_reader`.

### 1.3 Python Bindings
- [ ] **Task 1.3.1:** PyO3 Wrapper.
    -   `class ShmRing`: methods `send`, `recv`, `fileno`.
    -   Implement `__buffer__` protocol? (Maybe too complex for ring, prefer specific read methods).

## Phase 2: Arrow Data Plane
**Goal:** Zero-copy DataFrame transfer.

### 2.1 Arrow Integration
- [ ] **Task 2.1.1:** Rust Arrow.
    -   Add `arrow` crate.
    -   Implement `ArrowBuilder` that writes `RecordBatch` to an in-memory buffer.
- [ ] **Task 2.1.2:** SHM Writer.
    -   Write the Arrow IPC stream bytes directly into the Ring Buffer's data slots.
    -   Handle splitting if batch > slot size (Multipart IPC).

### 2.2 Python Mapping
- [ ] **Task 2.2.1:** Zero-Copy Reader.
    -   In Python: `pyarrow.ipc.open_stream(memoryview(slot_data))`.
    -   **Constraint:** This requires the data to be contiguous. Ring buffer wrap-around is a problem.
    -   **Solution:** Use `Dual Mapping` (mapping the same SHM region twice contiguously) OR strict "No Split" writing (skip to start if not enough space). **Decision: No Split strategy for simplicity.**

## Phase 3: The Pool Manager (Cooperative)
**Goal:** Share DB connections.

### 3.1 Manager Service
- [ ] **Task 3.1.1:** Service Struct.
    -   `PoolManager` with `sqlx::PgPool`.
    -   `HashMap<Token, Transaction>`.
- [ ] **Task 3.1.2:** Control Loop.
    -   Listen on `RequestRing`.
    -   Dispatch `Op::Query`.

### 3.2 Query Proxy
- [ ] **Task 3.2.1:** Execute & Stream.
    -   Run SQL.
    -   Stream rows -> `ArrowBuilder` -> `ShmRing`.
    -   Signal completion.

## Phase 4: Python Adapters
**Goal:** `vertebra` package integration.

### 4.1 SQLSpec Adapter
- [ ] **Task 4.1.1:** Connection Interface.
    -   `VertebraConnection` implements `sqlspec.Connection`.
    -   `execute()` serializes SQL to IPC.
    -   `fetchall()` reads Arrow IPC from Ring.
- [ ] **Task 4.1.2:** Integration Test.
    -   Spin up Manager.
    -   Run queries.
    -   Verify results match standard `psycopg`.

- [ ] **Task 4.3:** Litecore Plugin (`litecore-vertebra`).
    -   Implement `litecore::StatePlugin`.
    -   Initialize `ShmRing` at startup.
    -   Inject `Arc<ShmRing>` into request extensions.
