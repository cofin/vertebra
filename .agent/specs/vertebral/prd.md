# Product: Vertebra IPC (The Data Backbone)

## 1. Vision
**Vertebra IPC** is a standalone, high-performance Inter-Process Communication library specialized for **Data-Intensive Python Multiprocessing**.

Unlike generic IPC (pipes, sockets) or Python's `multiprocessing.Queue` (pickling overhead), Vertebra IPC focuses on **True Zero-Copy** data sharing using Shared Memory Ring Buffers and Apache Arrow.

**Core Philosophy:** "Compute locally, share globally." A pool of Python workers should behave like a single multi-threaded application regarding data access, sharing Database Pools, ML Models, and DataFrame results without serialization penalties.

## 2. Technical Architecture

### 2.1 The Shared Memory Ring
*   **Mechanism:** Memory-mapped file (`shm_open` on POSIX, `CreateFileMapping` on Windows).
*   **Memory Layout:**
    ```text
    +-------------------------------------------------------+
    | ShmHeader (64 bytes, Cache-Aligned)                   |
    | - magic: u32 = 0x56455254 ("VERT")                    |
    | - version: u32 = 1                                    |
    | - flags: u64 (Atomic)                                 |
    | - read_head: AtomicU64 (Cache Padding)                |
    | - write_head: AtomicU64 (Cache Padding)               |
    +-------------------------------------------------------+
    | Control Ring (Circular Buffer)                        |
    | - Slot 0: MsgHeader (16 bytes)                        |
    | - Slot 1: MsgHeader (16 bytes)                        |
    | ...                                                   |
    +-------------------------------------------------------+
    | Data Region (Arrow-Aligned Page-Aligned)              |
    | - Buffer 0 (Variable Length)                          |
    | - Buffer 1 ...                                        |
    +-------------------------------------------------------+
    ```

*   **Capabilities:**
    *   **Zero-Copy Reads:** Python maps the data region as a `memoryview` or `pyarrow.RecordBatch`.
    *   **Zero-Copy Writes:** Producers write directly to the SHM slot.
    *   **Optimistic GIL:** Python releases GIL only when waiting on an empty ring.

### 2.2 The Arrow Data Plane
*   **Native Format:** The IPC layer speaks Apache Arrow.
*   **Protocol:**
    1.  Producer (Rust/SQL) writes Arrow RecordBatch to Data Region.
    2.  Producer writes `(offset, length)` to Control Ring.
    3.  Consumer (Python) reads `(offset, length)`.
    4.  Consumer calls `pyarrow.ipc.open_stream(memoryview(shm)[offset:offset+length])`.
    5.  **Result:** A `pyarrow.Table` is created pointing to shared memory. No data is copied.

### 2.3 Cooperative Pooling (The "Global" Pool)
*   **Problem:** 10 workers * 10 conns = 100 open DB connections.
*   **Solution:** Centralized **Rust Pool Manager**.
*   **Flow:**
    1.  Worker sends `Acquire` op to Manager via Ring.
    2.  Manager checks `sqlx` pool.
    3.  If available, Manager returns `ConnectionToken`.
    4.  **Query Proxying:** For high performance, Workers send SQL strings to Manager. Manager executes and writes Arrow results to SHM.

## 3. Python Ecosystem Integrations

### 3.1 SQLSpec Adapter (`vertebra.extensions.sqlspec`)
*   **Config:** `VertebraPostgresConfig`
*   **Connection:** Instead of TCP, it opens a `ShmChannel`.
*   **Cursor:** `execute()` writes SQL to Ring; `fetchall()` maps resulting Arrow batch to Python objects (or returns Arrow directly if requested).

### 3.2 Advanced Alchemy Adapter (`vertebra.extensions.advanced_alchemy`)
*   **Repository:** Backend optimized for IPC.
*   **Cache:** Shared Memory Object Cache (Redis replacement for local-node caching).

## 4. File Structure Proposal

```text
vertebra-ipc/
├── Cargo.toml
├── pyproject.toml
├── src/
│   ├── rs/                  # Rust Core
│   │   ├── lib.rs
│   │   ├── shm.rs           # shm_open / mmap
│   │   ├── ring/            # SPSC/MPMC implementation
│   │   ├── arrow/           # Arrow integration
│   │   ├── pool/            # Connection Pooling logic
│   │   └── ffi.rs           # PyO3 bindings
│   └── py/                  # Python Package
│       ├── vertebra_ipc/
│       │   ├── __init__.py
│       │   ├── core.pyi     # Stubs
│       │   ├── client.py    # High-level Client
│       │   └── adapters/    # SQLSpec/Alchemy adapters
```

## 5. Success Metrics
*   **Latency:** IPC Overhead < 5µs (p99).
*   **Efficiency:** 50 Workers sharing 5 DB connections.
*   **Throughput:** 10GB/s data transfer.