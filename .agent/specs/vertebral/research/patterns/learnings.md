# Vertebra IPC Patterns & Learnings

## 1. Shared Memory Ring Buffer
*   **Layout:** 64-byte aligned header to avoid false sharing.
*   **SPSC (Single-Producer Single-Consumer):** Use `AtomicU64` head/tail indices. Fastest for 1:1 response channels.
*   **MPMC (Multi-Producer Multi-Consumer):** Use for the Request Channel (Broker <- Workers). Requires `AtomicU64` sequence numbers for ABA protection.
*   **Platform:**
    *   Linux: `memfd_create` (auto-cleanup).
    *   macOS: `shm_open` + `shm_unlink`.
    *   Windows: `CreateFileMapping`.

## 2. Notification (The Wakeup Problem)
*   **Polling:** `asyncio.sleep(0.001)` caps throughput at ~1000 RPS. Unacceptable.
*   **EventFD (Linux):** Zero-overhead notification. Integate with `loop.add_reader()`.
*   **Fallback:** Self-pipe pattern for macOS/Windows `add_reader` compatibility.
*   **Hybrid Wait:** Spin for 2-5µs (checking atomic head != tail) before falling back to syscall. This catches 90% of RPC traffic without a context switch.

## 3. Arrow Zero-Copy
*   **Protocol:** Producer writes Arrow IPC stream bytes to SHM.
*   **Consumer:** Maps `memoryview(shm_slot)` directly via `pyarrow.ipc.open_stream`.
*   **Constraint:** Data must be contiguous.
    *   **Solution:** "No Split" writing strategy. If a message wraps around the ring end, skip to start. Wastes some RAM, saves CPU cycles on reassembly.

## 4. Safety
*   **Arc Keepalive:** Python object (`ShmRing`) must hold `Arc<Mmap>` to prevent segfaults.
*   **Crash Recovery:** Use `f_seals` on Linux memfd to prevent accidental truncation.
