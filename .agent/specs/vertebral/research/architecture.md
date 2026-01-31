# Research: Vertebra IPC Architecture & Layout

## 1. Directory Structure
We will treat `vertebra-ipc` as a standalone workspace within the monorepo to ensure it can be published independently.

```text
/home/cody/code/c/vertebra/
├── crates/
│   └── vertebra-ipc/
│       ├── Cargo.toml
│       ├── pyproject.toml  (Maturin config)
│       ├── src/
│       │   ├── lib.rs      (Crate root, PyO3 module)
│       │   ├── memory/     (Low-level SHM)
│       │   │   ├── mod.rs
│       │   │   ├── mmap.rs
│       │   │   └── align.rs
│       │   ├── ring/       (Ring Buffer Logic)
│       │   │   ├── mod.rs
│       │   │   ├── spsc.rs
│       │   │   └── mpmc.rs
│       │   ├── protocol/   (Wire Format)
│       │   │   ├── mod.rs
│       │   │   └── arrow.rs
│       │   └── ipc.rs      (OS Signals - eventfd)
```

## 2. Memory Safety Strategy
*   **Unsafe Usage:** `unsafe` will be required for `mmap` pointer dereferencing.
*   **Encapsulation:** All unsafe pointer arithmetic must be encapsulated within the `ring` module.
*   **Boundary:** The Python `ShmRing` object will hold an `Arc<Mmap>` to ensure the memory region remains valid as long as Python holds a reference.

## 3. The Arrow Handshake
We will use the **Arrow C Data Interface** (`pyarrow.cffi`) to pass data.
1.  Rust allocates a `FFI_ArrowArray` struct on the stack.
2.  Rust populates it with pointers to the SHM region.
3.  Rust calls `Python::with_gil` to invoke `pyarrow.RecordBatch._import_from_c(ptr)`.
4.  **Benefit:** This is standard, robust, and supports all Arrow types.

## 4. Platform Specifics
*   **Linux:** `memfd_create` is preferred for anonymous SHM (auto-cleanup on process death).
*   **macOS:** `shm_open` with `shm_unlink`.
*   **Windows:** Named Shared Memory.
*   **Abstraction:** `trait SharedMemoryProvider`.
