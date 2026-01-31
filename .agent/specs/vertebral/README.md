# Vertebra IPC

**The Data Backbone for Python Multiprocessing.**

Vertebra IPC is a standalone library for **True Zero-Copy** Inter-Process Communication using Shared Memory and Apache Arrow.

## Philosophy

"Compute locally, share globally."
Standard Python multiprocessing suffers from pickling overhead and connection duplication (100 workers = 100 DB pools). Vertebra IPC solves this by centralizing data resources and sharing them via ring buffers.

## Features

*   **Shared Memory Ring:** High-performance circular buffers (SPSC/MPMC).
*   **Arrow-Native:** Write Arrow batches in Rust/SQL; read them in Python as `pyarrow.Table` with **zero copies**.
*   **Cooperative Pooling:** A centralized "Pool Manager" allows hundreds of workers to share a single Postgres/Redis pool.
*   **Python Integration:** Drop-in adapters for `sqlspec` and `advanced-alchemy`.

## Architecture

*   **Transport:** `shm_open` / `mmap`. No sockets in the data plane.
*   **Notification:** `eventfd` (Linux) / Self-pipe (macOS).
*   **Safety:** Atomic headers, crash-resilient layout.

## Roadmap

See `.agent/specs/vertebra-ipc/plan.md` for the implementation plan.
