---
title: "Gotchas & Non-Obvious Footguns Index"
description: "Index of Vertebra's runtime, PyO3 FFI, ASGI, shared-memory, and cross-platform IPC gotchas."
status: stable
tags:
  - gotchas
  - index
  - pyo3
  - shared-memory
---

# Gotchas & Non-Obvious Footguns Index

- [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](pyo3-and-python-runtime.md) — Dual `.so` `OnceLock<BatchBridge>` split-globals trap in `Topology::Embedded`, `#[pymodule(gil_used = false)]` + `GilAuditReport` on `3.14t`, `VERTEBRA_ASGI_DIRECT_SEND` vs `asyncio.current_task()`, unanchored `Request.body_view` lifetime, shared native vs fallback class names, `submit_raw_response` in-process behavior, and `bench` dependency group isolation.
- [Shared-Memory Rings & Cross-Platform IPC Gotchas](shm-and-cross-platform-ipc.md) — Single active `SlotView` borrow & `SeqCst` futex handshake, `MpmcRing::has_capacity` sequence check, macOS 31-byte `shm_open` limit, inherited notifier/memfd descriptors, `__ulock_wait` `ULF_NO_ERRNO` return codes, Apple Silicon 128-byte cache lines, `select_ring` boundary off-by-8, `VRES` vs `ResponseSlot` framing, and `matchit 0.9` `{name}` / `{*name}` normalization.
