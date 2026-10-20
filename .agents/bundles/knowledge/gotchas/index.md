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

- [PyO3 FFI, Embedded Globals & Python Runtime Gotchas](pyo3-and-python-runtime.md) — Dual `.so` `OnceLock<BatchBridge>` split-globals trap in `Topology::Embedded`, `VERTEBRA_ASGI_DIRECT_SEND` vs `asyncio.current_task()`, and `3.14t` `bench` dependency group isolation.
- [Shared-Memory Rings & Cross-Platform IPC Gotchas](shm-and-cross-platform-ipc.md) — `SlotView::release` requirement & `SeqCst` futex handshake, `MpmcRing::has_capacity` sequence check, macOS 31-byte `shm_open` limit, inherited macOS pipe FDs, `__ulock_wait` `ULF_NO_ERRNO` return codes, `matchit 0.9` `{*rest}` syntax, and Rodete registry overrides.
