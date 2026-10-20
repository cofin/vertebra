---
title: "Implementation Patterns Index"
description: "Index of Vertebra's nested implementation pattern chapters across Python, Rust/PyO3/SHM, and TypeScript."
status: stable
tags:
  - patterns
  - index
  - python
  - rust
  - typescript
---

# Implementation Patterns Index

Every pattern chapter contains code-truth blocks (`<!-- truth: start -->` / `<!-- truth: end -->`) anchored directly to `src/py/vertebra/`, `src/rs/`, and `src/ts/vertebra-client/`.

## Python (`python/`)

- [Module Structure, Class Layout & PEP 695 Typing](python/module-and-class-layout.md) — Top-level imports, PEP 562 lazy `__getattr__` package exports, strict class member ordering, Google-style docstrings without inline `#` comments, and PEP 695/604/585 typing.
- [Hot-Path Caching & Single-Wake Async Dispatch](python/hot-path-and-async-dispatch.md) — Startup `_handler_dispatch_spec` caching, vectorized `submit_results` / `submit_response`, 4-way `BatchDispatchPlan`, single-wake `AsyncRuntime`, and task-free ASGI coroutine driving.

## Rust & PyO3 (`rust/`)

- [PyO3 0.30.1 FFI & GIL Discipline](rust/ffi-and-gil-discipline.md) — `py.detach()` around blocking waits, `append_to_inittab!(_vertebra)`, lazy `DataSlab`-backed `#[pyclass]` wrappers, and `thiserror` error mapping.
- [Lock-Free Shared Memory & Sharded Concurrency](rust/lock-free-shm-and-sharding.md) — `#[repr(C, align(64))]` cache-line isolation, `Acquire`/`Release`/`SeqCst` atomic ordering, bounds-checked `unsafe` pointer arithmetic, 8-shard `RequestBatcher`, and 256-shard `InFlightRequests`.

## TypeScript (`typescript/`)

- [Client SDK & Framework Adapter Subpaths](typescript/client-and-adapter-subpaths.md) — Zero-dependency `@vertebra/client` core, subpath adapter exports (`react`, `vue`, `svelte`, `angular`, `htmx`, `vite`, `store`), Biome lint rules, and codegen alignment.
