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

- [Python Module Structure, Class Layout & PEP 695 Typing](python/module-and-class-layout.md) — Top-level imports, PEP 562 lazy `__getattr__` root exports, frozen `msgspec.Struct(..., gc=False)` value objects, strict class member ordering, Google-style docstrings without inline `#` comments, and PEP 695/604/585 typing.
- [Hot-Path Caching & Single-Wake Async Dispatch](python/hot-path-and-async-dispatch.md) — Startup `_handler_dispatch_spec` (`lru_cache(maxsize=512)`), `BatchDispatchPlan` partitioning (`sync_native`, `async_native`, `mounts`, `unmatched`), `AsyncRuntime` / `AsgiEventLoopPool` wakeups, `_drive_coro` ASGI stepping, and `_coerce_response`.
- [Serialization & Python Utility Conventions](python/serialization-and-utils.md) — Centralized `JSONSerializer` (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`), `dump_schema`, `Portal` sync/async bridging, `LoopShardedResource`, `SyncPool`, and `module_loader` guards.

## Rust & PyO3 (`rust/`)

- [PyO3 0.30.1 FFI & GIL Discipline](rust/ffi-and-gil-discipline.md) — `#[pymodule(gil_used = false)]` across `_vertebra`, `_bundler`, and `_storage`, `py.detach()` around blocking waits and file I/O, `append_to_inittab!(pymodule_init)`, lazy `Arc<BatchSnapshot>` request proxies (`freelist = 128`), and `pyo3::intern!` header/scope keys.
- [Lock-Free Shared Memory & Sharded Concurrency](rust/lock-free-shm-and-sharding.md) — `CacheLine<T>` isolation, `Acquire`/`Release`/`SeqCst` futex handshakes, reserve-then-rollback slab atomics, `RequestBatcher` task pool, and `ShmDispatcher` sharded `Mutex<InFlightRequests>` slabs.

## TypeScript (`typescript/`)

- [Client SDK Root Exports, Subpaths & Build Entries](typescript/client-and-adapter-subpaths.md) — Zero-dependency `@vertebra/client` core, subpath exports (`vite`, `react`, `vue`, `svelte`, `angular`, `htmx`, `store`, `wasm`, `arrow`, `live`, `apex`, `telemetry`), `tsup` build entries, Biome lint rules, and codegen parity.
