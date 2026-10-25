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

- [Python Module Structure, Class Layout & PEP 695 Typing](python/module-and-class-layout.md) — Top-level imports, PEP 810 `__lazy_modules__` + PEP 562 `__getattr__` lazy root exports, frozen `msgspec.Struct(..., gc=False)` value objects, strict class member ordering, Google-style docstrings without inline `#` comments, and PEP 695/604/585 typing across Python 3.12–3.15/3.15t.
- [Hot-Path Caching & Single-Wake Async Dispatch](python/hot-path-and-async-dispatch.md) — Startup `compile_handler_spec`, inline vectorized `"sync_inline"` batch execution (`submit_batch_raw`), `uvloop`-backed `EventLoopPool` and single-thread `loop.add_reader` Dekker polling, `O(1)` `MOUNT_HANDLER_BASE` mount dispatch, `drive_asgi_batch` / `drive_rsgi_batch`, and zero-copy `coerce_response`.
- [Serialization & Python Utility Conventions](python/serialization-and-utils.md) — Centralized `JSONSerializer` (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`), `dump_schema`, `Portal` sync/async bridging, `LoopShardedResource`, `SyncPool`, and `module_loader` guards.

## Rust & PyO3 (`rust/`)

- [PyO3 0.30.1 FFI & GIL Discipline](rust/ffi-and-gil-discipline.md) — `#[pymodule(gil_used = false)]` across `_vertebra`, `_bundler`, and `_storage`, conditional `freelist` / `immutable_type` on `Py_GIL_DISABLED`, PEP 782 `PyBytes::new_with`, PEP 814 `PyFrozenDict::empty` + `PyUnstable_SetImmortal` on Python 3.15, anchored `__getbuffer__` `Request`, and C-API `PyIter_Send` ASGI/RSGI drivers.
- [Lock-Free Shared Memory & Sharded Concurrency](rust/lock-free-shm-and-sharding.md) — `CacheLine<T>` isolation (`ABI_VERSION = 3`), Dekker `SeqCst` `consumer_sleeping` and `active_flags` handshakes, generation-synchronized cached SPSC heads, cache-line-padded MPMC sequence slots, lock-free `ShmDispatcher` batch routing, and `BackpressureController` sliding-window atomics.

## TypeScript (`typescript/`)

- [Client SDK Root Exports, Subpaths & Build Entries](typescript/client-and-adapter-subpaths.md) — Zero-dependency `@vertebra/client` core, subpath exports (`vite`, `react`, `vue`, `svelte`, `angular`, `htmx`, `store`, `wasm`, `arrow`, `live`, `apex`, `telemetry`), `tsup` build entries, Biome lint rules, and codegen parity.
