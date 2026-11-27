# Implementation Patterns Index

Every pattern chapter contains code-truth blocks anchored directly to `src/py/vertebra/`, `src/rs/`, and `src/ts/vertebra-client/`.

## Python (`python/`)

- [Python Module Structure, Class Layout & PEP 695 Typing](python/module-and-class-layout.md) — Lazy root exports, frozen `msgspec.Struct` value objects, class member ordering, docstring-only comments, and modern Python typing.
- [Hot-Path Caching & Single-Wake Async Dispatch](python/hot-path-and-async-dispatch.md) — Startup handler compilation, inline vectorized sync batches, single-wake event loops, mount dispatch, and zero-copy response coercion.
- [Serialization & Python Utility Conventions](python/serialization-and-utils.md) — Centralized JSON serialization, schema dumping, sync/async portals, loop-sharded resources, sync thread pools, and optional import guards.

## Rust & PyO3 (`rust/`)

- [PyO3 0.30.1 FFI & GIL Discipline](rust/ffi-and-gil-discipline.md) — Free-threaded module flags, GIL detachment, conditional freelists, cyclic GC, weakref caches, buffer anchoring, and cross-extension async bridges.
- [Lock-Free Shared Memory & Sharded Concurrency](rust/lock-free-shm-and-sharding.md) — Cache-line isolation, Dekker wakeup handshakes, cached SPSC heads, MPMC sequence slots, sharded dispatch, and atomic circuit breakers.

## TypeScript (`typescript/`)

- [Client SDK Root Exports, Subpaths & Build Entries](typescript/client-and-adapter-subpaths.md) — Zero-dependency `@vertebra/client` core, framework and protocol subpath exports, `tsup` entries, Biome rules, and codegen parity.
