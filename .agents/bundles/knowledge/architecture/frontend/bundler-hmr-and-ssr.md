---
name: frontend-bundler-hmr-and-ssr
description: Oxc Bundler, rolldown-notify SourceWatcher, rquickjs SsrPool with ArrayQueue context leasing and DashMap TTL cache, and BuildOrchestrator.
sources:
  - src/rs/vertebra-bundler/src/bundler.rs
  - src/rs/vertebra-bundler/src/hmr.rs
  - src/rs/vertebra-bundler/src/ssr.rs
  - src/py/vertebra/builder/_orchestrator.py
  - src/py/vertebra/builder/_fallback.py
---

# Oxc Bundler, HMR & QuickJS SSR Pool

[`vertebra-bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs) (`vertebra._bundler`) provides a Rust-native Oxc TypeScript/JSX compiler, filesystem watcher, and embedded QuickJS SSR worker pool, orchestrated from Python by [`BuildOrchestrator`](../../../../../src/py/vertebra/builder/_orchestrator.py).

## 1. Oxc TypeScript/JSX `Bundler` & `SourceWatcher` (`bundler.rs`, `hmr.rs`)

- **[`Bundler`](../../../../../src/rs/vertebra-bundler/src/bundler.rs)**: Parses `.ts`, `.tsx`, `.js`, and `.jsx` entrypoints via `oxc_parser`, strips TypeScript types and transforms JSX via `oxc_transformer`, resolves relative imports recursively, minifies output via `oxc_minifier`, generates source maps, and writes content-hashed filenames plus `.vertebra/manifest.json` (`AssetManifest`).
- **[`SourceWatcher`](../../../../../src/rs/vertebra-bundler/src/hmr.rs)**: Wraps `rolldown_notify::RecommendedWatcher` in a background thread that filters relevant frontend extensions, deduplicates paths inside a configurable debounce window, and exposes `next_change(timeout_ms)` / `drain_changes()` to Python while releasing the GIL (`py.detach`).

## 2. Pooled QuickJS SSR Engine (`SsrPool` in `ssr.rs`)

[`SsrPool`](../../../../../src/rs/vertebra-bundler/src/ssr.rs) executes server-side JavaScript rendering inside embedded `rquickjs` (`0.14`) runtimes without spawning Node.js:

1. **Lock-Free Context Leasing (`ArrayQueue<SendableWorker>`)**: Pre-initializes `pool_size` (default `4`) isolated `rquickjs::Runtime` + `rquickjs::Context` instances with a built-in `console` shim and evaluates the SSR bundle once per worker during pool construction.
2. **Pre-Parsed Component Expressions**: Component references are validated (`validate_component_name`) to prevent code injection and invoked inside `SsrPool::render` with the GIL released (`py.detach`).
3. **Concurrent `DashMap` TTL Cache**: When `cache_ttl_ms > 0`, `SsrPool::render` hashes `(component, props_json)`. Cache hits return rendered HTML from a sharded `DashMap` without acquiring a QuickJS worker; expired entries are evicted lazily or when capacity reaches `max_cache_entries`.

## 3. Build Orchestration & External Dev Server Fallback (`_orchestrator.py`, `_fallback.py`)

[`BuildOrchestrator`](../../../../../src/py/vertebra/builder/_orchestrator.py) coordinates four stages during `vtb js build` and `vtb dev`:

1. Extract backend Python route and model schemas ([`TypeExtractor`](../../../../../src/py/vertebra/builder/_codegen/_extractor.py)).
2. Write `.d.ts`, Zod schemas, and route manifest ([`TypeScriptGenerator`](../../../../../src/py/vertebra/builder/_codegen/_typescript.py), [`ZodGenerator`](../../../../../src/py/vertebra/builder/_codegen/_zod.py), [`ManifestGenerator`](../../../../../src/py/vertebra/builder/_codegen/_manifest.py)).
3. Generate browser `<script type="importmap">` vendor maps ([`_import_map.py`](../../../../../src/py/vertebra/builder/_import_map.py)).
4. Run `Bundler` (or delegate to [`DevServerProcess`](../../../../../src/py/vertebra/builder/_fallback.py) when a project uses an external Vite/Bun dev server fallback).
