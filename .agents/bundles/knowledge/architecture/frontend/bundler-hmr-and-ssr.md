---
type: Guide
title: "Oxc Bundler, HMR Watcher & QuickJS SSR Pool"
description: "How the vertebra-bundler PyO3 extension transforms TypeScript, watches files, and renders SSR in pooled QuickJS slots, and how BuildOrchestrator drives it."
status: stable
tags:
  - architecture
  - frontend
  - bundler
  - hmr
  - ssr
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-vertebra-bundler-src-lib-rs
    resource: src/rs/vertebra-bundler/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/lib.rs)"
  - id: src-vertebra-bundler-src-bundler-rs
    resource: src/rs/vertebra-bundler/src/bundler.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/bundler.rs)"
  - id: src-vertebra-bundler-src-config-rs
    resource: src/rs/vertebra-bundler/src/config.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/config.rs)"
  - id: src-vertebra-bundler-src-css-rs
    resource: src/rs/vertebra-bundler/src/css.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/css.rs)"
  - id: src-vertebra-bundler-src-hmr-rs
    resource: src/rs/vertebra-bundler/src/hmr.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/hmr.rs)"
  - id: src-vertebra-bundler-src-ssr-rs
    resource: src/rs/vertebra-bundler/src/ssr.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/ssr.rs)"
  - id: src-vertebra-bundler-src-declarations-rs
    resource: src/rs/vertebra-bundler/src/declarations.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/declarations.rs)"
  - id: src-vertebra-bundler-src-errors-rs
    resource: src/rs/vertebra-bundler/src/errors.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/errors.rs)"
  - id: src-bundler-pyi
    resource: src/py/vertebra/_bundler.pyi
    title: "Python implementation module (src/py/vertebra/_bundler.pyi)"
  - id: src-build-orchestrator-py
    resource: src/py/vertebra/build/_orchestrator.py
    title: "Python implementation module (src/py/vertebra/build/_orchestrator.py)"
  - id: src-build-config-py
    resource: src/py/vertebra/build/_config.py
    title: "Python implementation module (src/py/vertebra/build/_config.py)"
  - id: src-build-fallback-py
    resource: src/py/vertebra/build/_fallback.py
    title: "Python implementation module (src/py/vertebra/build/_fallback.py)"
  - id: src-build-import-map-py
    resource: src/py/vertebra/build/_import_map.py
    title: "Python implementation module (src/py/vertebra/build/_import_map.py)"
  - id: src-framework-ssr-init-py
    resource: src/py/vertebra/framework/ssr/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/ssr/__init__.py)"
---

# Oxc Bundler, HMR Watcher & QuickJS SSR Pool

<!-- truth: start -->
- `vertebra._bundler` (`#[pymodule(gil_used = false)]`) detaches the GIL (`py.detach`) across `Bundler.build`, `SSRPool.render`, and `IsolatedDeclarationsGenerator`.
- `Bundler.build` performs single-entry Oxc TypeScript/JSX transforms without import bundling; `BuildOrchestrator` coordinates codegen, `load_client_subpaths()` import maps, and native `_bundler` builds loaded via `ensure_module("vertebra._bundler")`.
- `SSRPool` pools QuickJS contexts with generation-tagged reloads and bounded render caching, returning `PoolExhausted` immediately when all slots are busy.
<!-- truth: end -->

[`vertebra-bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs)[^src-vertebra-bundler-src-lib-rs] builds the `_bundler` PyO3 module (`crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)]`, module constant `GIL_USED = False`). [`vertebra/_bundler.pyi`](../../../../../src/py/vertebra/_bundler.pyi)[^src-bundler-pyi] declares the stub types for `Bundler`, `BundleConfig`, `DevConfig`, `BuildResult`, `OutputFile`, `SourceWatcher`, `SSRPool`, `SSRResult`, `IsolatedDeclarationsGenerator`, `minify_css`, and `GIL_USED` as `vertebra._bundler`. When the native `_bundler` extension is not compiled, `AssetBundler` raises `UnsupportedBackendError` rather than emitting stub bundle files.

## Single-entry transform (`bundler.rs`, `config.rs`)

`Bundler.build(config)` releases the GIL (`py.detach`) and runs `build_internal`:

1. Read the entry file only. Imports are not resolved or inlined.
2. Parse with `oxc_parser`, build scoping with `oxc_semantic::SemanticBuilder`, strip TypeScript/JSX with `oxc_transformer` (`TransformOptions::default()`), and emit with `oxc_codegen` (`CodegenOptions { minify, .. }`).
3. Write `<outdir>/<entry_stem>.js` and return `BuildResult { outputs: [OutputFile], duration_ms, warnings }`.

There is no source map output, content hashing, asset manifest, or `oxc_minifier`. `BundleConfig` defaults: `outdir="dist"`, `minify=True`, `sourcemap=False`, `target="es2022"`; `build_internal` reads `entry`, `outdir`, and `minify`.

Other `Bundler` methods: `transform_file(path, source)` (GIL released), `check_file(path, source)` (parse errors), `start_dev(DevConfig)` / `stop_dev()`, `poll_events(timeout_ms=None)`, `notify_type_change(version=None)`, `notify_reload()`, `hmr_url()` (`ws://localhost:{port}/hmr`), and `is_dev_mode()`. `DevConfig` defaults: `watch_dirs=["src"]`, `hmr_port=5173`, `extensions=["ts","tsx","js","jsx"]`, `debounce_ms=100`. Errors map to `BundlerError` (`Io`, `Parse`, `Transform`, `Watcher`, `Ssr`, `PoolExhausted`) and surface in Python as `RuntimeError`.

## File watcher (`hmr.rs`)

`SourceWatcher` wraps a `rolldown-notify-debouncer-mini` debouncer over `RecommendedWatcher`. It filters events by extension and sends `HmrEvent::Update { path, timestamp }` on a tokio broadcast channel of capacity 100. `HmrEvent` serializes with `#[serde(tag = "type", rename_all = "lowercase")]` as `update`, `types`, `reload`, or `connected`.

The Python class (`PySourceWatcher`, exported as `SourceWatcher`) takes `(watch_dirs, extensions=None, debounce_ms=100)` and exposes `start()`, `stop()`, `poll_events(timeout_ms=None)` (drain of updated paths with optional blocking timeout in milliseconds), `notify_type_change(version)`, and `notify_reload()`. `HmrEvent` values stay in-process; the browser HMR socket is served by `vertebra-http` (see [Bundler & Client SDK](../../domains/frontend/bundler-and-client-sdk.md)).

## SSR pool (`ssr.rs`)

`SSRPool(bundle_path, pool_size=4, timeout_ms=1000)` builds `pool_size.max(1)` slots. Each slot is a `Mutex<Option<SlotEngine>>` holding its own `rquickjs::Runtime` + `Context::full`, a `console.log` shim, and the evaluated bundle.

- **Slot leasing**: `available: ArrayQueue<usize>` holds free slot indices. `render` pops one and returns `BundlerError::PoolExhausted` immediately when none is free; it does not wait.
- **Render contract**: the bundle must set `globalThis.__ssr_render(component, props)` returning `{ head: string[], body: string }`. The component name is a call argument; props are parsed in-engine with `JSON.parse(page_json)`.
- **Cache**: always on. Key is FNV-1a 64 over `component + ":" + page_json`; a hit requires the same pool `generation` and age under a fixed 60 s TTL (`cache_len` exposes the entry count).
- **Reload**: `invalidate()` re-reads the bundle, increments `generation`, and clears the cache; stale slots rebuild on next checkout. `shutdown()` clears slots and cache.

`SSRPool.render(component, page_json)` releases the GIL and returns `SSRResult(head, body)`. [`SyncEmbeddedSSRClient` and `AsyncEmbeddedSSRClient`](../../../../../src/py/vertebra/framework/ssr/__init__.py)[^src-framework-ssr-init-py] wrap `SSRPool` directly, while `SSRFrame.encode` and `SSRFrame.decode` handle 4-byte big-endian length-prefixed JSON framing for sidecar IPC.

## Declarations and CSS minification (`declarations.rs`, `css.rs`)

- `IsolatedDeclarationsGenerator(strip_internal=True).emit_dts(filename, source_text)` emits `.d.ts` text from an Oxc parse (`declarations.rs`[^src-vertebra-bundler-src-declarations-rs]).
- `minify_css(css_source)` strips comments and collapses whitespace outside quoted strings (`css.rs`[^src-vertebra-bundler-src-css-rs]).

## Build orchestration (`build/_orchestrator.py`, `build/_import_map.py`)

[`BuildOrchestrator(project_dir, config=None, *, app_path=None, hmr_base_url=None)`](../../../../../src/py/vertebra/build/_orchestrator.py)[^src-build-orchestrator-py] loads [`BuildConfig`](../../../../../src/py/vertebra/build/_config.py)[^src-build-config-py] from `[tool.vertebra]` (or `[tool.vtb]`) in `pyproject.toml`: `entry`, `outdir="dist"`, `bundler="native"`, `dev-port`, `codegen=False`, `codegen-output=".vertebra/generated"`, `app`, and `js.{dependencies,dev-dependencies}`.

| Method | Callers | Stages |
| --- | --- | --- |
| `build()` | `vtb js build`, `vtb build --target frontend\|all` | Returns `False` without `entry`; otherwise codegen (if `codegen`), import map + wheel SDK sync, bundler. |
| `dev()` | `vtb dev` (background thread) | Picks a dev port (`dev-port` or first free in 5173–5199), then runs the same stages in watch mode (`bundler.poll_events(200)`). |
| `codegen_only(output_dir=None)` | `vtb js generate-types` | Codegen only. |

1. **Codegen**: `import_app(app)`, `RouteCollector().collect(app)`, then write `types.d.ts`, `schemas.zod.ts`, `manifest.json`, `routes.ts`, and `litestar-vite.json` / `asyncapi.json` when an app plugin or state value exposes `export_vite_metadata` / `export_asyncapi_schema`. Skipped with a warning when `app` is unset. See [Codegen & Inertia](codegen-and-inertia.md).
2. **Import map & wheel SDK sync**: [`load_client_subpaths()`](../../../../../src/py/vertebra/build/_import_map.py)[^src-build-import-map-py] derives `@vertebra/client` and all subpath entries (`./vite`, `./react`, `./vue`, `./svelte`, `./angular`, `./htmx`, `./store`, `./hmr`, `./wasm`, `./arrow`, `./live`, `./apex`, `./telemetry`, `./router`, `./webmcp`, `./agent`) dynamically from bundled `client_dist/package.json` or repo `src/ts/vertebra-client/package.json`, `ImportMapGenerator.sync_wheel_client(project_dir)` copies wheel-bundled `client_dist/` assets into `.vertebra/vendor/@vertebra/client/`, and `ImportMapGenerator.write` writes `.vertebra/import_map.json` mapping `@vertebra/client*` subpaths alongside `npm:<name>@<version>` dependencies.
3. **Bundler**: `BuildOrchestrator._run_native` and `_dev_native` resolve the extension via `ensure_module("vertebra._bundler")` (`MissingDependencyError` triggers the Vite fallback). Production uses `BundleConfig(entry, outdir, minify=True, sourcemap=True)`; dev uses `DevConfig(entry, watch_dirs=[<project>/src, <app_dir>], hmr_port=<port>, extensions=["ts","tsx","js","jsx","py"])` and `start_dev`. When `bundler != "native"` or `_bundler` is unavailable, it falls back to Vite: [`DevServerProcess`](../../../../../src/py/vertebra/build/_fallback.py)[^src-build-fallback-py] and `ViteDevProxy` (`vite --port` or `npx vite --port`, sets `VERTEBRA_VITE_PROXY`) in dev, `run_vite_build` (`vite build`) in production.

Provenance sources: [^src-vertebra-bundler-src-bundler-rs], [^src-vertebra-bundler-src-config-rs], [^src-vertebra-bundler-src-hmr-rs], [^src-vertebra-bundler-src-ssr-rs], [^src-vertebra-bundler-src-errors-rs].

[^src-vertebra-bundler-src-lib-rs]: `src/rs/vertebra-bundler/src/lib.rs` — Rust implementation module (src/rs/vertebra-bundler/src/lib.rs).
[^src-vertebra-bundler-src-bundler-rs]: `src/rs/vertebra-bundler/src/bundler.rs` — Rust implementation module (src/rs/vertebra-bundler/src/bundler.rs).
[^src-vertebra-bundler-src-config-rs]: `src/rs/vertebra-bundler/src/config.rs` — Rust implementation module (src/rs/vertebra-bundler/src/config.rs).
[^src-vertebra-bundler-src-css-rs]: `src/rs/vertebra-bundler/src/css.rs` — Rust implementation module (src/rs/vertebra-bundler/src/css.rs).
[^src-vertebra-bundler-src-hmr-rs]: `src/rs/vertebra-bundler/src/hmr.rs` — Rust implementation module (src/rs/vertebra-bundler/src/hmr.rs).
[^src-vertebra-bundler-src-ssr-rs]: `src/rs/vertebra-bundler/src/ssr.rs` — Rust implementation module (src/rs/vertebra-bundler/src/ssr.rs).
[^src-vertebra-bundler-src-declarations-rs]: `src/rs/vertebra-bundler/src/declarations.rs` — Rust implementation module (src/rs/vertebra-bundler/src/declarations.rs).
[^src-vertebra-bundler-src-errors-rs]: `src/rs/vertebra-bundler/src/errors.rs` — Rust implementation module (src/rs/vertebra-bundler/src/errors.rs).
[^src-bundler-pyi]: `src/py/vertebra/_bundler.pyi` — Python implementation module (src/py/vertebra/_bundler.pyi).
[^src-build-orchestrator-py]: `src/py/vertebra/build/_orchestrator.py` — Python implementation module (src/py/vertebra/build/_orchestrator.py).
[^src-build-config-py]: `src/py/vertebra/build/_config.py` — Python implementation module (src/py/vertebra/build/_config.py).
[^src-build-fallback-py]: `src/py/vertebra/build/_fallback.py` — Python implementation module (src/py/vertebra/build/_fallback.py).
[^src-build-import-map-py]: `src/py/vertebra/build/_import_map.py` — Python implementation module (src/py/vertebra/build/_import_map.py).
[^src-framework-ssr-init-py]: `src/py/vertebra/framework/ssr/__init__.py` — Python implementation module (src/py/vertebra/framework/ssr/__init__.py).
