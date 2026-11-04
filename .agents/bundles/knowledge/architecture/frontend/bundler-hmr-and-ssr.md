---
type: Guide
title: "Oxc Bundler, HMR Watcher & QuickJS SSR Pool"
description: "How the vertebra-bundler PyO3 extension transforms TypeScript, watches files, and renders SSR in pooled QuickJS slots, and how BuildOrchestrator drives it."
status: stable
tags: [architecture, frontend, bundler, hmr, ssr]
sources:
  - resource: src/rs/vertebra-bundler/src/lib.rs
  - resource: src/rs/vertebra-bundler/src/bundler.rs
  - resource: src/rs/vertebra-bundler/src/config.rs
  - resource: src/rs/vertebra-bundler/src/hmr.rs
  - resource: src/rs/vertebra-bundler/src/ssr.rs
  - resource: src/rs/vertebra-bundler/src/declarations.rs
  - resource: src/rs/vertebra-bundler/src/errors.rs
  - resource: src/py/vertebra/_bundler.pyi
  - resource: src/py/vertebra/build/_orchestrator.py
  - resource: src/py/vertebra/build/_config.py
  - resource: src/py/vertebra/build/_fallback.py
  - resource: src/py/vertebra/build/_import_map.py
  - resource: src/py/vertebra/framework/ssr/__init__.py
---

# Oxc Bundler, HMR Watcher & QuickJS SSR Pool

[`vertebra-bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs) builds the `_bundler` PyO3 module (`crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)]`, module constant `GIL_USED = False`). [`vertebra/_bundler.pyi`](../../../../../src/py/vertebra/_bundler.pyi) declares the stub types for `Bundler`, `BundleConfig`, `DevConfig`, `BuildResult`, `OutputFile`, `SourceWatcher`, `SSRPool`, `SSRResult`, `IsolatedDeclarationsGenerator`, `minify_css`, `encode_ssr_worker_frame`, and `GIL_USED` as `vertebra._bundler`. When the native `_bundler` extension is not compiled, `AssetBundler` raises `UnsupportedBackendError` rather than emitting stub bundle files.

## Single-entry transform (`bundler.rs`, `config.rs`)

`Bundler.build(config)` releases the GIL (`py.detach`) and runs `build_internal`:

1. Read the entry file only. Imports are not resolved or inlined.
2. Parse with `oxc_parser`, build scoping with `oxc_semantic::SemanticBuilder`, strip TypeScript/JSX with `oxc_transformer` (`TransformOptions::default()`), and emit with `oxc_codegen` (`CodegenOptions { minify, .. }`).
3. Write `<outdir>/<entry_stem>.js` and return `BuildResult { outputs: [OutputFile], duration_ms, warnings }`.

There is no source map output, content hashing, asset manifest, or `oxc_minifier`. `BundleConfig` defaults: `outdir="dist"`, `minify=True`, `sourcemap=False`, `target="es2022"`, `external=[]`, `define={}`, `format="esm"`, `tree_shaking=True`, `splitting=False`; `build_internal` reads only `entry`, `outdir`, and `minify`.

Other `Bundler` methods: `transform_file(path, source)` (GIL released), `check_file(path, source)` (parse errors), `start_dev(DevConfig)` / `stop_dev()`, `notify_type_change(version)`, `notify_reload()`, `hmr_url()` (`ws://localhost:{port}/hmr`), `is_dev_mode()`, and the SSR methods below. `DevConfig` defaults: `watch_dirs=["src"]`, `hmr_port=5173`, `extensions=["ts","tsx","js","jsx"]`, `debounce_ms=100`. Errors map to `BundlerError` (`Io`, `Parse`, `Transform`, `Watcher`, `Ssr`, `PoolExhausted`) and surface in Python as `RuntimeError`.

## File watcher (`hmr.rs`)

`SourceWatcher` wraps a `rolldown-notify-debouncer-mini` debouncer over `RecommendedWatcher`. It filters events by extension and sends `HmrEvent::Update { path, timestamp }` on a tokio broadcast channel of capacity 100. `HmrEvent` serializes with `#[serde(tag = "type", rename_all = "lowercase")]` as `update`, `types`, `reload`, or `connected`.

The Python class (`PySourceWatcher`, exported as `SourceWatcher`) takes `(watch_dirs, extensions=["py","ts"], debounce_ms=100)` and exposes `start()`, `stop()`, `poll_events()` (non-blocking drain of updated paths, takes no arguments), `notify_type_change(version)`, and `notify_reload()`. `HmrEvent` values stay in-process; the browser HMR socket is served by `vertebra-http` (see [Bundler & Client SDK](../../domains/frontend/bundler-and-client-sdk.md)).

## SSR pool (`ssr.rs`)

`SSRPool::new(bundle_path, pool_size)` builds `pool_size.max(1)` slots (Python default `4`). Each slot is a `Mutex<Option<SlotEngine>>` holding its own `rquickjs::Runtime` + `Context::full`, a `console.log` shim, and the evaluated bundle.

- **Slot leasing**: `available: ArrayQueue<usize>` holds free slot indices. `render` pops one and returns `BundlerError::PoolExhausted` immediately when none is free; it does not wait.
- **Render contract**: the bundle must set `globalThis.__ssr_render(component, props)` returning `{ head: string[], body: string }`. The component name is a call argument; props are parsed in-engine with `JSON.parse(page_json)`.
- **Cache**: always on. Key is FNV-1a 64 over `component + ":" + page_json`; a hit requires the same pool `generation` and age under a fixed 60 s TTL. Entries are never evicted individually and the map has no size bound.
- **Reload**: `invalidate()` re-reads the bundle, increments `generation`, and clears the cache; stale slots rebuild on next checkout. `shutdown()` clears slots and cache.

`SSRPool.render` and `Bundler.ssr_render(component, page_json)` release the GIL. `Bundler.init_ssr(bundle_path, pool_size)`, `ssr_invalidate(paths)` (clears everything; `paths` is ignored), and `ssr_shutdown()` manage a pool owned by the `Bundler`. [`SyncEmbeddedSSRClient` and `AsyncEmbeddedSSRClient`](../../../../../src/py/vertebra/framework/ssr/__init__.py) wrap these methods.

## Declarations, CSS, and SSR framing (`declarations.rs`)

- `IsolatedDeclarationsGenerator(strip_internal=True).emit_dts(filename, source_text)` emits `.d.ts` text from an Oxc parse.
- `minify_css(css_source)` strips comments and collapses whitespace outside strings.
- `encode_ssr_worker_frame(component, props_json)` returns a 4-byte big-endian length prefix plus JSON, the same framing as Python `SSRFrame.to_wire_bytes()`.

## Build orchestration (`build/_orchestrator.py`)

[`BuildOrchestrator(project_dir, config=None, *, app_path=None, hmr_base_url=None)`](../../../../../src/py/vertebra/build/_orchestrator.py) loads [`BuildConfig`](../../../../../src/py/vertebra/build/_config.py) from `[tool.vertebra]` (or `[tool.vtb]`) in `pyproject.toml`: `entry`, `outdir="dist"`, `bundler="native"`, `dev-port`, `codegen=False`, `codegen-output=".vertebra/generated"`, `app`, and `js.{dependencies,dev-dependencies}`.

| Method | Callers | Stages |
| --- | --- | --- |
| `build()` | `vtb js build`, `vtb build --target frontend\|all` | Returns `False` without `entry`; otherwise codegen (if `codegen`), import map (if `js.dependencies`), bundler. |
| `dev()` | `vtb dev` (background thread) | Picks a dev port (`dev-port` or first free in 5173–5199), then the same stages in dev mode. |
| `codegen_only(output_dir=None)` | `vtb js generate-types` | Codegen only. |

1. **Codegen**: `import_app(app)`, `RouteCollector().collect(app)`, then write `types.d.ts`, `schemas.zod.ts`, `manifest.json`, `routes.ts`, and `litestar-vite.json` / `asyncapi.json` when an app plugin or state value exposes `export_vite_metadata` / `export_asyncapi_schema`. Skipped with a warning when `app` is unset. See [Codegen & Inertia](codegen-and-inertia.md).
2. **Import map**: [`ImportMapGenerator.write`](../../../../../src/py/vertebra/build/_import_map.py) writes `.vertebra/import_map.json` mapping each dependency spec to `npm:<name>@<version>`.
3. **Bundler**: `importlib.import_module("vertebra._bundler")`. Production uses `BundleConfig(entry, outdir, minify=True, sourcemap=True)`; dev uses `DevConfig(entry, watch_dirs=[<project>/src], hmr_port=<port>)` and `start_dev`. When `bundler != "native"` or the import raises `ImportError`, it falls back to Vite: [`DevServerProcess`](../../../../../src/py/vertebra/build/_fallback.py) and `ViteDevProxy` (`vite --port` or `npx vite --port`, sets `VERTEBRA_VITE_PROXY`) in dev, `run_vite_build` (`vite build`) in production.
