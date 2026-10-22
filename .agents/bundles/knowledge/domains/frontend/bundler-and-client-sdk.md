---
type: Guide
title: "Frontend Bundler, Codegen & Client SDK Domain"
description: "Domain vocabulary for the _bundler extension, the vertebra.build AssetBundler and codegen types, the dev HMR channels, and the @vertebra/client classes."
status: stable
tags: [domains, frontend, bundler, codegen, typescript]
sources:
  - resource: src/rs/vertebra-bundler/src/lib.rs
  - resource: src/py/vertebra/_bundler.py
  - resource: src/py/vertebra/build/__init__.py
  - resource: src/py/vertebra/build/_orchestrator.py
  - resource: src/py/vertebra/framework/dev.py
  - resource: src/py/vertebra/build/codegen/_extractor.py
  - resource: src/py/vertebra/build/codegen/plugin.py
  - resource: src/rs/vertebra-http/src/hmr.rs
  - resource: src/ts/vertebra-client/src/index.ts
  - resource: src/ts/vertebra-client/src/client/client.ts
  - resource: src/ts/vertebra-client/src/client/retry.ts
  - resource: src/ts/vertebra-client/src/react/cache.ts
  - resource: src/ts/vertebra-client/src/hmr/index.ts
---

# Frontend Bundler, Codegen & Client SDK Domain

The frontend surface spans the [`_bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs) PyO3 extension, the [`vertebra.build`](../../../../../src/py/vertebra/build/__init__.py) package, and the [`@vertebra/client`](../../../../../src/ts/vertebra-client/src/index.ts) TypeScript package. Mechanics live in [Bundler, HMR & SSR](../../architecture/frontend/bundler-hmr-and-ssr.md) and [Codegen & Inertia](../../architecture/frontend/codegen-and-inertia.md); packaging rules for the TS package live in [Client SDK Subpaths](../../patterns/typescript/client-and-adapter-subpaths.md).

## Rust bundler extension (`_bundler`)

| Symbol | Python import | Responsibility |
| --- | --- | --- |
| `Bundler`, `BundleConfig`, `DevConfig`, `BuildResult`, `OutputFile` | `vertebra._bundler` | Single-entry Oxc transform with optional minify, dev watcher control, and SSR pool ownership. |
| `SourceWatcher` (`PySourceWatcher`), `HmrEvent` | `_bundler` only | Debounced file watcher; Python drains changed paths with `poll_events()`. |
| `SsrPool`, `SsrResult` | `_bundler` only | QuickJS slot pool, fixed 60 s render cache, generation-based reload, `PoolExhausted` when all slots are busy. |
| `IsolatedDeclarationsGenerator`, `minify_css`, `encode_ssr_worker_frame` | `_bundler` only | `.d.ts` emission, CSS minification, 4-byte length-prefixed SSR frames. |
| `BundlerError` | raised as `RuntimeError` | `Io`, `Parse`, `Transform`, `Watcher`, `Ssr`, `PoolExhausted`. |

## Python build package (`vertebra.build`)

| Symbol | Module | Responsibility |
| --- | --- | --- |
| `BuildOrchestrator` | `build/_orchestrator.py` | Runs codegen, import map, and native bundler (Vite fallback) for `vtb js build`, `vtb dev`, `vtb build`, `vtb js generate-types`. |
| `BuildConfig`, `JsConfig` | `build/_config.py` | Frozen `msgspec.Struct(gc=False)` views of `[tool.vertebra]` / `[tool.vtb]`. |
| `DevServerProcess`, `run_vite_build` | `build/_fallback.py` | Vite dev server (`vite` or `npx vite`) and one-shot `run_vite_build`. |
| `ImportMapGenerator` | `build/_import_map.py` | `import_map.json` with `npm:<name>@<version>` specifiers. |
| `RouteCollector`, `RouteMetadata`, `TypeExtractor`, `TypeNode`, `TypeBridge` | `build/codegen/_extractor.py`, `framework/dev.py` | Route collection, type IR, and live dev type cache. |
| `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenAPIGenerator`, `ProtobufGenerator`, `ManifestGenerator`, `ProcedureClientGenerator` | `build/codegen/` | Codegen targets. |
| `CodegenPlugin`, `CodegenConfig`, `CodegenWatcher`, `HmrNotifier`, `HmrEventRouter`, `procedure`, `ProcedureRegistry` | `build/codegen/plugin.py`, `build/codegen/_watch.py`, `build/codegen/procedures.py` | Multi-target emission, debounced watcher, HMR signaling helpers, and `@procedure` registry. |
| `StandaloneBuilder`, `OnefileBuilder`, `BuildTarget` | `build/packaging/` | See [Packaging & PGO](packaging-and-pgo.md). |

## Dev HMR channels

- **Rust endpoint (active)**: when `VERTEBRA_DEV=1` (set by `vtb dev`), `vertebra-http` [`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs) serves `GET /__vertebra/hmr` (WebSocket upgrade or JSON status) and `POST /__vertebra/hmr/signal`. Events carry both `type` and `event` fields (`full-reload`, `types-update`). `HmrNotifier` posts to the signal path.
- **Browser client**: `HotReloadClient` (`src/hmr/index.ts`) defaults to `/__vertebra/hmr`, reconnects with [`computeBackoffDelay`](../../../../../src/ts/vertebra-client/src/client/retry.ts) (up to 20 attempts, 1 s base, 30 s cap), times out pings at 45 s, handles `connected`, `update`, `css-update`, `full-reload`, `ping` (replies `pong`), `types-update`, and `vertebra:types-changed`, and renders `#__vertebra_error_overlay`.
- **Python ASGI path**: [`DevMiddleware(app, type_bridge)`](../../../../../src/py/vertebra/framework/dev.py) serves `/__vertebra/types.d.ts`, `/types/version`, `/schemas.zod.ts`, `/routes.ts`, `/manifest.json`, `/client/hmr.js`, and a `/__vertebra/ws` socket fed by `broadcast_hmr_event`; it injects the HMR script into HTML responses. [`TypeBridge`](../../../../../src/py/vertebra/framework/dev.py) caches the generated files and versions them with a 12-char SHA-256.

## TypeScript SDK (`@vertebra/client`)

| Symbol | Source | Responsibility |
| --- | --- | --- |
| `HttpClient`, `ClientConfig`, `RetryConfig`, `RouteResponse` | `client/client.ts`, `client/types.ts` | `call`/`callWithResponse` with `{key}` path interpolation, per-call `{ headers, timeout }`, internal `AbortController` timeout (default 30 s), retries (`maxAttempts` 3, `initialDelay` 1000 ms, statuses 408/429/500/502/503/504) honoring `Retry-After`, and `subscribe()` via `WebSocketClient` at `/__vertebra/ws`. |
| `computeBackoffDelay` | `client/retry.ts` | Shared exponential delay with jitter (capped at 30 s). |
| `ClientError`, `NetworkError`, `TimeoutError`, `ServerError`, `ValidationError`, `throwParsedError` | `client/errors.ts` | Error hierarchy. |
| `QueryCache` | `react/cache.ts` | Query cache backing React hooks (`useQuery`, `useMutation`, `useInvalidateQueries`). |
| `SseClient` | `sse.ts` | EventSource client: 20 reconnects, 500 ms base delay, 1024 remembered idempotency keys (`BoundedLruSet`); reconnects append the last event id as a query parameter (`lastEventIdQueryParam`, default `lastEventId`). |
| `WebSocketClient`, `PubSubWireFrame` | `websocket.ts` | Pub/sub socket: 20 s heartbeat, 45 s timeout, per-topic last event id, 1024 idempotency keys (`BoundedLruSet`). |
| `SessionStore`, `store` | `store/index.ts` | Reactive store; keys prefixed `user:` persist to `localStorage` under `vtb:`. |
| `HotReloadClient` | `hmr/index.ts` | See above. |
| `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue` | `live/index.ts` | See [Live State](../framework/live-state.md). |
| `ArrowStreamClient` | `arrow.ts` | Holds an endpoint plus `Accept: application/vnd.apache.arrow.stream`; `hasIpcHeader(bytes)` detects Arrow stream/file headers. |
| `WasmClient` | `wasm.ts` | See [WASM](wasm.md). |
| `ApexRegion` | `apex/index.ts` | Oracle APEX region bridge (`readPageItem`, `writePageItem`, `triggerRegionEvent`). |
| `TraceContext` | `telemetry.ts` | W3C `traceparent` parse/format with random trace and span ids. |

The CLI `Console` and command set are covered in [CLI](../framework/cli.md).
