---
type: Guide
title: "Frontend Bundler, Codegen & Client SDK Domain"
description: "Domain vocabulary for the _bundler extension, the vertebra.build BuildOrchestrator and codegen types, the dev HMR channels, and the @vertebra/client SDK subpaths."
status: stable
tags:
  - domains
  - frontend
  - bundler
  - codegen
  - typescript
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:02:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-vertebra-bundler-src-lib-rs
    resource: src/rs/vertebra-bundler/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-bundler/src/lib.rs)"
  - id: src-bundler-pyi
    resource: src/py/vertebra/_bundler.pyi
    title: "Python implementation module (src/py/vertebra/_bundler.pyi)"
  - id: src-build-init-py
    resource: src/py/vertebra/build/__init__.py
    title: "Python implementation module (src/py/vertebra/build/__init__.py)"
  - id: src-build-orchestrator-py
    resource: src/py/vertebra/build/_orchestrator.py
    title: "Python implementation module (src/py/vertebra/build/_orchestrator.py)"
  - id: src-build-import-map-py
    resource: src/py/vertebra/build/_import_map.py
    title: "Python implementation module (src/py/vertebra/build/_import_map.py)"
  - id: src-framework-dev-py
    resource: src/py/vertebra/framework/dev.py
    title: "Python implementation module (src/py/vertebra/framework/dev.py)"
  - id: src-build-codegen-extractor-py
    resource: src/py/vertebra/build/codegen/_extractor.py
    title: "Python implementation module (src/py/vertebra/build/codegen/_extractor.py)"
  - id: src-build-codegen-plugin-py
    resource: src/py/vertebra/build/codegen/plugin.py
    title: "Python implementation module (src/py/vertebra/build/codegen/plugin.py)"
  - id: src-vertebra-http-src-hmr-rs
    resource: src/rs/vertebra-http/src/hmr.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/hmr.rs)"
  - id: src-src-index-ts
    resource: src/ts/vertebra-client/src/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/index.ts)"
  - id: src-src-client-client-ts
    resource: src/ts/vertebra-client/src/client/client.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/client/client.ts)"
  - id: src-src-client-retry-ts
    resource: src/ts/vertebra-client/src/client/retry.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/client/retry.ts)"
  - id: src-src-react-cache-ts
    resource: src/ts/vertebra-client/src/react/cache.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/react/cache.ts)"
  - id: src-src-react-index-tsx
    resource: src/ts/vertebra-client/src/react/index.tsx
    title: "TypeScript React adapter module (src/ts/vertebra-client/src/react/index.tsx)"
  - id: src-src-svelte-index-ts
    resource: src/ts/vertebra-client/src/svelte/index.ts
    title: "TypeScript Svelte store adapter module (src/ts/vertebra-client/src/svelte/index.ts)"
  - id: src-src-htmx-index-ts
    resource: src/ts/vertebra-client/src/htmx/index.ts
    title: "TypeScript HTMX adapter module (src/ts/vertebra-client/src/htmx/index.ts)"
  - id: src-src-vite-index-ts
    resource: src/ts/vertebra-client/src/vite/index.ts
    title: "TypeScript Vite plugin module (src/ts/vertebra-client/src/vite/index.ts)"
  - id: src-src-hmr-index-ts
    resource: src/ts/vertebra-client/src/hmr/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/hmr/index.ts)"
  - id: src-src-router-ts
    resource: src/ts/vertebra-client/src/router.ts
    title: "TypeScript client router module (src/ts/vertebra-client/src/router.ts)"
  - id: src-src-webmcp-ts
    resource: src/ts/vertebra-client/src/webmcp.ts
    title: "TypeScript WebMCP client module (src/ts/vertebra-client/src/webmcp.ts)"
  - id: src-src-agent-index-ts
    resource: src/ts/vertebra-client/src/agent/index.ts
    title: "TypeScript agent SDK module (src/ts/vertebra-client/src/agent/index.ts)"
---

# Frontend Bundler, Codegen & Client SDK Domain

<!-- truth: start -->
- `vertebra._bundler` (`Bundler`, `SourceWatcher`, `SSRPool`), `vertebra.build`, and `@vertebra/client` (`17` subpath exports including `./router`, `./webmcp`, and `./agent`) form Vertebra's integrated frontend transform, HMR, SSR, and typed client SDK surface.
- `tsup.config.ts` stages built `@vertebra/client` bundles into `src/py/vertebra/server/_js/bundle/` for wheel distribution, with `DevMiddleware`, `load_client_subpaths()`, and `get_vertebra_client_vite_aliases()` falling back to `src/ts/vertebra-client/` in source checkouts.
- `RouteRegistry` (`Record<string, RouteDefinition>`), `routeUrl`, and `wsUrl` drive typed HTTP/WebSocket URL construction, `QueryCache` keeps `staleTime: 0` Suspense queries epoch-stable until `invalidate()`, `svelte/store` backs `@vertebra/client/svelte` without runes, HTMX guards `detail` and form encodings, and WebMCP `requiresConfirmation` fails closed via `WebMCPConfirmationHandler` (`onConfirm`).
<!-- truth: end -->

The frontend surface spans the [`_bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs)[^src-vertebra-bundler-src-lib-rs] PyO3 extension, the [`vertebra.build`](../../../../../src/py/vertebra/build/__init__.py)[^src-build-init-py] package, and the [`@vertebra/client`](../../../../../src/ts/vertebra-client/src/index.ts)[^src-src-index-ts] TypeScript package. Mechanics live in [Bundler, HMR & SSR](../../architecture/frontend/bundler-hmr-and-ssr.md) and [Codegen & Inertia](../../architecture/frontend/codegen-and-inertia.md); packaging rules for the TS package live in [Client SDK Subpaths](../../patterns/typescript/client-and-adapter-subpaths.md).

## Rust bundler extension (`_bundler`)

| Symbol | Python import | Responsibility |
| --- | --- | --- |
| `Bundler`, `BundleConfig`, `DevConfig`, `BuildResult`, `OutputFile` | `vertebra._bundler` | Single-entry Oxc transform with optional minify and dev watcher control (`start_dev`, `stop_dev`, `poll_events`). |
| `SourceWatcher` (`PySourceWatcher`), `HmrEvent` | `_bundler` only | Debounced file watcher; Python drains changed paths with `poll_events(timeout_ms=None)`. |
| `SSRPool`, `SSRResult` | `_bundler` only | QuickJS slot pool, fixed 60 s render cache, generation-based reload, `PoolExhausted` when all slots are busy. |
| `IsolatedDeclarationsGenerator`, `minify_css` | `_bundler` only | `.d.ts` isolated declaration emission and CSS comment/whitespace minification. |
| `BundlerError` | raised as `RuntimeError` | `Io`, `Parse`, `Transform`, `Watcher`, `Ssr`, `PoolExhausted`. |

## Python build package (`vertebra.build`)

| Symbol | Module | Responsibility |
| --- | --- | --- |
| `BuildOrchestrator` | `build/_orchestrator.py` | Runs codegen, import map, and native bundler (`ensure_module("vertebra._bundler")`, Vite fallback) for `vtb js build`, `vtb dev`, `vtb build`, `vtb js generate-types`. |
| `BuildConfig`, `JsConfig` | `build/_config.py` | Frozen `msgspec.Struct(gc=False)` views of `[tool.vertebra]` / `[tool.vtb]`. |
| `DevServerProcess`, `run_vite_build` | `build/_fallback.py` | Vite dev server (`vite` or `npx vite`) and one-shot `run_vite_build`. |
| `ImportMapGenerator`, `load_client_subpaths` | [`build/_import_map.py`](../../../../../src/py/vertebra/build/_import_map.py)[^src-build-import-map-py] | Derives `@vertebra/client*` subpaths dynamically from `package.json`, syncs wheel `server/_js/bundle/` into `<output_dir>/client/`, and writes `import_map.json` with `npm:<name>@<version>` specifiers. |
| `RouteCollector`, `RouteMetadata`, `TypeExtractor`, `TypeNode`, `TypeBridge` | `build/codegen/_extractor.py`, `framework/dev.py` | Route collection, type IR, and live dev type cache. |
| `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenAPIGenerator`, `ProtobufGenerator`, `ProtobufParser`, `ManifestGenerator`, `ProcedureClientGenerator` | `build/codegen/` | Codegen targets. |
| `CodegenPlugin`, `CodegenConfig`, `CodegenWatcher`, `HmrNotifier`, `HmrEventRouter`, `procedure`, `ProcedureRegistry` | `build/codegen/plugin.py`, `build/codegen/procedures.py` | Multi-target emission, debounced watcher, HMR signaling helpers, and `@procedure` registry. |
| `StandaloneBuilder`, `OnefileBuilder`, `BuildTarget` | `build/packaging/` | See [Packaging & PGO](packaging-and-pgo.md). |

## Dev HMR channels

- **Rust endpoint (active)**: when `VERTEBRA_DEV=1` (set by `vtb dev`), `vertebra-http` [`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs)[^src-vertebra-http-src-hmr-rs] serves `GET /__vertebra/hmr` (WebSocket upgrade or JSON status) and `POST /__vertebra/hmr/signal`. Events carry both `type` and `event` fields (`full-reload`, `types-update`). `HmrNotifier` posts to the signal path.
- **Browser client**: `HotReloadClient` (`src/hmr/index.ts`) defaults to `/__vertebra/hmr`, reconnects with [`computeBackoffDelay`](../../../../../src/ts/vertebra-client/src/client/retry.ts)[^src-src-client-retry-ts] (up to 20 attempts, 1 s base, 30 s cap), times out pings at 45 s, handles `connected`, `update`, `css-update`, `full-reload`, `ping` (replies `pong`), `types-update`, and `vertebra:types-changed`, and renders `#__vertebra_error_overlay`.
- **Python ASGI path**: [`DevMiddleware(app, type_bridge)`](../../../../../src/py/vertebra/framework/dev.py)[^src-framework-dev-py] serves `/__vertebra/types.d.ts`, `/types/version`, `/schemas.zod.ts`, `/routes.ts`, `/manifest.json`, `/client/hmr.js`, and a `/__vertebra/ws` socket fed by `broadcast_hmr_event`; it injects the HMR script into HTML responses. [`TypeBridge`](../../../../../src/py/vertebra/framework/dev.py) caches the generated files and versions them with a 12-char SHA-256.

## TypeScript SDK (`@vertebra/client`)

| Symbol | Subpath / Source | Responsibility |
| --- | --- | --- |
| `HttpClient`, `ClientConfig`, `RetryConfig`, `RouteResponse` | `.` (`client/client.ts`, `client/types.ts`) | `call`/`callWithResponse` with `{key}` path interpolation, per-call `{ headers, timeout }`, internal `AbortController` timeout (default 30 s), retries (`maxAttempts` 3, `initialDelay` 1000 ms, statuses 408/429/500/502/503/504) honoring `Retry-After`, and `subscribe()` via `WebSocketClient` at `/__vertebra/ws`. |
| `computeBackoffDelay` | `client/retry.ts` | Shared exponential delay with jitter (capped at 30 s). |
| `ClientError`, `NetworkError`, `TimeoutError`, `ServerError`, `ValidationError`, `throwParsedError` | `.` (`client/errors.ts`) | Error hierarchy. |
| `QueryCache`, `useQuery`, `useMutation` | `./react` ([`react/cache.ts`](../../../../../src/ts/vertebra-client/src/react/cache.ts), [`react/index.tsx`](../../../../../src/ts/vertebra-client/src/react/index.tsx)[^src-src-react-index-tsx]) | Render-epoch `QueryCache` backing React 19 `use()` hooks (`useQuery`, `useMutation`, `useInvalidateQueries`). `getOrCreate` returns the settled promise within the active epoch even when `staleTime: 0` until `invalidate()` advances `baseEpoch` or per-key `epochs` and triggers `useSyncExternalStore` re-renders. |
| `setClient`, `getClient`, `createQuery`, `createMutation` | `./svelte` ([`svelte/index.ts`](../../../../../src/ts/vertebra-client/src/svelte/index.ts)[^src-src-svelte-index-ts]) | `svelte/store` (`writable`, `derived`, `Readable`) query and mutation stores without Svelte 5 runes, safe for plain `.ts` compilation. |
| `initHtmx` | `./htmx` ([`htmx/index.ts`](../../../../../src/ts/vertebra-client/src/htmx/index.ts)[^src-src-htmx-index-ts]) | Guards `htmx:configRequest` / `htmx:responseError` on non-null object `event.detail` and `detail.headers`, and skips setting `Content-Type: application/json` when `hasFormEncoding` detects `multipart/form-data` or `application/x-www-form-urlencoded` on headers, `detail.encoding`, `hx-encoding`/`enctype`, or ancestor `[hx-encoding], form[enctype]`. |
| `vitePlugin` | `./vite` ([`vite/index.ts`](../../../../../src/ts/vertebra-client/src/vite/index.ts)[^src-src-vite-index-ts]) | Resolves `virtual:vertebra-types` by fetching `/__vertebra/manifest.json` (`routes` -> `{ method, path }`, `websocketRoutes` -> `{ method: "WS", path }`) and polls `/__vertebra/types/version` for HMR invalidation. |
| `SseClient` | `.` (`sse.ts`) | EventSource client: 20 reconnects, 500 ms base delay, 1024 remembered idempotency keys (`BoundedLruSet`); reconnects append the last event id as a query parameter (`lastEventIdQueryParam`, default `lastEventId`). |
| `WebSocketClient`, `PubSubWireFrame` | `.` (`websocket.ts`) | Pub/sub socket: 20 s heartbeat, 45 s timeout, per-topic last event id, 1024 idempotency keys (`BoundedLruSet`). |
| `SessionStore`, `store` | `./store` (`store/index.ts`) | Reactive store; keys prefixed `user:` persist to `localStorage` under `vtb:`. |
| `HotReloadClient` | `./hmr` (`hmr/index.ts`) | Dev HMR socket client and error overlay (see above). |
| `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue` | `./live` (`live/index.ts`) | See [Live State](../framework/live-state.md). |
| `ArrowStreamClient` | `./arrow` (`arrow.ts`) | Holds an endpoint plus `Accept: application/vnd.apache.arrow.stream`; `hasIpcHeader(bytes)` detects Arrow stream/file headers. |
| `WASMClient`, `WASM_MAGIC_BYTES` | `./wasm` (`wasm.ts`) | See [WASM](wasm.md). |
| `ApexRegion` | `./apex` (`apex/index.ts`) | Oracle APEX region bridge (`readPageItem`, `writePageItem`, `triggerRegionEvent`). |
| `TraceContext` | `./telemetry` (`telemetry.ts`) | W3C `traceparent` parse/format/header injection with random trace and span ids. |
| [`RouteDefinition`, `RouteRegistry`, `routeUrl`, `wsUrl`, `interpolateRoutePath`](../../../../../src/ts/vertebra-client/src/router.ts)[^src-src-router-ts] | `./router` (`router.ts`) | `RouteRegistry` (`Readonly<Record<string, RouteDefinition>>`) plus typed HTTP and WebSocket (`ws:`/`wss:`) URL builders interpolating `{param}`, `{param:type}`, and `:param` placeholders plus query maps. |
| [`registerWebMCPTools`, `WebMCPConfirmationHandler`, `WebMCPRegistrationHandle`](../../../../../src/ts/vertebra-client/src/webmcp.ts)[^src-src-webmcp-ts] | `./webmcp` (`webmcp.ts`) | Fetches `/.well-known/webmcp.json` (or accepts an explicit `WebMCPManifest`), registers browser tools on `document.modelContext ?? navigator.modelContext` with `AbortSignal` disposal and HMR idempotency, enforces fail-closed `requiresConfirmation` gating (`onConfirm: WebMCPConfirmationHandler` or `globalThis.confirm`), and decodes responses via `parseMcpJsonRpcResponse`. |
| [`McpClient`, `A2aClient`, `AgentStreamClient`, `WorkflowLiveSubscriber`, `parseMcpJsonRpcResponse`](../../../../../src/ts/vertebra-client/src/agent/index.ts)[^src-src-agent-index-ts] | `./agent` (`agent/index.ts`) | MCP `2026-07-28` JSON-RPC/SSE client (`encodeMcpHeaderValue`, `MCP_PROTOCOL_VERSION`, `parseMcpJsonRpcResponse`), A2A `1.0` client with `ETag` `AgentCard` caching, streaming agent chat/confirmation client, and `LiveState`-backed workflow progress subscriber. |

The CLI `Console` and command set are covered in [CLI](../framework/cli.md).

Provenance sources: [^src-bundler-pyi], [^src-build-orchestrator-py], [^src-build-codegen-extractor-py], [^src-build-codegen-plugin-py], [^src-src-client-client-ts], [^src-src-react-cache-ts], [^src-src-hmr-index-ts].

[^src-vertebra-bundler-src-lib-rs]: `src/rs/vertebra-bundler/src/lib.rs` — Rust implementation module (src/rs/vertebra-bundler/src/lib.rs).
[^src-bundler-pyi]: `src/py/vertebra/_bundler.pyi` — Python implementation module (src/py/vertebra/_bundler.pyi).
[^src-build-init-py]: `src/py/vertebra/build/__init__.py` — Python implementation module (src/py/vertebra/build/__init__.py).
[^src-build-orchestrator-py]: `src/py/vertebra/build/_orchestrator.py` — Python implementation module (src/py/vertebra/build/_orchestrator.py).
[^src-build-import-map-py]: `src/py/vertebra/build/_import_map.py` — Python implementation module (src/py/vertebra/build/_import_map.py).
[^src-framework-dev-py]: `src/py/vertebra/framework/dev.py` — Python implementation module (src/py/vertebra/framework/dev.py).
[^src-build-codegen-extractor-py]: `src/py/vertebra/build/codegen/_extractor.py` — Python implementation module (src/py/vertebra/build/codegen/_extractor.py).
[^src-build-codegen-plugin-py]: `src/py/vertebra/build/codegen/plugin.py` — Python implementation module (src/py/vertebra/build/codegen/plugin.py).
[^src-vertebra-http-src-hmr-rs]: `src/rs/vertebra-http/src/hmr.rs` — Rust implementation module (src/rs/vertebra-http/src/hmr.rs).
[^src-src-index-ts]: `src/ts/vertebra-client/src/index.ts` — TypeScript client module (src/ts/vertebra-client/src/index.ts).
[^src-src-client-client-ts]: `src/ts/vertebra-client/src/client/client.ts` — TypeScript client module (src/ts/vertebra-client/src/client/client.ts).
[^src-src-client-retry-ts]: `src/ts/vertebra-client/src/client/retry.ts` — TypeScript client module (src/ts/vertebra-client/src/client/retry.ts).
[^src-src-react-cache-ts]: `src/ts/vertebra-client/src/react/cache.ts` — TypeScript client module (src/ts/vertebra-client/src/react/cache.ts).
[^src-src-react-index-tsx]: `src/ts/vertebra-client/src/react/index.tsx` — TypeScript React adapter module (src/ts/vertebra-client/src/react/index.tsx).
[^src-src-svelte-index-ts]: `src/ts/vertebra-client/src/svelte/index.ts` — TypeScript Svelte store adapter module (src/ts/vertebra-client/src/svelte/index.ts).
[^src-src-htmx-index-ts]: `src/ts/vertebra-client/src/htmx/index.ts` — TypeScript HTMX adapter module (src/ts/vertebra-client/src/htmx/index.ts).
[^src-src-vite-index-ts]: `src/ts/vertebra-client/src/vite/index.ts` — TypeScript Vite plugin module (src/ts/vertebra-client/src/vite/index.ts).
[^src-src-hmr-index-ts]: `src/ts/vertebra-client/src/hmr/index.ts` — TypeScript client module (src/ts/vertebra-client/src/hmr/index.ts).
[^src-src-router-ts]: `src/ts/vertebra-client/src/router.ts` — TypeScript client router module (src/ts/vertebra-client/src/router.ts).
[^src-src-webmcp-ts]: `src/ts/vertebra-client/src/webmcp.ts` — TypeScript WebMCP client module (src/ts/vertebra-client/src/webmcp.ts).
[^src-src-agent-index-ts]: `src/ts/vertebra-client/src/agent/index.ts` — TypeScript agent SDK module (src/ts/vertebra-client/src/agent/index.ts).
