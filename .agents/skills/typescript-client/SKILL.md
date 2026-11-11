---
name: typescript-client
description: Use when implementing or debugging the @vertebra/client TypeScript SDK, framework adapters (React, Vue, Svelte, Angular, HTMX, Vite), client router, WebMCP bridge, or bundler codegen in src/ts/vertebra-client.
---

<!-- truth: start -->
- **Package**: `src/ts/vertebra-client` (`@vertebra/client` v0.3.0, zero runtime dependencies, `sideEffects: false`, pure ESM + `.d.ts` via `tsup`).
- **Entrypoints**: Root `src/index.ts` exports `HttpClient`, `SseClient`, `WebSocketClient`, `SessionStore`, `LiveStateClient`, `routeUrl`, `registerWebMCPTools` (`WebMCPRegistrationHandle` over `document.modelContext ?? navigator.modelContext`), `ArrowStreamClient`, `WASMClient`, `ApexRegion`, `McpClient`/`A2aClient`, and typed client errors (`ClientError`, `NetworkError`, `TimeoutError`, `ServerError`, `ValidationError`); 16 subpath exports cover `./react`, `./vue`, `./svelte`, `./angular`, `./htmx`, `./vite`, `./store`, `./hmr`, `./live`, `./apex`, `./telemetry`, `./wasm`, `./arrow`, `./router`, `./webmcp`, and `./agent` (`TS-01`, `TS-02`).
- **Strictness**: `strict: true` and `noUncheckedIndexedAccess: true` in `tsconfig.json`; validate external JSON payloads before narrowing (`TS-03`).
- **Resilience**: Realtime transports (`WebSocketClient`, `SseClient`) enforce exponential backoff with random jitter (`computeBackoffDelay`) and clean up listeners on `dispose()`/`AbortSignal` (`TS-04`, `TS-05`).
- **Wheel Sync**: `@vertebra/client` ships inside the `vertebra-core` Python wheel under `src/py/vertebra/server/_js/bundle/` and is served over `/__vertebra/client/*` via `.vertebra/import_map.json` configured from `[tool.vertebra.js]` (`TS-07`).
- **Verify**: `cd src/ts/vertebra-client && bunx biome check . && bun run test && bun run build`
<!-- truth: end -->

# TypeScript Client SDK & Bundler (`typescript-client`)

Implementation guide for `@vertebra/client` (`src/ts/vertebra-client/`), framework adapters, type-safe RPC/router codegen, WebMCP browser tool registration, and Rust bundler integration.

## Architecture & Domain References

- [Bundler and client SDK](../../bundles/knowledge/domains/frontend/bundler-and-client-sdk.md)
- [Bundler HMR and SSR](../../bundles/knowledge/architecture/frontend/bundler-hmr-and-ssr.md)
- [Codegen and Inertia](../../bundles/knowledge/architecture/frontend/codegen-and-inertia.md)
- [Client and adapter subpaths](../../bundles/knowledge/patterns/typescript/client-and-adapter-subpaths.md)
- SDK and bundler reference: [`references/sdk-and-bundler.md`](references/sdk-and-bundler.md)
- Review checklist & suppressions: [`review-typescript-client`](../review-typescript-client/SKILL.md)

## Core Implementation Rules

1. **Subpath Isolation & Tree-Shaking (`TS-01`, `TS-02`, `TS-06`)**:
   - Keep `src/index.ts` free of peer framework imports (`react`, `vue`, `svelte`, `@angular/core`, `vite`).
   - Keep every subpath (`src/react/index.tsx`, `src/vue/index.ts`, `src/svelte/index.ts`, `src/angular/index.ts`, `src/htmx/index.ts`, `src/vite/index.ts`, `src/store/index.ts`, `src/hmr/index.ts`, `src/live/index.ts`, `src/apex/index.ts`, `src/telemetry.ts`, `src/wasm.ts`, `src/arrow.ts`, `src/router.ts`, `src/webmcp.ts`, `src/agent/index.ts`) aligned across `package.json` `exports`, `tsup.config.ts` `entry`, and `tsconfig.json`.
   - Avoid module-scope timers, `window` mutations, or `fetch` calls at import time so `sideEffects: false` remains accurate.

2. **Transport Lifecycle & Reconnection (`TS-04`, `TS-05`)**:
   - Every realtime subscription, `EventSource`, `WebSocket`, and `AbortController` must release listeners and timers on `close()`/`dispose()`.
   - Reconnect loops (`src/client/retry.ts`) must compute bounded exponential backoff with randomized jitter.

3. **Codegen & Wheel Bundle Parity (`TS-07`)**:
   - Keep TypeScript interfaces synchronized with Python `TypeExtractor`/`TypeBridge` (`src/py/vertebra/build/codegen/`) and sync built `dist/` files into `src/py/vertebra/server/_js/bundle/`.

## Verification

```bash
cd src/ts/vertebra-client && bunx biome check .
cd src/ts/vertebra-client && bun run test
cd src/ts/vertebra-client && bun run build
```
