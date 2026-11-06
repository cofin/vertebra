---
type: Pattern
title: "Client SDK Root Exports, Subpaths & Build Entries"
description: "Rules for the @vertebra/client root entry, package.json subpath exports, tsup build entries, optional peers, and codegen parity."
status: stable
tags: [patterns, typescript, client-sdk, packaging]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
  - id: src-tsup-config-ts
    resource: src/ts/vertebra-client/tsup.config.ts
    title: "TypeScript client module (src/ts/vertebra-client/tsup.config.ts)"
  - id: src-tsconfig-json
    resource: src/ts/vertebra-client/tsconfig.json
    title: "TypeScript client module (src/ts/vertebra-client/tsconfig.json)"
  - id: src-biome-json
    resource: src/ts/vertebra-client/biome.json
    title: "TypeScript client module (src/ts/vertebra-client/biome.json)"
  - id: src-src-index-ts
    resource: src/ts/vertebra-client/src/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/index.ts)"
  - id: src-src-router-ts
    resource: src/ts/vertebra-client/src/router.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/router.ts)"
  - id: src-src-webmcp-ts
    resource: src/ts/vertebra-client/src/webmcp.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/webmcp.ts)"
  - id: src-src-react-index-tsx
    resource: src/ts/vertebra-client/src/react/index.tsx
    title: "TypeScript client module (src/ts/vertebra-client/src/react/index.tsx)"
  - id: src-src-svelte-index-ts
    resource: src/ts/vertebra-client/src/svelte/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/svelte/index.ts)"
  - id: src-src-vue-index-ts
    resource: src/ts/vertebra-client/src/vue/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/vue/index.ts)"
  - id: src-src-angular-index-ts
    resource: src/ts/vertebra-client/src/angular/index.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/angular/index.ts)"
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
---

# Client SDK Root Exports, Subpaths & Build Entries

[`@vertebra/client`](../../../../../src/ts/vertebra-client/package.json)[^src-package-json] is an ESM-only package (`"type": "module"`) with no runtime `dependencies`. Optional peers are `react ^19.0.0`, `vue ^3.5.0`, `@angular/core ^22.0.0`, and `vite ^8.0.0`; Svelte is a devDependency only and HTMX is not a dependency.

## Root entry (`src/index.ts`)

The root entry re-exports the core client and every feature module. Framework adapters are subpath-only.

<!-- truth: start -->
- **Package**: `@vertebra/client` (`"type": "module"`, zero runtime `dependencies`, shipped in the `vertebra-core` Python wheel under `src/py/vertebra/server/_js/bundle/`).
- **Root Entry (`src/index.ts`)**: Re-exports `HttpClient`, `SseClient`, `WebSocketClient`, `SessionStore`, `LiveStateClient`, `HotReloadClient`, `routeUrl`, `wsUrl`, `registerWebMCPTools`, `ArrowStreamClient`, `WASMClient`, `TraceContext`, `ApexRegion`, and `McpClient`/`A2aClient`.
- **16 Subpath Exports**: `./react`, `./vue`, `./svelte`, `./angular`, `./htmx`, `./vite`, `./store`, `./hmr`, `./live`, `./apex`, `./telemetry`, `./wasm`, `./arrow`, `./router`, `./webmcp`, and `./agent` (kept in lockstep across `package.json` `exports` and `tsup.config.ts` `entry`).
- **Framework Isolation**: Peer framework imports (`react`, `vue`, `svelte`, `@angular/core`, `vite`) live strictly in their subpath modules and are marked `external` in `tsup.config.ts`.
- **Codegen & Hash Parity**: Route types and `fnv1aStateId` (64-bit) / `fnv1aFieldId` (32-bit) stay synchronized with `src/py/vertebra/build/codegen/` and `LiveState`.
<!-- truth: end -->

```typescript
export {
  A2aClient,
  type A2aClientOptions,
  AgentStreamClient,
  type AgentStreamClientOptions,
  encodeMcpHeaderValue,
  MCP_PROTOCOL_VERSION,
  McpClient,
  type McpClientOptions,
  type WorkflowLiveStatePayload,
  WorkflowLiveSubscriber,
} from "./agent/index";
export {
  type ApexGlobalNamespace,
  type ApexItemHandle,
  type ApexOfflineMutationEntry,
  type ApexOfflineOperation,
  ApexOfflineOutbox,
  ApexRegion,
  type ApexRegionBridgeOptions,
  type IslandDescriptor,
} from "./apex/index";
export {
  ARROW_CONTINUATION_MARKER,
  ARROW_FILE_MAGIC,
  ARROW_STREAM_MIME,
  ArrowStreamClient,
  type ArrowStreamOptions,
} from "./arrow";
export { HttpClient } from "./client/client";
export * from "./client/errors";
export { computeBackoffDelay } from "./client/retry";
export type { ClientConfig, RetryConfig, RouteResponse } from "./client/types";
export { HotReloadClient, initHotReload, type ServerMessage } from "./hmr/index";
export {
  fnv1aFieldId,
  fnv1aStateId,
  HybridLogicalClock,
  LiveStateClient,
  type LiveStateListener,
  OfflineDeltaQueue,
  type StateDeltaMessage,
  StateOpcode,
  type StateOpcodeValue,
} from "./live/index";
export {
  interpolateRoutePath,
  type RouteUrlOptions,
  routeUrl,
  wsUrl,
} from "./router";
export {
  SseClient,
  type SseClientOptions,
  type SseEventHandler,
  type SseMessageMeta,
} from "./sse";
export { SessionStore, store } from "./store/index";
export { TraceContext } from "./telemetry";
export {
  WASM_MAGIC_BYTES,
  WASMClient,
  type WASMClientOptions,
} from "./wasm";
export {
  type BrowserModelContext,
  type BrowserModelContextTool,
  type RegisterWebMCPOptions,
  registerWebMCPTools,
  type WebMCPManifest,
  type WebMCPRegistrationHandle,
  type WebMCPToolDescriptor,
} from "./webmcp";
export {
  type PubSubWireFrame,
  WebSocketClient,
  type WebSocketClientOptions,
} from "./websocket";
```

`SseClient`, `WebSocketClient`, `HotReloadClient`, `initHotReload`, `routeUrl`, `wsUrl`, and `registerWebMCPTools` are exported from the root entry.

## Subpath exports vs tsup entries

`package.json` declares 17 export keys; [`tsup.config.ts`](../../../../../src/ts/vertebra-client/tsup.config.ts)[^src-tsup-config-ts] builds every exported entry.

| Export | Source | Contents | tsup entry |
| --- | --- | --- | --- |
| `.` | `src/index.ts` | Everything above | yes |
| `./react` | `src/react/index.tsx` | `ClientProvider`, `ClientContext`, `useClient`, `useQuery`, `useMutation`, `useInvalidateQueries`, `QueryCache`/`queryCache` | yes (`src/react/index.tsx`) |
| `./vue` | `src/vue/index.ts` | `provideClient`, `useClient`, `useQuery`, `useMutation` | yes |
| `./svelte` | `src/svelte/index.ts` | `setClient`, `getClient`, `createQuery`/`createMutation` | yes |
| `./angular` | `src/angular/index.ts` | `@Injectable ClientService`, signal-based `injectQuery`/`injectMutation` | yes |
| `./htmx` | `src/htmx/index.ts` | `initHtmx(config)` | yes |
| `./vite` | `src/vite/index.ts` | `vitePlugin(options)` (also default export), `VitePluginOptions` | yes |
| `./store` | `src/store/index.ts` | `SessionStore`, `store` | yes |
| `./hmr` | `src/hmr/index.ts` | `HotReloadClient`, `initHotReload`, `ServerMessage` | yes |
| `./live` | `src/live/index.ts` | `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, `StateOpcode`, `fnv1aStateId`, `fnv1aFieldId` | yes |
| `./apex` | `src/apex/index.ts` | `ApexRegion`, `ApexOfflineOutbox` | yes |
| `./telemetry` | `src/telemetry.ts` | `TraceContext` | yes |
| `./wasm` | `src/wasm.ts` | `WASMClient`, `WASM_MAGIC_BYTES` | yes |
| `./arrow` | `src/arrow.ts` | `ArrowStreamClient`, `ARROW_*` constants | yes |
| `./router` | `src/router.ts` | `routeUrl`, `wsUrl`, `interpolateRoutePath`, `RouteUrlOptions` | yes |
| `./webmcp` | `src/webmcp.ts` | `registerWebMCPTools`, `WebMCPRegistrationHandle`, `WebMCPManifest`, `WebMCPToolDescriptor`, `RegisterWebMCPOptions` | yes |
| `./agent` | `src/agent/index.ts` | `McpClient`, `A2aClient`, `AgentStreamClient`, `WorkflowLiveSubscriber` | yes |

`tsup.config.ts` `onSuccess` copies the full `dist/` directory into `src/py/vertebra/server/_js/bundle/` (untracked) for `DevMiddleware` and `ImportMapGenerator.sync_wheel_client`. `ImportMapGenerator.load_client_subpaths` derives `_CLIENT_SUBPATHS` dynamically from `src/ts/vertebra-client/package.json` `exports` so subpath additions never drift. When `src/py/vertebra/server/_js/bundle/` is absent in source checkouts (such as `make py-test` running before `make ts-build`), `DevMiddleware` and `ImportMapGenerator.sync_wheel_client` fall back to `src/ts/vertebra-client/dist/` or `src/ts/vertebra-client/src/`.

## Rules

- **Add a subpath in three places**: the source module, a `package.json` `exports` key (`types` + `import`), and a tsup `entry` with the exact filename. Re-export it from `src/index.ts` only when root consumers need it.
- **Keep adapters out of the root**: framework adapters import their peer (`react`, `vue`, `svelte`, `@angular/core`, `vite`), all of which tsup marks `external`.
- **TypeScript settings** ([`tsconfig.json`](../../../../../src/ts/vertebra-client/tsconfig.json)[^src-tsconfig-json]): `strict`, `target: "ES2022"`, `module: "ESNext"`, `moduleResolution: "Bundler"`, `jsx: "react-jsx"`, `lib: ES2022 + DOM`, declarations, declaration maps, and source maps.
- **Lint**: Biome via `make ts-lint` (`bunx biome check --write --unsafe .`) and `make lint-all` (`bunx biome check .`). Tests run with `make ts-test` (Vitest over `tests/unit/` and the top-level `tests/*.test.ts` suites).
- **Codegen parity**: client route contracts (`RouteResponse`, `HttpClient.call` path interpolation) must stay aligned with [`_typescript.py`](../../../../../src/py/vertebra/build/codegen/_typescript.py), [`_manifest.py`](../../../../../src/py/vertebra/build/codegen/_manifest.py), and [`zod.py`](../../../../../src/py/vertebra/build/codegen/zod.py).
- **Cross-language ids**: `fnv1aStateId` (FNV-1a 64, `bigint`) and `fnv1aFieldId` (FNV-1a 32) must match Python `LiveState.compute_state_id` / `compute_field_id` and Rust `state_id` / `field_id` (see [Live State](../../domains/framework/live-state.md)).

Provenance sources: [^src-biome-json], [^src-src-index-ts], [^src-src-router-ts], [^src-src-webmcp-ts], [^src-src-react-index-tsx], [^src-src-svelte-index-ts], [^src-src-vue-index-ts], [^src-src-angular-index-ts], [^src-makefile].

[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-tsup-config-ts]: `src/ts/vertebra-client/tsup.config.ts` — TypeScript client module (src/ts/vertebra-client/tsup.config.ts).
[^src-tsconfig-json]: `src/ts/vertebra-client/tsconfig.json` — TypeScript client module (src/ts/vertebra-client/tsconfig.json).
[^src-biome-json]: `src/ts/vertebra-client/biome.json` — TypeScript client module (src/ts/vertebra-client/biome.json).
[^src-src-index-ts]: `src/ts/vertebra-client/src/index.ts` — TypeScript client module (src/ts/vertebra-client/src/index.ts).
[^src-src-router-ts]: `src/ts/vertebra-client/src/router.ts` — TypeScript client module (src/ts/vertebra-client/src/router.ts).
[^src-src-webmcp-ts]: `src/ts/vertebra-client/src/webmcp.ts` — TypeScript client module (src/ts/vertebra-client/src/webmcp.ts).
[^src-src-react-index-tsx]: `src/ts/vertebra-client/src/react/index.tsx` — TypeScript client module (src/ts/vertebra-client/src/react/index.tsx).
[^src-src-svelte-index-ts]: `src/ts/vertebra-client/src/svelte/index.ts` — TypeScript client module (src/ts/vertebra-client/src/svelte/index.ts).
[^src-src-vue-index-ts]: `src/ts/vertebra-client/src/vue/index.ts` — TypeScript client module (src/ts/vertebra-client/src/vue/index.ts).
[^src-src-angular-index-ts]: `src/ts/vertebra-client/src/angular/index.ts` — TypeScript client module (src/ts/vertebra-client/src/angular/index.ts).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
