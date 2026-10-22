---
type: Pattern
title: "Client SDK Root Exports, Subpaths & Build Entries"
description: "Rules for the @vertebra/client root entry, package.json subpath exports, tsup build entries, optional peers, and codegen parity."
status: stable
tags: [patterns, typescript, client-sdk, packaging]
sources:
  - resource: src/ts/vertebra-client/package.json
  - resource: src/ts/vertebra-client/tsup.config.ts
  - resource: src/ts/vertebra-client/tsconfig.json
  - resource: src/ts/vertebra-client/biome.json
  - resource: src/ts/vertebra-client/src/index.ts
  - resource: src/ts/vertebra-client/src/react/index.tsx
  - resource: src/ts/vertebra-client/src/svelte/index.ts
  - resource: src/ts/vertebra-client/src/angular/index.ts
  - resource: Makefile
---

# Client SDK Root Exports, Subpaths & Build Entries

[`@vertebra/client`](../../../../../src/ts/vertebra-client/package.json) is an ESM-only package (`"type": "module"`) with no runtime `dependencies`. Optional peers are `react ^19.0.0`, `vue ^3.5.0`, `@angular/core ^22.0.0`, and `vite ^8.0.0`; Svelte is a devDependency only and HTMX is not a dependency.

## Root entry (`src/index.ts`)

The root entry re-exports the core client and every feature module. Framework adapters are subpath-only.

<!-- truth: start -->
```typescript
export {
  type ApexGlobalNamespace,
  type ApexItemHandle,
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
export { RealtimeChannel } from "./client/ws";
export { HotReloadClient, type ServerMessage } from "./hmr/index";
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
  SseClient,
  type SseClientOptions,
  type SseEventHandler,
  type SseMessageMeta,
} from "./sse";
export { SessionStore, store } from "./store/index";
export { TraceContext } from "./telemetry";
export {
  WASM_MAGIC_BYTES,
  WasmClient,
  type WasmClientOptions,
} from "./wasm";
export {
  type PubSubWireFrame,
  WebSocketClient,
  type WebSocketClientOptions,
} from "./websocket";
```
<!-- truth: end -->

`SseClient`, `WebSocketClient`, `RealtimeChannel`, and `HotReloadClient` ship only from the root entry.

## Subpath exports vs tsup entries

`package.json` declares 13 export keys; [`tsup.config.ts`](../../../../../src/ts/vertebra-client/tsup.config.ts) lists the build entries.

| Export | Source | Contents | tsup entry |
| --- | --- | --- | --- |
| `.` | `src/index.ts` | Everything above | yes |
| `./react` | `src/react/index.tsx` | `ClientProvider`, `ClientContext`, `useClient`, `useQuery`, `useMutation`, `useInvalidateQueries`, `QueryCache`/`queryCache` | listed as `src/react/index.ts` (file is `.tsx`) |
| `./vue` | `src/vue/index.ts` | `provideClient`, `useClient`, `useQuery`, `useMutation` | yes |
| `./svelte` | `src/svelte/index.ts` | `setClient`, `getClient`, rune-based (`$state`) `createQuery`/`createMutation` | yes |
| `./angular` | `src/angular/index.ts` | `@Injectable ClientService`, signal-based `injectQuery`/`injectMutation` | yes |
| `./htmx` | `src/htmx/index.ts` | `initHtmx(config)` | yes |
| `./vite` | `src/vite/index.ts` | `vitePlugin(options)` (also default export), `VitePluginOptions` | yes |
| `./store` | `src/store/index.ts` | `SessionStore`, `store` | yes |
| `./live` | `src/live/index.ts` | `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, `StateOpcode`, `fnv1aStateId`, `fnv1aFieldId` | yes |
| `./apex` | `src/apex/index.ts` | `ApexRegion` | yes |
| `./telemetry` | `src/telemetry.ts` | `TraceContext` | yes |
| `./wasm` | `src/wasm.ts` | `WasmClient`, `WASM_MAGIC_BYTES` | **no** |
| `./arrow` | `src/arrow.ts` | `ArrowStreamClient`, `ARROW_*` constants | **no** |

`src/hmr/index.ts` is a tsup entry with no export key; tsup `onSuccess` copies `dist/index.js` and `dist/hmr/index.js` into `src/py/vertebra/server/_js/bundle/` (untracked) for `DevMiddleware`.

Gotcha: `./wasm` and `./arrow` resolve to `dist/wasm.js` / `dist/arrow.js`, which tsup does not emit, and the `./react` entry path does not match `src/react/index.tsx`. Import these symbols from the root entry until the entries are fixed.

## Rules

- **Add a subpath in three places**: the source module, a `package.json` `exports` key (`types` + `import`), and a tsup `entry` with the exact filename. Re-export it from `src/index.ts` only when root consumers need it.
- **Keep adapters out of the root**: framework adapters import their peer (`react`, `vue`, `svelte`, `@angular/core`, `vite`), all of which tsup marks `external`.
- **TypeScript settings** ([`tsconfig.json`](../../../../../src/ts/vertebra-client/tsconfig.json)): `strict`, `target: "ES2022"`, `module: "ESNext"`, `moduleResolution: "Bundler"`, `jsx: "react-jsx"`, `lib: ES2022 + DOM`, declarations, declaration maps, and source maps.
- **Lint**: Biome via `make ts-lint` (`bunx biome check --write --unsafe .`) and `make lint-all` (`bunx biome check .`). Tests run with `make ts-test` (Vitest over `tests/unit/` and the top-level `tests/*.test.ts` suites).
- **Codegen parity**: client route contracts (`RouteResponse`, `HttpClient.call` path interpolation) must stay aligned with [`_typescript.py`](../../../../../src/py/vertebra/build/codegen/_typescript.py), [`zod.py`](../../../../../src/py/vertebra/build/codegen/zod.py), and [`_manifest.py`](../../../../../src/py/vertebra/build/codegen/_manifest.py).
- **Cross-language ids**: `fnv1aStateId` (FNV-1a 64, `bigint`) and `fnv1aFieldId` (FNV-1a 32) must match Python `LiveState.compute_state_id` / `compute_field_id` and Rust `state_id` / `field_id` (see [Live State](../../domains/framework/live-state.md)).
