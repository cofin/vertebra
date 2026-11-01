# `@vertebra/client` Subpaths, Transports, and Wheel Packaging

## Package Subpath Matrix

| Subpath | Source File | Purpose |
| :--- | :--- | :--- |
| `@vertebra/client` | `src/index.ts` | `HttpClient`, `SseClient`, `WebSocketClient`, `SessionStore`, `LiveStateClient`, `HotReloadClient`, `routeUrl`, `wsUrl`, `registerWebMCPTools`, `ArrowStreamClient`, `WASMClient`, `TraceContext`, `ApexRegion`, `McpClient`, `A2aClient` |
| `@vertebra/client/react` | `src/react/index.tsx` | `ClientProvider`, `useClient`, `useQuery`, `useMutation`, `useInvalidateQueries`, `QueryCache` |
| `@vertebra/client/vue` | `src/vue/index.ts` | `provideClient`, `useClient`, `useQuery`, `useMutation` |
| `@vertebra/client/svelte` | `src/svelte/index.ts` | `setClient`, `getClient`, `createQuery`, `createMutation` |
| `@vertebra/client/angular` | `src/angular/index.ts` | `@Injectable ClientService`, signal-based `injectQuery`, `injectMutation` |
| `@vertebra/client/htmx` | `src/htmx/index.ts` | `initHtmx(config)` integration |
| `@vertebra/client/vite` | `src/vite/index.ts` | `vitePlugin(options)` and `VitePluginOptions` |
| `@vertebra/client/store` | `src/store/index.ts` | `SessionStore` and `store` singleton |
| `@vertebra/client/hmr` | `src/hmr/index.ts` | `HotReloadClient`, `initHotReload`, `ServerMessage` |
| `@vertebra/client/live` | `src/live/index.ts` | `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, `StateOpcode`, `fnv1aStateId`, `fnv1aFieldId` |
| `@vertebra/client/apex` | `src/apex/index.ts` | `ApexRegion` `<vertebra-island>` custom elements and `ApexOfflineOutbox` |
| `@vertebra/client/telemetry` | `src/telemetry.ts` | W3C `TraceContext` propagation |
| `@vertebra/client/wasm` | `src/wasm.ts` | `WASMClient` and `WASM_MAGIC_BYTES` |
| `@vertebra/client/arrow` | `src/arrow.ts` | `ArrowStreamClient` and `ARROW_*` constants |
| `@vertebra/client/router` | `src/router.ts` | `routeUrl`, `wsUrl`, `interpolateRoutePath`, `RouteUrlOptions` |
| `@vertebra/client/webmcp` | `src/webmcp.ts` | `registerWebMCPTools`, `WebMCPManifest`, `WebMCPToolDescriptor` (`window.navigator.modelContext`) |
| `@vertebra/client/agent` | `src/agent/index.ts` | `McpClient`, `A2aClient`, `AgentStreamClient`, `WorkflowLiveSubscriber` |

## Native Wheel Distribution (`src/py/vertebra/server/_js/bundle/`)

- `@vertebra/client` is distributed inside the `vertebra-core` Python wheel under `src/py/vertebra/server/_js/bundle/` and served over HTTP (`/__vertebra/client/*`) by `src/py/vertebra/framework/dev.py`.
- `tsup.config.ts` `onSuccess` and `ImportMapGenerator.sync_wheel_client` sync all subpath entry points and `.d.ts` declaration files on build, and projects manage JS dependencies via `[tool.vtb.js]` in `pyproject.toml` (`vtb js install` generating `.vertebra/importmap.json`).
