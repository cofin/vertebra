# Bundler, QuickJS SSR & TypeScript SDK

Vertebra pairs its Python and Rust backend with a built-in frontend bundler (`vertebra-bundler` / `vertebra._bundler`) and the `@vertebra/client` TypeScript SDK (`src/ts/vertebra-client/`).

## Built-In JavaScript Bundler (`vertebra-bundler`)

`vertebra-bundler` embeds the Oxc parser/transformer and Rolldown module bundler inside the `vertebra._bundler` native extension (`Bundler`):

- **No External Node Build Server**: Compiles TypeScript (`.ts`, `.tsx`), JSX, and CSS entrypoints directly from Python (`vtb js build` or `Bundler().build(BundleConfig(...))`).
- **Asset Manifests & `.d.ts` Generation**: Emits content-hashed asset manifests (`BuildResult`) and fast `.d.ts` declarations (`IsolatedDeclarationsGenerator`).
- **Hot Module Replacement (HMR)**: In `vtb dev` mode, file changes watched by `SourceWatcher` trigger incremental rebuilds and push update frames to connected browsers via `@vertebra/client/hmr` (`HotReloadClient`).

## QuickJS Server-Side Rendering Pool (`SSRPool` / `SyncEmbeddedSSRClient` & `AsyncEmbeddedSSRClient`)

Instead of sending SSR requests over localhost HTTP to a separate Node.js process, `vertebra-bundler` runs an in-process pool of QuickJS JavaScript runtimes (`SSRPool`):

1. Each QuickJS worker loads the compiled SSR bundle once at startup.
2. `SyncEmbeddedSSRClient.render(page)` and `AsyncEmbeddedSSRClient.render(page)` send the Inertia page payload to a pooled QuickJS worker in memory while releasing the Python GIL (`py.detach`).
3. The rendered HTML head and body fragments (`SSRResult`) return directly to Python.

## `@vertebra/client` Subpath Architecture

The `@vertebra/client` package provides 15 tree-shakeable ESM entry points:

| Subpath | Responsibility |
| --- | --- |
| `@vertebra/client` | Core `HttpClient`, typed error classes (`ClientError`, `ValidationError`), `SseClient`, `WebSocketClient`, and barrel re-exports |
| `@vertebra/client/live` | Binary `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, `StateOpcode`, and FNV-1a field/state hashing |
| `@vertebra/client/agent` | `McpClient`, `A2aClient`, `AgentStreamClient`, and `WorkflowLiveSubscriber` |
| `@vertebra/client/apex` | `ApexRegion` island mounting and `ApexOfflineOutbox` mutation synchronization |
| `@vertebra/client/arrow` | Binary Apache Arrow IPC stream fetcher (`ArrowStreamClient`) |
| `@vertebra/client/wasm` | Browser WebAssembly module loader and execution client (`WASMClient`) |
| `@vertebra/client/telemetry` | W3C `TraceContext` (`traceparent`) parser, generator, and header injector |
| `@vertebra/client/react`, `/vue`, `/svelte`, `/angular`, `/htmx`, `/vite` | Framework hooks (`useQuery`, `useMutation`), stores, services, HTMX helper (`initHtmx`), and Vite plugin (`vitePlugin`) |
