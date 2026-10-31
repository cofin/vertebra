# Bundler, QuickJS SSR & TypeScript SDK

Vertebra pairs its Python and Rust backend with an embedded frontend toolchain (`vertebra-bundler` / `vertebra._bundler`) and the `@vertebra/client` TypeScript SDK (`src/ts/vertebra-client/`).

## Embedded Rolldown/Oxc Bundler (`vertebra-bundler`)

`vertebra-bundler` embeds the Rust-native Oxc parser/transformer and Rolldown module bundler inside the `vertebra._bundler` PyO3 extension (`Bundler`):

- **Zero External Node Build Daemon**: Compiles TypeScript (`.ts`, `.tsx`), JSX, and CSS entrypoints directly from Python (`vtb js build` or `Bundler().build(BundleConfig(...))`).
- **Content-Hashed Manifests**: Emits deterministic asset manifests consumed by `vertebra.framework.inertia` and static file middleware.
- **Hot Module Replacement (HMR)**: In `vtb dev` mode, file changes trigger incremental rebuilds and broadcast update frames to connected browsers via `@vertebra/client/hmr` (`HotReloadClient`).

## Pooled QuickJS Server-Side Rendering (`EmbeddedSsrClient`)

Instead of proxying SSR requests over localhost HTTP to a separate Node.js process, `vertebra-bundler` manages a thread-safe pool of embedded QuickJS runtimes:

1. Each QuickJS worker pre-evaluates the compiled SSR bundle once at startup.
2. `EmbeddedSsrClient.render(page)` dispatches the Inertia page payload over an in-memory channel (`SsrFrame`) while releasing the Python GIL (`py.detach`).
3. The rendered HTML head/body fragments (`SsrResult`) return directly to the Rust/Python response pipeline in sub-millisecond latency.

## `@vertebra/client` Subpath Architecture

The `@vertebra/client` package is organized into tree-shakeable ESM subpaths with zero runtime framework lock-in:

| Subpath | Responsibility |
| --- | --- |
| `@vertebra/client` | Core `HttpClient`, typed error hierarchy (`ClientError`, `ValidationError`), `SseClient`, `WebSocketClient`, and re-exports |
| `@vertebra/client/live` | Binary `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue`, and FNV-1a field/state hashing |
| `@vertebra/client/agent` | `McpClient`, `A2aClient`, `AgentStreamClient`, and `WorkflowLiveSubscriber` |
| `@vertebra/client/apex` | `ApexRegion` island mounting and `ApexOfflineOutbox` mutation synchronization |
| `@vertebra/client/arrow` | Streaming Apache Arrow IPC record batch reader (`ArrowStreamClient`) |
| `@vertebra/client/wasm` | Browser WebAssembly module loader and execution client (`WasmClient`) |
| `@vertebra/client/telemetry` | W3C `TraceContext` (`traceparent` / `tracestate`) generation and header injection |
| `@vertebra/client/react`, `/vue`, `/svelte`, `/angular`, `/htmx`, `/vite` | Framework-specific hooks, stores, directives, and Vite plugin integration |
