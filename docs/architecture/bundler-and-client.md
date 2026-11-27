# Bundler, QuickJS SSR & TypeScript SDK

Vertebra pairs its Python and Rust backend with a built-in frontend bundler (`vertebra-bundler`) and the `@vertebra/client` TypeScript SDK (`src/ts/vertebra-client/`).

## Built-In JavaScript Bundler

`vertebra-bundler` embeds the Oxc parser and transformer alongside the Rolldown module bundler inside the `vertebra._bundler` extension:

- **No External Node Build Server**: Compiles TypeScript (`.ts`, `.tsx`), JSX, and CSS entrypoints directly from Python via `vtb js build` or the `Bundler` API.
- **Asset Manifests & `.d.ts` Generation**: Emits content-hashed asset manifests and isolated TypeScript declaration files.
- **Hot Module Replacement (HMR)**: In `vtb dev`, the file watcher triggers incremental rebuilds and pushes update frames to browsers over `@vertebra/client/hmr`.

## QuickJS Server-Side Rendering Pool

Instead of sending SSR requests over localhost HTTP to a Node.js sidecar, `vertebra-bundler` runs an in-process pool of QuickJS runtimes:

1. Each QuickJS worker loads the compiled SSR bundle once at startup.
2. Sync and async SSR clients send the Inertia page payload to a pooled worker in memory while releasing the Python GIL.
3. The rendered HTML head and body fragments return directly to Python.

## TypeScript Client Subpaths

`@vertebra/client` exposes tree-shakeable ESM entry points:

| Subpath | Responsibility |
| --- | --- |
| `@vertebra/client` | Core HTTP client, typed error classes, SSE and WebSocket clients, and root re-exports |
| `@vertebra/client/live` | Binary live-state sync, hybrid logical clocks, offline delta queuing, and FNV-1a hashing |
| `@vertebra/client/agent` | MCP, A2A, agent streaming, and workflow live-state clients |
| `@vertebra/client/apex` | Oracle APEX island mounting and offline mutation outbox |
| `@vertebra/client/arrow` | Binary Apache Arrow IPC stream fetcher |
| `@vertebra/client/wasm` | Browser WebAssembly module loader and execution client |
| `@vertebra/client/telemetry` | W3C `traceparent` parser, generator, and header injector |
| `@vertebra/client/react`, `/vue`, `/svelte`, `/angular`, `/htmx`, `/vite` | Framework hooks, stores, services, HTMX request helper, and Vite plugin |
