---
name: domain-frontend-bundler-and-client-sdk
description: Canonical domain types for Vertebra's Rust Oxc bundler/SSR pool, Python codegen orchestrator, CLI console, and @vertebra/client TypeScript SDK.
sources:
  - src/rs/vertebra-bundler/src/lib.rs
  - src/py/vertebra/builder/_orchestrator.py
  - src/py/vertebra/builder/_codegen/_extractor.py
  - src/py/vertebra/cli/tui.py
  - src/ts/vertebra-client/src/index.ts
---

# Frontend Bundler, Codegen & TypeScript SDK Domain

[`vertebra-bundler`](../../../../../src/rs/vertebra-bundler/src/lib.rs), [`vertebra.builder`](../../../../../src/py/vertebra/builder/__init__.py), [`vertebra.cli`](../../../../../src/py/vertebra/cli/__init__.py), and [`@vertebra/client`](../../../../../src/ts/vertebra-client/src/index.ts) share a unified domain vocabulary across Rust, Python, and TypeScript.

## 1. Rust Bundler, Watcher & SSR Pool (`vertebra._bundler`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`Bundler`](../../../../../src/rs/vertebra-bundler/src/bundler.rs) / [`BundleOutput`](../../../../../src/rs/vertebra-bundler/src/bundler.rs) / [`AssetManifest`](../../../../../src/rs/vertebra-bundler/src/bundler.rs) | `vertebra_bundler::bundler` | Oxc TypeScript/JSX parser, transformer, dependency collector, minifier, and content-hashed asset emitter. |
| [`SourceWatcher`](../../../../../src/rs/vertebra-bundler/src/hmr.rs) / [`ChangeEvent`](../../../../../src/rs/vertebra-bundler/src/hmr.rs) | `vertebra_bundler::hmr` | `rolldown-notify` debounced filesystem watcher for HMR reload triggers. |
| [`SsrPool`](../../../../../src/rs/vertebra-bundler/src/ssr.rs) | `vertebra_bundler::ssr` | Pooled `rquickjs` (`0.14`) SSR engine with `ArrayQueue` context leasing and `DashMap` FNV-1a TTL cache. |

## 2. Python Build Orchestrator, Codegen & CLI (`vertebra.builder`, `vertebra.cli`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildOrchestrator`](../../../../../src/py/vertebra/builder/_orchestrator.py) / [`BuildConfig`](../../../../../src/py/vertebra/builder/_config.py) | `vertebra.builder._orchestrator` | Coordinates type extraction, `.d.ts`/Zod/manifest generation, import maps, and `Bundler` compilation. |
| [`DevServerProcess`](../../../../../src/py/vertebra/builder/_fallback.py) | `vertebra.builder._fallback` | Optional external Vite/Bun dev server subprocess manager and readiness prober. |
| [`TypeExtractor`](../../../../../src/py/vertebra/builder/_codegen/_extractor.py) / [`TypeNode`](../../../../../src/py/vertebra/builder/_codegen/_ir.py) | `vertebra.builder._codegen` | Introspects `App` routes and Python models (`dataclass`, `TypedDict`, `msgspec.Struct`, Pydantic) into IR for [`TypeScriptGenerator`](../../../../../src/py/vertebra/builder/_codegen/_typescript.py), [`ZodGenerator`](../../../../../src/py/vertebra/builder/_codegen/_zod.py), and [`ManifestGenerator`](../../../../../src/py/vertebra/builder/_codegen/_manifest.py). |
| [`Console`](../../../../../src/py/vertebra/cli/tui.py) | `vertebra.cli.tui` | Terminal output renderer with Rich integration and plain ANSI fallback. |

## 3. TypeScript SDK (`@vertebra/client`)

| Symbol | Subpath | Responsibility |
|---|---|---|
| [`HttpClient`](../../../../../src/ts/vertebra-client/src/client/client.ts) / [`ClientConfig`](../../../../../src/ts/vertebra-client/src/client/types.ts) | `@vertebra/client` | Type-safe HTTP client with path parameter interpolation, request/response interceptors, `AbortSignal`, and exponential backoff (`computeBackoffDelay`). |
| [`RealtimeChannel`](../../../../../src/ts/vertebra-client/src/client/ws.ts) | `@vertebra/client` | Auto-reconnecting WebSocket client with typed event listeners, heartbeat pings, and offline queue buffering. |
| [`SessionStore`](../../../../../src/ts/vertebra-client/src/store/index.ts) | `@vertebra/client` (`./store`) | Reactive client session/state store with `localStorage` persistence and `subscribe()` notifications. |
| [`HotReloadClient`](../../../../../src/ts/vertebra-client/src/hmr/index.ts) | `@vertebra/client` | Dev-mode HMR WebSocket client handling `reload`, `css-update`, and error overlay rendering. |
