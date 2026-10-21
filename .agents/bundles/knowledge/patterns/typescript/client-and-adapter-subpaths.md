---
type: Pattern
title: "Client SDK & Framework Adapter Subpaths"
description: "Zero-dependency @vertebra/client core, tree-shakeable framework adapter subpaths, Biome formatting, and Python codegen alignment."
status: stable
tags: [patterns, typescript, client-sdk, adapters, codegen]
sources:
  - resource: src/ts/vertebra-client/package.json
  - resource: src/ts/vertebra-client/src/index.ts
  - resource: src/ts/vertebra-client/src/client/client.ts
  - resource: src/ts/vertebra-client/src/client/types.ts
  - resource: src/ts/vertebra-client/src/sse.ts
  - resource: src/ts/vertebra-client/src/websocket.ts
  - resource: src/ts/vertebra-client/biome.json
---

# Client SDK & Framework Adapter Subpaths

[`@vertebra/client`](../../../../../src/ts/vertebra-client/package.json) provides a zero-runtime-dependency TypeScript core (`HttpClient`, `RealtimeChannel`, `SessionStore`, `HotReloadClient`) plus tree-shakeable subpath exports for six frontend frameworks and build tools.

## 1. Canonical Root Exports (`src/ts/vertebra-client/src/index.ts`)

The root `@vertebra/client` entrypoint exports only clean domain symbols (`HttpClient`, `RealtimeChannel`, `HotReloadClient`, `SessionStore`)—never legacy `Vertebra*` prefixes:

<!-- truth: start -->
```typescript
export { HttpClient } from "./client/client";
export * from "./client/errors";
export { computeBackoffDelay } from "./client/retry";
export type { ClientConfig, RetryConfig, RouteResponse } from "./client/types";
export { RealtimeChannel } from "./client/ws";
export { HotReloadClient, type ServerMessage } from "./hmr/index";
export { SessionStore, store } from "./store/index";
```
<!-- truth: end -->

## 2. Isolated Subpath Exports (`package.json`)

Framework and tooling integrations live in dedicated subdirectories exported in [`package.json`](../../../../../src/ts/vertebra-client/package.json):

- `@vertebra/client/react` ([`src/ts/vertebra-client/src/react/cache.ts`](../../../../../src/ts/vertebra-client/src/react/cache.ts)) — React 19 hooks and `QueryCache`
- `@vertebra/client/vue` ([`src/ts/vertebra-client/src/vue/index.ts`](../../../../../src/ts/vertebra-client/src/vue/index.ts)) — Vue 3.5 composables
- `@vertebra/client/svelte` ([`src/ts/vertebra-client/src/svelte/index.ts`](../../../../../src/ts/vertebra-client/src/svelte/index.ts)) — Svelte 5 store bindings
- `@vertebra/client/angular` ([`src/ts/vertebra-client/src/angular/index.ts`](../../../../../src/ts/vertebra-client/src/angular/index.ts)) — Angular 22 DI service bindings
- `@vertebra/client/htmx` ([`src/ts/vertebra-client/src/htmx/index.ts`](../../../../../src/ts/vertebra-client/src/htmx/index.ts)) — HTMX extension helpers
- `@vertebra/client/vite` ([`src/ts/vertebra-client/src/vite/index.ts`](../../../../../src/ts/vertebra-client/src/vite/index.ts)) — Vite 8 plugin and dev proxy helpers
- `@vertebra/client/store` ([`src/ts/vertebra-client/src/store/index.ts`](../../../../../src/ts/vertebra-client/src/store/index.ts)) — Standalone `SessionStore`

## 3. Strict TypeScript, Biome & Codegen Parity

- **TypeScript ([`tsconfig.json`](../../../../../src/ts/vertebra-client/tsconfig.json))**: `strict: true`, `noUncheckedIndexedAccess: true`, `moduleResolution: "bundler"`, `target: "ES2022"`.
- **Biome ([`biome.json`](../../../../../src/ts/vertebra-client/biome.json))**: Enforced via `make ts-lint` (`bunx biome check --write --unsafe .`) and `make lint-all` (`bunx biome check .`).
- **Codegen Alignment**: Any change to [`RouteResponse`](../../../../../src/ts/vertebra-client/src/client/types.ts) or client route contracts must be updated in lockstep with [`_typescript.py`](../../../../../src/py/vertebra/builder/_codegen/_typescript.py), [`_zod.py`](../../../../../src/py/vertebra/builder/_codegen/_zod.py), and [`_manifest.py`](../../../../../src/py/vertebra/builder/_codegen/_manifest.py).
