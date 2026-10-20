---
name: pattern-typescript-client-and-adapter-subpaths
description: Zero-dependency @vertebra/client core, tree-shakeable framework adapter subpaths, Biome formatting, and Python codegen alignment.
sources:
  - src/ts/vertebra-client/package.json
  - src/ts/vertebra-client/src/index.ts
  - src/ts/vertebra-client/src/client/client.ts
  - src/ts/vertebra-client/src/client/types.ts
  - src/ts/vertebra-client/biome.json
---

# Client SDK & Framework Adapter Subpaths

[`@vertebra/client`](../../../../../src/ts/vertebra-client/package.json) provides a zero-runtime-dependency TypeScript core (`HttpClient`, `RealtimeChannel`, `SessionStore`, `HotReloadClient`) plus tree-shakeable subpath exports for six frontend frameworks and build tools.

## 1. Canonical Root Exports (`src/index.ts`)

The root `@vertebra/client` entrypoint exports only clean domain symbols (zero `Vertebra*` prefixes):

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

- `@vertebra/client/react` ([`src/react/cache.ts`](../../../../../src/ts/vertebra-client/src/react/cache.ts)) — React 19 hooks and `QueryCache`
- `@vertebra/client/vue` ([`src/vue/index.ts`](../../../../../src/ts/vertebra-client/src/vue/index.ts)) — Vue 3.5 composables
- `@vertebra/client/svelte` ([`src/svelte/index.ts`](../../../../../src/ts/vertebra-client/src/svelte/index.ts)) — Svelte 5 store bindings
- `@vertebra/client/angular` ([`src/angular/index.ts`](../../../../../src/ts/vertebra-client/src/angular/index.ts)) — Angular 22 DI service bindings
- `@vertebra/client/htmx` ([`src/htmx/index.ts`](../../../../../src/ts/vertebra-client/src/htmx/index.ts)) — HTMX extension helpers
- `@vertebra/client/vite` ([`src/vite/index.ts`](../../../../../src/ts/vertebra-client/src/vite/index.ts)) — Vite 8 plugin and dev proxy helpers
- `@vertebra/client/store` ([`src/store/index.ts`](../../../../../src/ts/vertebra-client/src/store/index.ts)) — Standalone `SessionStore`

## 3. Strict TypeScript, Biome & Codegen Parity

- **TypeScript ([`tsconfig.json`](../../../../../src/ts/vertebra-client/tsconfig.json))**: `strict: true`, `noUncheckedIndexedAccess: true`, `moduleResolution: "bundler"`, `target: "ES2022"`.
- **Biome ([`biome.json`](../../../../../src/ts/vertebra-client/biome.json))**: Enforced via `make ts-lint` (`bunx biome check --write --unsafe .`) and `make lint-all` (`bunx biome check .`).
- **Codegen Alignment**: Any change to [`RouteResponse`](../../../../../src/ts/vertebra-client/src/client/types.ts) or client route contracts must be updated in lockstep with [`_typescript.py`](../../../../../src/py/vertebra/builder/_codegen/_typescript.py), [`_zod.py`](../../../../../src/py/vertebra/builder/_codegen/_zod.py), and [`_manifest.py`](../../../../../src/py/vertebra/builder/_codegen/_manifest.py).
