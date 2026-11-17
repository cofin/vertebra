---
name: review-typescript-client
description: Use when reviewing TypeScript client SDK code, Biome linting, tsup builds, framework adapters, bundler integration, or Inertia/SSR/WebMCP interfaces.
---

<!-- truth: start -->
- **Exports**: Root `index.ts` exports canonical domain types; adapters (`react`, `vue`, `svelte` via `svelte/store`, `angular`, `htmx`, `vite`) use subpaths (`TS-01`).
- **Dependencies**: Core package maintains zero runtime dependencies with optional `peerDependenciesMeta` (`TS-02`).
- **Strictness**: `strict` and `noUncheckedIndexedAccess` enabled in `tsconfig.json`, Biome `noExplicitAny: "error"`, and no type assertions on network input (`TS-03`).
- **Codegen & Vite**: Route registries (`{ method, path }`, `{ method: "WS", path }`) and runtime Vite `/__vertebra/manifest.json` fetches match Python codegen (`TS-04`).
- **Realtime & WebMCP**: Reconnection enforces exponential backoff with jitter (`TS-05`); WebMCP `requiresConfirmation` tools fail closed via `onConfirm` / `confirm`.
- **Tree-shaking**: Package marks `"sideEffects": false` with pure ESM `tsup` entrypoints (`TS-06`).
- **Suppressions**: Respect `FP-TS-01` in the shared catalog.
<!-- truth: end -->

# TypeScript Client Review (`review-typescript-client`)

## Scope

- Paths: `[areas.typescript-client]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `TS-01`..`TS-08` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Bundler and client SDK](../../bundles/knowledge/domains/frontend/bundler-and-client-sdk.md)
- [Bundler HMR and SSR](../../bundles/knowledge/architecture/frontend/bundler-hmr-and-ssr.md)
- [Codegen and Inertia](../../bundles/knowledge/architecture/frontend/codegen-and-inertia.md)
- [Client and adapter subpaths](../../bundles/knowledge/patterns/typescript/client-and-adapter-subpaths.md)
- Codegen parity reference: [`references/codegen-parity.md`](references/codegen-parity.md)

## Review Process

1. **Subpath boundaries**: Ensure adapters (`react`, `vue`, `svelte` using `svelte/store` without runes, `angular`, `htmx` with `detail`/form-encoding guards, `vite`) declare optional peers and do not contaminate the core bundle (`TS-01`, `TS-02`, `TS-06`).
2. **Tree-shaking & runtime dependencies**: Verify core package declares zero runtime `dependencies`, `"sideEffects": false`, and matching `package.json` `exports` / `tsup.config.ts` entries (`TS-02`, `TS-06`).
3. **Type & lint safety**: Check `strict` + `noUncheckedIndexedAccess` in `tsconfig.json`, Biome `noExplicitAny: "error"`, and absence of `any` or unvalidated index/network casts (`TS-03`).
4. **Contract & Vite synchronization**: Verify `RouteRegistry` (`{ method, path }` and WebSocket `{ method: "WS", path }`) and `vitePlugin` runtime `/__vertebra/manifest.json` fetch stay aligned with Python `routes.ts` / `manifest.json` codegen (`TS-04`).
5. **Realtime & WebMCP safety**: Confirm listeners and timers are cleanly removed on channel teardown (`TS-05`), and `registerWebMCPTools` enforces fail-closed `requiresConfirmation` gating (`onConfirm` / `confirm`) plus `parseMcpJsonRpcResponse`.
6. **Lint and test hygiene**: Validate Biome, `tsc --noEmit`, and ensure Vitest runs with fake timers (`TS-08`).

## Suppressions Index

Shared catalog: [`false-positive-suppressions.md`](../review-core/references/false-positive-suppressions.md).

- `FP-TS-01`: Zero dependency client backoff

## Focused Commands

```bash
cd src/ts/vertebra-client && bunx biome check .
cd src/ts/vertebra-client && bunx tsc --noEmit
cd src/ts/vertebra-client && bun run test
cd src/ts/vertebra-client && bun run build
```
