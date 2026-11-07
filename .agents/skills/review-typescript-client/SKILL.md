---
name: review-typescript-client
description: Use when reviewing TypeScript client SDK code, Biome linting, tsup builds, framework adapters, bundler integration, or Inertia/SSR/WebMCP interfaces.
---

<!-- truth: start -->
- **Exports**: Root `index.ts` exports canonical domain types; adapters use subpaths (`TS-01`).
- **Dependencies**: Core package maintains zero runtime dependencies (`TS-02`).
- **Strictness**: `strict` and `noUncheckedIndexedAccess` enabled with no type assertions on network input (`TS-03`).
- **Realtime**: Reconnection logic enforces exponential backoff with random jitter (`TS-05`).
- **Tree-shaking**: Package marks `sideEffects: false` with pure ESM entrypoints (`TS-06`).
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

1. **Subpath boundaries**: Ensure adapters (React, Vue, Svelte, Vite) do not contaminate core bundle (`TS-01`, `TS-06`).
2. **Zero dependencies**: Verify core package declares zero runtime dependencies in `package.json` (`TS-02`).
3. **Type safety**: Check that `any` and unvalidated index accesses are prohibited (`TS-03`).
4. **Contract synchronization**: Verify TypeScript type definitions match Python codegen outputs (`TS-04`).
5. **Realtime lifecycle**: Confirm listeners and timers are cleanly removed on channel teardown (`TS-05`).
6. **Lint and test hygiene**: Validate Biome cleanliness and ensure Vitest runs with fake timers (`TS-08`).

## Suppressions Index

Shared catalog: [`false-positive-suppressions.md`](../review-core/references/false-positive-suppressions.md).

- `FP-TS-01`: Zero dependency client backoff

## Focused Commands

```bash
cd src/ts/vertebra-client && bunx biome check .
cd src/ts/vertebra-client && bun run test
cd src/ts/vertebra-client && bun run build
```
