---
type: Task
id: mcp-webmcp-core:t07
title: Upgrade TypeScript webmcp.ts bridge to document.modelContext.registerTool with signal disposal
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- mcp-webmcp-core:t06
files:
- src/ts/vertebra-client/src/webmcp.ts
- src/ts/vertebra-client/src/index.ts
tests:
- src/ts/vertebra-client/tests/agent.test.ts
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 2
claimed_by: executor
claimed_at: '2026-11-06T22:39:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T22:43:00Z'
last_verified_commit: "f8b6f844977a94d92d179cd8e7130a8c53a178e0"
verification_evidence: "make ts-test (54 passed), biome check, and tsup build passed."
created_at: '2026-11-06T06:36:00Z'
updated_at: '2026-11-06T22:44:00Z'
commit: "f8b6f844977a94d92d179cd8e7130a8c53a178e0"
tags:
- typescript
- webmcp
- client
requirements:
- R8
---

# Upgrade TypeScript webmcp.ts bridge to document.modelContext.registerTool with signal disposal

## Objective

Update `src/ts/vertebra-client/src/webmcp.ts` to detect `document.modelContext ?? navigator.modelContext`, call `registerTool(tool, { signal })` returning a `WebMCPRegistrationHandle` (`tools`, `errors`, `dispose()`), isolate per-tool registration failures, stay idempotent across HMR/SPA navigation, forward `{ signal }` in `execute(input, { signal })`, and remove legacy `provideContext`.

## Context

Read `../spec.md` and `.agents/skills/typescript-client/SKILL.md`. Rebuild the client bundle (`bun run --cwd src/ts/vertebra-client build`) after TypeScript edits so wheel-bundled assets stay synchronized.

## Steps

1. Add failing Vitest cases in `src/ts/vertebra-client/tests/agent.test.ts` covering `document.modelContext` priority over `navigator.modelContext`, `{ signal }` forwarding to both `registerTool` and `execute` `fetch`, `dispose()` / `AbortController.abort()` unregistration, HMR re-registration idempotency, per-tool registration error isolation, `title`/`outputSchema` forwarding, and absence of `provideContext`.
2. Update `src/ts/vertebra-client/src/webmcp.ts` and `src/ts/vertebra-client/src/index.ts` exports to implement the new `WebMCPRegistrationHandle` and `BrowserModelContext` contract.
3. Run Vitest, Biome, and `tsup` build verification, and commit `feat(client): upgrade WebMCP bridge to document.modelContext with signal disposal`.

## Verification

```bash
make ts-test
bun run --cwd src/ts/vertebra-client lint
bun run --cwd src/ts/vertebra-client build
```

## Acceptance Criteria

- [x] R8 (WebMCP): `registerWebMCPTools` supports `document.modelContext`, `AbortSignal` disposal, HMR idempotency, and per-tool error isolation without `provideContext`.

## Notes & Discoveries

None.
