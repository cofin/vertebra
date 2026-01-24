# Analysis

## Goal
Ensure Vertebra's native Litestar pipeline (route reflection, router matching, native runner compilation, ASGI bridge) is compatible with the real-world Litestar apps on this machine:

- `~/code/litestar/litestar-sqlstack`
- `~/code/litestar/litestar-fullstack-spa`
- `~/code/litestar/litestar-fullstack-inertia`
- `~/code/g/dma/accelerator`

This includes robust route coverage, DI/guards, request/response handling, Inertia workflows, static file handling, and WebSocket behavior. Expand tests to prevent regressions and consider leveraging Bun’s static file serving for Litestar static file configs.

## Complexity
Complex. This spans multiple external app patterns, crosses Python/TS/Rust layers, and requires new compatibility testing and routing/static file design decisions.

## Planning Summary (from multi-step planner)

### Phase A — App Survey & Route/Component Matrix
- Enumerate route/handler patterns across the four Litestar apps.
- Capture Litestar components in use: controllers, routers, auto-discovery plugins, guards, DI providers, session/middleware, channels, static files, Vite/Inertia integration, uploads, streaming, and WebSockets.

### Phase B — Native Pipeline Gap Analysis
- Trace native pipeline flow:
  - Litestar route reflection → router build → ASGI bridge “direct.http” → worker native runner.
- Identify incompatibilities (controller route handler resolution, DI handling, request parsing, response handling, static file routing, WebSocket semantics).
- Document the current failure mode (requires user-provided log).

### Phase C — Design Options & Test Coverage
- Define compatibility scope and fallback behavior:
  - Supported feature set for native pipeline.
  - When to drop to ASGI.
- Evaluate static file interception:
  - Extract static file config from Litestar and map to Bun’s static file server.
- Expand integration tests:
  - Test coverage for representative route types from each app.
  - Compatibility matrix test that asserts route coverage and fallback consistency.

### Deliverables
- Compatibility matrix for Litestar features vs native pipeline.
- Proposed changes to reflection/compile/dispatch to support controllers, routers, DI, request parsing, response types.
- Static file handling proposal with Bun.
- Expanded test plan covering SPA, Inertia, SQLStack, DMA patterns.

## Dependencies & Risks
- The reported failure log is missing (user-provided snippet required).
- External apps may evolve; test harness should be robust to non-essential differences.
- Native pipeline changes must preserve ASGI fallback behavior and avoid regressions.
