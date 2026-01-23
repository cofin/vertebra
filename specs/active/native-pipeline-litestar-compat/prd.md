# PRD: Native Pipeline Compatibility for Real-World Litestar Apps

## 1. Intelligence Context

### 1.1 Complexity Assessment

**Complexity: Complex**

This work touches multiple layers (Python, Bun/TypeScript, Rust router), spans cross-repo compatibility requirements, and demands changes in routing, DI, request/response handling, and static file serving. It also requires significant new test coverage with real-world patterns. The scope includes both runtime behavior and documentation/test infrastructure.

### 1.2 Detected Stack and Commands

**Repository:** `vertebra` (current working repo)

**Primary languages:** Python 3.12+, TypeScript (Bun), Rust

**Build/Test commands (from `CLAUDE.md` / `README.md` / `AGENTS.md`):**

- `make install` — full setup (Python + Bun + Rust)
- `make test` — Python tests
- `make js-test` — Bun/TypeScript tests
- `make rs-test` — Rust tests
- `make test-all` — full test suite
- `make lint` / `make lint-all` — format + lint across the stack
- Integration tests: `uv run pytest src/py/tests -m integration --integration`

**Docs system:** No Sphinx or MkDocs detected in this repo (no `docs/conf.py` or `mkdocs.yml`). Documentation lives as Markdown in `docs/`.

**PRD location:** `specs/active/native-pipeline-litestar-compat/`

### 1.3 Similar Implementations Found

1. `src/py/vertebra/extensions/litestar/native.py` — Litestar route reflection and router build.
2. `src/py/vertebra/extensions/litestar/compiler.py` — native pipeline compilation (guards + DI + handler runner).
3. `src/py/vertebra/server/worker.py` — native pipeline activation and direct HTTP dispatch.
4. `src/ts/src/ipc/asgi.ts` — Bun-side routing and “direct.http” selection.
5. `src/ts/src/server/router.ts` — Bun static file serving path.
6. `src/py/tests/integration/litestar/test_full_compatibility.py` — minimal native compatibility test.

### 1.4 Patterns to Follow

- **Litestar integration should respect app-level and controller-level configuration** (guards, dependencies, signature namespace, auth toggles).
- **Native pipeline must be conservative**: only route direct if clearly supported; otherwise fallback to ASGI.
- **Compatibility should be verifiable**: use a route capability matrix and tests that encode expected support/fallback behavior.
- **Static file support should be explicit**: avoid implicit assumptions about Litestar static routes or Vite paths.

### 1.5 External Applications Reviewed (Source of Requirements)

- `~/code/litestar/litestar-sqlstack`
- `~/code/litestar/litestar-fullstack-spa`
- `~/code/litestar/litestar-fullstack-inertia`
- `~/code/g/dma/accelerator`

These apps represent the most common patterns used by the maintainer and are the target compatibility set for the native pipeline.

---

## 2. Problem Statement

Vertebra’s native Litestar pipeline currently supports only a narrow subset of Litestar’s real-world patterns. It reflects routes, builds a router, and compiles lightweight runners, but it assumes that:

- handlers are simple functions,
- dependencies are exclusively `Provide` values,
- request body is JSON and maps to `data`,
- responses are JSON-serializable objects,
- route handlers are always discoverable in `app.route_handlers`.

Real Litestar applications violate these assumptions. The reviewed apps use controllers, auto-discovery plugins, session middleware, Inertia responses, static file routers, multipart uploads, websockets, streaming responses, and complex dependency injection frameworks (Dishka / SQLSpec). The native pipeline must either support these features or intentionally fall back to ASGI. In the current state, the pipeline risks breaking these apps or failing to start at all.

Additionally, there is an identified failure in the current test case (error log referenced but not included), indicating a gap in pipeline behavior that is already observable. There is also insufficient test coverage to guard against regressions across realistic Litestar patterns.

The problem to solve:

1. **Compatibility:** Ensure the native pipeline works with the real-world Litestar apps listed above, or reliably falls back to ASGI for unsupported routes without crashing.
2. **Coverage:** Add comprehensive tests that encode the compatibility expectations and mimic real app patterns.
3. **Static Files:** Decide whether and how to use Bun’s static file server to intercept Litestar static routes, including Vite assets and uploaded file mounts.

### 2.1 User Stories and Use Cases

These user stories clarify what “works with all apps” means in practice.

1) **Maintainer running DMA locally**\n
As a maintainer, I run `dma` with `vertebra serve` and expect all API routes, WebSocket endpoints, and file uploads to function identically to the ASGI-only flow. The native pipeline should never break multipart uploads, streaming, or session-authenticated web flows. If those features are unsupported, the pipeline must transparently fallback to ASGI without user intervention.

2) **Maintainer running Inertia app in dev mode**\n
As a maintainer, I use the Inertia fullstack app and expect Inertia redirects, flash messages, and session state to behave correctly. The native pipeline should not interfere with Inertia-specific response semantics or content-type expectations. If native routing is incompatible, it must automatically route those requests via ASGI.

3) **Maintainer running SQLStack or Fullstack SPA**\n
As a maintainer, I expect JSON APIs to be fast and correct. Native pipeline should handle the common “JSON in, JSON out” routes with Provide dependencies and guards. Complex DI patterns or custom response types must still work through fallback.

4) **Maintainer enabling Bun static serving**\n
As a maintainer, I want Bun to serve SPA static assets when it is safe and configured, but I do not want it to unintentionally expose private upload directories or break Litestar static route behavior. Static serving must be explicit and reversible via configuration.

### 2.2 Non-Goals

The following are explicitly out of scope for this phase:

- Full reimplementation of Litestar’s DI engine or request/response lifecycle in native pipeline.\n
- Native support for complex response types (SSE, streaming with backpressure, multipart/form-data parsing) unless specifically targeted.\n
- Automatic inference of every Litestar plugin’s behavior. Plugins beyond core Litestar and Vite/Inertia are expected to trigger ASGI fallback unless proven safe.\n
- Providing production-grade web asset pipeline for every possible Litestar asset configuration. The initial goal is Vite SPA assets with a safe fallback for other cases.

---

## 3. Acceptance Criteria

### 3.1 Core Compatibility

- Native pipeline **does not crash** when reflecting or compiling routes for any of the four real apps.
- For each app, all routes are classified as either:
  - **Native-compatible** (served via direct pipeline), or
  - **ASGI-fallback** (served via standard ASGI flow).
- The classification must be explicit and testable (not implicit or accidental).

### 3.2 Controller and Router Support

- Controller-based routes and router-based routes are correctly discovered and compiled (or explicitly marked as fallback).
- Route auto-discovery via domain plugins (SQLStack, fullstack SPA, DMA) is supported in route reflection without start-up errors.

### 3.3 Dependency and Guard Handling

- Guards and `Provide` dependencies resolve in correct order for native routes.
- For any route using non-`Provide` DI (e.g., SQLSpec filters, Dishka injection), either:
  - native resolution is implemented, or
  - the route is marked as ASGI fallback.

### 3.4 Request Parsing and Parameter Handling

- Native routes support JSON request bodies for the `data` parameter.
- Typed path params in native routes (e.g., `int`, `uuid`) are converted correctly or else force fallback.
- Multipart/form-data routes are explicitly classified as ASGI fallback unless full support is implemented.

### 3.5 Response Behavior

- Native routes correctly handle status codes and headers for simple JSON responses.
- Non-JSON responses (redirects, streams, file responses, HTML) are either handled or forced into ASGI fallback.

### 3.6 WebSockets

- WebSocket routes are either fully supported in native pipeline or always marked for ASGI fallback.
- The fallback decision is deterministic and testable.

### 3.7 Static Files

- Static file routes (e.g., `create_static_files_router`) do not break the native pipeline.
- If Bun’s static file server is used for Litestar static files, the mapping is explicit, configurable, and tested.
- If Bun static serving is not used, ASGI fallback must be correct and documented.

### 3.8 Test Coverage

- New tests cover the main route patterns in the four apps.
- There is a compatibility matrix test that asserts expected native vs fallback behavior.
- The reported failure case is reproduced and fixed with a regression test once the full log is provided.

### 3.9 Documentation and Skills

- Update relevant documentation (e.g., `docs/arch.md` or `docs/architecture/*`) to reflect the compatibility matrix and pipeline behavior.
- Note any new patterns for possible inclusion in the `vertebra` skill.

---

## 4. Technical Approach

### 4.1 High-Level Strategy

The native pipeline must be **capability-aware**. Instead of assuming all routes are compatible, the pipeline should:

1. Reflect routes.
2. For each route, evaluate whether it is safely compatible with native execution.
3. Insert only compatible routes into the native router.
4. Force all other routes into ASGI flow.

This ensures correctness and allows incremental expansion of native support without breaking app behavior.

### 4.2 Route Reflection Enhancements

Current behavior:

- `reflect_routes` inspects `app.routes` and handles only `HTTPRoute` and `WebSocketRoute`.
- It stores `handler.fn` and `handler_id = id(handler.fn)`.

Required updates:

- Include richer metadata in the `LitestarRoute` structure:
  - route handler object (not just `.fn`), so compiler can use actual Litestar handler APIs.
  - route opts (e.g., `exclude_from_auth`, `component`, `media_type`).
  - any attached response model or return types.
  - signals indicating request parsing requirements (`Body`, `RequestEncodingType`).
- Detect and handle `ASGIRoute` or static file routes. If found:
  - Mark as ASGI fallback by default.
  - Optionally emit metadata for static file mapping (see Section 4.8).

### 4.3 Native Router Construction

Current behavior:

- `build_router` converts Litestar paths to matchit format by regex substitution.

Required updates:

- Ensure path conversion respects typed and constrained parameters (UUID, path, slug). The converter must be robust for patterns like `{param:uuid}` and `{param:path}`.
- Include trailing slash variants if Litestar normalizes them.
- If a route is identified as unsupported, it should **not** be inserted into the native router, preventing Bun from dispatching it as `direct.http`.

### 4.4 Handler Compilation and Resolution

Current behavior:

- `AsgiWorker.start` compiles native runners by searching `self._app.route_handlers` for `h.fn == route.handler`.

Issues:

- Controller handlers are not always present in `app.route_handlers` as function-based objects.
- Auto-discovered routes may not be directly in the original `route_handlers` list.

Required updates:

- Use the handler object from reflection (`LitestarRoute.handler_obj`) directly for compilation.
- Avoid searching `app.route_handlers` or, if necessary, map routes to handler objects via the `HTTPRoute` and handler mapping itself.

### 4.5 Dependency Injection and Guards

Current behavior:

- `compile_di` only understands `Provide` dependencies and basic dependency ordering.

Required updates:

- Introduce a **native DI capability classifier**:
  - If dependencies are only `Provide` with sync/async callables and no advanced parameter types, treat as native-compatible.
  - If dependencies involve `Dependency` or custom DI frameworks (Dishka, SQLSpec filters), mark as ASGI fallback.
- Optionally extend native DI to support:
  - Dependency injection from connection/request context.
  - Basic `Dependency` values that map to request, query, or path data.

### 4.6 Request Parsing and Parameter Handling

Current behavior:

- Only `data` param triggers JSON parsing.
- Path param conversion supports `int` only.

Required updates:

- Add path param conversion for common Litestar converters:
  - `uuid` → `UUID`
  - `path` → string (no conversion)
  - `str`, `int`, `float`, `bool` as needed
- Only allow native routing for JSON request bodies unless multipart support is added.
- If a handler uses `Body(media_type=RequestEncodingType.MULTI_PART)` or typed `UploadFile`, it must be marked fallback.
- If the handler expects `Request` or `ASGIConnection` with session access, treat as fallback unless a compatible request wrapper is implemented.

### 4.7 Response Handling

Current behavior:

- Native pipeline always returns JSON, status 200, and ignores headers.

Required updates:

- Define a `NativeResponse` interface or wrapper, enabling:
  - Status code overrides
  - Headers
  - JSON or text responses
- Ensure that returning `Response` objects triggers appropriate handling.
- Mark routes that return `Redirect`, `Stream`, `FileResponse`, or Inertia response types as fallback unless explicitly supported.

### 4.8 Static File Interception via Bun

There are three options to consider; the recommended approach is a hybrid strategy.

**Option A: Always fallback to ASGI for static routes**

- Safest; no mapping required.
- Cons: no performance gain from Bun file serving.

**Option B: Map Litestar static routes to Bun**

- Parse `create_static_files_router` and/or static route metadata to derive mount points.
- Extend Bun router to support multiple mount points with per-route config (path, directory, cache headers).
- Requires additional configuration channel between Python and Bun.

**Option C: Hybrid (recommended)**

- Use Bun static server for Vite output (`/static`, `/assets`) if the Litestar Vite config indicates local assets.
- Always fallback to ASGI for custom static mounts like `/uploads`.
- Capture Vite paths from Litestar config (if `asset_url` is local and `bundle_dir` is known).

Implementation approach for hybrid:

1. During Litestar app start, extract Vite config (if `litestar_vite` plugin is present) and send to Bun server configuration.
2. Configure Bun `staticDir` to the Vite bundle directory when it is safe to do so.
3. Ensure that non-asset static routes are not inserted into native router (ASGI fallback).

### 4.9 Compatibility Classification and Router Policy

Introduce a per-route compatibility check with explicit reasons. The route classifier should consider:

- Handler type (function vs controller method)
- Dependencies (Provide only vs advanced DI)
- Guard types (simple sync/async vs complex)
- Request body parsing requirements (JSON only vs multipart)
- Response type expectations (JSON vs redirect/stream/file)
- WebSocket usage
- Static file or ASGI route type

The classifier outputs:

- `NativeSupported`: safe to serve via direct pipeline
- `AsgiFallback`: must go through standard ASGI

Only `NativeSupported` routes are added to the native router. This prevents Bun from sending `direct.http` messages for unsupported routes.

### 4.10 Compatibility Matrix

Create a canonical compatibility matrix to guide tests and documentation. Example (initial, to be refined):

| Feature / Pattern | Native Supported | ASGI Fallback | Notes |
| --- | --- | --- | --- |
| Function-based handler, JSON body | Yes | No | Baseline support |
| Controller-based handler | Yes (if resolved) | No | Requires handler resolution fix |
| Provide DI only | Yes | No | If dependencies are only Provide |
| Dishka injection (Inject) | No | Yes | Needs explicit support or fallback |
| SQLSpec filter dependencies | No | Yes | Likely fallback initially |
| Inertia response types | No | Yes | Must use ASGI |
| Multipart Upload (`UploadFile`) | No | Yes | ASGI only |
| WebSocket routes | No | Yes | Until full interface implemented |
| Static file routes | No | Yes | Except Vite assets via Bun |
| Redirects / Streams | No | Yes | Unless explicitly supported |

This matrix should be updated with each new supported feature.

### 4.11 Observability and Debuggability

Native routing must be transparent to the developer. Add explicit instrumentation and debug aids:

- **Route classification logs**: At startup, log a summary of how many routes were marked `NativeSupported` vs `AsgiFallback`. Include representative reasons for fallback (e.g., `multipart`, `inertia`, `static`, `websocket`, `unsupported_di`).\n
- **Per-request tracing**: When a request is routed via `direct.http`, include a trace field in logs that clearly indicates native vs fallback, plus the handler id and matched path pattern.\n
- **Compatibility report output**: Provide a structured report (JSON or plain text) that can be generated in tests or on startup, listing each route, its classification, and rationale. This can be attached to test artifacts or printed in CI for local-only checks.\n
- **Failure mode transparency**: If a route was incorrectly classified and fails at runtime, ensure the error message includes “native pipeline” indicators and the route classification metadata to aid debugging.\n
- **Feature flags**: Extend `VERTEBRA_NATIVE` with an optional allowlist/denylist for route names or paths to simplify local debugging when a route misbehaves.\n

These measures reduce the time to identify misclassifications and will be important while native support is being expanded incrementally.

---

## 5. Testing Strategy

### 5.1 Unit Tests

Add or extend unit tests for:

- Route reflection:
  - Controller-based handlers
  - Router nesting
  - ASGI and static routes detection
- Compiler:
  - Provide dependencies with nested dependencies
  - Guard execution ordering
  - Path param conversion (int, uuid)
- Response handling:
  - Native response with status/headers
  - JSON serialization errors

### 5.2 Integration Tests

Add integration tests in `src/py/tests/integration/litestar/` that encode patterns from the four apps:

1. Controller-based route test
2. Domain auto-discovery simulation (mimic DomainPlugin behavior)
3. Inertia-style route returning non-JSON response (expect fallback)
4. Multipart upload route (expect fallback)
5. WebSocket route (expect fallback)
6. Static file route (expect fallback or Bun static serving)

### 5.3 Compatibility Matrix Tests

Create a dedicated compatibility matrix test that:

- Reflects routes from a set of representative test apps or fixtures.
- Asserts each route is classified as native-supported or fallback.
- Verifies that unsupported routes are not inserted into the native router.

### 5.4 Local-Only Compatibility Tests

Because the full apps live in external repositories (not necessarily in CI), introduce a local-only test suite (e.g., mark with `@pytest.mark.local` or `@pytest.mark.integration`) that:

- Imports the real apps (`litestar-sqlstack`, `fullstack-spa`, `fullstack-inertia`, `dma/accelerator`) if present.
- Runs route reflection and classifier to ensure there are no crashes.
- Produces a compatibility report for maintainers.

This preserves CI stability while enabling maintainers to validate real apps locally.

### 5.5 Regression Test for Reported Failure

Once the missing error log is provided, reproduce the failure in a dedicated test that asserts:

- The failure is resolved.
- The compatibility classifier handles the triggering condition.

---

## 6. Implementation Notes

### 6.1 Known Constraints

- Litestar has a rich DI and request/response system. Recreating full semantics in native pipeline is out of scope.
- The recommended approach is a **safe subset** of native support, with strict fallback behavior for complex routes.

### 6.2 Risk Areas

- Incorrect classification could send unsupported routes into native pipeline, breaking app behavior.
- Mis-handled static file mapping could expose private upload directories via Bun unintentionally.
- WebSocket handling requires careful parity with Litestar’s semantics; fallback is safest initially.

### 6.3 Documentation Updates

Update the following docs to clarify native pipeline scope and compatibility:

- `docs/arch.md` — include a section on native pipeline and fallback policy.
- `docs/architecture/error-handling.md` — document behavior for unsupported routes and fallback.
- `docs/progress.md` — track compatibility milestones.

### 6.4 Skill Updates

Potentially update `~/.codex/skills/vertebra/SKILL.md` after implementation to include:

- Native pipeline compatibility matrix
- Static file handling guidance
- Litestar integration best practices

### 6.5 Open Questions

- What is the exact failure log referenced by the user? (Required to finalize regression test.)
- Which routes in the four apps should be prioritized for native support (beyond JSON API routes)?
- Should Inertia routes ever be native-supported, or always ASGI?

---

## 7. Success Criteria (Expanded)

The effort is complete when:

- All routes in the four target apps can be reflected and classified without runtime errors.
- Native pipeline correctly handles basic JSON API routes (controllers or function-based) with Provide dependencies.
- Non-supported routes are reliably dispatched to ASGI.
- Static file behavior is explicitly defined and tested (Bun static or ASGI).
- Test coverage includes representative app patterns and a compatibility matrix test.
- Documentation accurately describes the native pipeline’s scope and fallback rules.

A final compatibility report should be generated for the four target apps and stored with test artifacts for review.

---

## 8. Appendix: App Pattern Summary

### SQLStack

- Domain auto-discovery
- SQLSpec plugin
- Vite SPA
- Provide dependencies and guard-based auth
- Primarily JSON APIs

### Fullstack SPA

- Advanced Alchemy
- OAuth2 and email services
- Background jobs (SAQ)
- Heavy controller usage
- Guard-based authorization

### Fullstack Inertia

- Inertia responses and redirect semantics
- Sessions and flash
- `create_static_files_router` for uploads
- Page components with `component=...`

### DMA Accelerator

- WebSockets + Channels plugin
- Multipart uploads and streaming
- SQLSpec filters
- Domain auto-discovery
- Session middleware and server-side sessions

These patterns define the compatibility requirements for the native pipeline.
