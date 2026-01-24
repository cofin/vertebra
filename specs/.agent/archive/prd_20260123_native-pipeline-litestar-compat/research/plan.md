# Research Plan and Findings

## Scope and Objective
The objective is to make Vertebra’s native Litestar pipeline compatible with the real-world Litestar apps on this machine, and to expand test coverage so regressions are caught early. The apps include:

- `~/code/litestar/litestar-sqlstack`
- `~/code/litestar/litestar-fullstack-spa`
- `~/code/litestar/litestar-fullstack-inertia`
- `~/code/g/dma/accelerator`

The native pipeline covers these subsystems in Vertebra:

- Route reflection and router build: `src/py/vertebra/extensions/litestar/native.py`
- Route compilation: `src/py/vertebra/extensions/litestar/compiler.py`
- Native dispatch in the Python worker: `src/py/vertebra/server/worker.py`
- Direct HTTP selection and routing in Bun: `src/ts/src/ipc/asgi.ts`
- Static file support in Bun router: `src/ts/src/server/router.ts`

The end state is a compatibility matrix that shows which Litestar features are supported in native pipeline execution, which features force a fallback to ASGI, and how we test those decisions across real apps.

---

## App Survey: Route and Component Patterns

### 1) `litestar-sqlstack` (SQLSpec + Dishka + Vite)

Key patterns and files:

- App initialization via plugin: `src/sqlstack/server/asgi.py` creates `Litestar(..., plugins=[ApplicationCore()])`.
- `ApplicationCore` configures everything in `src/sqlstack/server/core.py`:
  - OpenAPI config with `OpenAPIConfig` and `ScalarRenderPlugin`.
  - CORS and session middleware.
  - Plugins: `StructlogPlugin`, `GranianPlugin`, `SQLSpecPlugin`, `ProblemDetailsPlugin`, `WorkerPlugin`, `DomainPlugin`, `VitePlugin` in `src/sqlstack/server/plugins.py`.
  - DI using Dishka container (`make_litestar_container` in `src/sqlstack/ioc.py`) and `setup_dishka` hooks.
  - App-level dependencies via `Provide`, e.g. `current_user` dependency.

Routes and handlers:

- Controllers in `src/sqlstack/domain/*/controllers/*` use `Controller` with `@get`, `@post`, `@patch`, `@delete` and typed path parameters like `{user_id:uuid}`.
- Example controllers:
  - `src/sqlstack/domain/accounts/controllers/_user.py` uses CRUD on `/api/users` with typed UUID path params and `Provide` dependencies.
  - `src/sqlstack/domain/system/controllers/_system.py` has system routes and media types.
  - `src/sqlstack/domain/web/controllers/_web.py` uses HTML responses.
- Domain auto-discovery via `DomainPlugin` in `src/sqlstack/utils/domains.py`, which scans `sqlstack.domain.*.controllers` and optionally wraps controllers in Dishka router (LitestarRouter).

Front-end integration:

- Vite config in `src/sqlstack/lib/settings.py` uses `ViteConfig(mode="spa", runtime=RuntimeConfig(executor="bun"))` and asset URLs like `/static/`.

Implications for native pipeline:

- Heavy use of plugin-based initialization and auto-discovered controllers.
- SQLSpec filters and DI via `Provide` and Dishka routers.
- Typed path params (UUID), non-JSON payloads likely exist.
- Potential HTML responses and non-JSON response types.

### 2) `litestar-fullstack-spa` (Advanced Alchemy + SAQ + Vite + OAuth)

Key patterns and files:

- `ApplicationCore` in `src/py/app/server/core.py` configures OpenAPI, security, dependencies, and a large plugin set.
- `plugins.py` registers `SQLAlchemyPlugin`, `SAQPlugin`, `VitePlugin`, `OAuth2ProviderPlugin`, `EmailPlugin`, `ProblemDetailsPlugin`, `StructlogPlugin`, and `DomainPlugin`.
- Domain auto-discovery in `src/py/app/utils/domain/*` similar to SQLStack.
- Controller modules in `src/py/app/domain/*/controllers/*` with heavy CRUD, guard usage, and dependency injection via `Provide`.

Route patterns:

- `Controller` subclasses with `path` or per-method paths.
- Routes include `exclude_from_auth`, custom `security` overrides, and `guards` for authorization.
- Typed path params: UUID, string slugs, etc.
- OAuth flows using dependencies like `Provide(oauth2_authorize_callback)`.

Front-end integration:

- Vite config in `src/py/app/lib/settings.py` uses `mode="spa"` with `RuntimeConfig(executor="bun")` and `asset_url="/static/"`.

Implications for native pipeline:

- Extensive use of guards and dependencies with varied resolution order.
- Heavy reliance on `Provide` plus app-level dependencies.
- Multiple route metadata patterns (`component=...`, `exclude_from_auth`, `security=[]`).
- Responses include non-trivial status codes and custom errors.

### 3) `litestar-fullstack-inertia` (Inertia + Vite + Sessions + Static Uploads)

Key patterns and files:

- `ApplicationCore` in `app/server/core.py` sets up:
  - `create_static_files_router(...)` for uploads at `/uploads`.
  - Session store using `SQLAlchemyStore` with `StoreRegistry`.
  - Template config, CSRF config, and logging hooks.
  - Multiple controllers listed explicitly (not auto-discovered in the provided code), including admin and web.
- Vite config in `app/lib/settings.py` uses `ViteConfig` with `inertia=InertiaConfig(...)` and `RuntimeConfig(executor="bun")`.
- Inertia features used throughout controllers:
  - `InertiaRedirect`, `InertiaExternalRedirect`, `InertiaBack`, `flash`, and `share` from `litestar_vite.inertia`.
  - `component="..."` attribute on handlers for page component mapping.
  - `Request.session` manipulation and access to session data.
  - Inertia-specific redirect codes (303/307) enforced by middleware.

Route patterns:

- Many routes are page components (Inertia) rather than JSON APIs.
- Controllers use `Request` heavily for sessions, flash messages, and redirect logic.
- Routes include `exclude_from_auth` and `include_in_schema=False` for web pages.
- Static file serving uses Litestar’s `create_static_files_router` (important for intercepting with Bun).

Implications for native pipeline:

- Native pipeline must understand or gracefully bypass Inertia page responses.
- Native pipeline must not break request.session or the Inertia response format.
- Static file routes must be handled either via ASGI or by the Bun static server.

### 4) `dma/accelerator` (SPA + WebSockets + Uploads + SQLSpec)

Key patterns and files:

- `ApplicationCore` in `src/py/dma/server/core.py` uses:
  - Session middleware via `SessionMiddleware` and `ServerSideSessionBackend`.
  - Plugins: `Structlog`, `Granian`, `SQLSpecPlugin`, `ProblemDetailsPlugin`, `WorkerPlugin`, `VitePlugin`, `ChannelsPlugin`, `DomainPlugin`.
  - Auto-discovery of controllers, jobs, listeners via `DomainPlugin`.
  - A broad signature namespace with many filters and service types.
- Vite config in `src/py/dma/lib/settings.py` uses `ViteConfig(mode="spa")` and sets `asset_url` to `/static/`.
- Controllers include:
  - WebSocket endpoints in `src/py/dma/domain/workspaces/controllers/_workspace.py` using `@websocket` and `WebSocket` APIs.
  - Multipart upload endpoints in `src/py/dma/domain/workspaces/controllers/_workspace_file.py` using `RequestEncodingType.MULTI_PART` and `UploadFile`.
  - Streaming responses with `Stream` and redirect responses with `Redirect`.
  - Query filters via `create_filter_dependencies` from SQLSpec.

Implications for native pipeline:

- WebSocket support requires a faithful socket interface (accept, send_json, close) or fallback to ASGI.
- Multipart parsing and `UploadFile` are currently not supported by native request parsing.
- Streaming responses, redirects, and non-JSON output must be supported or routed to ASGI.

---

## Native Pipeline Architecture (Vertebra)

### Reflection and Router

- `reflect_routes` in `src/py/vertebra/extensions/litestar/native.py` traverses `app.routes` (flattened) and only handles `HTTPRoute` and `WebSocketRoute`.
- It collects:
  - `path`, `methods`, `handler.fn`, `guards`, `dependencies`.
  - `handler_id` is currently `id(handler.fn)`.
- `build_router` converts Litestar paths (`/path/{param:type}`) into `matchit` paths (`/path/:param`), and inserts `/{METHOD}{path}` into the Rust router.

Observations:

- It does not consider `ASGIRoute` (often used for static files or mounted apps).
- It does not capture the original handler object; only `handler.fn` and resolved guards/dependencies.
- It assumes every route can be mapped into a direct route by the router.

### Compiler

- `PipelineCompiler` in `src/py/vertebra/extensions/litestar/compiler.py` compiles:
  - Guards into a single async function.
  - A simplified DI resolver from `Provide` dependencies.
  - A runner that executes guards → resolves deps → calls handler.

Critical limitations observed:

- DI resolver only supports `Provide` dependencies and only resolves those directly referenced in handler signature.
- It does not support:
  - `Dependency` injection (Litestar’s parameter-level injection)
  - `Body` / request parsing with non-JSON types
  - `Parameter` type conversion beyond `int`
  - `Request`/`ASGIConnection` typed injection (it uses the raw `VertebraRequest` object)
- It hardcodes `data` for JSON body parsing.

### Worker Native Dispatch

- `AsgiWorker.start` compiles native routes if `VERTEBRA_NATIVE` is enabled and app is a Litestar instance.
- It calls `reflect_routes`, then attempts:

  ```python
  runner = compiler.compile_route(next(h for h in self._app.route_handlers if h.fn == route.handler))
  ```

This implies:

- It expects the original handler object to be in `self._app.route_handlers` and to expose `.fn`.
- This is fragile for controller-based routes, router-based routes, or domain-plugin discovered routes.
- If the `next(...)` call fails, startup will error. If it does not fail but returns the wrong handler, compile results can be incorrect.

### Bun Bridge and Router

- `AsgiBridge.handle` in `src/ts/src/ipc/asgi.ts` matches routes using `ShmRouter`, and if matched:
  - Sends `direct.http` to Python.
  - Includes `handler_id` and `path_params`.
- If not matched, ASGI fallback is used.

Static file serving in Bun:

- `createRouter` in `src/ts/src/server/router.ts` uses `staticDir` to serve static assets for GET/HEAD, by reading with `Bun.file(...)`.
- Static support is single-directory, with a basic fallback; it currently doesn’t map Litestar static routers or multiple mount points.

---

## Compatibility Gaps Between Apps and Native Pipeline

### 1) Handler Resolution and Controller Support

- Real apps use controllers (class-based handlers) and auto-discovery via domain plugins.
- The native pipeline searches `self._app.route_handlers` for `.fn` to compile, which likely fails for:
  - Controller-based handlers
  - Router-based handlers
  - Plugin-injected handlers
- This is likely the root cause of the current failure (requires actual log to confirm).

### 2) Dependency Injection Semantics

- Apps rely on:
  - `Provide` dependencies (app and controller level)
  - `create_filter_dependencies` from SQLSpec (returns `Dependency` and filtering rules)
  - `Inject[...]` from Dishka (DMA)
- The native compiler only resolves `Provide` dependencies and only from handler signature. It doesn’t resolve or execute Litestar DI lifecycles.

### 3) Request Parsing & Parameter Handling

- Native `VertebraRequest` only supports JSON body parsing via `json.loads`.
- Apps require:
  - Multipart parsing (`RequestEncodingType.MULTI_PART` + `UploadFile`)
  - Form or query params, including typed converters (UUID, Path, IP address, etc.)
  - `Request` object semantics (sessions, cookies, scope, headers)

### 4) Response Handling

- `_handle_direct_http` assumes return is JSON-serializable, always returns status 200, and ignores headers.
- Apps require:
  - Redirects (Inertia / auth flows)
  - Response types like `Redirect`, `Stream`, file responses, and HTML pages
  - Error responses with proper status codes and body shapes

### 5) WebSockets

- DMA uses `WebSocket` and `channels` plugin (send_json, accept, close semantics).
- Native pipeline uses a minimal socket wrapper without accept or JSON helpers.
- A safe approach is to always fallback to ASGI for WebSocket handlers unless fully implemented.

### 6) Static Files

- Inertia app uses `create_static_files_router` for uploads (not represented in `reflect_routes`).
- DMA and SQLStack rely on `/static/` assets and Vite output, but this is configured at the Litestar level.
- Bun’s static server can serve a single directory and does not understand multiple mount points or custom headers.

---

## Static File Interception Strategy (Bun)

### Option A: ASGI-only for static routes

- Easiest and safest: static routes always fall back to ASGI.
- Pros: no mapping complexity, fully honors Litestar features.
- Cons: loses Bun static serving performance, might be slower in production.

### Option B: Extract static config and map to Bun server

- Parse Litestar app config to locate static file routes or `create_static_files_router` instances.
- Feed a derived `staticDir` or `staticRoutes` config into Bun.
- Challenges:
  - Litestar supports multiple static directories and mount points.
  - `create_static_files_router` uses specific route handler logic (path, caching, headers) that Bun router must replicate.
  - App-level static settings (asset URL, CSP headers, caching) need to be captured.

### Option C: Hybrid approach

- Use Bun static server for the primary Vite output directory (e.g. `/static/` or `/assets/`).
- Always fallback to ASGI for any Litestar static routers beyond the main asset directory (e.g. `/uploads`).
- This matches current app usage: Vite assets can be served by Bun, while upload serving remains in Litestar.

---

## Test Coverage Expansion Needs

Current Vertebra tests cover basic routing/DI and reflection but do not mirror app-level patterns:

- `src/py/tests/integration/litestar/test_full_compatibility.py` only covers a simple route using a mocked handler wrapper.
- No tests simulate controller routes, routers, or domain auto-discovery.
- No tests cover response types (Redirect, Stream), multipart parsing, WebSocket flows, or Inertia response behavior.

Required additions:

1) Controller-based routing tests
- Build a minimal Litestar app with `Controller` subclasses and `Router` instances.
- Ensure native pipeline can resolve handlers and build runners.

2) Dependency graph coverage
- Use `Provide` and `create_filter_dependencies` and confirm native DI works or falls back.

3) Response behavior
- Test redirect and status code responses. Validate headers and body handling.

4) Multipart parsing
- Simulate request with `RequestEncodingType.MULTI_PART` and ensure fallback or explicit support.

5) Static file routes
- Use `create_static_files_router` and check whether native pipeline properly avoids direct routing or uses Bun static server.

6) WebSockets
- Ensure `@websocket` routes are never marked as native unless WebSocket behavior is fully supported.

7) Compatibility matrix tests
- Create a test that reflects routes from sample apps and verifies for each route whether it is:
  - fully supported by native pipeline, or
  - explicitly marked as ASGI fallback.

---

## Evidence and Missing Failure Detail

The user provided a reference to a current failure log (“Pasted Content 2814 chars”), but the log is not included. The PRD should include a requirement to capture this exact failure, reproduce it, and add a regression test. Likely failure candidates based on the code:

- `StopIteration` during native runner compilation when controller handlers are not found in `self._app.route_handlers`.
- Mismatch between handler IDs and compiled runners due to controller/route mapping.
- JSON serialization failure for non-dict responses in native pipeline.

---

## Summary of Patterns to Preserve

Across all apps, the following Litestar components and conventions are essential:

- `Controller` subclasses with route handler decorators and typed path params.
- `Router` and `DomainPlugin` auto-discovery.
- `Provide` dependencies and app-level DI.
- Guards and security settings, including `exclude_from_auth` and custom security.
- Vite plugin integration (`ViteConfig`, `RuntimeConfig(executor="bun")`).
- Inertia support (`InertiaRedirect`, `flash`, `share`).
- Static file routes (`create_static_files_router`).
- WebSockets and channel plugins.
- Multipart uploads and `UploadFile`.
- Response types: redirects, streams, file responses, and non-JSON content.

All of these must be considered in the native pipeline support or explicitly declared as ASGI-fallback-only in a compatibility matrix.
