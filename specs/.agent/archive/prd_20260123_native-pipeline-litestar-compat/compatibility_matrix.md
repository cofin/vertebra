# Native Pipeline Compatibility Matrix

## Target Applications
This matrix tracks the compatibility of the Vertebra Native Pipeline with four reference applications that represent real-world Litestar usage patterns.

| Application | Pattern | Native Support Status | Fallback Reason (Phase 1) |
| :--- | :--- | :--- | :--- |
| **Litestar SQLStack** | Controller Routing | ✅ Planned | - |
| | `create_filter_dependencies` | ❌ Fallback | Dynamic dependency factories returning `Dependency()` |
| | Dishka `Inject[...]` | ❌ Fallback | Custom DI injection not natively compiled |
| | `UUID` Path Params | ✅ Planned | - |
| | Pydantic Response | ✅ Planned | - |
| **Fullstack SPA** | Controller Routing | ✅ Planned | - |
| | `AsyncSession` Dependency | ❌ Fallback | Requires SQLAlchemy plugin / middleware context |
| | Manual `Response` return | ✅ Planned | Needs `NativeResponse` wrapper implementation |
| | `exclude_from_auth` | ✅ Planned | Route metadata reflection |
| **Fullstack Inertia** | `InertiaRedirect` | ❌ Fallback | Custom response type |
| | `NoProps` (Inertia) | ❌ Fallback | Custom response processing |
| | `File` Response | ❌ Fallback | File serving semantics |
| | `request.session` | ❌ Fallback | Session middleware dependency |
| **DMA Accelerator** | `Multipart/Form-Data` | ❌ Fallback | Streaming parser complexity (See `enhancements.md`) |
| | WebSockets | ❌ Fallback | No native WS interface yet |
| | Dishka Injection | ❌ Fallback | Custom DI |
| | `requires_superuser` Guard | ✅ Planned | Standard callable guard |

## Feature Capability Classification

The Native Pipeline Capability Classifier (`classify_route`) will use these rules to determine routing.

### 1. Native Supported (Direct Dispatch)
*Routes that match ALL of the following criteria:*

- **Handler:** Function or Controller method.
- **Method:** GET, POST, PUT, PATCH, DELETE.
- **Request Body:** None or JSON (`data: Model`).
- **Path Params:** `str`, `int`, `float`, `bool`, `uuid`, `path`.
- **Dependencies:** Only simple `Provide` callables (sync/async) or no dependencies.
- **Response:** Pydantic Model, `dict`, `str`, `bytes`, or `NativeResponse` wrapper.
- **Guards:** Standard sync/async callables.

### 2. ASGI Fallback (Proxy Dispatch)
*Routes that contain ANY of the following:*

- **Request Encoding:** `Multipart/Form-Data`.
- **Dependencies:** `Dependency()` object, `Inject[]` (Dishka/SQLSpec), `Request`, `ASGIConnection`, `WebSocket`.
- **Response Types:** `File`, `Stream`, `Redirect`, `Template`, `InertiaResponse`.
- **Protocol:** WebSocket.
- **Static Files:** Any route served by `StaticFilesRouter` (unless intercepted by Bun Static strategy).

## Implementation Priority

1.  **Reflect & Detect:** Ensure all routes above are correctly identified and classified.
2.  **Safeguard:** Ensure "Fallback" routes are **never** inserted into the Native Router.
3.  **Expand:** Incrementally add support for `UUID` params, `NativeResponse` wrapper, and basic `Provide` DI to move items from "Fallback" to "Native".
