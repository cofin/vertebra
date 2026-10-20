---
name: domain-framework-routing-and-app
description: Canonical domain types for Vertebra's Python HTTP application container, router, Inertia v3 protocol, and JSON serialization.
sources:
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/framework/_router.py
  - src/py/vertebra/framework/_inertia.py
  - src/py/vertebra/utils/serializers/_json.py
  - src/py/vertebra/utils/module_loader.py
---

# Python Framework, Routing & Serialization Domain

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py) and [`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py) define the Python-facing application container, declarative router, Inertia.js v3 protocol middleware, and JSON serialization primitives.

## 1. Application Container & Routing (`_app.py`, `_router.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`App`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Top-level HTTP application container; owns route registration (`@app.get`, `@app.post`, `@app.put`, `@app.delete`, `@app.patch`, `@app.route`), sub-router inclusion (`include_router`), sub-app mounting (`mount`), and lifecycle hooks (`on_startup`, `on_shutdown`). |
| [`Router`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Declarative Python route group with a URL `prefix` and shared `middleware` list; composes into `App` or parent `Router` instances via `include_router`. |
| [`RouteDef`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Immutable dataclass describing a registered route (`method`, `path`, `handler`, `name`, `middleware`) and computing its deterministic FNV-1a 64-bit `handler_id`. |
| [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Introspected handler metadata (`fn`, `is_async`, `param_names`, `param_types`, `return_type`) used by both runtime dispatch and static TypeScript/Zod codegen. |
| [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) | `vertebra._vertebra` | Zero-copy PyO3 request wrapper slicing `method`, `path`, `query`, `headers`, and `body` directly from `DataSlab`. |
| [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) | `vertebra._vertebra` | PyO3 HTTP response (`status`, `headers`, `body`, `request_id`). |

## 2. Inertia.js v3 Protocol (`_inertia.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`InertiaMiddleware`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Negotiates full-page HTML shell rendering vs `X-Inertia: true` JSON visits, `409 Conflict` (`X-Inertia-Location`) version mismatches, partial prop filtering (`X-Inertia-Partial-Data` / `Except`), and `303 See Other` redirects. |
| [`InertiaConfig`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Configuration dataclass holding `version`, `root_view`, `ssr_enabled`, `ssr_render` callback, and `shared_props` providers. |
| [`PageProps`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Inertia v3 page payload (`component`, `props`, `url`, `version`, `encryptHistory`, `clearHistory`, `deferredProps`, `mergeProps`). |

## 3. Serialization & Optional Dependencies (`utils/`)

- **[`JSONSerializer`](../../../../../src/py/vertebra/utils/serializers/_json.py)** (`encode_json`, `decode_json`, `encode_json_bytes`): Encodes and decodes JSON using `msgspec.json` directly (`Encoder`, `Decoder`) with fallback to stdlib `json`.
- **[`OptionalDependency`](../../../../../src/py/vertebra/utils/module_loader.py)** (`import_app`, `ensure_packages`, `module_to_os_path`): Resolves `"module:attribute"` application targets and guards optional dependencies with actionable `uv add <pkg>` error diagnostics.
