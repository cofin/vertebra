---
type: Guide
title: "Python Framework, Routing & Serialization Domain"
description: "Canonical domain types for Vertebra's Python HTTP application container, router, Inertia v3 protocol, and JSON serialization."
status: stable
tags: [domains, framework, routing, serialization, python]
sources:
  - resource: src/py/vertebra/framework/__init__.py
  - resource: src/py/vertebra/framework/_app.py
  - resource: src/py/vertebra/framework/_router.py
  - resource: src/py/vertebra/framework/_inertia.py
  - resource: src/py/vertebra/framework/_types.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/utils/module_loader.py
  - resource: src/py/vertebra/_typing.py
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/response.rs
---

# Python Framework, Routing & Serialization Domain

[`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py) and [`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py) define the Python-facing application container, declarative router, Inertia.js v3 protocol middleware, and JSON serialization primitives.

## 1. Application Container & Routing (`_app.py`, `_router.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`App`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Top-level HTTP container (`@app.get`, `@app.post`, `@app.put`, `@app.delete`, `@app.patch`), `register(Router \| handler)`, `add_route(RouteDef)`, `mount(prefix, sub_app, protocol="auto")`, `on_startup`/`on_shutdown` hooks (`startup()`, `shutdown()`), and `handler_index: dict[int, HandlerInfo]`. |
| [`Router`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Prefix route group (`__slots__ = ("prefix", "routes")`) that collects functions decorated with module-level `get`/`post`/`put`/`delete`/`patch` via `register(handler)` and attaches to `App.register(router)`. |
| [`RouteDef`](../../../../../src/py/vertebra/framework/_router.py) / [`ParamSpec`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Frozen `msgspec.Struct(gc=False, array_like=True)` with `method`, `path`, `handler`, and compiled `segments: tuple[str \| ParamSpec, ...]`. Stable FNV-1a 64-bit route IDs come from `handler_id(method, path)`; `extract_path_params(template, path)` coerces typed path parameters (`int`, `float`, `bool`, `uuid`, `path`, `str`). |
| [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Frozen `msgspec.Struct(gc=False, array_like=True)` storing per-handler index metadata (`handler_id`, `method`, `path`, `handler_name`) in `App.handler_index`. |
| [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) | `vertebra._vertebra` | Lazy PyO3 proxy (`frozen, freelist = 128`) over `Arc<BatchSnapshot>` + index (`method`, `path`, `raw_path`, `query`, `raw_query`, `body`, zero-copy `body_view`, `OnceLock`-cached `headers`, `handler_id`, `flags`, `request_id`, `asgi_scope()`, `rsgi_protocol()`). |
| [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) | `vertebra._vertebra` | PyO3 HTTP response (`status`, `headers: Vec<(Vec<u8>, Vec<u8>)>`, `body: Bytes`, `request_id`, `Response.from_buffer`). Pure-Python fallback `Headers`/`Request`/`Response` structs live in [`_types.py`](../../../../../src/py/vertebra/framework/_types.py). |

## 2. Inertia.js v3 Protocol (`_inertia.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`InertiaMiddleware`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Negotiates full-page HTML shell rendering (with optional `SsrClient`) vs `X-Inertia: true` JSON visits, `409 Conflict` (`X-Inertia-Location`) version mismatches, partial prop filtering (`X-Inertia-Partial-Data` / `Except`), and provides the `should_redirect_303()` helper. |
| [`InertiaConfig`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Frozen `msgspec.Struct(gc=False)` configuring `enabled`, `root_template`, `version`, `ssr_enabled`, `ssr_mode` (`"native"`/`"sidecar"`), `ssr_url`, `ssr_timeout`, `ssr_pool_size`, `ssr_entry`, `sidecar_port`, `shared_data`, and `manifest_path`. |
| [`InertiaResponse`](../../../../../src/py/vertebra/framework/_inertia.py) / [`inertia`](../../../../../src/py/vertebra/framework/_inertia.py) | `vertebra.framework._inertia` | Frozen `msgspec.Struct(gc=False)` returned by handlers (`component`, `props`, `encrypt_history`, `clear_history`); `build_page_object` emits the Inertia v3 page dict including `deferredProps`, `mergeProps`, `prependProps`, `deepMergeProps`, `scrollProps`, `onceProps`, and `resetProps`. |

## 3. Serialization, Module Loading & Subdomains (`utils/`, `framework/*`)

- **[`JSONSerializer`](../../../../../src/py/vertebra/utils/serializers/_json.py)** (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`, `get_default_serializer`): Centralized `msgspec.json` (`Encoder`/`Decoder`) serializer with stdlib `json` fallback.
- **[`module_loader`](../../../../../src/py/vertebra/utils/module_loader.py) & [`OptionalDependency`](../../../../../src/py/vertebra/_typing.py)**: `import_app`, `import_optional`, `import_string`, `module_to_os_path`, and `ensure_<pkg>()` guards raising `MissingDependencyError` (`uv add <pkg>` hint), paired with boolean `OptionalDependency` flags.
- **Framework Subdomains & Live State**: `vertebra.framework` eagerly exports 16 capability packages documented in [Framework Subdomains](subdomains.md) and [Live State Synchronization](live-state.md).
