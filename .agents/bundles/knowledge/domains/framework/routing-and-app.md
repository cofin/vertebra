---
type: Guide
title: "Python Framework, Routing & Serialization Domain"
description: "Canonical domain types for Vertebra's Python HTTP application container, router, Inertia v3 protocol, PEP 810 lazy exports, and JSON and MessagePack serialization."
status: stable
tags:
  - domains
  - framework
  - routing
  - serialization
  - python
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
sources:
  - id: src-init-py
    resource: src/py/vertebra/__init__.py
    title: "Python implementation module (src/py/vertebra/__init__.py)"
  - id: src-framework-init-py
    resource: src/py/vertebra/framework/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/__init__.py)"
  - id: src-framework-app-py
    resource: src/py/vertebra/framework/_app.py
    title: "Python implementation module (src/py/vertebra/framework/_app.py)"
  - id: src-framework-router-py
    resource: src/py/vertebra/framework/_router.py
    title: "Python implementation module (src/py/vertebra/framework/_router.py)"
  - id: src-framework-welcome-py
    resource: src/py/vertebra/framework/_welcome.py
    title: "Python implementation module (src/py/vertebra/framework/_welcome.py)"
  - id: src-framework-welcome-html
    resource: src/py/vertebra/framework/assets/welcome.html
    title: "Development welcome HTML5 template (src/py/vertebra/framework/assets/welcome.html)"
  - id: src-framework-welcome-css
    resource: src/py/vertebra/framework/assets/welcome.css
    title: "Development welcome stylesheet (src/py/vertebra/framework/assets/welcome.css)"
  - id: src-framework-inertia-init-py
    resource: src/py/vertebra/framework/inertia/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/inertia/__init__.py)"
  - id: src-utils-serializers-json-py
    resource: src/py/vertebra/utils/serializers/_json.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_json.py)"
  - id: src-utils-serializers-msgpack-py
    resource: src/py/vertebra/utils/serializers/_msgpack.py
    title: "Python implementation module (src/py/vertebra/utils/serializers/_msgpack.py)"
  - id: src-utils-module-loader-py
    resource: src/py/vertebra/utils/module_loader.py
    title: "Python implementation module (src/py/vertebra/utils/module_loader.py)"
  - id: src-typing-py
    resource: src/py/vertebra/typing.py
    title: "Python implementation module (src/py/vertebra/typing.py)"
  - id: src-vertebra-py-src-request-rs
    resource: src/rs/vertebra-py/src/request.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/request.rs)"
  - id: src-vertebra-py-src-response-rs
    resource: src/rs/vertebra-py/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-py/src/response.rs)"
---

# Python Framework, Routing & Serialization Domain

<!-- truth: start -->
- `App`, `Router`, `Controller`, and `Route` compose hierarchical DI, guards, and OpenAPI metadata while hashing normalized route paths with FNV-1a identically to Rust `vertebra-http/src/routing.rs`.
- `WelcomePageRenderer` loads externalized `src/py/vertebra/framework/assets/welcome.html` and `welcome.css` via `importlib.resources` and binds unclaimed `GET /` only in debug or `VERTEBRA_DEV=1` mode without mutating `app.routes`.
- `vertebra/__init__.py` and `vertebra/framework/__init__.py` enforce PEP 810 `__lazy_modules__` + PEP 562 `__getattr__` lazy imports, and all JSON and MessagePack serialization routes through `JSONSerializer` and `MsgPackSerializer` in `vertebra.utils.serializers`.
<!-- truth: end -->

[`vertebra`](../../../../../src/py/vertebra/__init__.py)[^src-init-py], [`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py)[^src-framework-init-py], and [`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py) define the Python-facing application container, declarative router, PEP 810 `__lazy_modules__` exports, Inertia.js v3 protocol middleware, and JSON/MessagePack serialization primitives.

## 1. Application Container & Routing (`_app.py`, `_router.py`, `_welcome.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`App`](../../../../../src/py/vertebra/framework/_app.py)[^src-framework-app-py] | `vertebra.framework._app` | Top-level HTTP container (`@app.get`, `@app.post`, `@app.put`, `@app.delete`, `@app.patch`, each accepting `sync_to_thread`, `dependencies`, `guards`, `tags`, `status_code`), `debug: bool = False`, `ensure_dev_welcome_route()`, `enable_docs()`, `register(Router \| Controller \| ProcedureRouter \| Procedure \| Route \| handler)`, `add_route(Route)`, `call_procedure(name, payload)`, `mount(prefix, sub_app, protocol="auto")`, `on_startup`/`on_shutdown` hooks (`startup()`, `shutdown()`), and `handler_index: dict[int, HandlerInfo]`. |
| [`WelcomePageRenderer`](../../../../../src/py/vertebra/framework/_welcome.py)[^src-framework-welcome-py] / [`WelcomeSnapshot`](../../../../../src/py/vertebra/framework/_welcome.py) | `vertebra.framework._welcome` | Compiles externalized [`welcome.html`](../../../../../src/py/vertebra/framework/assets/welcome.html)[^src-framework-welcome-html] and [`welcome.css`](../../../../../src/py/vertebra/framework/assets/welcome.css)[^src-framework-welcome-css] package assets via `importlib.resources` (`_load_compiled_welcome_template`) to render the offline-ready HTML5 development welcome page (`collect_snapshot(app)`, `render_html(app)`, `make_handler(app)`) automatically bound to unclaimed `GET /` when `App(debug=True)` or `VERTEBRA_DEV=1` is active without mutating `app.routes`. |
| [`Controller`](../../../../../src/py/vertebra/framework/_router.py)[^src-framework-router-py] | `vertebra.framework._router` | Declarative class-based route and procedure group (`path`, `dependencies`, `guards`, `tags`, `sync_to_thread`) supporting both singleton instance binding and constructor dependency injection via `get_routes()` and `get_procedures()`. |
| [`Router`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Prefix route group (`__slots__ = ("dependencies", "guards", "prefix", "procedures", "routes", "tags")`) that collects functions decorated with module-level `get`/`post`/`put`/`delete`/`patch`, `Controller` classes/instances, `Procedure` instances, and nested `Router` instances via `register(target)` and `route_handlers`. |
| [`Route`](../../../../../src/py/vertebra/framework/_router.py) / [`ParamSpec`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Frozen `msgspec.Struct(gc=False)` (`cache_hash=True` on `ParamSpec`) with `method`, `path`, `handler`, `sync_to_thread: bool = False`, `dependencies`, `guards`, `tags`, `status_code`, and compiled `segments: tuple[str \| ParamSpec, ...]`. Stable FNV-1a 64-bit route IDs come from `handler_id(method, path)`; `extract_path_params(template, path)` coerces typed path parameters (`int`, `float`, `bool`, `uuid`, `path`, `str`). |
| [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Frozen `msgspec.Struct(gc=False, cache_hash=True)` storing per-handler index metadata (`handler_id`, `method`, `path`, `handler_name`, `sync_to_thread`) in `App.handler_index`. |
| [`Request`](../../../../../src/rs/vertebra-py/src/request.rs)[^src-vertebra-py-src-request-rs] | `vertebra._vertebra` | Lazy PyO3 proxy (`frozen, immutable_type`, conditional `freelist = 128`) over `Arc<BatchSnapshot>` + index implementing PEP 688 `__buffer__` (`memoryview(req)` and anchored zero-copy `body_view`), `OnceLock`-cached `headers`, `method`, `path`, `raw_path`, `query`, `raw_query: bytes`, `body`, `handler_id`, `flags`, `request_id`, `asgi_scope()`, and `rsgi_protocol()`. |
| [`Response`](../../../../../src/rs/vertebra-py/src/response.rs)[^src-vertebra-py-src-response-rs] | `vertebra._vertebra` | PyO3 HTTP response (`immutable_type`, conditional `freelist = 128`, `status`, `headers: Vec<(Vec<u8>, Vec<u8>)>`, `body: Bytes`, `request_id`, `Response.from_buffer`, `PyBytes::new_with` `to_bytes`). Pure-Python fallback `Headers`/`Request`/`Response` structs live in [`_router.py`](../../../../../src/py/vertebra/framework/_router.py). |

## 2. Inertia.js v3 Protocol (`inertia/__init__.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`SyncInertiaMiddleware` / `AsyncInertiaMiddleware`](../../../../../src/py/vertebra/framework/inertia/__init__.py)[^src-framework-inertia-init-py] | `vertebra.framework.inertia` | Negotiates full-page HTML shell rendering (with optional `SyncSSRClient` / `AsyncSSRClient`) vs `X-Inertia: true` JSON visits, `409 Conflict` (`X-Inertia-Location`) version mismatches, partial prop filtering (`X-Inertia-Partial-Data` / `Except`), and provides `SyncInertiaMiddleware.should_redirect_303()`. |
| [`InertiaConfig`](../../../../../src/py/vertebra/framework/inertia/__init__.py) | `vertebra.framework.inertia` | Frozen `msgspec.Struct(gc=False)` configuring `enabled`, `root_template`, `version`, `ssr_enabled`, `ssr_mode` (`"native"`/`"sidecar"`), `ssr_url`, `ssr_timeout`, `ssr_pool_size`, `ssr_entry`, `sidecar_port`, `shared_data`, and `manifest_path`. |
| [`InertiaResponse`](../../../../../src/py/vertebra/framework/inertia/__init__.py) / [`inertia`](../../../../../src/py/vertebra/framework/inertia/__init__.py) | `vertebra.framework.inertia` | Frozen `msgspec.Struct(gc=False)` returned by handlers (`component`, `props`, `encrypt_history`, `clear_history`); `InertiaPageBuilder.build_page_object` emits the Inertia v3 page dict including `deferredProps`, `mergeProps`, `prependProps`, `deepMergeProps`, `scrollProps`, `onceProps`, and `resetProps`. |

## 3. Serialization, Lazy Modules & Subdomains (`utils/`, `typing.py`, `framework/*`)

- **[`JSONSerializer`](../../../../../src/py/vertebra/utils/serializers/_json.py)[^src-utils-serializers-json-py]** (`get_default_json_serializer`, `encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`) & **[`MsgPackSerializer`](../../../../../src/py/vertebra/utils/serializers/_msgpack.py)[^src-utils-serializers-msgpack-py]** (`get_default_msgpack_serializer`, `encode_msgpack_bytes`, `encode_msgpack_into`, `decode_msgpack`): Centralized `msgspec.json` and `msgspec.msgpack` serializers with reusable `bytearray` buffer encoding and typed `msgspec` struct/adapter caches.
- **[`module_loader`](../../../../../src/py/vertebra/utils/module_loader.py)[^src-utils-module-loader-py] & [`vertebra.typing`](../../../../../src/py/vertebra/typing.py)[^src-typing-py]**: `import_app`, cached `import_optional` / `ensure_module` (`reset_dependency_cache`), `import_string`, and public struct/sentinel/exception exports (`BaseStruct`, `FrozenBaseStruct`, `Empty`, `EmptyType`, `UNSET`, `VertebraError`, `MissingDependencyError`, `ImproperConfigurationError`, `UnsupportedBackendError`, `OptionalDependency`).
- **PEP 810 `__lazy_modules__` & Framework Subdomains**: Both [`vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py) and [`vertebra/framework/__init__.py`](../../../../../src/py/vertebra/framework/__init__.py) declare PEP 810 `__lazy_modules__` alongside PEP 562 `__getattr__` for fast startup on Python 3.12–3.15+, exposing the 14 capability subdomains documented in [Framework Subdomains](subdomains.md) and [Live State Synchronization](live-state.md).

[^src-init-py]: `src/py/vertebra/__init__.py` — Python implementation module (src/py/vertebra/__init__.py).
[^src-framework-init-py]: `src/py/vertebra/framework/__init__.py` — Python implementation module (src/py/vertebra/framework/__init__.py).
[^src-framework-app-py]: `src/py/vertebra/framework/_app.py` — Python implementation module (src/py/vertebra/framework/_app.py).
[^src-framework-router-py]: `src/py/vertebra/framework/_router.py` — Python implementation module (src/py/vertebra/framework/_router.py).
[^src-framework-welcome-py]: `src/py/vertebra/framework/_welcome.py` — Python implementation module (src/py/vertebra/framework/_welcome.py).
[^src-framework-welcome-html]: `src/py/vertebra/framework/assets/welcome.html` — Development welcome HTML5 template (src/py/vertebra/framework/assets/welcome.html).
[^src-framework-welcome-css]: `src/py/vertebra/framework/assets/welcome.css` — Development welcome stylesheet (src/py/vertebra/framework/assets/welcome.css).
[^src-framework-inertia-init-py]: `src/py/vertebra/framework/inertia/__init__.py` — Python implementation module (src/py/vertebra/framework/inertia/__init__.py).
[^src-utils-serializers-json-py]: `src/py/vertebra/utils/serializers/_json.py` — Python implementation module (src/py/vertebra/utils/serializers/_json.py).
[^src-utils-serializers-msgpack-py]: `src/py/vertebra/utils/serializers/_msgpack.py` — Python implementation module (src/py/vertebra/utils/serializers/_msgpack.py).
[^src-utils-module-loader-py]: `src/py/vertebra/utils/module_loader.py` — Python implementation module (src/py/vertebra/utils/module_loader.py).
[^src-typing-py]: `src/py/vertebra/typing.py` — Python implementation module (src/py/vertebra/typing.py).
[^src-vertebra-py-src-request-rs]: `src/rs/vertebra-py/src/request.rs` — Rust implementation module (src/rs/vertebra-py/src/request.rs).
[^src-vertebra-py-src-response-rs]: `src/rs/vertebra-py/src/response.rs` — Rust implementation module (src/rs/vertebra-py/src/response.rs).
