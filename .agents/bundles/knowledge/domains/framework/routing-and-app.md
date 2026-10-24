---
type: Guide
title: "Python Framework, Routing & Serialization Domain"
description: "Canonical domain types for Vertebra's Python HTTP application container, router, Inertia v3 protocol, PEP 810 lazy exports, and JSON serialization."
status: stable
tags: [domains, framework, routing, serialization, python]
sources:
  - resource: src/py/vertebra/__init__.py
  - resource: src/py/vertebra/framework/__init__.py
  - resource: src/py/vertebra/framework/_app.py
  - resource: src/py/vertebra/framework/_router.py
  - resource: src/py/vertebra/framework/inertia/__init__.py
  - resource: src/py/vertebra/utils/serializers/_json.py
  - resource: src/py/vertebra/utils/serializers/_msgpack.py
  - resource: src/py/vertebra/utils/module_loader.py
  - resource: src/py/vertebra/typing.py
  - resource: src/rs/vertebra-py/src/request.rs
  - resource: src/rs/vertebra-py/src/response.rs
---

# Python Framework, Routing & Serialization Domain

[`vertebra`](../../../../../src/py/vertebra/__init__.py), [`vertebra.framework`](../../../../../src/py/vertebra/framework/__init__.py), and [`vertebra.utils`](../../../../../src/py/vertebra/utils/__init__.py) define the Python-facing application container, declarative router, PEP 810 `__lazy_modules__` exports, Inertia.js v3 protocol middleware, and JSON/MessagePack serialization primitives.

## 1. Application Container & Routing (`_app.py`, `_router.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`App`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Top-level HTTP container (`@app.get`, `@app.post`, `@app.put`, `@app.delete`, `@app.patch`, each accepting `sync_to_thread: bool = False`), `register(Router \| handler)`, `add_route(RouteDef)`, `mount(prefix, sub_app, protocol="auto")`, `on_startup`/`on_shutdown` hooks (`startup()`, `shutdown()`), and `handler_index: dict[int, HandlerInfo]`. |
| [`Router`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Prefix route group (`__slots__ = ("prefix", "routes")`) that collects functions decorated with module-level `get`/`post`/`put`/`delete`/`patch` (`sync_to_thread: bool = False`) via `register(handler)` and attaches to `App.register(router)`. |
| [`RouteDef`](../../../../../src/py/vertebra/framework/_router.py) / [`ParamSpec`](../../../../../src/py/vertebra/framework/_router.py) | `vertebra.framework._router` | Frozen `msgspec.Struct(gc=False)` (`cache_hash=True` on `ParamSpec`) with `method`, `path`, `handler`, `sync_to_thread: bool = False`, and compiled `segments: tuple[str \| ParamSpec, ...]`. Stable FNV-1a 64-bit route IDs come from `handler_id(method, path)`; `extract_path_params(template, path)` coerces typed path parameters (`int`, `float`, `bool`, `uuid`, `path`, `str`). |
| [`HandlerInfo`](../../../../../src/py/vertebra/framework/_app.py) | `vertebra.framework._app` | Frozen `msgspec.Struct(gc=False, cache_hash=True)` storing per-handler index metadata (`handler_id`, `method`, `path`, `handler_name`, `sync_to_thread`) in `App.handler_index`. |
| [`Request`](../../../../../src/rs/vertebra-py/src/request.rs) | `vertebra._vertebra` | Lazy PyO3 proxy (`frozen, immutable_type`, conditional `freelist = 128`) over `Arc<BatchSnapshot>` + index implementing PEP 688 `__buffer__` (`memoryview(req)` and anchored zero-copy `body_view`), `OnceLock`-cached `headers`, `method`, `path`, `raw_path`, `query`, `raw_query: bytes`, `body`, `handler_id`, `flags`, `request_id`, `asgi_scope()`, and `rsgi_protocol()`. |
| [`Response`](../../../../../src/rs/vertebra-py/src/response.rs) | `vertebra._vertebra` | PyO3 HTTP response (`immutable_type`, conditional `freelist = 128`, `status`, `headers: Vec<(Vec<u8>, Vec<u8>)>`, `body: Bytes`, `request_id`, `Response.from_buffer`, `PyBytes::new_with` `to_bytes`). Pure-Python fallback `Headers`/`Request`/`Response` structs live in [`_router.py`](../../../../../src/py/vertebra/framework/_router.py). |

## 2. Inertia.js v3 Protocol (`inertia/__init__.py`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`InertiaMiddleware`](../../../../../src/py/vertebra/framework/inertia/__init__.py) | `vertebra.framework.inertia` | Negotiates full-page HTML shell rendering (with optional `SsrClient`) vs `X-Inertia: true` JSON visits, `409 Conflict` (`X-Inertia-Location`) version mismatches, partial prop filtering (`X-Inertia-Partial-Data` / `Except`), and provides `InertiaMiddleware.should_redirect_303()`. |
| [`InertiaConfig`](../../../../../src/py/vertebra/framework/inertia/__init__.py) | `vertebra.framework.inertia` | Frozen `msgspec.Struct(gc=False)` configuring `enabled`, `root_template`, `version`, `ssr_enabled`, `ssr_mode` (`"native"`/`"sidecar"`), `ssr_url`, `ssr_timeout`, `ssr_pool_size`, `ssr_entry`, `sidecar_port`, `shared_data`, and `manifest_path`. |
| [`InertiaResponse`](../../../../../src/py/vertebra/framework/inertia/__init__.py) / [`inertia`](../../../../../src/py/vertebra/framework/inertia/__init__.py) | `vertebra.framework.inertia` | Frozen `msgspec.Struct(gc=False)` returned by handlers (`component`, `props`, `encrypt_history`, `clear_history`); `InertiaPageBuilder.build_page_object` emits the Inertia v3 page dict including `deferredProps`, `mergeProps`, `prependProps`, `deepMergeProps`, `scrollProps`, `onceProps`, and `resetProps`. |

## 3. Serialization, Lazy Modules & Subdomains (`utils/`, `typing.py`, `framework/*`)

- **[`JSONSerializer`](../../../../../src/py/vertebra/utils/serializers/_json.py)** (`encode_json`, `encode_json_bytes`, `encode_json_into`, `decode_json`, `format_json`, `get_default_serializer`) & **[`MsgpackCodec`](../../../../../src/py/vertebra/utils/serializers/_msgpack.py)**: Centralized `msgspec.json` and `msgspec.msgpack` serializers.
- **[`module_loader`](../../../../../src/py/vertebra/utils/module_loader.py) & [`vertebra.typing`](../../../../../src/py/vertebra/typing.py)**: `import_app`, cached `import_optional` / `ensure_module` (`reset_dependency_cache`), `import_string`, and public struct/sentinel/exception exports (`BaseStruct`, `FrozenBaseStruct`, `Empty`, `EmptyType`, `UNSET`, `VertebraError`, `MissingDependencyError`, `ImproperConfigurationError`, `UnsupportedBackendError`, `OptionalDependency`).
- **PEP 810 `__lazy_modules__` & Framework Subdomains**: Both [`vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py) and [`vertebra/framework/__init__.py`](../../../../../src/py/vertebra/framework/__init__.py) declare PEP 810 `__lazy_modules__` alongside PEP 562 `__getattr__` for fast startup on Python 3.12–3.15+, exposing the 14 capability subdomains documented in [Framework Subdomains](subdomains.md) and [Live State Synchronization](live-state.md).
