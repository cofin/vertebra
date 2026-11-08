---
type: Concept
title: "Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django"
description: "First-class framework plugins, matchit 0.9 RouteCompiler, Rust middleware offload specs, FastPathProvider dispatch, and ecosystem bridges across Litestar, Starlette, FastAPI, Flask, and Django."
status: stable
tags:
  - domains
  - extensions
  - litestar
  - starlette
  - fastapi
  - flask
  - django
  - fastpath
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:codex
    at: "2026-11-08T16:07:37Z"
    commit: 99317bed609e62a73d8b0036d444c1268ac2eb1d
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-extensions-litestar-init-py
    resource: src/py/vertebra/extensions/litestar/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/__init__.py)"
  - id: src-extensions-litestar-config-py
    resource: src/py/vertebra/extensions/litestar/config.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/config.py)"
  - id: src-extensions-litestar-plugin-py
    resource: src/py/vertebra/extensions/litestar/plugin.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)"
  - id: src-extensions-litestar-router-py
    resource: src/py/vertebra/extensions/litestar/router.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/router.py)"
  - id: src-extensions-litestar-offload-py
    resource: src/py/vertebra/extensions/litestar/offload.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/offload.py)"
  - id: src-extensions-litestar-fastpath-py
    resource: src/py/vertebra/extensions/litestar/fastpath.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py)"
  - id: src-extensions-litestar-channels-py
    resource: src/py/vertebra/extensions/litestar/channels.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/channels.py)"
  - id: src-extensions-litestar-store-py
    resource: src/py/vertebra/extensions/litestar/store.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/store.py)"
  - id: src-extensions-litestar-openapi-py
    resource: src/py/vertebra/extensions/litestar/openapi.py
    title: "Python implementation module (src/py/vertebra/extensions/litestar/openapi.py)"
  - id: src-extensions-starlette-init-py
    resource: src/py/vertebra/extensions/starlette/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/starlette/__init__.py)"
  - id: src-extensions-starlette-mcp-py
    resource: src/py/vertebra/extensions/starlette/mcp.py
    title: "Python implementation module (src/py/vertebra/extensions/starlette/mcp.py)"
  - id: src-extensions-fastapi-init-py
    resource: src/py/vertebra/extensions/fastapi/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/fastapi/__init__.py)"
  - id: src-extensions-flask-init-py
    resource: src/py/vertebra/extensions/flask/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/flask/__init__.py)"
  - id: src-extensions-flask-mcp-py
    resource: src/py/vertebra/extensions/flask/mcp.py
    title: "Python implementation module (src/py/vertebra/extensions/flask/mcp.py)"
  - id: src-extensions-django-init-py
    resource: src/py/vertebra/extensions/django/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/django/__init__.py)"
  - id: src-extensions-django-mcp-py
    resource: src/py/vertebra/extensions/django/mcp.py
    title: "Python implementation module (src/py/vertebra/extensions/django/mcp.py)"
  - id: src-agent-mcp-routes-py
    resource: src/py/vertebra/agent/mcp/routes.py
    title: "Shared ASGI and WSGI MCP route apps (src/py/vertebra/agent/mcp/routes.py)"
  - id: src-agent-mcp-service-py
    resource: src/py/vertebra/agent/mcp/service.py
    title: "Shared MCP JSON-RPC protocol core (src/py/vertebra/agent/mcp/service.py)"
  - id: src-agent-mcp-webmcp-py
    resource: src/py/vertebra/agent/mcp/webmcp.py
    title: "Browser WebMCP manifest builder (src/py/vertebra/agent/mcp/webmcp.py)"
  - id: src-server-channels-py
    resource: src/py/vertebra/server/_channels.py
    title: "Engine-neutral channel fan-out and capability bridges"
  - id: src-server-routing-py
    resource: src/py/vertebra/server/_routing.py
    title: "Python implementation module (src/py/vertebra/server/_routing.py)"
  - id: src-server-fastpath-core-py
    resource: src/py/vertebra/server/_fastpath_core.py
    title: "Shared FastPathProvider registry and DirectPlan compilation primitives"
  - id: src-server-fastpath-py
    resource: src/py/vertebra/server/_fastpath.py
    title: "Python implementation module (src/py/vertebra/server/_fastpath.py)"
---

# Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django

<!-- truth: start -->
- Host-framework extensions (`vertebra.extensions.{litestar,starlette,fastapi,flask,django}`) expose `VertebraPlugin` and `VertebraConfig` with lazy optional imports so uninstalled frameworks incur zero import cost.
- `RouteCompiler`, `OffloadPlan`, `FastPath`, and `FastPathProvider` (`_fastpath_core.py`) compile framework routes into `matchit 0.9` tables, Rust CORS/compression/static offloads, and `DirectPlan` C-API dispatch plans.
- Starlette, FastAPI, Flask, and Django MCP integrations delegate to shared `MCPCore`, `AsgiMcpApp`, `WsgiMcpApp`, `WebMCPConfig`, and `build_webmcp_manifest` primitives in `vertebra.agent.mcp`.
- `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter` are lazy public Litestar exports backed by explicitly configured `AsyncObjectStore`; persistent TTL envelopes, confined namespaces, and virtual filesystem GET/HEAD contracts are tested against native Memory/Local storage [^src-extensions-litestar-store-py].
<!-- truth: end -->

Vertebra provides symmetric, zero-coupling extensions for **Litestar** (`vertebra.extensions.litestar`), **Starlette** (`vertebra.extensions.starlette`), **FastAPI** (`vertebra.extensions.fastapi`), **Flask** (`vertebra.extensions.flask`), and **Django** (`vertebra.extensions.django`). Each extension shares the same canonical domain vocabulary (`RouteCompiler`, `RouterTable`, `ExtractedRoute` / `RouteEntry`, `CompiledRoute`, `FastPath`, `OffloadPlan`, `offload_middlewares`), routes path converter normalization through [`vertebra.server._routing`](../../../../../src/py/vertebra/server/_routing.py)[^src-server-routing-py] without importing `vertebra.framework.*`, registers its framework detector and builder in [`vertebra.server._fastpath_core`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py] (`FastPathProvider`, `register_fastpath_provider`, `resolve_fastpath_provider`, `DirectPlan`, `QueryParamSpec`), and uses [`vertebra.server._fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] (`wrap_asgi_fastpath`, `resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, `attach_framework_fastpath`) for zero-closure C-API `send_raw_response` dispatch and automatic `App.mount("/prefix", sub_app)` fastpath stacking across all 6 frameworks.

## 1. Litestar Integration (`vertebra.extensions.litestar`)


`ObjectStoreLitestarStore(store, namespace=None)` accepts an existing `AsyncObjectStore`, a `StorageConfig`, or a nonempty URI; missing configuration raises `ImproperConfigurationError`. Both adapters share `_resolve_object_store`; existing MemoryStore defaults remain independent. Store values always contain `b"VTBSTORE\x01"` plus centralized MessagePack `(expires_at_ms, payload)`. This envelope keeps TTLs persistent when Local storage strips custom metadata; expiring writes also carry `vertebra-expires-at-ms` attributes. Corrupt magic/version or typed payload decoding fails explicitly. Reads delete expired values, renewal affects only live entries already carrying expiry, and `expires_in` returns nonnegative whole seconds for live expiring values and `None` for missing, expired, or immortal entries.

`with_namespace` creates hierarchical children beneath `vertebra-store/`; each logical key is a `~`-prefixed UTF-8 hex segment. Paginated `delete_all` removes descendants while preserving sibling prefixes and unrelated objects. Native listed paths/cursors escape `~` to `%7E`; the adapter unquotes them before deletion and continuation reuse. Empty bytes remain present values. Both classes appear in module/package `__all__`, TYPE_CHECKING imports, and `_EXPORT_MAP` [^src-extensions-litestar-store-py].

`ObjectStoreFileSystemAdapter(store, base_path="")` exposes read-only virtual-root paths beneath a validated relative base prefix. Mount it with `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)`. `info` resolves metadata or populated directory prefixes and rejects parent/backslash traversal; `open(..., mode="rb")` returns the existing native async reader with async reads/context management and synchronous nonblocking seek/tell. Static GET/HEAD tests verify exact bytes, Content-Length, missing paths, and confinement. Range delivery uses a separate Litestar `Stream` handler around `StreamingStorageResponse.from_async_store` and `aiter_chunks`, copying status/headers and explicitly converting chunks to bytes at this response boundary. Static filesystem integration alone does not implement HTTP Range parsing.


- **[`VertebraPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] & [`VertebraConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)[^src-extensions-litestar-config-py]**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, registers `app_config.state["vertebra_plugin"] = self`, clears `app_config.compression_config = None` strictly when running under an active Vertebra Rust ingress (`VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1`) and `offload_middlewares` confirms a supported Rust compression codec (`zstd`, `gzip`, `brotli`), and discovers ecosystem plugins (`LitestarMCP` / `MCPConfig` -> `mcp_endpoint`, `LitestarA2A` / `A2AConfig` -> `a2a_endpoint`, Vite, OpenAPI/Scalar, SQLSpec, Advanced Alchemy, and SAQ/worker lanes) directly into `app_config.state["vertebra_ecosystem_bridges"]`. During `on_cli_init(cli)`, replaces Litestar's `run` command on `LitestarGroup` (`_build_litestar_run_command`) so `litestar run` launches the native Vertebra supervisor with full support for standard `litestar run` flags and Vertebra runtime flags (`--workers`, `--topology`, `--threads`, `--drain-timeout`, `--jit` / `--no-jit`, `--no-banner`).
- **[`VertebraPlugin.ensure_finalized`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Idempotent post-`Litestar.__init__` hook invoked by `enter_litestar_server_lifespan`, `AsgiRunner`, `FastPath.dispatch`, and `vertebra-http`'s `extract_offload_json_from_app` to swap in `ChannelsBackend` (`swap_channels_backend`) and `MemoryStore` (`swap_response_cache_store`) when `VertebraConfig.swap_memory_channels` / `swap_memory_response_cache` are enabled (both default `False`), compile the finalized Litestar route table via `RouteCompiler`, extract `OffloadPlan` into `VERTEBRA_OFFLOAD_CONFIG`, compile `FastPath`, and bind `app.state`.
- **[`RouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py)[^src-extensions-litestar-router-py] / [`RouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py)**: Introspects Litestar routes into [`ExtractedRoute`](../../../../../src/py/vertebra/extensions/litestar/router.py) items (`RouteCompiler.compile`, `RouteCompiler.extract_entries`, `extract_routes`, `extract_route_entries`) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`normalize_path_for_matchit`, `convert_path_params`).
- **[`offload_middlewares`](../../../../../src/py/vertebra/extensions/litestar/offload.py)[^src-extensions-litestar-offload-py] / [`OffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py)**: Extracts Litestar `CORSConfig`, `CompressionConfig`, static mounts (reading `_static_files_config` without triggering Litestar deprecation warnings), and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`to_server_config`, `to_server_config_json`).
- **[`FastPath`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py] & [`CompiledRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, kw_only=True)` route table compiled by `compile_fastpath_routes` using shared [`DirectPlan`](../../../../../src/py/vertebra/server/_fastpath_core.py) and [`QueryParamSpec`](../../../../../src/py/vertebra/server/_fastpath_core.py) primitives. `_compile_direct_signature_plan` unwraps Litestar 2.x/3.0 `FromPath[T]`, `FromQuery[T]`, and `JSONBody[T]` (`Annotated` markers) into zero-`Request` plans while falling back to `KwargsModel` for constrained `Parameter(gt=...)` / `FromHeader` / `FromCookie` markers. `FastPath.dispatch` and `FastPath.wrap_asgi` read bodies zero-copy via `AsgiScope.body_bytes()`, emit unary responses in 1 C-API call via `send.send_raw_response`, close unawaited `ASGIFileResponse.file_info` coroutines when intercepting `http.response.pathsend`, and route DI/DTO/`File`/`ServerSentEvent` handlers through [`dispatch_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py).
- **[`ChannelsBackend`](../../../../../src/py/vertebra/extensions/litestar/channels.py)[^src-extensions-litestar-channels-py]** & **[`MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)[^src-extensions-litestar-store-py]**: Engine-neutral [`vertebra.server._channels.ChannelBus`](../../../../../src/py/vertebra/server/_channels.py)[^src-server-channels-py] and monotonic-clock TTL `ResponseCacheStore` implementations with zero dependency on `vertebra.framework.*`. `ChannelsBackend` subclasses `ChannelBus`, re-exported from `vertebra.framework.channels`, with local fan-out, bounded byte history, explicit `NotificationSource` and `AsyncChangeSource` pumps, and `ChangeFeed` spill hydration. A shared notification source/feed delivers local publications once; absent feeds retain local fan-out. A change source sharing the bus feed already delivers on its event topic, so the explicit pump skips that duplicate. A different channel override adds one delivery on the override topic while preserving independent feed-topic observation; an equal override delivers once on the original topic. Sources without a shared non-null feed retain pump delivery. Litestar streams use protected queue registration hooks and shield queue cleanup during cancellation. Engine transport factories and `vertebra.db.channels` are removed. `MemoryStore` prefixes keys with `key_prefix` (default `vertebra`), deletes with `SCAN` + `UNLINK`, sends `PX` TTLs, and returns `None` from `expires_in` for keys without expiry.

## 2. Starlette, FastAPI, Flask & Django Extensions

Starlette, FastAPI, Flask, and Django MCP adapters delegate protocol dispatch to [`MCPCore`](../../../../../src/py/vertebra/agent/mcp/service.py)[^src-agent-mcp-service-py], [`AsgiMcpApp`](../../../../../src/py/vertebra/agent/mcp/routes.py)[^src-agent-mcp-routes-py], [`WsgiMcpApp`](../../../../../src/py/vertebra/agent/mcp/routes.py), [`WebMCPConfig`](../../../../../src/py/vertebra/agent/mcp/webmcp.py)[^src-agent-mcp-webmcp-py], and [`build_webmcp_manifest`](../../../../../src/py/vertebra/agent/mcp/webmcp.py).

| Extension Package | Plugin & Config | Route Compiler & FastPath | Ecosystem & Data Bridges |
|---|---|---|---|
| [`vertebra.extensions.starlette`](../../../../../src/py/vertebra/extensions/starlette/__init__.py)[^src-extensions-starlette-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (`Mount`/`Route`/`WebSocketRoute` -> `matchit 0.9`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, inline `sync_to_thread=False` default, `msgspec.Struct`, `pydantic.BaseModel`, `@dataclass`, `@attrs.define`), `offload_middlewares` (`GZipMiddleware`, `CORSMiddleware`, `StaticFiles`) | `SyncObjectStore` / `AsyncObjectStore`, `CacheStore`, `ChannelsBackend`, `SHMQueueBackend` / `SHMExecutionBackend`, `JWTAuthBackend`, `RateLimitGuard`, `OpenAPIConfig` (`mount_docs_routes`), [`MCPRegistry`](../../../../../src/py/vertebra/extensions/starlette/mcp.py)[^src-extensions-starlette-mcp-py] (`mount_mcp_routes` over `AsgiMcpApp` and `MCPCore`, `@mcp_tool`, `@mcp_resource`, `@mcp_prompt`, `WebMCPConfig`, `build_webmcp_manifest`), `ViteExtension` |
| [`vertebra.extensions.fastapi`](../../../../../src/py/vertebra/extensions/fastapi/__init__.py)[^src-extensions-fastapi-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (`APIRoute`), `CompiledDependantPlan` (`compile_dependant_plan` topological DAG with `needs_request` tracking and inline `sync_to_thread=False`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, `MsgspecJSONResponse`, lazy `starlette.requests.Request` skipped when `needs_request=False`, `decode_pydantic_json` + `__pydantic_serializer__.to_json` / `decode_msgspec` C-API bypass) | `SyncObjectStore` / `AsyncObjectStore`, `CacheStore`, `ChannelsBackend`, `SHMQueueBackend` / `SHMExecutionBackend`, `JWTAuthBackend`, `RateLimitGuard`, `OpenAPIConfig` (`mount_docs_routes`), `MCPRegistry` (`mount_mcp_routes` over `AsgiMcpApp` and `MCPCore`, `WebMCPConfig`, `build_webmcp_manifest`), `ViteExtension` |
| [`vertebra.extensions.flask`](../../../../../src/py/vertebra/extensions/flask/__init__.py)[^src-extensions-flask-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (Werkzeug `app.url_map.iter_rules()` -> `matchit 0.9`), `FastPath` (supports both sync `def` and `async def` views via `step_coro_sync`, `dispatch_async`, `wrap_asgi`, `wrap_wsgi`, and `_WsgiStartResponse.send_raw_response` over `WsgiInput.decode_msgspec` / `decode_pydantic_json`), `offload_middlewares` (`Flask-Compress`, `Flask-CORS`, `static_folder`) | `CacheBackend` / `FlaskCachingBackend`, `SyncObjectStore` / `AsyncObjectStore`, `MailBackend` (`MailConfig`, `MailMessage`), `ChannelsBackend`, `QueueBackend`, `JWTAuthGuard`, `RateLimitGuard`, `ViteAssetBridge` (`ViteConfig`, `attach_vite`), `OpenAPIConfig` (`attach_openapi_docs`), [`MCPConfig` / `MCPRegistry`](../../../../../src/py/vertebra/extensions/flask/mcp.py)[^src-extensions-flask-mcp-py] (`attach_mcp_routes` over `WsgiMcpApp` and `MCPCore`, `WebMCPConfig`, `build_webmcp_manifest`) |
| [`vertebra.extensions.django`](../../../../../src/py/vertebra/extensions/django/__init__.py)[^src-extensions-django-init-py] | `VertebraPlugin`, `VertebraConfig`, `VertebraAppConfig` | `RouteCompiler` (`URLPattern` / `URLResolver`), `FastPath` (`@vertebra_view`, pre-allocated stateless `MsgspecView` instances, sync `def` and `async def` views across `dispatch`, `dispatch_async`, `dispatch_wsgi` with `send_raw_response`, and `wrap_asgi`), `offload_middlewares` (`GZipMiddleware`, `CorsMiddleware`, `WhiteNoiseMiddleware`, `SecurityMiddleware`), `ORMBridge` (`orm_to_struct_vectorcall`) | Native `vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,mssql,duckdb,bigquery,spanner}` (`DatabaseWrapper` sharing pools and `LeaseBroker` across frameworks via `DatabaseRegistry.default()`, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer`, `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, PgBouncer `schema_editor()` session switch), `ArrowQuerySet` (`to_arrow`, `to_schema`, `from_arrow`), `CacheBackend`, `DjangoStorage` (`SyncObjectStore` / `AsyncObjectStore`), `EmailBackend`, `ChannelLayer`, `QueuesBackend`, `SecurityBridge` (`JWTAuthGuard`, `RateLimitGuard`), `TemplateBridge`, `ViteBridge` (`{% load vertebra_vite %}`), `OpenAPIConfig`, [`MCPConfig` / `MCPRegistry`](../../../../../src/py/vertebra/extensions/django/mcp.py)[^src-extensions-django-mcp-py] (`attach_mcp_routes`, `dispatch_mcp_request` over `WsgiMcpApp` and `MCPCore`, `WebMCPConfig`, `build_webmcp_manifest`) |

## 3. First-Party Litestar Ecosystem Subpackages (`extensions/litestar/*`)

All `litestar-*` ecosystem integrations are nested strictly under [`vertebra.extensions.litestar`](../../../../../src/py/vertebra/extensions/litestar/__init__.py)[^src-extensions-litestar-init-py]:

| Subpackage | Canonical Helpers & Adapters | Upstream Integration |
|---|---|---|
| `queues` | `SHMQueueBackend`, `SHMExecutionBackend`, `create_shm_queue_config()`, `create_sqlspec_queue_config(config)` | Wraps `litestar_queues` (`QueuePlugin`, `QueueConfig`) with Vertebra SHM ring buffers or SQLSpec database queue backends. |
| `security` | `JWTAuthenticator`, `JWTAuthenticationMechanism`, `IngressIdentityMechanism`, `RateLimitGuard`, `create_jwt_authentication_mechanism()`, `create_ingress_identity_mechanism()`, `exempt_vertebra_routes()`, `configure_vertebra_exclusions()` | Wraps `litestar_security` (`SecurityPlugin`, `SecurityConfig`) with zero-copy bearer/ingress identity slots, JWT authentication, and Rust rate-limit guards. |
| `autowire` | `AutowireBridgeIntegration`, `create_autowire_plugin()`, `ensure_autowire_before_route_compiler()` | Wraps `litestar_autowire` with domain package scanning and route-compiler ordering. |
| `email` | `SESEmailBackend`, `SESEmailConfig` | Bridges `litestar_email` to Vertebra transactional SES email delivery. |
| `mcp` | `create_mcp_plugin()`, `create_shm_mcp_task_config()`, `JSONRPCFrameAccelerator`, `ObjectStoreSkillSource`, `WellKnownAgentCardOffload` | Bridges `litestar_mcp` (`LitestarMCP`, `LitestarA2A`) with Rust JSON-RPC frame acceleration, SHM task backends, object-store skills, and cached `.well-known` discovery. |
| `vite` | `create_vite_plugin()`, `get_vertebra_client_path()`, `get_vertebra_client_vite_aliases()` | Bridges `litestar_vite` (`RuntimeConfig(executor="bun")`) with `vertebra-bundler` (Rolldown/Oxc) asset compilation, bundled `@vertebra/client` alias injection, and Inertia SSR. |
| [`openapi`](../../../../../src/py/vertebra/extensions/litestar/openapi.py)[^src-extensions-litestar-openapi-py] | `OpenAPIConfig`, `ScalarDocsConfig`, `SwaggerDocsConfig`, `RedocDocsConfig`, `ElementsDocsConfig`, `RapiDocDocsConfig`, `AsyncAPIDocsConfig`, `ScalarSpecSource`, `build_docs_route_handlers()` | Configurable multi-spec API documentation portal (Scalar default, Swagger UI, Redoc, Elements, RapiDoc, and AsyncAPI 3.0). |
| `htmx` | `create_htmx_plugin()` | Configures `litestar_htmx` (`HTMXPlugin`, `HTMXConfig`) for server-rendered HTML applications. |
| `saq` | `create_saq_plugin()` | Configures `litestar_saq` (`SAQPlugin`, `SAQConfig`) background job scheduling. |
| `asyncapi` | `create_asyncapi_plugin()` | Configures `litestar_asyncapi` (`AsyncAPIPlugin`, `AsyncAPIConfig`) event documentation. |

[^src-extensions-litestar-init-py]: `src/py/vertebra/extensions/litestar/__init__.py` — Python implementation module (src/py/vertebra/extensions/litestar/__init__.py).
[^src-extensions-litestar-config-py]: `src/py/vertebra/extensions/litestar/config.py` — Python implementation module (src/py/vertebra/extensions/litestar/config.py).
[^src-extensions-litestar-plugin-py]: `src/py/vertebra/extensions/litestar/plugin.py` — Python implementation module (src/py/vertebra/extensions/litestar/plugin.py).
[^src-extensions-litestar-router-py]: `src/py/vertebra/extensions/litestar/router.py` — Python implementation module (src/py/vertebra/extensions/litestar/router.py).
[^src-extensions-litestar-offload-py]: `src/py/vertebra/extensions/litestar/offload.py` — Python implementation module (src/py/vertebra/extensions/litestar/offload.py).
[^src-extensions-litestar-fastpath-py]: `src/py/vertebra/extensions/litestar/fastpath.py` — Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py).
[^src-extensions-litestar-channels-py]: `src/py/vertebra/extensions/litestar/channels.py` — Python implementation module (src/py/vertebra/extensions/litestar/channels.py).
[^src-extensions-litestar-store-py]: `src/py/vertebra/extensions/litestar/store.py` — Python implementation module (src/py/vertebra/extensions/litestar/store.py).
[^src-extensions-litestar-openapi-py]: `src/py/vertebra/extensions/litestar/openapi.py` — Python implementation module (src/py/vertebra/extensions/litestar/openapi.py).
[^src-extensions-starlette-init-py]: `src/py/vertebra/extensions/starlette/__init__.py` — Python implementation module (src/py/vertebra/extensions/starlette/__init__.py).
[^src-extensions-starlette-mcp-py]: `src/py/vertebra/extensions/starlette/mcp.py` — Python implementation module (src/py/vertebra/extensions/starlette/mcp.py).
[^src-extensions-fastapi-init-py]: `src/py/vertebra/extensions/fastapi/__init__.py` — Python implementation module (src/py/vertebra/extensions/fastapi/__init__.py).
[^src-extensions-flask-init-py]: `src/py/vertebra/extensions/flask/__init__.py` — Python implementation module (src/py/vertebra/extensions/flask/__init__.py).
[^src-extensions-flask-mcp-py]: `src/py/vertebra/extensions/flask/mcp.py` — Python implementation module (src/py/vertebra/extensions/flask/mcp.py).
[^src-extensions-django-init-py]: `src/py/vertebra/extensions/django/__init__.py` — Python implementation module (src/py/vertebra/extensions/django/__init__.py).
[^src-extensions-django-mcp-py]: `src/py/vertebra/extensions/django/mcp.py` — Python implementation module (src/py/vertebra/extensions/django/mcp.py).
[^src-agent-mcp-routes-py]: `src/py/vertebra/agent/mcp/routes.py` — Shared ASGI and WSGI MCP route apps (src/py/vertebra/agent/mcp/routes.py).
[^src-agent-mcp-service-py]: `src/py/vertebra/agent/mcp/service.py` — Shared MCP JSON-RPC protocol core (src/py/vertebra/agent/mcp/service.py).
[^src-agent-mcp-webmcp-py]: `src/py/vertebra/agent/mcp/webmcp.py` — Browser WebMCP manifest builder (src/py/vertebra/agent/mcp/webmcp.py).
[^src-server-channels-py]: `src/py/vertebra/server/_channels.py` — Engine-neutral channel fan-out, bounded replay, and capability stream bridges.
[^src-server-routing-py]: `src/py/vertebra/server/_routing.py` — Python implementation module (src/py/vertebra/server/_routing.py).
[^src-server-fastpath-core-py]: `src/py/vertebra/server/_fastpath_core.py` — Shared FastPathProvider registry and DirectPlan compilation primitives.
[^src-server-fastpath-py]: `src/py/vertebra/server/_fastpath.py` — Python implementation module (src/py/vertebra/server/_fastpath.py).
