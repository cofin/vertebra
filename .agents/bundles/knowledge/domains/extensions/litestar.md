---
type: Concept
title: 'Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django'
description: First-class framework plugins, matchit 0.9 RouteCompiler, Rust middleware
  offload specs, FastPathProvider dispatch, and ecosystem bridges across Litestar,
  Starlette, FastAPI, Flask, and Django.
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
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:flow-executor
  at: '2026-11-13T20:35:00Z'
  commit: 54a5103d
- by: agent:flow-sync
  at: '2026-11-12T01:15:00Z'
  commit: c8722c76
- by: agent:codex
  at: '2026-11-08T18:27:30.456016Z'
  commit: 067fa901
sources:
- id: src-extensions-litestar-init-py
  resource: src/py/vertebra/extensions/litestar/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/__init__.py)
- id: src-extensions-litestar-config-py
  resource: src/py/vertebra/extensions/litestar/config.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/config.py)
- id: src-extensions-litestar-plugin-py
  resource: src/py/vertebra/extensions/litestar/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/plugin.py)
- id: src-extensions-litestar-router-py
  resource: src/py/vertebra/extensions/litestar/router.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/router.py)
- id: src-extensions-litestar-offload-py
  resource: src/py/vertebra/extensions/litestar/offload.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/offload.py)
- id: src-extensions-litestar-fastpath-py
  resource: src/py/vertebra/extensions/litestar/fastpath.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/fastpath.py)
- id: src-extensions-litestar-channels-py
  resource: src/py/vertebra/extensions/litestar/channels.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/channels.py)
- id: src-extensions-litestar-store-py
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/store.py)
- id: src-extensions-litestar-openapi-py
  resource: src/py/vertebra/extensions/litestar/openapi.py
  title: Python implementation module (src/py/vertebra/extensions/litestar/openapi.py)
- id: src-extensions-starlette-init-py
  resource: src/py/vertebra/extensions/starlette/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/starlette/__init__.py)
- id: src-extensions-starlette-mcp-py
  resource: src/py/vertebra/extensions/starlette/mcp.py
  title: Python implementation module (src/py/vertebra/extensions/starlette/mcp.py)
- id: src-extensions-fastapi-init-py
  resource: src/py/vertebra/extensions/fastapi/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/fastapi/__init__.py)
- id: src-extensions-flask-init-py
  resource: src/py/vertebra/extensions/flask/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/flask/__init__.py)
- id: src-extensions-flask-mcp-py
  resource: src/py/vertebra/extensions/flask/mcp.py
  title: Python implementation module (src/py/vertebra/extensions/flask/mcp.py)
- id: src-extensions-django-init-py
  resource: src/py/vertebra/extensions/django/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/django/__init__.py)
- id: src-extensions-django-mcp-py
  resource: src/py/vertebra/extensions/django/mcp.py
  title: Python implementation module (src/py/vertebra/extensions/django/mcp.py)
- id: src-agent-mcp-routes-py
  resource: src/py/vertebra/agent/mcp/routes.py
  title: Shared ASGI and WSGI MCP route apps (src/py/vertebra/agent/mcp/routes.py)
- id: src-agent-mcp-service-py
  resource: src/py/vertebra/agent/mcp/service.py
  title: Shared MCP JSON-RPC protocol core (src/py/vertebra/agent/mcp/service.py)
- id: src-agent-mcp-webmcp-py
  resource: src/py/vertebra/agent/mcp/webmcp.py
  title: Browser WebMCP manifest builder (src/py/vertebra/agent/mcp/webmcp.py)
- id: src-server-channels-py
  resource: src/py/vertebra/server/_channels.py
  title: Engine-neutral channel fan-out and capability bridges
- id: src-server-routing-py
  resource: src/py/vertebra/server/_routing.py
  title: Python implementation module (src/py/vertebra/server/_routing.py)
- id: src-server-fastpath-core-py
  resource: src/py/vertebra/server/_fastpath_core.py
  title: Shared FastPathProvider registry and DirectPlan compilation primitives
- id: src-server-fastpath-py
  resource: src/py/vertebra/server/_fastpath.py
  title: Python implementation module (src/py/vertebra/server/_fastpath.py)
- id: src-extensions-django-storage-py
  resource: src/py/vertebra/extensions/django/storage.py
  title: Deconstructible storage, storage-bound File, and migration serializers
---

# Framework Extensions: Litestar, Starlette, FastAPI, Flask, and Django

<!-- truth: start -->
- Host-framework extensions (`vertebra.extensions.{litestar,starlette,fastapi,flask,django}`) expose `VertebraPlugin` and `VertebraConfig` with lazy optional imports so uninstalled frameworks incur zero import cost.
- `RouteCompiler`, `OffloadPlan`, `FastPath`, and `FastPathProvider` (`_fastpath_core.py`) compile framework routes into `matchit 0.9` tables, Rust CORS/compression/static offloads, and `DirectPlan` C-API dispatch plans.
- Starlette, FastAPI, Flask, and Django MCP integrations delegate to shared `MCPCore`, `AsgiMcpApp`, `WsgiMcpApp`, `SkillCatalog`, `MCPCore.validate()`, and `build_webmcp_manifest` primitives; Litestar delegates `/mcp` to `litestar-mcp>=0.20.0` (`create_mcp_plugin`) with well-known skill list merging.
- `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter` are lazy public Litestar exports backed by explicitly configured `AsyncObjectStore`; persistent TTL envelopes, confined namespaces, and virtual filesystem GET/HEAD contracts are tested against native Memory/Local storage [^src-extensions-litestar-store-py].
<!-- truth: end -->

Vertebra provides symmetric, zero-coupling extensions for **Litestar** (`vertebra.extensions.litestar`), **Starlette** (`vertebra.extensions.starlette`), **FastAPI** (`vertebra.extensions.fastapi`), **Flask** (`vertebra.extensions.flask`), and **Django** (`vertebra.extensions.django`). Each extension shares the same canonical domain vocabulary (`RouteCompiler`, `RouterTable`, `ExtractedRoute` / `RouteEntry`, `CompiledRoute`, `FastPath`, `OffloadPlan`, `offload_middlewares`), routes path converter normalization through [`vertebra.server._routing`](../../../../../src/py/vertebra/server/_routing.py)[^src-server-routing-py] without importing `vertebra.framework.*`, is detected and built by a lazy built-in `FastPathProvider` (`detect(app)`, `build(app, *, mount_prefix="")` returning `FastPath.from_app(...)`) in [`vertebra.server._fastpath_core`](../../../../../src/py/vertebra/server/_fastpath_core.py)[^src-server-fastpath-core-py] (`register_fastpath_provider`, `resolve_fastpath_provider`, `DirectPlan`, `QueryParamSpec`), and uses [`vertebra.server._fastpath`](../../../../../src/py/vertebra/server/_fastpath.py)[^src-server-fastpath-py] (`wrap_asgi_fastpath`, `resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, `attach_framework_fastpath`) for zero-closure C-API `send_raw_response` dispatch and automatic `App.mount("/prefix", sub_app)` fastpath stacking across all 6 frameworks.

## 1. Litestar Integration (`vertebra.extensions.litestar`)


`ObjectStoreLitestarStore(store, namespace=None)` accepts an existing `AsyncObjectStore`, a `StorageConfig`, or a nonempty URI; missing configuration raises `ImproperConfigurationError`. Both adapters share `_resolve_object_store`; existing MemoryStore defaults are independent. Store values always contain `b"VTBSTORE\x01"` plus centralized MessagePack `(expires_at_ms, payload)`. This envelope keeps TTLs persistent when Local storage strips custom metadata; expiring writes also carry `vertebra-expires-at-ms` attributes. Corrupt magic/version or typed payload decoding fails explicitly. Reads delete expired values, renewal affects only live entries already carrying expiry, and `expires_in` returns nonnegative whole seconds for live expiring values and `None` for missing, expired, or immortal entries.

`with_namespace` creates hierarchical children beneath `vertebra-store/`; each logical key is a `~`-prefixed UTF-8 hex segment. Paginated `delete_all` removes descendants while preserving sibling prefixes and unrelated objects. Native listed paths/cursors escape `~` to `%7E`; the adapter unquotes them before deletion and continuation reuse. Empty bytes are valid stored values. Both classes appear in module/package `__all__`, TYPE_CHECKING imports, and `_EXPORT_MAP` [^src-extensions-litestar-store-py].

`ObjectStoreFileSystemAdapter(store, base_path="")` exposes read-only virtual-root paths beneath a validated relative base prefix. Mount it with `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)`. `info` resolves metadata or populated directory prefixes and rejects parent/backslash traversal; `open(..., mode="rb")` returns the existing native async reader with async reads/context management and synchronous nonblocking seek/tell. Static GET/HEAD tests verify exact bytes, Content-Length, missing paths, and confinement. Range delivery uses a separate Litestar `Stream` handler that awaits `StreamingStorageResponse.from_async_store` before consuming `aiter_chunks`, copying status/headers and explicitly converting chunks to bytes at this response boundary. Static filesystem integration alone does not implement HTTP Range parsing. The [persistent TTL envelope decision](../../decisions/adr-005-persistent-object-store-ttl-envelopes.md) records the storage format and metadata trade-off.


- **[`VertebraPlugin`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)[^src-extensions-litestar-plugin-py] & [`VertebraConfig`](../../../../../src/py/vertebra/extensions/litestar/config.py)[^src-extensions-litestar-config-py]**: Implements `InitPluginProtocol` and `CLIPluginProtocol`. During `on_app_init`, registers `app_config.state["vertebra_config"] = self._config` and `app_config.state["vertebra_plugin"] = self`, clears `app_config.compression_config = None` when `app_config.compression_config` uses `gzip`/`brotli`/`br`/`zstd`, `app_config.before_send` is empty, and `_is_rust_offload_active()` (`VERTEBRA_SERVER_ACTIVE=1` or `VERTEBRA_RUST_OFFLOAD=1`) holds, attaches explicit `mcp_config`/`a2a_config`/`agent_config` integrations (`app_config.state["litestar_mcp_plugin"]`, `app_config.state["litestar_a2a_plugin"]`, `AgentPlugin`) and runs `AutowirePlugin` once, sets `FASTPATH_ATTR` (`FastPath`), and serializes `offload_middlewares(app_config, config=self._config, strip_python_middleware=False)` into `app_config.state["vertebra_offload_config_json"]`. During `on_cli_init(cli)`, replaces Litestar's `run` command on `LitestarGroup` (`_build_litestar_run_command`) so `litestar run` launches the native Vertebra supervisor with full support for standard `litestar run` flags and Vertebra runtime flags (`--workers`, `--topology`, `--threads`, `--loop`, `--drain-timeout`, `--jit` / `--no-jit`, `--no-banner`).
- **[`VertebraPlugin.ensure_finalized`](../../../../../src/py/vertebra/extensions/litestar/plugin.py) & [`enter_litestar_server_lifespan`](../../../../../src/py/vertebra/extensions/litestar/plugin.py)**: Idempotent post-`Litestar.__init__` hook invoked by `enter_litestar_server_lifespan`, `AsgiRunner`, `FastPath.dispatch`, and `vertebra-http`'s `extract_offload_json_from_app` to swap in `MemoryStore` (`swap_response_cache_store`) when `VertebraConfig.swap_memory_response_cache` is enabled (default `False`), compile the finalized Litestar route table via `RouteCompiler`, extract `OffloadPlan` into `VERTEBRA_OFFLOAD_CONFIG`, compile `FastPath`, and bind `app.state`.
- **[`RouteCompiler`](../../../../../src/py/vertebra/extensions/litestar/router.py)[^src-extensions-litestar-router-py] / [`RouterTable`](../../../../../src/py/vertebra/extensions/litestar/router.py)**: Introspects Litestar routes into [`ExtractedRoute`](../../../../../src/py/vertebra/extensions/litestar/router.py) items (`RouteCompiler.compile`, `RouteCompiler.extract_entries`, `extract_routes`, `extract_route_entries`) and rewrites `{param:type}` paths to `matchit 0.9` syntax (`normalize_path_for_matchit`, `convert_path_params`).
- **[`offload_middlewares`](../../../../../src/py/vertebra/extensions/litestar/offload.py)[^src-extensions-litestar-offload-py] / [`OffloadPlan`](../../../../../src/py/vertebra/extensions/litestar/offload.py)**: Extracts Litestar `CORSConfig`, `CompressionConfig`, static mounts (reading `_static_files_config` without triggering Litestar deprecation warnings), and `OpenAPIConfig` into frozen `CorsOffloadSpec`, `CompressionOffloadSpec`, `StaticMountOffloadSpec`, and `OpenApiOffloadSpec` descriptors (`to_server_config`, `to_server_config_json`).
- **[`FastPath`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)[^src-extensions-litestar-fastpath-py] & [`CompiledRoute`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py)**: Frozen `msgspec.Struct(gc=False, kw_only=True)` route table compiled by `compile_fastpath_routes` using shared [`DirectPlan`](../../../../../src/py/vertebra/server/_fastpath_core.py) and [`QueryParamSpec`](../../../../../src/py/vertebra/server/_fastpath_core.py) primitives. `_compile_direct_signature_plan` unwraps Litestar 2.x/3.0 `FromPath[T]`, `FromQuery[T]`, and `JSONBody[T]` (`Annotated` markers) into zero-`Request` plans while falling back to `KwargsModel` for constrained `Parameter(gt=...)` / `FromHeader` / `FromCookie` markers. `FastPath.dispatch` and `FastPath.wrap_asgi` read bodies zero-copy via `AsgiScope.body_bytes()`, emit unary responses in 1 C-API call via `send.send_raw_response`, close unawaited `ASGIFileResponse.file_info` coroutines when intercepting `http.response.pathsend`, and route DI/DTO/`File`/`ServerSentEvent` handlers through [`dispatch_handler_direct`](../../../../../src/py/vertebra/extensions/litestar/fastpath.py).
- **[`ChannelsBackend` / `create_channels_plugin`](../../../../../src/py/vertebra/extensions/litestar/channels.py)[^src-extensions-litestar-channels-py]** & **[`ValkeyStore` / `RedisStore` / `MemoryStore`](../../../../../src/py/vertebra/extensions/litestar/store.py)[^src-extensions-litestar-store-py]**: `ChannelsBackend` subclasses [`ChannelBus`](../../../../../src/py/vertebra/server/_channels.py)[^src-server-channels-py] (`create_channels_plugin`) across Memory, Valkey, Redis, PostgreSQL, and Oracle via `SyncNotificationSource` and `AsyncNotificationSource` with origin-node deduplication, bounded replay, `ChangeFeed` spill hydration, and shielded stream cleanup. `ValkeyStore` (`vertebra._valkey`), `RedisStore` (`vertebra._redis`), and `MemoryStore` implement `litestar.stores.base.Store` over `BaseNativeKVStore` with RESP3 `CLIENT TRACKING` L1 invalidation, `key_prefix` isolation, `SCAN` + `UNLINK` deletion, and millisecond `PX` TTLs.

## 2. Starlette, FastAPI, Flask & Django Extensions

Starlette, FastAPI, Flask, and Django MCP adapters delegate protocol dispatch to [`MCPCore`](../../../../../src/py/vertebra/agent/mcp/service.py)[^src-agent-mcp-service-py], [`AsgiMcpApp`](../../../../../src/py/vertebra/agent/mcp/routes.py)[^src-agent-mcp-routes-py], [`WsgiMcpApp`](../../../../../src/py/vertebra/agent/mcp/routes.py), [`WebMCPConfig`](../../../../../src/py/vertebra/agent/mcp/webmcp.py)[^src-agent-mcp-webmcp-py], and [`build_webmcp_manifest`](../../../../../src/py/vertebra/agent/mcp/webmcp.py).

| Extension Package | Plugin & Config | Route Compiler & FastPath | Ecosystem & Data Bridges |
|---|---|---|---|
| [`vertebra.extensions.starlette`](../../../../../src/py/vertebra/extensions/starlette/__init__.py)[^src-extensions-starlette-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (`Mount`/`Route`/`WebSocketRoute` -> `matchit 0.9`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, inline `sync_to_thread=False` default, `msgspec.Struct`, `pydantic.BaseModel`, `@dataclass`, `@attrs.define`), `offload_middlewares` (`GZipMiddleware`, `CORSMiddleware`, `StaticFiles`) | `SyncObjectStore` / `AsyncObjectStore`, `CacheStore` (`ValkeyStore`/`RedisStore`), `ChannelsBackend`, `QueueService` (`attach_queue_service`, `queue_lifespan`), `JWTAuthBackend`, `RateLimitGuard`, `OpenAPIConfig` (`mount_docs_routes`), [`MCPRegistry`](../../../../../src/py/vertebra/extensions/starlette/mcp.py)[^src-extensions-starlette-mcp-py], `ViteExtension` |
| [`vertebra.extensions.fastapi`](../../../../../src/py/vertebra/extensions/fastapi/__init__.py)[^src-extensions-fastapi-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (`APIRoute`), `CompiledDependantPlan` (`compile_dependant_plan` topological DAG with `needs_request` tracking and inline `sync_to_thread=False`), `FastPath` (`wrap_asgi_fastpath`, `send_raw_response`, `MsgspecJSONResponse`, lazy `starlette.requests.Request` skipped when `needs_request=False`, `decode_pydantic_json` + `__pydantic_serializer__.to_json` / `decode_msgspec` C-API bypass) | `SyncObjectStore` / `AsyncObjectStore`, `CacheStore`, `ChannelsBackend`, `QueueService` (`attach_queue_service`, `queue_lifespan`), `JWTAuthBackend`, `RateLimitGuard`, `OpenAPIConfig` (`mount_docs_routes`), `MCPRegistry` (`mount_mcp_routes`), `ViteExtension` |
| [`vertebra.extensions.flask`](../../../../../src/py/vertebra/extensions/flask/__init__.py)[^src-extensions-flask-init-py] | `VertebraPlugin`, `VertebraConfig` | `RouteCompiler` (Werkzeug `app.url_map.iter_rules()` -> `matchit 0.9`), `FastPath` (supports both sync `def` and `async def` views via `step_coro_sync`, `dispatch_async`, `wrap_asgi`, `wrap_wsgi`, and `_WsgiStartResponse.send_raw_response` over `WsgiInput.decode_msgspec` / `decode_pydantic_json`), `offload_middlewares` (`Flask-Compress`, `Flask-CORS`, `static_folder`) | `CacheBackend` / `FlaskCachingBackend`, `SyncObjectStore` / `AsyncObjectStore`, `MailBackend` (`MailConfig`, `MailMessage`), `ChannelsBackend`, `QueueBackend` (`QueueService` + `Container.scope()`), `JWTAuthGuard`, `RateLimitGuard`, `ViteAssetBridge` (`ViteConfig`, `attach_vite`), `OpenAPIConfig`, [`MCPConfig` / `MCPRegistry`](../../../../../src/py/vertebra/extensions/flask/mcp.py)[^src-extensions-flask-mcp-py] |
| [`vertebra.extensions.django`](../../../../../src/py/vertebra/extensions/django/__init__.py)[^src-extensions-django-init-py] | `VertebraPlugin`, `VertebraConfig`, `VertebraAppConfig` | `RouteCompiler` (`URLPattern` / `URLResolver`), `FastPath` (`@vertebra_view`, pre-allocated stateless `MsgspecView` instances, sync `def` and `async def` views across `dispatch`, `dispatch_async`, `dispatch_wsgi` with `send_raw_response`, and `wrap_asgi`), `offload_middlewares` (`GZipMiddleware`, `CorsMiddleware`, `WhiteNoiseMiddleware`, `SecurityMiddleware`), `ORMBridge` (`orm_to_struct_vectorcall`) | Native Django `6.0+` `vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,spanner}` (`DatabaseWrapper` over `vertebra.db.dbapi` `SyncDBAPIModule`), `ArrowQuerySet`, `CacheBackend(BaseCache)` (`SyncCacheBackend`/`AsyncCacheBackend`), `TaskBackend(BaseTaskBackend)` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`, `vertebra_worker` management command with `Container.scope()`), distributed `ChannelLayer` (`ChannelBus`), `DjangoStorage`, `EmailBackend`, `SecurityBridge`, `TemplateBridge`, `ViteBridge`, `OpenAPIConfig`, [`MCPConfig` / `MCPRegistry`](../../../../../src/py/vertebra/extensions/django/mcp.py)[^src-extensions-django-mcp-py] |

`DjangoStorage` is a deconstructible Django `Storage` backend over an explicitly configured native store. Its storage-bound `File` reopens the qualified object and refreshes its size, including cached `FieldFile` reuse. `aopen` awaits metadata before constructing the synchronous reader so construction performs no synchronous storage I/O. Modification time comes from native metadata; creation/access timestamps raise `NotImplementedError` [^src-extensions-django-storage-py].

Migration serialization uses Django's public `MigrationWriter.register_serializer` for each provider configuration, preserving constructor fields and public `vertebra.storage` names. Live native handles are nonportable migration inputs. The binary `File` specialization exists only for typing; runtime annotations resolve without modifying Django classes [^src-extensions-django-storage-py].

## 3. First-Party Litestar Ecosystem Subpackages (`extensions/litestar/*`)

All `litestar-*` ecosystem integrations live under [`vertebra.extensions.litestar`](../../../../../src/py/vertebra/extensions/litestar/__init__.py)[^src-extensions-litestar-init-py].

| Subpackage | Canonical Helpers & Adapters | Upstream Integration |
|---|---|---|
| `queues` | `StoreQueueBackend`, `StoreQueueBackendConfig`, `SHMQueueBackend`, `SHMQueueBackendConfig`, `SHMExecutionBackend`, `ContainerTaskDependencyProvider`, `create_store_queue_config()`, `create_shm_queue_config()`, `create_sqlspec_queue_config()` | Wraps `litestar_queues>=0.14.0` (`QueuePlugin`, `QueueConfig`, `queue_store_class`/`events_store_class`) with engine-neutral `AsyncQueueStore` backends, SHM ring wakeups, SQLSpec stores, and `Container.scope()` task DI. |
| `security` | `JWTAuthenticator`, `JWTAuthenticationMechanism`, `IngressIdentityMechanism`, `RateLimitGuard`, `create_jwt_authentication_mechanism()`, `create_ingress_identity_mechanism()`, `exempt_vertebra_routes()`, `configure_vertebra_exclusions()` | Wraps `litestar_security` (`SecurityPlugin`, `SecurityConfig`) with zero-copy bearer/ingress identity slots, JWT authentication, and Rust rate-limit guards. |
| `autowire` | `AutowireBridgeIntegration`, `create_autowire_plugin()`, `ensure_autowire_before_route_compiler()` | Wraps `litestar_autowire` with domain package scanning and route-compiler ordering. |
| `email` | `SESEmailBackend`, `SESEmailConfig` | Bridges `litestar_email` to Vertebra transactional SES email delivery. |
| `mcp` | `create_mcp_plugin()`, `create_shm_mcp_task_config()`, `JSONRPCFrameAccelerator`, `ObjectStoreSkillSource`, `WellKnownAgentCardOffload` | Bridges `litestar_mcp>=0.20.0` (`LitestarMCP`, `LitestarA2A`) dual-mode delegation, well-known skill list merging, Rust JSON-RPC frame acceleration, SHM task backends, object-store skills, and cached `.well-known` discovery. |
| `vite` | `create_vite_plugin()`, `get_vertebra_client_path()`, `get_vertebra_client_vite_aliases()` | Bridges `litestar_vite` (`RuntimeConfig(executor="bun")`) with `vertebra-bundler` (Rolldown/Oxc) asset compilation, bundled `@vertebra/client` alias injection, and Inertia SSR. |
| [`openapi`](../../../../../src/py/vertebra/extensions/litestar/openapi.py)[^src-extensions-litestar-openapi-py] | `OpenAPIConfig`, `ScalarDocsConfig`, `SwaggerDocsConfig`, `RedocDocsConfig`, `ElementsDocsConfig`, `RapiDocDocsConfig`, `AsyncAPIDocsConfig`, `ScalarSpecSource`, `build_docs_route_handlers()` | Configurable multi-spec API documentation portal (Scalar default, Swagger UI, Redoc, Elements, RapiDoc, and AsyncAPI 3.0). |
| `htmx` | `create_htmx_plugin()` | Configures `litestar_htmx` (`HTMXPlugin`, `HTMXConfig`) for server-rendered HTML applications. |
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

[^src-extensions-django-storage-py]: `src/py/vertebra/extensions/django/storage.py` — Native Django storage and portable configuration serialization.
