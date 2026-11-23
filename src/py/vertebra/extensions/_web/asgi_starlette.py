"""Shared lazy-import helpers for Starlette and FastAPI extension modules."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from vertebra.framework.channels import create_channels_backend
from vertebra.framework.openapi import OpenAPIConfig
from vertebra.framework.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    MemoryConfig,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
)
from vertebra.framework.vite import ViteBridge, ViteConfig
from vertebra.server._channels import ChannelBus
from vertebra.server._offload import (
    CompressionOffloadSpec,
    CorsOffloadSpec,
    OffloadPlan,
    OpenApiOffloadSpec,
    StaticMountOffloadSpec,
    is_rust_offload_active,
)
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from vertebra.build._assets import AssetManifest, EarlyHintsSpec
    from vertebra.framework.inertia import InertiaConfig

__all__ = (
    "BaseViteExtension",
    "attach_state_channels",
    "attach_state_storage",
    "build_openapi_offload_spec",
    "extract_asgi_openapi_spec",
    "extract_asgi_static_mounts",
    "has_non_offloadable_asgi_middleware",
    "mount_starlette_docs_routes",
    "offload_asgi_middlewares",
    "set_app_state_attr",
)

_OFFLOADABLE_MIDDLEWARE_NAMES: frozenset[str] = frozenset({"GZipMiddleware", "CORSMiddleware"})


def set_app_state_attr(app: Any, key: str, value: Any) -> None:
    """Assign ``key`` on ``app.state`` supporting both mapping and attribute namespaces."""
    state = getattr(app, "state", None)
    if state is None:
        return
    if isinstance(state, dict):
        state[key] = value
    else:
        setattr(state, key, value)


def build_openapi_offload_spec(schema_path: str, schema_dict: dict[str, Any]) -> OpenApiOffloadSpec:
    """Serialize an OpenAPI schema mapping into a hashed ``OpenApiOffloadSpec``."""
    body_bytes = bytes(encode_json_bytes(schema_dict))
    digest = hashlib.sha256(body_bytes).hexdigest()[:16]
    return OpenApiOffloadSpec(
        schema_path=schema_path,
        body=body_bytes,
        etag=f'"{digest}"',
        content_type="application/json",
    )


def attach_state_channels(
    app: Any,
    backend_cls: type[Any],
    backend: Any = None,
    *,
    url: str | None = None,
    history: int = 0,
    notification_source: Any = None,
    sync_notification_source: Any = None,
    change_source: Any = None,
    change_feed: Any = None,
    node_id: str | None = None,
    maxsize: int = 256,
    **kwargs: Any,
) -> Any:
    """Resolve and attach a channels backend instance onto ``app.state.channels``."""
    if isinstance(backend, (backend_cls, ChannelBus)):
        resolved = backend
    elif (
        isinstance(backend, str)
        or url is not None
        or notification_source is not None
        or sync_notification_source is not None
    ):
        raw_backend = create_channels_backend(
            backend=backend,
            url=url,
            history=history,
            notification_source=notification_source,
            sync_notification_source=sync_notification_source,
            change_source=change_source,
            change_feed=change_feed,
            node_id=node_id,
            maxsize=maxsize,
            **kwargs,
        )
        resolved = backend_cls(
            backend=raw_backend,
            history=history,
            notification_source=raw_backend.notification_source,
            sync_notification_source=raw_backend.sync_notification_source,
            change_source=raw_backend.change_source,
            change_feed=raw_backend.change_feed,
            node_id=raw_backend.node_id,
            maxsize=maxsize,
        )
    else:
        resolved = backend_cls()
    set_app_state_attr(app, "channels", resolved)
    return resolved


def attach_state_storage(
    app: Any,
    store_or_registry: SyncStorageRegistry
    | AsyncStorageRegistry
    | SyncObjectStore
    | AsyncObjectStore
    | StorageConfig
    | str
    | None = None,
    *,
    async_mode: bool = False,
) -> SyncStorageRegistry | AsyncStorageRegistry:
    """Attach a ``SyncStorageRegistry`` or ``AsyncStorageRegistry`` onto ``app.state.storage``."""
    registry: SyncStorageRegistry | AsyncStorageRegistry
    if isinstance(store_or_registry, (SyncStorageRegistry, AsyncStorageRegistry)):
        registry = store_or_registry
    elif async_mode or isinstance(store_or_registry, AsyncObjectStore):
        registry = AsyncStorageRegistry(default_store=store_or_registry or MemoryConfig())
    else:
        registry = SyncStorageRegistry(default_store=store_or_registry or MemoryConfig())
    set_app_state_attr(app, "storage", registry)
    return registry


class BaseViteExtension:
    """Shared Vite/Inertia integration attaching bridge, manifest, and EarlyHints to ``app.state``."""

    __slots__ = ("_bridge", "_inertia_config")

    def __init__(
        self,
        *,
        manifest_path: str | Path | None = None,
        manifest: AssetManifest | None = None,
        early_hints: EarlyHintsSpec | None = None,
        inertia_config: InertiaConfig | None = None,
        config: ViteConfig | None = None,
    ) -> None:
        resolved_config = config or ViteConfig(manifest_path=manifest_path)
        self._bridge = ViteBridge(resolved_config, manifest=manifest)
        if early_hints is not None:
            self._bridge._early_hints = early_hints
        self._inertia_config = inertia_config

    @property
    def bridge(self) -> ViteBridge:
        """Return the underlying neutral ``ViteBridge``."""
        return self._bridge

    @property
    def manifest(self) -> AssetManifest | None:
        """Return the loaded ``AssetManifest`` when available."""
        return self._bridge.manifest

    @property
    def early_hints(self) -> EarlyHintsSpec:
        """Return the active ``EarlyHintsSpec``."""
        return self._bridge.early_hints

    def hmr_client_tag(self, *, csp_nonce: str | None = None) -> str:
        """Return the Vite HMR client script tag via the underlying ``ViteBridge``."""
        return self._bridge.hmr_client_tag(csp_nonce=csp_nonce)

    def asset_tags(
        self,
        entry: str,
        *,
        csp_nonce: str | None = None,
        preload_dynamic: bool | None = None,
    ) -> str:
        """Return script and link HTML tags for ``entry`` via the underlying ``ViteBridge``."""
        return self._bridge.asset_tags(
            entry,
            csp_nonce=csp_nonce,
            preload_dynamic=preload_dynamic,
        )

    def _attach_state(self, app: Any) -> Any:
        set_app_state_attr(app, "vite", self)
        set_app_state_attr(app, "vite_bridge", self._bridge)
        set_app_state_attr(app, "asset_manifest", self._bridge.manifest)
        set_app_state_attr(app, "early_hints", self._bridge.early_hints)
        return app


def extract_asgi_static_mounts(app: Any) -> tuple[StaticMountOffloadSpec, ...]:
    """Extract mounted ``StaticFiles`` apps from a Starlette/FastAPI route list."""
    mounts: list[StaticMountOffloadSpec] = []
    for route in getattr(app, "routes", None) or ():
        if type(route).__name__ != "Mount":
            continue
        mounted_app = getattr(route, "app", None)
        if mounted_app is None or type(mounted_app).__name__ != "StaticFiles":
            continue
        path_prefix = str(getattr(route, "path", "") or "/")
        directories: list[str] = []
        raw_dir = getattr(mounted_app, "directory", None)
        if raw_dir is not None:
            directories.append(str(raw_dir))
        for extra_dir in getattr(mounted_app, "all_directories", ()) or ():
            dir_str = str(extra_dir)
            if dir_str not in directories:
                directories.append(dir_str)
        if directories:
            mounts.append(
                StaticMountOffloadSpec(
                    path_prefix=path_prefix,
                    directories=tuple(directories),
                    html_mode=bool(getattr(mounted_app, "html", False)),
                    send_as_attachment=False,
                    headers={},
                )
            )
    return tuple(mounts)


def extract_asgi_openapi_spec(
    app: Any,
    *,
    config: Any = None,
    check_state: bool = True,
) -> OpenApiOffloadSpec | None:
    """Build an ``OpenApiOffloadSpec`` from app state or ``app.openapi()``."""
    if check_state:
        state = getattr(app, "state", None)
        if state is not None:
            direct_spec = getattr(state, "vertebra_openapi_spec", None)
            if isinstance(direct_spec, OpenApiOffloadSpec):
                return direct_spec
            raw_schema = getattr(state, "openapi_schema", None)
            if isinstance(raw_schema, dict):
                openapi_cfg = getattr(config, "openapi_config", None) if config is not None else None
                schema_path = str(getattr(openapi_cfg, "schema_path", "/openapi.json") or "/openapi.json")
                return build_openapi_offload_spec(schema_path, raw_schema)

    openapi_url = getattr(app, "openapi_url", None)
    openapi_fn = getattr(app, "openapi", None)
    if isinstance(openapi_url, str) and openapi_url and callable(openapi_fn):
        try:
            schema_dict = openapi_fn()
        except Exception:
            return None
        if isinstance(schema_dict, dict):
            return build_openapi_offload_spec(openapi_url, schema_dict)
    return None


def offload_asgi_middlewares(
    app: Any,
    *,
    config: Any = None,
    strip_python_middleware: bool = False,
    internal_middleware_names: frozenset[str] = frozenset(),
    check_state_openapi: bool = True,
) -> OffloadPlan:
    """Extract GZip, CORS, StaticFiles mounts, and OpenAPI schema from a Starlette/FastAPI app."""
    enable_compression = True if config is None else bool(getattr(config, "offload_compression", True))
    enable_cors = True if config is None else bool(getattr(config, "offload_cors", True))
    enable_static = True if config is None else bool(getattr(config, "offload_static_files", True))
    enable_openapi = True if config is None else bool(getattr(config, "offload_openapi", True))

    compression_spec: CompressionOffloadSpec | None = None
    cors_spec: CorsOffloadSpec | None = None
    retained_middleware: list[Any] = []
    removed_any = False
    rust_active = is_rust_offload_active()

    for mw in list(getattr(app, "user_middleware", None) or ()):
        cls_name = getattr(getattr(mw, "cls", None), "__name__", "")
        if cls_name in internal_middleware_names:
            retained_middleware.append(mw)
            continue
        kwargs = dict(getattr(mw, "kwargs", None) or {})
        args = tuple(getattr(mw, "args", None) or ())

        if cls_name == "GZipMiddleware" and enable_compression and compression_spec is None:
            min_size = int(kwargs.get("minimum_size", args[0] if args else 500))
            compresslevel = int(kwargs.get("compresslevel", args[1] if len(args) > 1 else 9))
            compression_spec = CompressionOffloadSpec(
                backend="gzip",
                minimum_size=min_size,
                gzip_compress_level=compresslevel,
                brotli_quality=4,
                zstd_level=3,
                exclude_patterns=(),
                exclude_opt_key=None,
                excluded_paths=(),
            )
            if strip_python_middleware and rust_active:
                removed_any = True
                continue
        elif cls_name == "CORSMiddleware" and enable_cors and cors_spec is None:
            allow_origin_regex = kwargs.get("allow_origin_regex")
            cors_spec = CorsOffloadSpec(
                allow_origins=tuple(str(o) for o in kwargs.get("allow_origins", ())),
                allow_methods=tuple(str(m).upper() for m in kwargs.get("allow_methods", ("GET",))),
                allow_headers=tuple(str(h) for h in kwargs.get("allow_headers", ())),
                allow_credentials=bool(kwargs.get("allow_credentials", False)),
                expose_headers=tuple(str(h) for h in kwargs.get("expose_headers", ())),
                max_age=int(kwargs.get("max_age", 600)),
                allow_origin_regex=str(allow_origin_regex) if allow_origin_regex is not None else None,
            )
            if strip_python_middleware and rust_active:
                removed_any = True
                continue

        retained_middleware.append(mw)

    if removed_any and hasattr(app, "user_middleware"):
        app.user_middleware = retained_middleware
        build_stack = getattr(app, "build_middleware_stack", None)
        if callable(build_stack):
            app.middleware_stack = build_stack()

    return OffloadPlan(
        compression=compression_spec,
        cors=cors_spec,
        static_mounts=extract_asgi_static_mounts(app) if enable_static else (),
        openapi=extract_asgi_openapi_spec(app, config=config, check_state=check_state_openapi)
        if enable_openapi
        else None,
    )


def has_non_offloadable_asgi_middleware(
    app: Any,
    *,
    internal_middleware_names: frozenset[str] = frozenset(),
) -> bool:
    """Return True if the app registers custom Python middleware beyond offloaded GZip/CORS."""
    rust_active = is_rust_offload_active()
    for mw in getattr(app, "user_middleware", None) or ():
        cls_name = getattr(getattr(mw, "cls", None), "__name__", "")
        if cls_name in internal_middleware_names:
            continue
        if not (rust_active and cls_name in _OFFLOADABLE_MIDDLEWARE_NAMES):
            return True
    return False


def mount_starlette_docs_routes(
    app: Any,
    config: OpenAPIConfig | None = None,
    *,
    require_starlette: bool = True,
) -> dict[str, str]:
    """Mount configured Scalar, Swagger UI, AsyncAPI, Redoc, Elements, and RapiDoc routes onto ``app``."""
    resolved = config or OpenAPIConfig()
    rendered = resolved.render_docs_routes()
    routes = getattr(app, "routes", None)
    if not isinstance(routes, list):
        return rendered

    if require_starlette:
        routing_mod: Any = ensure_module("starlette.routing", package="starlette", extra="starlette")
        responses_mod: Any = ensure_module("starlette.responses", package="starlette", extra="starlette")
    else:
        routing_mod = import_optional("starlette.routing")
        responses_mod = import_optional("starlette.responses")
    existing_paths = {getattr(r, "path", None) for r in routes}

    for path, html_body in rendered.items():
        if path in existing_paths:
            continue

        async def _serve_portal(_request: Any, _html: str = html_body) -> Any:
            if responses_mod is not None and hasattr(responses_mod, "HTMLResponse"):
                return responses_mod.HTMLResponse(_html)
            return _html

        if routing_mod is not None and hasattr(routing_mod, "Route"):
            routes.append(
                routing_mod.Route(
                    path,
                    endpoint=_serve_portal,
                    methods=["GET"],
                    include_in_schema=False,
                )
            )
        else:
            routes.append(
                SimpleNamespace(
                    path=path,
                    endpoint=_serve_portal,
                    methods=["GET"],
                    name=f"_vertebra_docs_{path.strip('/')}",
                )
            )
    return rendered
