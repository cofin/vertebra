"""Framework baseline benchmark applications backed by the shared ``DataBackend``.

Provides lazy-loaded factories for Django-Bolt, Spikard, Litestar, FastAPI,
Django, and Flask exposing identical TechEmpower (``/json``, ``/db``,
``/queries``, ``/updates``, ``/cached-queries``, ``/health``) and
object-storage (``GET``/``PUT /object/{key}``) endpoints. Every optional
framework dependency is resolved via ``import_optional`` inside its factory so
importing this module never fails when comparator packages are absent.
"""

from __future__ import annotations

import inspect
import os
import sys
import types
from collections.abc import Callable
from typing import Annotated, Any, Final

from tools.benchmarks.apps._data_backend import (
    JSON_HELLO_PAYLOAD,
    DataBackend,
    clamp_query_count,
    get_default_backend,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "SUPPORTED_FRAMEWORK_BASELINES",
    "build_django_bolt_command",
    "create_blacksheep_app",
    "create_django_app",
    "create_django_bolt_app",
    "create_django_wsgi_app",
    "create_fastapi_app",
    "create_flask_app",
    "create_litestar_app",
    "create_spikard_data_app",
)

SUPPORTED_FRAMEWORK_BASELINES: Final[tuple[str, ...]] = (
    "django_bolt",
    "spikard",
    "litestar",
    "fastapi",
    "django",
    "django_wsgi",
    "flask",
    "blacksheep",
)

_JSON_MEDIA_TYPE: Final[str] = "application/json"
_TEXT_MEDIA_TYPE: Final[str] = "text/plain"
_BINARY_MEDIA_TYPE: Final[str] = "application/octet-stream"
_DJANGO_URLCONF_NAME: Final[str] = "_vertebra_bench_django_urls"
_DJANGO_WSGI_URLCONF_NAME: Final[str] = "_vertebra_bench_django_wsgi_urls"
_DJANGO_BOLT_SETTINGS_KEY: Final[str] = "test-token-placeholder"

type ResponseFactory = Callable[[bytes, int, str], Any]


class _DataEndpointBundle:
    """Shared async/sync endpoint handler constructors across benchmark web frameworks."""

    __slots__ = ("_backend", "_make_response")

    def __init__(self, backend: DataBackend, make_response: ResponseFactory) -> None:
        self._backend = backend
        self._make_response = make_response

    def make_async_core(self) -> dict[str, Any]:
        """Build independent async handler closures for signature-bound frameworks."""
        backend = self._backend
        resp = self._make_response

        async def json_endpoint() -> Any:
            return resp(JSON_HELLO_PAYLOAD, 200, _JSON_MEDIA_TYPE)

        async def db_endpoint() -> Any:
            return resp(encode_json_bytes(await backend.get_world()), 200, _JSON_MEDIA_TYPE)

        async def queries_endpoint(queries: str | None = None) -> Any:
            return resp(encode_json_bytes(await backend.get_worlds(clamp_query_count(queries))), 200, _JSON_MEDIA_TYPE)

        async def updates_endpoint(queries: str | None = None) -> Any:
            return resp(
                encode_json_bytes(await backend.update_worlds(clamp_query_count(queries))), 200, _JSON_MEDIA_TYPE
            )

        async def cached_queries_endpoint(count: str | None = None) -> Any:
            return resp(
                encode_json_bytes(await backend.get_cached_worlds(clamp_query_count(count))), 200, _JSON_MEDIA_TYPE
            )

        async def get_object_endpoint(key: str) -> Any:
            payload = await backend.get_object(key)
            if payload is None:
                return resp(b"Not Found", 404, _TEXT_MEDIA_TYPE)
            return resp(payload, 200, _BINARY_MEDIA_TYPE)

        async def health_endpoint() -> Any:
            return resp(b"ok", 200, _TEXT_MEDIA_TYPE)

        return {
            "json": json_endpoint,
            "db": db_endpoint,
            "queries": queries_endpoint,
            "updates": updates_endpoint,
            "cached": cached_queries_endpoint,
            "get_object": get_object_endpoint,
            "health": health_endpoint,
        }

    async def put_payload_async(self, key: str, data: bytes) -> Any:
        """Persist an object asynchronously and return a JSON response."""
        size = await self._backend.put_object(key, data)
        return self._make_response(encode_json_bytes({"key": key, "size": size}), 200, _JSON_MEDIA_TYPE)

    def make_get_object_sync(self, key: str) -> Any:
        """Fetch an object synchronously and format HTTP 200/404 response."""
        payload = self._backend.get_object_sync(key)
        if payload is None:
            return self._make_response(b"Not Found", 404, _TEXT_MEDIA_TYPE)
        return self._make_response(payload, 200, _BINARY_MEDIA_TYPE)

    def put_payload_sync(self, key: str, data: bytes) -> Any:
        """Persist an object synchronously and return a JSON response."""
        size = self._backend.put_object_sync(key, data)
        return self._make_response(encode_json_bytes({"key": key, "size": size}), 200, _JSON_MEDIA_TYPE)


def build_django_bolt_command(host: str, port: int, workers: int) -> list[str]:
    """Build the ``django-bolt`` ``runbolt`` command invocation."""
    return [
        sys.executable,
        "-m",
        "django",
        "runbolt",
        "--host",
        host,
        "--port",
        str(port),
        "--processes",
        str(max(1, workers)),
    ]


def create_django_bolt_app(backend: DataBackend | None = None) -> Any:
    """Create a ``django-bolt`` ``BoltAPI`` app backed by ``DataBackend``."""
    django_bolt = _require_framework("django_bolt", "django-bolt", "bench-comparators")
    _ensure_django_settings(installed_apps=["django_bolt"])
    api = django_bolt.BoltAPI()
    response_cls = getattr(django_bolt, "Response", None)

    def _make_response(content: bytes, status_code: int, media_type: str) -> Any:
        if response_cls is not None:
            return response_cls(content, status_code=status_code, media_type=media_type)
        return content

    bundle = _DataEndpointBundle(_resolve_backend(backend), _make_response)
    handlers = bundle.make_async_core()

    async def put_object_endpoint(key: str, request: Any) -> Any:
        raw_body: Any = getattr(request, "body", b"")
        if callable(raw_body):
            raw_body = raw_body()
        if inspect.isawaitable(raw_body):
            raw_body = await raw_body
        data = raw_body if isinstance(raw_body, bytes) else bytes(raw_body)
        return await bundle.put_payload_async(key, data)

    _register_decorator_routes(api, handlers, put_object_endpoint)
    return api


def create_litestar_app(backend: DataBackend | None = None) -> Any:
    """Create a Litestar ASGI application backed by ``DataBackend``."""
    litestar_mod = _require_framework("litestar", "litestar", "bench-comparators")
    litestar_params = import_optional("litestar.params")
    response_cls = litestar_mod.Response
    request_cls = litestar_mod.Request
    bundle = _DataEndpointBundle(
        _resolve_backend(backend),
        lambda content, status_code, media_type: response_cls(
            content=content, status_code=status_code, media_type=media_type
        ),
    )
    handlers = bundle.make_async_core()

    async def put_object_endpoint(key: str, request: Any) -> Any:
        return await bundle.put_payload_async(key, await request.body())

    parameter_cls: Any = getattr(litestar_params, "QueryParameter", None) if litestar_params is not None else None
    path_parameter_cls: Any = getattr(litestar_params, "PathParameter", None) if litestar_params is not None else None
    if parameter_cls is not None:
        handlers["queries"].__annotations__ = {
            "queries": Annotated[str | None, parameter_cls(name="queries", required=False)],
            "return": Any,
        }
        handlers["updates"].__annotations__ = {
            "queries": Annotated[str | None, parameter_cls(name="queries", required=False)],
            "return": Any,
        }
        handlers["cached"].__annotations__ = {
            "count": Annotated[str | None, parameter_cls(name="count", required=False)],
            "return": Any,
        }
    key_annotation: Any = Annotated[str, path_parameter_cls()] if path_parameter_cls is not None else str
    handlers["get_object"].__annotations__ = {"key": key_annotation, "return": Any}
    put_object_endpoint.__annotations__ = {"key": key_annotation, "request": request_cls, "return": Any}

    return litestar_mod.Litestar(
        route_handlers=[
            litestar_mod.get("/json")(handlers["json"]),
            litestar_mod.get("/db")(handlers["db"]),
            litestar_mod.get("/queries")(handlers["queries"]),
            litestar_mod.get("/updates")(handlers["updates"]),
            litestar_mod.get("/cached-queries")(handlers["cached"]),
            litestar_mod.get("/object/{key:str}")(handlers["get_object"]),
            litestar_mod.put("/object/{key:str}")(put_object_endpoint),
            litestar_mod.get("/health")(handlers["health"]),
        ],
        openapi_config=None,
    )


def create_fastapi_app(backend: DataBackend | None = None) -> Any:
    """Create a FastAPI ASGI application backed by ``DataBackend``."""
    fastapi_mod = _require_framework("fastapi", "fastapi", "bench-comparators")
    app = fastapi_mod.FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    response_cls = fastapi_mod.Response
    bundle = _DataEndpointBundle(
        _resolve_backend(backend),
        lambda content, status_code, media_type: response_cls(
            content=content, status_code=status_code, media_type=media_type
        ),
    )
    handlers = bundle.make_async_core()

    async def put_object_endpoint(key: str, request: Any) -> Any:
        return await bundle.put_payload_async(key, await request.body())

    put_object_endpoint.__annotations__ = {"key": str, "request": fastapi_mod.Request, "return": Any}
    _register_decorator_routes(app, handlers, put_object_endpoint)
    return app


def create_django_app(backend: DataBackend | None = None) -> Any:
    """Create a standard Django ASGI application backed by ``DataBackend``."""
    return _create_django_interface_app(backend, is_async=True)


def create_django_wsgi_app(backend: DataBackend | None = None) -> Any:
    """Create a standard sync Django WSGI application backed by ``DataBackend``."""
    return _create_django_interface_app(backend, is_async=False)


def create_flask_app(backend: DataBackend | None = None) -> Any:
    """Create a Flask WSGI application backed by ``DataBackend`` sync methods."""
    flask_mod = _require_framework("flask", "flask", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    app = flask_mod.Flask("vertebra_bench_flask")
    response_cls = flask_mod.Response
    flask_request = flask_mod.request
    bundle = _DataEndpointBundle(
        resolved_backend,
        lambda content, status_code, media_type: response_cls(content, status=status_code, mimetype=media_type),
    )

    app.add_url_rule(
        "/json",
        "json_endpoint",
        lambda: response_cls(JSON_HELLO_PAYLOAD, status=200, mimetype=_JSON_MEDIA_TYPE),
    )
    app.add_url_rule(
        "/db",
        "db_endpoint",
        lambda: response_cls(
            encode_json_bytes(resolved_backend.get_world_sync()), status=200, mimetype=_JSON_MEDIA_TYPE
        ),
    )
    app.add_url_rule(
        "/queries",
        "queries_endpoint",
        lambda: response_cls(
            encode_json_bytes(resolved_backend.get_worlds_sync(clamp_query_count(flask_request.args.get("queries")))),
            status=200,
            mimetype=_JSON_MEDIA_TYPE,
        ),
    )
    app.add_url_rule(
        "/updates",
        "updates_endpoint",
        lambda: response_cls(
            encode_json_bytes(
                resolved_backend.update_worlds_sync(clamp_query_count(flask_request.args.get("queries")))
            ),
            status=200,
            mimetype=_JSON_MEDIA_TYPE,
        ),
    )
    app.add_url_rule(
        "/cached-queries",
        "cached_queries_endpoint",
        lambda: response_cls(
            encode_json_bytes(
                resolved_backend.get_cached_worlds_sync(clamp_query_count(flask_request.args.get("count")))
            ),
            status=200,
            mimetype=_JSON_MEDIA_TYPE,
        ),
    )
    app.add_url_rule("/object/<key>", "get_object_endpoint", bundle.make_get_object_sync, methods=["GET"])
    app.add_url_rule(
        "/object/<key>",
        "put_object_endpoint",
        lambda key: bundle.put_payload_sync(key, flask_request.get_data()),
        methods=["PUT"],
    )
    app.add_url_rule(
        "/health",
        "health_endpoint",
        lambda: response_cls(b"ok", status=200, mimetype=_TEXT_MEDIA_TYPE),
    )
    return app


def create_spikard_data_app(backend: DataBackend | None = None) -> Any:
    """Create a Spikard application backed by ``DataBackend``."""
    spikard_mod = _require_framework("spikard", "spikard", "bench")
    response_cls = spikard_mod.Response
    app = spikard_mod.App(
        config=spikard_mod.ServerConfig(
            host=os.environ.get("SPIKARD_HOST", "127.0.0.1"),
            port=int(os.environ.get("SPIKARD_PORT", "8005")),
            compression=None,
            enable_request_id=False,
            rate_limit=None,
        )
    )

    def _spikard_response(content: bytes, status_code: int, media_type: str) -> Any:
        text = content.decode("latin1") if media_type == _BINARY_MEDIA_TYPE else content.decode("utf-8")
        return response_cls(content=text, status_code=status_code, headers={"content-type": media_type})

    bundle = _DataEndpointBundle(_resolve_backend(backend), _spikard_response)
    handlers = bundle.make_async_core()

    async def put_object_endpoint(key: str, body: bytes = b"") -> Any:
        data = body if isinstance(body, bytes) else str(body).encode("latin1")
        return await bundle.put_payload_async(key, data)

    _register_decorator_routes(app, handlers, put_object_endpoint)
    return app


def create_blacksheep_app(backend: DataBackend | None = None) -> Any:
    """Create a BlackSheep ASGI application backed by ``DataBackend``."""
    blacksheep_mod = _require_framework("blacksheep", "blacksheep", "bench-comparators")
    router_cls = getattr(blacksheep_mod, "Router", None)
    app = blacksheep_mod.Application(router=router_cls()) if router_cls is not None else blacksheep_mod.Application()
    response_cls = blacksheep_mod.Response
    content_cls = blacksheep_mod.Content
    bundle = _DataEndpointBundle(
        _resolve_backend(backend),
        lambda content, status_code, media_type: response_cls(
            status_code, content=content_cls(media_type.encode("ascii"), content)
        ),
    )
    handlers = bundle.make_async_core()

    async def put_object_endpoint(key: str, request: Any) -> Any:
        body = await request.read() or b""
        return await bundle.put_payload_async(key, body)

    put_object_endpoint.__annotations__ = {"key": str, "request": blacksheep_mod.Request, "return": Any}
    app.router.add_get("/json", handlers["json"])
    app.router.add_get("/db", handlers["db"])
    app.router.add_get("/queries", handlers["queries"])
    app.router.add_get("/updates", handlers["updates"])
    app.router.add_get("/cached-queries", handlers["cached"])
    app.router.add_get("/object/{key}", handlers["get_object"])
    app.router.add_put("/object/{key}", put_object_endpoint)
    app.router.add_get("/health", handlers["health"])
    return app


def _register_decorator_routes(app: Any, handlers: dict[str, Any], put_object_endpoint: Any) -> None:
    app.get("/json")(handlers["json"])
    app.get("/db")(handlers["db"])
    app.get("/queries")(handlers["queries"])
    app.get("/updates")(handlers["updates"])
    app.get("/cached-queries")(handlers["cached"])
    app.get("/object/{key}")(handlers["get_object"])
    app.put("/object/{key}")(put_object_endpoint)
    app.get("/health")(handlers["health"])


def _create_django_interface_app(backend: DataBackend | None, *, is_async: bool) -> Any:
    django_mod = _require_framework("django", "django", "bench-comparators")
    django_http = _require_framework("django.http", "django", "bench-comparators")
    django_urls = _require_framework("django.urls", "django", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    http_response = django_http.HttpResponse
    urlconf_name = _DJANGO_URLCONF_NAME if is_async else _DJANGO_WSGI_URLCONF_NAME

    if is_async:
        django_iface = _require_framework("django.core.asgi", "django", "bench-comparators")

        async def json_view(_request: Any) -> Any:
            return http_response(JSON_HELLO_PAYLOAD, status=200, content_type=_JSON_MEDIA_TYPE)

        async def db_view(_request: Any) -> Any:
            return http_response(
                encode_json_bytes(await resolved_backend.get_world()), status=200, content_type=_JSON_MEDIA_TYPE
            )

        async def queries_view(request: Any) -> Any:
            rows = await resolved_backend.get_worlds(clamp_query_count(request.GET.get("queries")))
            return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

        async def updates_view(request: Any) -> Any:
            rows = await resolved_backend.update_worlds(clamp_query_count(request.GET.get("queries")))
            return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

        async def cached_queries_view(request: Any) -> Any:
            rows = await resolved_backend.get_cached_worlds(clamp_query_count(request.GET.get("count")))
            return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

        async def object_view(request: Any, key: str) -> Any:
            if request.method == "PUT":
                size = await resolved_backend.put_object(key, bytes(request.body))
                return http_response(
                    encode_json_bytes({"key": key, "size": size}), status=200, content_type=_JSON_MEDIA_TYPE
                )
            if request.method == "GET":
                payload = await resolved_backend.get_object(key)
                if payload is None:
                    return http_response(b"Not Found", status=404, content_type=_TEXT_MEDIA_TYPE)
                return http_response(payload, status=200, content_type=_BINARY_MEDIA_TYPE)
            return http_response(b"Method Not Allowed", status=405, content_type=_TEXT_MEDIA_TYPE)

        async def health_view(_request: Any) -> Any:
            return http_response(b"ok", status=200, content_type=_TEXT_MEDIA_TYPE)

        urls = [
            django_urls.path("json", json_view),
            django_urls.path("db", db_view),
            django_urls.path("queries", queries_view),
            django_urls.path("updates", updates_view),
            django_urls.path("cached-queries", cached_queries_view),
            django_urls.path("object/<str:key>", object_view),
            django_urls.path("health", health_view),
        ]
    else:
        django_iface = _require_framework("django.core.wsgi", "django", "bench-comparators")

        def s_object_view(request: Any, key: str) -> Any:
            if request.method == "PUT":
                size = resolved_backend.put_object_sync(key, bytes(request.body))
                return http_response(
                    encode_json_bytes({"key": key, "size": size}), status=200, content_type=_JSON_MEDIA_TYPE
                )
            if request.method == "GET":
                payload = resolved_backend.get_object_sync(key)
                if payload is None:
                    return http_response(b"Not Found", status=404, content_type=_TEXT_MEDIA_TYPE)
                return http_response(payload, status=200, content_type=_BINARY_MEDIA_TYPE)
            return http_response(b"Method Not Allowed", status=405, content_type=_TEXT_MEDIA_TYPE)

        urls = [
            django_urls.path(
                "json", lambda _r: http_response(JSON_HELLO_PAYLOAD, status=200, content_type=_JSON_MEDIA_TYPE)
            ),
            django_urls.path(
                "db",
                lambda _r: http_response(
                    encode_json_bytes(resolved_backend.get_world_sync()), status=200, content_type=_JSON_MEDIA_TYPE
                ),
            ),
            django_urls.path(
                "queries",
                lambda r: http_response(
                    encode_json_bytes(resolved_backend.get_worlds_sync(clamp_query_count(r.GET.get("queries")))),
                    status=200,
                    content_type=_JSON_MEDIA_TYPE,
                ),
            ),
            django_urls.path(
                "updates",
                lambda r: http_response(
                    encode_json_bytes(resolved_backend.update_worlds_sync(clamp_query_count(r.GET.get("queries")))),
                    status=200,
                    content_type=_JSON_MEDIA_TYPE,
                ),
            ),
            django_urls.path(
                "cached-queries",
                lambda r: http_response(
                    encode_json_bytes(resolved_backend.get_cached_worlds_sync(clamp_query_count(r.GET.get("count")))),
                    status=200,
                    content_type=_JSON_MEDIA_TYPE,
                ),
            ),
            django_urls.path("object/<str:key>", s_object_view),
            django_urls.path("health", lambda _r: http_response(b"ok", status=200, content_type=_TEXT_MEDIA_TYPE)),
        ]

    class _DjangoURLConfModule(types.ModuleType):
        urlpatterns: list[Any]

    urlconf_module = _DjangoURLConfModule(urlconf_name)
    urlconf_module.urlpatterns = urls
    sys.modules[urlconf_name] = urlconf_module

    _ensure_django_settings(root_urlconf=urlconf_name)
    django_apps = _require_framework("django.apps", "django", "bench-comparators")
    if not django_apps.apps.ready:
        django_mod.setup()
    django_urls.clear_url_caches()
    return django_iface.get_asgi_application() if is_async else django_iface.get_wsgi_application()


def _require_framework(module_name: str, package_name: str, dependency_group: str) -> Any:
    module = import_optional(module_name)
    if module is None:
        msg = (
            f"Optional benchmark framework {package_name!r} ({module_name}) is not installed. "
            f"Install it with: uv sync --group {dependency_group}"
        )
        raise ImportError(msg)
    return module


def _resolve_backend(backend: DataBackend | None) -> DataBackend:
    return get_default_backend() if backend is None else backend


def _ensure_django_settings(
    *,
    installed_apps: list[str] | None = None,
    root_urlconf: str | None = None,
) -> Any:
    django_conf = _require_framework("django.conf", "django", "bench-comparators")
    settings = django_conf.settings
    if not settings.configured:
        settings.configure(
            DEBUG=False,
            SECRET_KEY=_DJANGO_BOLT_SETTINGS_KEY,
            ALLOWED_HOSTS=["*"],
            MIDDLEWARE=[],
            INSTALLED_APPS=installed_apps or [],
            ROOT_URLCONF=root_urlconf or _DJANGO_URLCONF_NAME,
            DATABASES={},
            DEFAULT_CHARSET="utf-8",
        )
    elif root_urlconf is not None:
        settings.ROOT_URLCONF = root_urlconf
    return settings


_LAZY_APP_FACTORIES: Final[dict[str, Any]] = {
    "litestar_app": create_litestar_app,
    "fastapi_app": create_fastapi_app,
    "django_app": create_django_app,
    "django_wsgi_app": create_django_wsgi_app,
    "flask_app": create_flask_app,
    "blacksheep_app": create_blacksheep_app,
}


def __getattr__(name: str) -> Any:
    """Lazily build framework ASGI/WSGI app instances on attribute access."""
    factory = _LAZY_APP_FACTORIES.get(name)
    if factory is not None:
        return factory()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
