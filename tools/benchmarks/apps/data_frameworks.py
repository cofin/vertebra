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
)

_JSON_MEDIA_TYPE: Final[str] = "application/json"
_TEXT_MEDIA_TYPE: Final[str] = "text/plain"
_BINARY_MEDIA_TYPE: Final[str] = "application/octet-stream"
_DJANGO_URLCONF_NAME: Final[str] = "_vertebra_bench_django_urls"
_DJANGO_WSGI_URLCONF_NAME: Final[str] = "_vertebra_bench_django_wsgi_urls"
_DJANGO_BOLT_SETTINGS_KEY: Final[str] = "test-token-placeholder"


def _require_framework(module_name: str, package_name: str, dependency_group: str) -> Any:
    """Import an optional framework module or raise an actionable ``ImportError``.

    Args:
        module_name: Python import path to load via ``import_optional``.
        package_name: Distribution or package name to surface in the error.
        dependency_group: ``uv`` dependency group that provides the package.

    Returns:
        Imported module object.

    Raises:
        ImportError: When the optional framework package is not installed.
    """
    module = import_optional(module_name)
    if module is None:
        msg = (
            f"Optional benchmark framework {package_name!r} ({module_name}) is not installed. "
            f"Install it with: uv sync --group {dependency_group}"
        )
        raise ImportError(msg)
    return module


def _resolve_backend(backend: DataBackend | None) -> DataBackend:
    """Return the provided ``DataBackend`` or the process-local default."""
    return get_default_backend() if backend is None else backend


def _ensure_django_settings(
    *,
    installed_apps: list[str] | None = None,
    root_urlconf: str | None = None,
) -> Any:
    """Configure minimal in-memory Django settings if not already configured.

    Args:
        installed_apps: Optional list of Django ``INSTALLED_APPS`` entries.
        root_urlconf: Optional ``ROOT_URLCONF`` module path.

    Returns:
        Django's ``settings`` object.
    """
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


def build_django_bolt_command(host: str, port: int, workers: int) -> list[str]:
    """Build the ``django-bolt`` ``runbolt`` command invocation.

    Args:
        host: Bind address for the server.
        port: Bind TCP port for the server.
        workers: Number of worker processes to spawn.

    Returns:
        Command argv list invoking Django's ``runbolt`` management command with
        ``--host``, ``--port``, and ``--processes`` flags.
    """
    resolved_workers = max(1, workers)
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
        str(resolved_workers),
    ]


def create_django_bolt_app(backend: DataBackend | None = None) -> Any:
    """Create a ``django-bolt`` ``BoltAPI`` app backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``django_bolt.BoltAPI`` instance.

    Raises:
        ImportError: If ``django-bolt`` or ``django`` is not installed.
    """
    django_bolt = _require_framework("django_bolt", "django-bolt", "bench-comparators")
    _ensure_django_settings(installed_apps=["django_bolt"])
    resolved_backend = _resolve_backend(backend)
    api = django_bolt.BoltAPI()
    response_cls = getattr(django_bolt, "Response", None)

    def _make_response(content: bytes, status_code: int, media_type: str) -> Any:
        if response_cls is not None:
            return response_cls(content, status_code=status_code, media_type=media_type)
        return content

    async def json_endpoint() -> Any:
        """Return the TechEmpower JSON hello payload."""
        return _make_response(JSON_HELLO_PAYLOAD, 200, _JSON_MEDIA_TYPE)

    async def db_endpoint() -> Any:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return _make_response(encode_json_bytes(row), 200, _JSON_MEDIA_TYPE)

    async def queries_endpoint(queries: str | None = None) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.get_worlds(count)
        return _make_response(encode_json_bytes(rows), 200, _JSON_MEDIA_TYPE)

    async def updates_endpoint(queries: str | None = None) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.update_worlds(count)
        return _make_response(encode_json_bytes(rows), 200, _JSON_MEDIA_TYPE)

    async def cached_queries_endpoint(count: str | None = None) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(count)
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return _make_response(encode_json_bytes(rows), 200, _JSON_MEDIA_TYPE)

    async def get_object_endpoint(key: str) -> Any:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = await resolved_backend.get_object(key)
        if payload is None:
            return _make_response(b"Not Found", 404, _TEXT_MEDIA_TYPE)
        return _make_response(payload, 200, _BINARY_MEDIA_TYPE)

    async def put_object_endpoint(key: str, request: Any) -> Any:
        """Store an object payload by key and return its persisted byte length."""
        raw_body: Any = getattr(request, "body", b"")
        if callable(raw_body):
            raw_body = raw_body()
        if inspect.isawaitable(raw_body):
            raw_body = await raw_body
        data = raw_body if isinstance(raw_body, bytes) else bytes(raw_body)
        size = await resolved_backend.put_object(key, data)
        return _make_response(encode_json_bytes({"key": key, "size": size}), 200, _JSON_MEDIA_TYPE)

    async def health_endpoint() -> Any:
        """Return a lightweight readiness payload."""
        return _make_response(b"ok", 200, _TEXT_MEDIA_TYPE)

    api.get("/json")(json_endpoint)
    api.get("/db")(db_endpoint)
    api.get("/queries")(queries_endpoint)
    api.get("/updates")(updates_endpoint)
    api.get("/cached-queries")(cached_queries_endpoint)
    api.get("/object/{key}")(get_object_endpoint)
    api.put("/object/{key}")(put_object_endpoint)
    api.get("/health")(health_endpoint)
    return api


def create_litestar_app(backend: DataBackend | None = None) -> Any:
    """Create a Litestar ASGI application backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``litestar.Litestar`` ASGI application.

    Raises:
        ImportError: If ``litestar`` is not installed.
    """
    litestar_mod = _require_framework("litestar", "litestar", "bench-comparators")
    litestar_params = import_optional("litestar.params")
    resolved_backend = _resolve_backend(backend)
    litestar_cls = litestar_mod.Litestar
    response_cls = litestar_mod.Response
    request_cls = litestar_mod.Request
    get_decorator = litestar_mod.get
    put_decorator = litestar_mod.put
    parameter_cls: Any = getattr(litestar_params, "QueryParameter", None) if litestar_params is not None else None
    path_parameter_cls: Any = getattr(litestar_params, "PathParameter", None) if litestar_params is not None else None

    async def json_endpoint() -> Any:
        """Return the TechEmpower JSON hello payload."""
        return response_cls(content=JSON_HELLO_PAYLOAD, status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def db_endpoint() -> Any:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return response_cls(content=encode_json_bytes(row), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def queries_endpoint(queries: str | None = None) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.get_worlds(count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def updates_endpoint(queries: str | None = None) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.update_worlds(count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def cached_queries_endpoint(count: str | None = None) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(count)
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def get_object_endpoint(key: str) -> Any:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = await resolved_backend.get_object(key)
        if payload is None:
            return response_cls(content=b"Not Found", status_code=404, media_type=_TEXT_MEDIA_TYPE)
        return response_cls(content=payload, status_code=200, media_type=_BINARY_MEDIA_TYPE)

    async def put_object_endpoint(key: str, request: Any) -> Any:
        """Store an object payload by key and return its persisted byte length."""
        body = await request.body()
        size = await resolved_backend.put_object(key, body)
        return response_cls(
            content=encode_json_bytes({"key": key, "size": size}),
            status_code=200,
            media_type=_JSON_MEDIA_TYPE,
        )

    async def health_endpoint() -> Any:
        """Return a lightweight readiness payload."""
        return response_cls(content=b"ok", status_code=200, media_type=_TEXT_MEDIA_TYPE)

    if parameter_cls is not None:
        queries_endpoint.__annotations__ = {
            "queries": Annotated[str | None, parameter_cls(name="queries", required=False)],
            "return": Any,
        }
        updates_endpoint.__annotations__ = {
            "queries": Annotated[str | None, parameter_cls(name="queries", required=False)],
            "return": Any,
        }
        cached_queries_endpoint.__annotations__ = {
            "count": Annotated[str | None, parameter_cls(name="count", required=False)],
            "return": Any,
        }
    key_annotation: Any = Annotated[str, path_parameter_cls()] if path_parameter_cls is not None else str
    get_object_endpoint.__annotations__ = {"key": key_annotation, "return": Any}
    put_object_endpoint.__annotations__ = {"key": key_annotation, "request": request_cls, "return": Any}

    return litestar_cls(
        route_handlers=[
            get_decorator("/json")(json_endpoint),
            get_decorator("/db")(db_endpoint),
            get_decorator("/queries")(queries_endpoint),
            get_decorator("/updates")(updates_endpoint),
            get_decorator("/cached-queries")(cached_queries_endpoint),
            get_decorator("/object/{key:str}")(get_object_endpoint),
            put_decorator("/object/{key:str}")(put_object_endpoint),
            get_decorator("/health")(health_endpoint),
        ],
        openapi_config=None,
    )


def create_fastapi_app(backend: DataBackend | None = None) -> Any:
    """Create a FastAPI ASGI application backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``fastapi.FastAPI`` ASGI application.

    Raises:
        ImportError: If ``fastapi`` is not installed.
    """
    fastapi_mod = _require_framework("fastapi", "fastapi", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    app = fastapi_mod.FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    response_cls = fastapi_mod.Response
    request_cls = fastapi_mod.Request

    async def json_endpoint() -> Any:
        """Return the TechEmpower JSON hello payload."""
        return response_cls(content=JSON_HELLO_PAYLOAD, status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def db_endpoint() -> Any:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return response_cls(content=encode_json_bytes(row), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def queries_endpoint(queries: str | None = None) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.get_worlds(count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def updates_endpoint(queries: str | None = None) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.update_worlds(count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def cached_queries_endpoint(count: str | None = None) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(count)
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return response_cls(content=encode_json_bytes(rows), status_code=200, media_type=_JSON_MEDIA_TYPE)

    async def get_object_endpoint(key: str) -> Any:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = await resolved_backend.get_object(key)
        if payload is None:
            return response_cls(content=b"Not Found", status_code=404, media_type=_TEXT_MEDIA_TYPE)
        return response_cls(content=payload, status_code=200, media_type=_BINARY_MEDIA_TYPE)

    async def put_object_endpoint(key: str, request: Any) -> Any:
        """Store an object payload by key and return its persisted byte length."""
        body = await request.body()
        size = await resolved_backend.put_object(key, body)
        return response_cls(
            content=encode_json_bytes({"key": key, "size": size}),
            status_code=200,
            media_type=_JSON_MEDIA_TYPE,
        )

    async def health_endpoint() -> Any:
        """Return a lightweight readiness payload."""
        return response_cls(content=b"ok", status_code=200, media_type=_TEXT_MEDIA_TYPE)

    put_object_endpoint.__annotations__ = {"key": str, "request": request_cls, "return": Any}
    app.get("/json")(json_endpoint)
    app.get("/db")(db_endpoint)
    app.get("/queries")(queries_endpoint)
    app.get("/updates")(updates_endpoint)
    app.get("/cached-queries")(cached_queries_endpoint)
    app.get("/object/{key}")(get_object_endpoint)
    app.put("/object/{key}")(put_object_endpoint)
    app.get("/health")(health_endpoint)
    return app


def create_django_app(backend: DataBackend | None = None) -> Any:
    """Create a standard Django ASGI application backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Django ASGI application callable with identical benchmark routes.

    Raises:
        ImportError: If ``django`` is not installed.
    """
    django_mod = _require_framework("django", "django", "bench-comparators")
    django_http = _require_framework("django.http", "django", "bench-comparators")
    django_urls = _require_framework("django.urls", "django", "bench-comparators")
    django_asgi = _require_framework("django.core.asgi", "django", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    http_response = django_http.HttpResponse

    async def json_view(_request: Any) -> Any:
        """Return the TechEmpower JSON hello payload."""
        return http_response(JSON_HELLO_PAYLOAD, status=200, content_type=_JSON_MEDIA_TYPE)

    async def db_view(_request: Any) -> Any:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return http_response(encode_json_bytes(row), status=200, content_type=_JSON_MEDIA_TYPE)

    async def queries_view(request: Any) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(request.GET.get("queries"))
        rows = await resolved_backend.get_worlds(count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    async def updates_view(request: Any) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(request.GET.get("queries"))
        rows = await resolved_backend.update_worlds(count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    async def cached_queries_view(request: Any) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(request.GET.get("count"))
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    async def object_view(request: Any, key: str) -> Any:
        """Handle ``GET`` and ``PUT`` object-storage operations for ``key``."""
        if request.method == "PUT":
            size = await resolved_backend.put_object(key, bytes(request.body))
            return http_response(
                encode_json_bytes({"key": key, "size": size}),
                status=200,
                content_type=_JSON_MEDIA_TYPE,
            )
        if request.method == "GET":
            payload = await resolved_backend.get_object(key)
            if payload is None:
                return http_response(b"Not Found", status=404, content_type=_TEXT_MEDIA_TYPE)
            return http_response(payload, status=200, content_type=_BINARY_MEDIA_TYPE)
        return http_response(b"Method Not Allowed", status=405, content_type=_TEXT_MEDIA_TYPE)

    async def health_view(_request: Any) -> Any:
        """Return a lightweight readiness payload."""
        return http_response(b"ok", status=200, content_type=_TEXT_MEDIA_TYPE)

    class _DjangoURLConfModule(types.ModuleType):
        """Dynamic module carrying Django ``urlpatterns`` for benchmark routing."""

        urlpatterns: list[Any]

    urlconf_module = _DjangoURLConfModule(_DJANGO_URLCONF_NAME)
    urlconf_module.urlpatterns = [
        django_urls.path("json", json_view),
        django_urls.path("db", db_view),
        django_urls.path("queries", queries_view),
        django_urls.path("updates", updates_view),
        django_urls.path("cached-queries", cached_queries_view),
        django_urls.path("object/<str:key>", object_view),
        django_urls.path("health", health_view),
    ]
    sys.modules[_DJANGO_URLCONF_NAME] = urlconf_module

    _ensure_django_settings(root_urlconf=_DJANGO_URLCONF_NAME)
    django_apps = _require_framework("django.apps", "django", "bench-comparators")
    if not django_apps.apps.ready:
        django_mod.setup()
    django_urls.clear_url_caches()
    return django_asgi.get_asgi_application()


def create_django_wsgi_app(backend: DataBackend | None = None) -> Any:
    """Create a standard sync Django WSGI application backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Django WSGI application callable with identical benchmark routes.

    Raises:
        ImportError: If ``django`` is not installed.
    """
    django_mod = _require_framework("django", "django", "bench-comparators")
    django_http = _require_framework("django.http", "django", "bench-comparators")
    django_urls = _require_framework("django.urls", "django", "bench-comparators")
    django_wsgi = _require_framework("django.core.wsgi", "django", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    http_response = django_http.HttpResponse

    def json_view(_request: Any) -> Any:
        """Return the TechEmpower JSON hello payload."""
        return http_response(JSON_HELLO_PAYLOAD, status=200, content_type=_JSON_MEDIA_TYPE)

    def db_view(_request: Any) -> Any:
        """Return a single randomly selected World row as JSON."""
        row = resolved_backend.get_world_sync()
        return http_response(encode_json_bytes(row), status=200, content_type=_JSON_MEDIA_TYPE)

    def queries_view(request: Any) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(request.GET.get("queries"))
        rows = resolved_backend.get_worlds_sync(count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    def updates_view(request: Any) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(request.GET.get("queries"))
        rows = resolved_backend.update_worlds_sync(count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    def cached_queries_view(request: Any) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(request.GET.get("count"))
        rows = resolved_backend.get_cached_worlds_sync(resolved_count)
        return http_response(encode_json_bytes(rows), status=200, content_type=_JSON_MEDIA_TYPE)

    def object_view(request: Any, key: str) -> Any:
        """Handle ``GET`` and ``PUT`` object-storage operations for ``key``."""
        if request.method == "PUT":
            size = resolved_backend.put_object_sync(key, bytes(request.body))
            return http_response(
                encode_json_bytes({"key": key, "size": size}),
                status=200,
                content_type=_JSON_MEDIA_TYPE,
            )
        if request.method == "GET":
            payload = resolved_backend.get_object_sync(key)
            if payload is None:
                return http_response(b"Not Found", status=404, content_type=_TEXT_MEDIA_TYPE)
            return http_response(payload, status=200, content_type=_BINARY_MEDIA_TYPE)
        return http_response(b"Method Not Allowed", status=405, content_type=_TEXT_MEDIA_TYPE)

    def health_view(_request: Any) -> Any:
        """Return a lightweight readiness payload."""
        return http_response(b"ok", status=200, content_type=_TEXT_MEDIA_TYPE)

    class _DjangoWsgiURLConfModule(types.ModuleType):
        """Dynamic module carrying sync Django WSGI ``urlpatterns``."""

        urlpatterns: list[Any]

    urlconf_module = _DjangoWsgiURLConfModule(_DJANGO_WSGI_URLCONF_NAME)
    urlconf_module.urlpatterns = [
        django_urls.path("json", json_view),
        django_urls.path("db", db_view),
        django_urls.path("queries", queries_view),
        django_urls.path("updates", updates_view),
        django_urls.path("cached-queries", cached_queries_view),
        django_urls.path("object/<str:key>", object_view),
        django_urls.path("health", health_view),
    ]
    sys.modules[_DJANGO_WSGI_URLCONF_NAME] = urlconf_module

    _ensure_django_settings(root_urlconf=_DJANGO_WSGI_URLCONF_NAME)
    django_apps = _require_framework("django.apps", "django", "bench-comparators")
    if not django_apps.apps.ready:
        django_mod.setup()
    django_urls.clear_url_caches()
    return django_wsgi.get_wsgi_application()


def create_flask_app(backend: DataBackend | None = None) -> Any:
    """Create a Flask WSGI application backed by ``DataBackend`` sync methods.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``flask.Flask`` WSGI application.

    Raises:
        ImportError: If ``flask`` is not installed.
    """
    flask_mod = _require_framework("flask", "flask", "bench-comparators")
    resolved_backend = _resolve_backend(backend)
    app = flask_mod.Flask("vertebra_bench_flask")
    response_cls = flask_mod.Response
    flask_request = flask_mod.request

    def json_endpoint() -> Any:
        """Return the TechEmpower JSON hello payload."""
        return response_cls(JSON_HELLO_PAYLOAD, status=200, mimetype=_JSON_MEDIA_TYPE)

    def db_endpoint() -> Any:
        """Return a single randomly selected World row as JSON."""
        row = resolved_backend.get_world_sync()
        return response_cls(encode_json_bytes(row), status=200, mimetype=_JSON_MEDIA_TYPE)

    def queries_endpoint() -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(flask_request.args.get("queries"))
        rows = resolved_backend.get_worlds_sync(count)
        return response_cls(encode_json_bytes(rows), status=200, mimetype=_JSON_MEDIA_TYPE)

    def updates_endpoint() -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(flask_request.args.get("queries"))
        rows = resolved_backend.update_worlds_sync(count)
        return response_cls(encode_json_bytes(rows), status=200, mimetype=_JSON_MEDIA_TYPE)

    def cached_queries_endpoint() -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(flask_request.args.get("count"))
        rows = resolved_backend.get_cached_worlds_sync(resolved_count)
        return response_cls(encode_json_bytes(rows), status=200, mimetype=_JSON_MEDIA_TYPE)

    def get_object_endpoint(key: str) -> Any:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = resolved_backend.get_object_sync(key)
        if payload is None:
            return response_cls(b"Not Found", status=404, mimetype=_TEXT_MEDIA_TYPE)
        return response_cls(payload, status=200, mimetype=_BINARY_MEDIA_TYPE)

    def put_object_endpoint(key: str) -> Any:
        """Store an object payload by key and return its persisted byte length."""
        size = resolved_backend.put_object_sync(key, flask_request.get_data())
        return response_cls(
            encode_json_bytes({"key": key, "size": size}),
            status=200,
            mimetype=_JSON_MEDIA_TYPE,
        )

    def health_endpoint() -> Any:
        """Return a lightweight readiness payload."""
        return response_cls(b"ok", status=200, mimetype=_TEXT_MEDIA_TYPE)

    app.get("/json")(json_endpoint)
    app.get("/db")(db_endpoint)
    app.get("/queries")(queries_endpoint)
    app.get("/updates")(updates_endpoint)
    app.get("/cached-queries")(cached_queries_endpoint)
    app.get("/object/<key>")(get_object_endpoint)
    app.put("/object/<key>")(put_object_endpoint)
    app.get("/health")(health_endpoint)
    return app


def create_spikard_data_app(backend: DataBackend | None = None) -> Any:
    """Create a Spikard application backed by ``DataBackend``.

    Args:
        backend: Optional ``DataBackend`` instance. Uses the process-local
            default backend when omitted.

    Returns:
        Configured ``spikard.App`` instance with standardized data routes.

    Raises:
        ImportError: If ``spikard`` is not installed.
    """
    spikard_mod = _require_framework("spikard", "spikard", "bench")
    resolved_backend = _resolve_backend(backend)
    app_cls = spikard_mod.App
    response_cls = spikard_mod.Response
    server_config_cls = spikard_mod.ServerConfig

    app = app_cls(
        config=server_config_cls(
            host=os.environ.get("SPIKARD_HOST", "127.0.0.1"),
            port=int(os.environ.get("SPIKARD_PORT", "8005")),
            compression=None,
            enable_request_id=False,
            rate_limit=None,
        )
    )

    async def json_endpoint() -> Any:
        """Return the TechEmpower JSON hello payload."""
        return response_cls(
            content=JSON_HELLO_PAYLOAD.decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def db_endpoint() -> Any:
        """Return a single randomly selected World row as JSON."""
        row = await resolved_backend.get_world()
        return response_cls(
            content=encode_json_bytes(row).decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def queries_endpoint(queries: str | None = None) -> Any:
        """Return ``queries`` randomly selected World rows as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.get_worlds(count)
        return response_cls(
            content=encode_json_bytes(rows).decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def updates_endpoint(queries: str | None = None) -> Any:
        """Read and update ``queries`` random World rows and return them as JSON."""
        count = clamp_query_count(queries)
        rows = await resolved_backend.update_worlds(count)
        return response_cls(
            content=encode_json_bytes(rows).decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def cached_queries_endpoint(count: str | None = None) -> Any:
        """Return ``count`` cached World rows as JSON."""
        resolved_count = clamp_query_count(count)
        rows = await resolved_backend.get_cached_worlds(resolved_count)
        return response_cls(
            content=encode_json_bytes(rows).decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def get_object_endpoint(key: str) -> Any:
        """Fetch an object payload by key or return HTTP 404 when absent."""
        payload = await resolved_backend.get_object(key)
        if payload is None:
            return response_cls(
                content="Not Found",
                status_code=404,
                headers={"content-type": _TEXT_MEDIA_TYPE},
            )
        return response_cls(
            content=payload.decode("latin1"),
            status_code=200,
            headers={"content-type": _BINARY_MEDIA_TYPE},
        )

    async def put_object_endpoint(key: str, body: bytes = b"") -> Any:
        """Store an object payload by key and return its persisted byte length."""
        data = body if isinstance(body, bytes) else str(body).encode("latin1")
        size = await resolved_backend.put_object(key, data)
        return response_cls(
            content=encode_json_bytes({"key": key, "size": size}).decode("utf-8"),
            status_code=200,
            headers={"content-type": _JSON_MEDIA_TYPE},
        )

    async def health_endpoint() -> Any:
        """Return a lightweight readiness payload."""
        return response_cls(
            content="ok",
            status_code=200,
            headers={"content-type": _TEXT_MEDIA_TYPE},
        )

    app.get("/json")(json_endpoint)
    app.get("/db")(db_endpoint)
    app.get("/queries")(queries_endpoint)
    app.get("/updates")(updates_endpoint)
    app.get("/cached-queries")(cached_queries_endpoint)
    app.get("/object/{key}")(get_object_endpoint)
    app.put("/object/{key}")(put_object_endpoint)
    app.get("/health")(health_endpoint)
    return app


_LAZY_APP_FACTORIES: Final[dict[str, Any]] = {
    "litestar_app": create_litestar_app,
    "fastapi_app": create_fastapi_app,
    "django_app": create_django_app,
    "django_wsgi_app": create_django_wsgi_app,
    "flask_app": create_flask_app,
}


def __getattr__(name: str) -> Any:
    """Lazily build framework ASGI/WSGI app instances on attribute access."""
    factory = _LAZY_APP_FACTORIES.get(name)
    if factory is not None:
        return factory()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
