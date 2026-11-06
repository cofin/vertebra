"""Shared route-compilation primitives, scalar converters, and FastPathProvider registry."""

from __future__ import annotations

import inspect
import sys
import uuid
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

import msgspec

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "BODYLESS_HTTP_METHODS",
    "UNSET_DEFAULT",
    "DirectPlan",
    "FastPathProvider",
    "QueryParamSpec",
    "coerce_path_param",
    "get_fastpath_providers",
    "parse_bool_query",
    "parse_lenient_bool_query",
    "register_fastpath_provider",
    "resolve_fastpath_provider",
    "resolve_scalar_query_converter",
    "resolve_wsgi_relative_path",
    "scalar_query_converter",
    "serialize_litestar_http_exception",
    "serialize_starlette_http_exception",
    "step_coro_sync",
    "unregister_fastpath_provider",
)

BODYLESS_HTTP_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "DELETE", "OPTIONS"})
UNSET_DEFAULT: object = object()


@runtime_checkable
class FastPathProvider(Protocol):
    """Structural protocol for per-framework FastPath discovery and lazy compilation."""

    name: str

    def detect(self, app: Any) -> bool:
        """Return ``True`` when ``app`` belongs to this framework provider."""
        ...

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Compile and return a framework ``FastPath`` instance for ``app``."""
        ...


class QueryParamSpec(msgspec.Struct, frozen=True, gc=False):
    """Compiled scalar query parameter extraction specification for ``DirectPlan``."""

    name: str
    """Query parameter name."""

    converter: Callable[[str], Any]
    """Scalar string-to-value converter callable."""

    default: Any
    """Default parameter value or ``UNSET_DEFAULT`` when required."""


class DirectPlan(msgspec.Struct, frozen=True, gc=False):
    """Zero-allocation compiled signature execution plan for pure route handlers."""

    fn: Callable[..., Any]
    """Underlying route handler callable."""

    is_async: bool
    """Whether ``fn`` is an asynchronous coroutine callable."""

    status_code: int
    """Default HTTP response status code."""

    media_type: str
    """Default response media type string."""

    path_param_names: tuple[str, ...]
    """Ordered path parameter names accepted by ``fn``."""

    query_params: tuple[QueryParamSpec, ...]
    """Compiled scalar query parameter specifications."""

    body_param: str | None
    """Optional request body parameter name."""

    body_type: Any
    """Target request body annotation or ``Any``."""

    json_headers: list[tuple[bytes, bytes]]
    """Pre-encoded headers for JSON responses."""

    text_headers: list[tuple[bytes, bytes]]
    """Pre-encoded headers for plain-text responses."""

    raw_bytes_headers: list[tuple[bytes, bytes]]
    """Pre-encoded headers for raw byte responses."""

    max_body_size: int | None = None
    """Optional per-route request payload byte ceiling."""


def parse_bool_query(raw: str) -> bool:
    """Parse a boolean query parameter strictly, raising ``ValueError`` on unrecognized tokens."""
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    msg = f"invalid boolean query parameter: {raw!r}"
    raise ValueError(msg)


def parse_lenient_bool_query(raw: str) -> bool:
    """Parse a truthy boolean query token leniently."""
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def resolve_scalar_query_converter(annotation: Any) -> Callable[[str], Any] | None:
    """Resolve a strict scalar converter for ``annotation``, or ``None`` for non-scalar types."""
    if annotation is inspect.Parameter.empty or annotation is Any or annotation is str:
        return str
    if annotation is int:
        return int
    if annotation is float:
        return float
    if annotation is bool:
        return parse_bool_query
    return None


def scalar_query_converter(annotation: Any) -> Callable[[str], Any]:
    """Resolve a scalar query converter defaulting unknown annotations to ``str``."""
    if annotation is int:
        return int
    if annotation is float:
        return float
    if annotation is bool:
        return parse_lenient_bool_query
    return str


def coerce_path_param(raw: str, converter: str) -> Any:
    """Coerce a raw URL path segment according to its named route converter."""
    if converter == "int":
        return int(raw)
    if converter == "float":
        return float(raw)
    if converter == "uuid":
        return uuid.UUID(raw)
    return raw


def resolve_wsgi_relative_path(environ: Any, mount_prefix: str = "") -> str:
    """Extract the WSGI ``PATH_INFO`` normalized against ``mount_prefix`` or ``SCRIPT_NAME``."""
    raw_path = str(environ.get("PATH_INFO", "/"))
    prefix = mount_prefix or str(environ.get("SCRIPT_NAME", "") or "")
    if prefix:
        norm = prefix.rstrip("/")
        if norm:
            if raw_path == norm:
                return "/"
            if raw_path.startswith(f"{norm}/"):
                return raw_path[len(norm) :]
    return raw_path or "/"


def step_coro_sync(coro: Any) -> tuple[bool, Any]:
    """Step ``coro`` once synchronously and return ``(True, value)`` if it finishes without suspending."""
    try:
        coro.send(None)
    except StopIteration as stop:
        return True, stop.value
    finally:
        close_fn = getattr(coro, "close", None)
        if callable(close_fn):
            close_fn()
    return False, None


def serialize_litestar_http_exception(exc: Exception) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    """Serialize a Litestar-style ``HTTPException`` into ``(status, headers, body)``."""
    status_code = getattr(exc, "status_code", None)
    if not isinstance(status_code, int) or isinstance(status_code, bool) or not (100 <= status_code <= 599):
        return None
    if not hasattr(exc, "detail"):
        return None

    detail = getattr(exc, "detail", "")
    payload: dict[str, Any] = {
        "status_code": status_code,
        "detail": detail if detail else str(exc),
    }
    extra = getattr(exc, "extra", None)
    if extra is not None:
        payload["extra"] = extra

    headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/json")]
    exc_headers = getattr(exc, "headers", None)
    if isinstance(exc_headers, dict):
        for hk, hv in exc_headers.items():
            headers.append((str(hk).lower().encode("latin-1"), str(hv).encode("latin-1")))
    elif isinstance(exc_headers, list | tuple):
        for pair in exc_headers:
            if isinstance(pair, list | tuple) and len(pair) == 2:
                k_raw, v_raw = pair
                kb = k_raw.lower() if isinstance(k_raw, bytes) else str(k_raw).lower().encode("latin-1")
                vb = v_raw if isinstance(v_raw, bytes) else str(v_raw).encode("latin-1")
                headers.append((kb, vb))

    return (status_code, headers, encode_json_bytes(payload))


def serialize_starlette_http_exception(exc: Exception) -> tuple[int, list[tuple[bytes, bytes]], bytes] | None:
    """Serialize a Starlette or FastAPI ``HTTPException`` into ``(status, headers, body)``."""
    status_code = getattr(exc, "status_code", None)
    detail = getattr(exc, "detail", None)
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        return None
    headers: list[tuple[bytes, bytes]] = []
    raw_hdrs = getattr(exc, "headers", None)
    if isinstance(raw_hdrs, dict):
        for k, v in raw_hdrs.items():
            headers.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
    if isinstance(detail, str):
        body = detail.encode("utf-8")
        headers.append((b"content-type", b"text/plain; charset=utf-8"))
    else:
        body = encode_json_bytes({"detail": detail})
        headers.append((b"content-type", b"application/json"))
    headers.append((b"content-length", str(len(body)).encode("ascii")))
    return status_code, headers, body


def register_fastpath_provider(provider: FastPathProvider, *, prepend: bool = False) -> None:
    """Register or replace a ``FastPathProvider`` in the server provider table."""
    unregister_fastpath_provider(provider.name)
    if prepend:
        _PROVIDERS.insert(0, provider)
    else:
        _PROVIDERS.append(provider)


def unregister_fastpath_provider(name: str) -> bool:
    """Remove any registered ``FastPathProvider`` with ``provider.name == name``."""
    removed = False
    for idx in range(len(_PROVIDERS) - 1, -1, -1):
        if _PROVIDERS[idx].name == name:
            _PROVIDERS.pop(idx)
            removed = True
    return removed


def get_fastpath_providers() -> tuple[FastPathProvider, ...]:
    """Return an immutable snapshot of registered ``FastPathProvider`` entries."""
    return tuple(_PROVIDERS)


def resolve_fastpath_provider(app: Any) -> FastPathProvider | None:
    """Return the first registered ``FastPathProvider`` matching ``app``, or ``None``."""
    if app is None:
        return None
    for provider in _PROVIDERS:
        if provider.detect(app):
            return provider
    return None


def _is_loaded_instance(app: Any, module_name: str, attr_name: str) -> bool:
    """Check ``isinstance(app, cls)`` only when ``module_name`` is already imported in ``sys.modules``."""
    mod = sys.modules.get(module_name)
    if mod is None:
        return False
    cls = getattr(mod, attr_name, None)
    return isinstance(cls, type) and isinstance(app, cls)


class _LitestarFastPathProvider:
    """Lazy FastPath provider for Litestar v2/v3 applications."""

    name = "litestar"

    def detect(self, app: Any) -> bool:
        """Detect Litestar applications via loaded class check or structural Litestar attributes."""
        if _is_loaded_instance(app, "litestar", "Litestar"):
            return True
        return hasattr(app, "routes") and (hasattr(app, "compression_config") or hasattr(app, "response_cache_config"))

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Build ``vertebra.extensions.litestar.fastpath.FastPath`` lazily."""
        mod = import_optional("vertebra.extensions.litestar.fastpath")
        if mod is None:
            return None
        return mod.FastPath.from_app(app, mount_prefix=mount_prefix)


class _FastAPIFastPathProvider:
    """Lazy FastPath provider for FastAPI applications."""

    name = "fastapi"

    def detect(self, app: Any) -> bool:
        """Detect FastAPI applications before Starlette via ``dependency_overrides``."""
        if _is_loaded_instance(app, "fastapi", "FastAPI"):
            return True
        return hasattr(app, "routes") and hasattr(app, "dependency_overrides")

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Build ``vertebra.extensions.fastapi.fastpath.FastPath`` lazily."""
        mod = import_optional("vertebra.extensions.fastapi.fastpath")
        if mod is None:
            return None
        return mod.FastPath.from_app(app, mount_prefix=mount_prefix)


class _StarletteFastPathProvider:
    """Lazy FastPath provider for Starlette applications."""

    name = "starlette"

    def detect(self, app: Any) -> bool:
        """Detect Starlette applications that are not FastAPI subclasses."""
        if hasattr(app, "dependency_overrides"):
            return False
        if _is_loaded_instance(app, "starlette.applications", "Starlette"):
            return True
        return hasattr(app, "routes") and hasattr(app, "user_middleware")

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Build ``vertebra.extensions.starlette.fastpath.FastPath`` lazily."""
        mod = import_optional("vertebra.extensions.starlette.fastpath")
        if mod is None:
            return None
        return mod.FastPath.from_app(app, mount_prefix=mount_prefix)


class _FlaskFastPathProvider:
    """Lazy FastPath provider for Flask applications."""

    name = "flask"

    def detect(self, app: Any) -> bool:
        """Detect Flask applications via loaded class check or ``url_map`` + ``view_functions``."""
        if _is_loaded_instance(app, "flask", "Flask"):
            return True
        return hasattr(app, "url_map") and hasattr(app, "view_functions")

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Build ``vertebra.extensions.flask.fastpath.FastPath`` lazily."""
        mod = import_optional("vertebra.extensions.flask.fastpath")
        if mod is None:
            return None
        return mod.FastPath.from_app(app, mount_prefix=mount_prefix)


class _DjangoFastPathProvider:
    """Lazy FastPath provider for Django WSGI/ASGI handlers and URL configurations."""

    name = "django"

    def detect(self, app: Any) -> bool:
        """Detect Django handlers or URL configuration namespaces."""
        if _is_loaded_instance(app, "django.core.handlers.base", "BaseHandler"):
            return True
        return hasattr(app, "urlpatterns") or hasattr(app, "urlconf")

    def build(self, app: Any, *, mount_prefix: str = "") -> Any:
        """Build ``vertebra.extensions.django.fastpath.FastPath`` lazily."""
        mod = import_optional("vertebra.extensions.django.fastpath")
        if mod is None:
            return None
        return mod.FastPath.from_app(app, mount_prefix=mount_prefix)


_PROVIDERS: list[FastPathProvider] = [
    _LitestarFastPathProvider(),
    _FastAPIFastPathProvider(),
    _StarletteFastPathProvider(),
    _FlaskFastPathProvider(),
    _DjangoFastPathProvider(),
]
