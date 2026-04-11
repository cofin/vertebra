"""ASGI 3.0 HTTP scope builder."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

__all__ = ["AsgiScope", "build_scope"]

_UNBUILT = object()

# Pre-allocated constants shared across all scopes (immutable).
_ASGI_VERSION: dict[str, str] = {"version": "3.0", "spec_version": "2.3"}
_SCOPE_TYPE = "http"

# Template dict with all static keys pre-set.  dict.copy() is a single
# C-level memcpy of the hash table — much cheaper than building 14 keys
# from scratch per request.
_SCOPE_TEMPLATE: dict[str, Any] = {
    "type": _SCOPE_TYPE,
    "asgi": _ASGI_VERSION,
    "http_version": "1.1",
    "method": "",
    "headers": _UNBUILT,
    "path": "",
    "raw_path": b"",
    "query_string": b"",
    "root_path": "",
    "scheme": "http",
    "server": None,
    "client": None,
    "state": {},
    "extensions": {},
}

# Shared empty dicts for state/extensions — ASGI spec allows mutation so
# each scope needs its own instance, but we avoid the literal overhead by
# calling dict() which is marginally faster than {} in tight loops on
# CPython < 3.14.  On 3.14+ they're equivalent; either way this is a
# minor micro-opt vs the template copy being the big win.
_EMPTY_DICT = dict


def _build_headers(request: Any) -> list[tuple[bytes, bytes]]:
    """Materialise headers from VertebraHeaders proxy."""
    hdrs = request.headers
    if hasattr(hdrs, "asgi_items"):
        return hdrs.asgi_items()  # type: ignore[no-any-return]
    return [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in hdrs.items()]


class AsgiScope(dict):  # type: ignore[type-arg]
    """ASGI scope dict that defers header list construction."""

    __slots__ = ("_request",)

    def __init__(self, base: dict[str, Any], request: Any) -> None:
        super().__init__(base)
        self._request = request

    def __getitem__(self, key: str) -> Any:
        value = super().__getitem__(key)
        if key == "headers" and value is _UNBUILT:
            value = _build_headers(self._request)
            super().__setitem__("headers", value)
        return value

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


def build_scope(request: Any, *, root_path: str = "") -> Any:
    """Build an ASGI 3.0 HTTP connection scope from a VertebraRequest.

    Fast path returns a Rust-backed ``AsgiScope`` pyclass that lazily
    materializes each key on first access. Middleware that needs a real
    ``dict`` can call ``scope.copy()``.
    """
    # Fast path: Native request returns a lazy AsgiScope pyclass.
    if hasattr(request, "asgi_scope"):
        scope = request.asgi_scope()
        if root_path:
            scope["root_path"] = root_path
        return scope

    scope = _SCOPE_TEMPLATE.copy()

    scope["http_version"] = getattr(request, "http_version", "1.1")
    scope["method"] = request.method.upper()
    scope["path"] = request.path
    scope["raw_path"] = getattr(request, "raw_path", None) or request.path.encode("latin-1")
    scope["query_string"] = getattr(request, "raw_query", None) or (request.query or "").encode("latin-1")
    scope["root_path"] = root_path
    scope["scheme"] = getattr(request, "scheme", "http")
    # Each scope must own its own mutable state/extensions dicts.
    scope["state"] = _EMPTY_DICT()
    scope["extensions"] = _EMPTY_DICT()

    server_host = getattr(request, "server_host", None)
    if server_host is not None:
        scope["server"] = (server_host, getattr(request, "server_port", None))

    client_host = getattr(request, "client_host", None)
    if client_host is not None:
        scope["client"] = (client_host, getattr(request, "client_port", None))

    return AsgiScope(scope, request)
