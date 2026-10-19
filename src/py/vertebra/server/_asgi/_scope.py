"""ASGI 3.0 HTTP scope builder."""

from typing import Any, cast

__all__ = ("AsgiScope", "build_scope")

_UNBUILT = object()
_ASGI_VERSION: dict[str, str] = {"version": "3.0", "spec_version": "2.3"}
_SCOPE_TYPE = "http"

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

_EMPTY_DICT = dict


class AsgiScope(dict[str, Any]):
    """ASGI scope dict that defers header list construction until first access."""

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
        """Return the value for key if present, materializing lazy headers if needed."""
        try:
            return self[key]
        except KeyError:
            return default


def build_scope(request: Any, *, root_path: str = "") -> Any:
    """Build an ASGI 3.0 HTTP connection scope from a Request.

    The native fast path returns a Rust-backed ``AsgiScope`` pyclass that
    lazily materializes each key on first access.
    """
    if hasattr(request, "asgi_scope"):
        return request.asgi_scope(root_path) if root_path else request.asgi_scope()

    scope = _SCOPE_TEMPLATE.copy()
    scope["http_version"] = getattr(request, "http_version", "1.1")
    scope["method"] = request.method.upper()
    scope["path"] = request.path
    scope["raw_path"] = getattr(request, "raw_path", None) or request.path.encode("latin-1")
    scope["query_string"] = getattr(request, "raw_query", None) or (request.query or "").encode("latin-1")
    scope["root_path"] = root_path
    scope["scheme"] = getattr(request, "scheme", "http")
    scope["state"] = _EMPTY_DICT()
    scope["extensions"] = _EMPTY_DICT()

    server_host = getattr(request, "server_host", None)
    if server_host is not None:
        scope["server"] = (server_host, getattr(request, "server_port", None))

    client_host = getattr(request, "client_host", None)
    if client_host is not None:
        scope["client"] = (client_host, getattr(request, "client_port", None))

    return AsgiScope(scope, request)


def _build_headers(request: Any) -> list[tuple[bytes, bytes]]:
    """Materialize ASGI byte header pairs from a request headers mapping."""
    hdrs = request.headers
    if hasattr(hdrs, "asgi_items"):
        return cast("list[tuple[bytes, bytes]]", hdrs.asgi_items())
    return [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in hdrs.items()]
