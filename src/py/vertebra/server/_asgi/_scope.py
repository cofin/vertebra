"""ASGI 3.0 HTTP scope builder."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

__all__ = ["AsgiScope", "build_scope"]

_UNBUILT = object()

# Pre-allocated constants shared across all scopes (immutable).
_ASGI_VERSION: dict[str, str] = {"version": "3.0", "spec_version": "2.3"}
_SCOPE_TYPE = "http"


def _build_headers(request: Any) -> list[tuple[bytes, bytes]]:
    """Materialise headers from VertebraHeaders proxy."""
    # Use optimized asgi_items() if available (lazy proxy)
    if hasattr(request.headers, "asgi_items"):
        return request.headers.asgi_items()
    return [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in request.headers.items()]


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


def build_scope(request: Any, *, root_path: str = "") -> AsgiScope:
    """Build an ASGI 3.0 HTTP connection scope from a VertebraRequest."""
    # Use zero-copy raw bytes directly from Rust if available to avoid re-encoding
    raw_path: bytes = getattr(request, "raw_path", None)
    if raw_path is None:
        raw_path = request.path.encode("latin-1")

    query_string: bytes = getattr(request, "raw_query", None)
    if query_string is None:
        query_str = request.query or ""
        query_string = query_str.encode("latin-1")

    base: dict[str, Any] = {
        "type": _SCOPE_TYPE,
        "asgi": _ASGI_VERSION,
        "http_version": getattr(request, "http_version", "1.1"),
        "method": request.method.upper(),
        "headers": _UNBUILT,
        "path": request.path,
        "raw_path": raw_path,
        "query_string": query_string,
        "root_path": root_path,
        "scheme": getattr(request, "scheme", "http"),
        "server": None,
        "client": None,
        "state": {},
        "extensions": {},
    }

    # Only build tuples when attributes actually exist.
    server_host = getattr(request, "server_host", None)
    if server_host is not None:
        base["server"] = (server_host, getattr(request, "server_port", None))

    client_host = getattr(request, "client_host", None)
    if client_host is not None:
        base["client"] = (client_host, getattr(request, "client_port", None))

    return AsgiScope(base, request)
