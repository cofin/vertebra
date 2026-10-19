"""Request and Response type stubs.

These are placeholder dataclasses. Full implementations with header
parsing, body streaming, and WSGI/ASGI bridging live in later chapters.
"""

from dataclasses import dataclass, field

__all__ = ("Headers", "Request", "Response")


@dataclass
class Headers:
    """Immutable view of HTTP headers."""

    _data: list[tuple[str, str]] = field(default_factory=list)

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return first header value for name (case-insensitive)."""
        name_lower = name.lower()
        for k, v in self._data:
            if k.lower() == name_lower:
                return v
        return default

    def items(self) -> list[tuple[str, str]]:
        """Return all header pairs."""
        return list(self._data)


@dataclass
class Request:
    """Minimal HTTP request stub.

    Populated by the native batch runner in later chapters.
    """

    method: str = "GET"
    path: str = "/"
    query_string: bytes = b""
    headers: Headers = field(default_factory=Headers)
    body: bytes = b""
    handler_id: int = 0


@dataclass
class Response:
    """Minimal HTTP response stub."""

    status: int = 200
    headers: Headers = field(default_factory=Headers)
    body: bytes = b""
