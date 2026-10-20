"""Lazy WSGI environ mapping over Request."""

import sys
from collections.abc import Iterator, Mapping
from typing import Any
from urllib.parse import quote

__all__ = ("BodyStream", "WsgiEnviron")

_MISSING: object = object()

_REQUIRED_KEYS: tuple[str, ...] = (
    "REQUEST_METHOD",
    "SCRIPT_NAME",
    "PATH_INFO",
    "QUERY_STRING",
    "SERVER_NAME",
    "SERVER_PORT",
    "SERVER_PROTOCOL",
    "wsgi.version",
    "wsgi.url_scheme",
    "wsgi.input",
    "wsgi.errors",
    "wsgi.multithread",
    "wsgi.multiprocess",
    "wsgi.run_once",
)

_HTTP_PREFIX = "HTTP_"
_WSGI_VERSION = (1, 0)
_STDERR = sys.stderr


class BodyStream:
    """Read-only file-like wrapper over body bytes for wsgi.input."""

    __slots__ = ("_pos", "_view")

    def __init__(self, body: bytes) -> None:
        self._view: memoryview = memoryview(body)
        self._pos: int = 0

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes from the request body."""
        if size == -1:
            chunk = bytes(self._view[self._pos :])
            self._pos = len(self._view)
        else:
            chunk = bytes(self._view[self._pos : self._pos + size])
            self._pos += len(chunk)
        return chunk

    def readline(self, hint: int = -1) -> bytes:
        """Read a single newline-delimited line from the request body."""
        start = self._pos
        end = len(self._view)
        if hint != -1:
            end = min(end, self._pos + hint)
        for i in range(start, end):
            if self._view[i] == ord(b"\n"):
                end = i + 1
                break
        chunk = bytes(self._view[start:end])
        self._pos = end
        return chunk

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read all remaining lines from the request body."""
        lines: list[bytes] = []
        total = 0
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            total += len(line)
            if hint != -1 and total >= hint:
                break
        return lines

    def __iter__(self) -> Iterator[bytes]:
        while True:
            line = self.readline()
            if not line:
                break
            yield line


class WsgiEnviron(Mapping[str, Any]):
    """Lazy read-only WSGI environ mapping backed by a Request."""

    __slots__ = ("_cache", "_req")

    def __init__(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> None:
        self._req = request
        self._cache: dict[str, Any] = {
            "REQUEST_METHOD": request.method.upper(),
            "SCRIPT_NAME": "",
            "PATH_INFO": quote(request.path, safe="/:@!$&'()*+,;="),
            "QUERY_STRING": request.query or "",
            "SERVER_NAME": server_name,
            "SERVER_PORT": str(server_port),
            "SERVER_PROTOCOL": getattr(request, "http_version", None) or "HTTP/1.1",
            "wsgi.version": _WSGI_VERSION,
            "wsgi.url_scheme": url_scheme,
            "wsgi.errors": _STDERR,
            "wsgi.multithread": True,
            "wsgi.multiprocess": True,
            "wsgi.run_once": False,
        }

    def __getitem__(self, key: str) -> Any:
        cache = self._cache
        cached = cache.get(key, _MISSING)
        if cached is not _MISSING:
            return cached
        value = self._compute(key)
        if value is _MISSING:
            raise KeyError(key)
        cache[key] = value
        return value

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        try:
            self[key]
        except KeyError:
            return False
        return True

    def __iter__(self) -> Iterator[str]:
        yield from _REQUIRED_KEYS
        req = self._req
        if req.headers.get("content-type") is not None:
            yield "CONTENT_TYPE"
        if req.headers.get("content-length") is not None:
            yield "CONTENT_LENGTH"
        for name in req.headers:
            upper = name.upper().replace("-", "_")
            if upper not in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                yield _HTTP_PREFIX + upper

    def __len__(self) -> int:
        req = self._req
        base = len(_REQUIRED_KEYS)
        if req.headers.get("content-type") is not None:
            base += 1
        if req.headers.get("content-length") is not None:
            base += 1
        for name in req.headers:
            upper = name.upper().replace("-", "_")
            if upper not in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                base += 1
        return base

    def __repr__(self) -> str:
        return f"WsgiEnviron(method={self._req.method!r}, path={self._req.path!r})"

    def _compute(self, key: str) -> Any:
        req = self._req
        if key == "wsgi.input":
            return BodyStream(req.body or b"")
        if key == "CONTENT_TYPE":
            return req.headers.get("content-type", "")
        if key == "CONTENT_LENGTH":
            return req.headers.get("content-length", "")
        if key.startswith(_HTTP_PREFIX):
            header_name = key[5:].lower().replace("_", "-")
            value = req.headers.get(header_name)
            if value is not None:
                return value
        return _MISSING
