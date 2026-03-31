"""Lazy WSGI environ mapping over VertebraRequest."""

import sys
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    pass

__all__ = ["WsgiEnviron", "BodyStream"]

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


class BodyStream:
    """Read-only file-like wrapper over body bytes for wsgi.input."""

    __slots__ = ("_view", "_pos")

    def __init__(self, body: bytes) -> None:
        self._view: memoryview = memoryview(body)
        self._pos: int = 0

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            chunk = bytes(self._view[self._pos :])
            self._pos = len(self._view)
        else:
            chunk = bytes(self._view[self._pos : self._pos + size])
            self._pos += len(chunk)
        return chunk

    def readline(self, hint: int = -1) -> bytes:
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


def _header_to_cgi(name: str) -> str:
    upper = name.upper().replace("-", "_")
    if upper in ("CONTENT_TYPE", "CONTENT_LENGTH"):
        return upper
    return _HTTP_PREFIX + upper


class WsgiEnviron(Mapping[str, Any]):
    """Lazy read-only WSGI environ mapping backed by a VertebraRequest."""

    __slots__ = ("_req", "_server_name", "_server_port", "_url_scheme", "_cache", "_path_info")

    def __init__(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> None:
        self._req = request
        self._server_name: str = server_name
        self._server_port: int = server_port
        self._url_scheme: str = url_scheme
        self._cache: dict[str, Any] | None = None
        # Pre-quote path info once
        self._path_info: str = quote(request.path, safe="/:@!$&'()*+,;=")

    def _ensure_cache(self) -> dict[str, Any]:
        if self._cache is None:
            self._cache = {}
        return self._cache

    def _compute(self, key: str) -> Any:  # noqa: PLR0911
        req = self._req

        if key == "REQUEST_METHOD":
            return req.method.upper()
        if key == "SCRIPT_NAME":
            return ""
        if key == "PATH_INFO":
            return self._path_info
        if key == "QUERY_STRING":
            return req.query or ""
        if key == "SERVER_NAME":
            return self._server_name
        if key == "SERVER_PORT":
            return str(self._server_port)
        if key == "SERVER_PROTOCOL":
            return getattr(req, "http_version", None) or "HTTP/1.1"
        if key == "wsgi.version":
            return (1, 0)
        if key == "wsgi.url_scheme":
            return self._url_scheme
        if key == "wsgi.input":
            return BodyStream(req.body or b"")
        if key == "wsgi.errors":
            return sys.stderr
        if key == "wsgi.multithread":
            return True
        if key == "wsgi.multiprocess":
            return True
        if key == "wsgi.run_once":
            return False
        if key == "CONTENT_TYPE":
            return req.headers.get("content-type", "")
        if key == "CONTENT_LENGTH":
            return req.headers.get("content-length", "")
        if key.startswith(_HTTP_PREFIX):
            cgi_suffix = key[len(_HTTP_PREFIX) :]
            header_name = cgi_suffix.lower().replace("_", "-")
            value = req.headers.get(header_name)
            if value is not None:
                return value
            return _MISSING
        return _MISSING

    def __getitem__(self, key: str) -> Any:
        cache = self._ensure_cache()
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
