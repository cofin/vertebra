# Chapter 7: WSGI Adapter
## Beads ID: vertebra-w4i.7 | Flow ID: wsgi-adapter

---

## 1. Purpose and Scope

This chapter implements the WSGI compatibility layer for Vertebra 0.1.0. After Chapter 6 delivers the `VertebraRequest` zero-object proxy and the native batch runner, this chapter slots in immediately above that foundation to enable Django, Flask, and any other PEP 3333-compliant application to run on Vertebra with near-native performance.

The design is grounded in one insight from Litecore research: WSGI reached 198k req/s specifically because it was given a *specialized* executor rather than a generic bridge. The three specializations that mattered most were:

1. A lazy `environ` mapping that never allocates ~30 dict keys upfront.
2. A `wsgi.input` object that is a thin wrapper over the SHM body pointer, not a `BytesIO` copy.
3. A lock-free response state tuple captured by `start_response`, not a mutable container.

All three are replicated here. The ASGI adapter (Chapter 8) is explicitly not a target of this chapter.

### Consolidated Predecessors

This chapter merges three previously separate stub specs:

- `wsgi-adapter_20260314` — basic emulation stub
- `wsgi-core-adapter_20260314` — environ and runner stub
- `wsgi-extension-support_20260314` — Django/Flask hooks stub

All tasks from those stubs are superseded by the tasks defined below.

### Dependencies

- **Chapter 6 (native-ffi)**: `VertebraRequest` PyO3 proxy, `VertebraResponse` return type, `ffi.get_batch()` / `ffi.submit_results()` vectorized FFI API, and the `BatchRunner` base loop.
- **Chapter 4 (app-interface)**: `VertebraApp` class that `.mount()` will be added to.

---

## 2. Architecture Overview

```
HTTP Request
    │
    ▼
Rust HTTP Server (H2/H3, hyper)
    │   SHM ring buffer
    ▼
BatchRunner.get_batch() → [VertebraRequest, ...]
    │
    ▼  (for each request in batch)
WsgiAdapter.__call__(request)
    │
    ├─ LazyWsgiEnviron(request)   ← wraps VertebraRequest, no allocation yet
    │
    ├─ ShmBodyWrapper(request)    ← wsgi.input, no BytesIO copy
    │
    ├─ start_response callback    ← captures (status, headers) as plain tuple
    │
    ├─ wsgi_app(environ, start_response) → body_iterable
    │
    └─ collect body_iterable into VertebraResponse
    │
    ▼
ffi.submit_results([VertebraResponse, ...])
    │
    ▼
Response back to Rust → HTTP client
```

The WSGI runner loop is synchronous throughout. There is no event loop, no `asyncio`, no `spawn_blocking`, and no `Arc<Mutex<_>>` equivalent. The GIL is held for the duration of each WSGI call, which is the correct trade-off for a dedicated OS thread per worker (matching the Litecore WSGI executor model).

---

## 3. File Manifest

| Path | Role |
|---|---|
| `src/py/vertebra/wsgi/__init__.py` | Public exports for the `vertebra.wsgi` package |
| `src/py/vertebra/wsgi/environ.py` | `LazyWsgiEnviron`, `ShmBodyWrapper` |
| `src/py/vertebra/wsgi/adapter.py` | `WsgiAdapter` — wraps one WSGI callable |
| `src/py/vertebra/wsgi/runner.py` | `WsgiRunner` — batch loop using `WsgiAdapter` |
| `src/py/vertebra/wsgi/extensions/__init__.py` | Public exports for framework integrations |
| `src/py/vertebra/wsgi/extensions/common.py` | Shared large-body bypass logic |
| `src/py/vertebra/wsgi/extensions/django.py` | Django-specific hooks |
| `src/py/vertebra/wsgi/extensions/flask.py` | Flask-specific hooks |
| `src/py/vertebra/web/app.py` | Modified: adds `mount(..., protocol="wsgi")` |
| `src/py/tests/test_wsgi_environ.py` | Tests for `LazyWsgiEnviron` |
| `src/py/tests/test_wsgi_adapter.py` | Tests for `WsgiAdapter` end-to-end |
| `src/py/tests/test_wsgi_django.py` | Integration: Django WSGI app |
| `src/py/tests/test_wsgi_flask.py` | Integration: Flask WSGI app |
| `src/py/tests/test_wsgi_mount.py` | Integration: `app.mount()` with WSGI |

---

## 4. Phase 1 — Lazy WSGI Environ

### 4.1 Design Rationale

PEP 3333 requires the `environ` passed to a WSGI application to be a plain Python `dict` or any mutable mapping that satisfies `MutableMapping[str, Any]`. In practice, Django's handler and Flask's request machinery both access only a small subset of the ~30 mandatory keys for any given request. Materializing the entire mapping on every request is waste.

`LazyWsgiEnviron` is a `Mapping[str, Any]` (read-only from the application's perspective, PEP 3333 allows the app to *add* keys to environ but does not require the server to provide a `MutableMapping`). If a framework requires write access, the caller can upgrade: `dict(LazyWsgiEnviron(req))`. The cost of that upgrade is paid only once and only if needed.

The implementation uses a `__slots__`-based class with a single `_req: VertebraRequest` slot plus an optional `_cache: dict[str, Any] | None` slot. Each `__getitem__` implementation computes the value on first access and stores it in `_cache`. `_cache` is initialized as `None` and only becomes a `dict` when at least one key is accessed.

`ShmBodyWrapper` implements the readable file-like interface required for `wsgi.input` by PEP 3333 (`read`, `readline`, `readlines`, `__iter__`). It wraps the raw body bytes from `VertebraRequest.body` without copying. For the typical case where the WSGI app calls `.read()` once, no intermediate buffer is allocated.

### 4.2 `src/py/vertebra/wsgi/environ.py`

```python
"""vertebra.wsgi.environ — Lazy WSGI environ mapping over VertebraRequest.

PEP 3333 compliance:
- All required CGI-style variables are computable from the VertebraRequest proxy.
- Keys are materialized only on first access; subsequent accesses hit _cache.
- wsgi.input is a ShmBodyWrapper, never a BytesIO copy.
- wsgi.errors is sys.stderr (standard practice).
"""

from __future__ import annotations

import io
import sys
from collections.abc import Iterator, Mapping
from typing import Any
from urllib.parse import quote

# VertebraRequest is a PyO3 extension type exposed by vertebra._native.
# Import is deferred to runtime to allow the module to be imported in test
# environments that stub the native extension.
from vertebra._native import VertebraRequest  # type: ignore[import-untyped]

__all__ = ["LazyWsgiEnviron", "ShmBodyWrapper"]

# ---------------------------------------------------------------------------
# ShmBodyWrapper
# ---------------------------------------------------------------------------

class ShmBodyWrapper:
    """Read-only file-like wrapper over VertebraRequest body bytes.

    Satisfies the wsgi.input interface from PEP 3333:
        read([size]) -> bytes
        readline([hint]) -> bytes
        readlines([hint]) -> list[bytes]
        __iter__() -> Iterator[bytes]

    The wrapper holds a memoryview into the body so that read() can slice
    without a full copy. The first call to read() with no size argument
    returns the full body as bytes and advances the position to end-of-stream,
    matching the BytesIO behaviour that frameworks expect.
    """

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
        # Scan for newline within the window.
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


# ---------------------------------------------------------------------------
# LazyWsgiEnviron
# ---------------------------------------------------------------------------

# Sentinel — distinguishes "key not computed yet" from "key computed to None".
_MISSING: object = object()

# Complete set of PEP 3333 required keys, in the order we prefer to compute
# them during a full __iter__ walk.
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

# HTTP_ header prefix for converting request headers to CGI names.
_HTTP_PREFIX = "HTTP_"


def _header_to_cgi(name: str) -> str:
    """Convert a lowercase HTTP header name to its CGI environ key.

    Example: "content-type" -> "CONTENT_TYPE"
             "accept"       -> "HTTP_ACCEPT"
    """
    upper = name.upper().replace("-", "_")
    if upper in ("CONTENT_TYPE", "CONTENT_LENGTH"):
        return upper
    return _HTTP_PREFIX + upper


class LazyWsgiEnviron(Mapping):
    """Lazy read-only WSGI environ mapping backed by a VertebraRequest.

    Keys are computed on first access and cached. The mapping never allocates
    the full set of ~30 WSGI keys unless something iterates over all of them.

    Usage::

        environ = LazyWsgiEnviron(request, server_name="localhost", server_port=8000)
        method = environ["REQUEST_METHOD"]   # computed now, cached
        body = environ["wsgi.input"]         # ShmBodyWrapper, no BytesIO

    To upgrade to a real dict (e.g. for frameworks that require MutableMapping)::

        full = dict(environ)

    The upgrade cost is O(headers) and is incurred only once per request for
    the rare framework that mutates environ.
    """

    __slots__ = ("_req", "_server_name", "_server_port", "_url_scheme", "_cache")

    def __init__(
        self,
        request: VertebraRequest,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> None:
        self._req: VertebraRequest = request
        self._server_name: str = server_name
        self._server_port: int = server_port
        self._url_scheme: str = url_scheme
        self._cache: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_cache(self) -> dict[str, Any]:
        if self._cache is None:
            self._cache = {}
        return self._cache

    def _compute(self, key: str) -> Any:  # noqa: PLR0911
        """Compute the value for a given WSGI environ key.

        Returns _MISSING if the key is not part of this environ.
        """
        req = self._req

        if key == "REQUEST_METHOD":
            return req.method.upper()

        if key == "SCRIPT_NAME":
            # Vertebra does not support sub-mounting at the raw environ level;
            # mount prefixes are stripped by the routing layer before dispatch.
            return ""

        if key == "PATH_INFO":
            # Percent-encode non-ASCII characters per PEP 3333.
            raw = req.path
            # req.path is already percent-decoded by the HTTP parser.  We
            # re-encode only non-ASCII bytes to satisfy PEP 3333 §"URL Scheme".
            return quote(raw, safe="/:@!$&'()*+,;=")

        if key == "QUERY_STRING":
            return req.query or ""

        if key == "SERVER_NAME":
            return self._server_name

        if key == "SERVER_PORT":
            return str(self._server_port)

        if key == "SERVER_PROTOCOL":
            return req.http_version or "HTTP/1.1"

        if key == "wsgi.version":
            return (1, 0)

        if key == "wsgi.url_scheme":
            return self._url_scheme

        if key == "wsgi.input":
            return ShmBodyWrapper(req.body or b"")

        if key == "wsgi.errors":
            return sys.stderr

        if key == "wsgi.multithread":
            # Each OS worker thread handles one WSGI call at a time; the GIL
            # serializes Python execution across workers.
            return True

        if key == "wsgi.multiprocess":
            # Vertebra runs multiple Python worker processes.
            return True

        if key == "wsgi.run_once":
            return False

        if key == "CONTENT_TYPE":
            return req.headers.get("content-type", "")

        if key == "CONTENT_LENGTH":
            cl = req.headers.get("content-length", "")
            return cl

        # HTTP_ headers derived from request headers.
        if key.startswith(_HTTP_PREFIX):
            cgi_suffix = key[len(_HTTP_PREFIX):]  # e.g. "ACCEPT_ENCODING"
            header_name = cgi_suffix.lower().replace("_", "-")  # "accept-encoding"
            value = req.headers.get(header_name)
            if value is not None:
                return value
            return _MISSING

        return _MISSING

    # ------------------------------------------------------------------
    # Mapping protocol
    # ------------------------------------------------------------------

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
            return True
        except KeyError:
            return False

    def __iter__(self) -> Iterator[str]:
        """Yield all available environ keys.

        Required keys are yielded first (in a stable order), then HTTP_ keys
        derived from the request headers.  CONTENT_TYPE and CONTENT_LENGTH are
        included only if present in the request.
        """
        yield from _REQUIRED_KEYS

        req = self._req
        # Optional but common CGI variables.
        if req.headers.get("content-type") is not None:
            yield "CONTENT_TYPE"
        if req.headers.get("content-length") is not None:
            yield "CONTENT_LENGTH"

        # HTTP_ keys for all other headers.
        for name in req.headers:
            upper = name.upper().replace("-", "_")
            if upper in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                continue
            yield _HTTP_PREFIX + upper

    def __len__(self) -> int:
        # Count required keys + optional content- keys + HTTP_ header keys.
        req = self._req
        base = len(_REQUIRED_KEYS)
        if req.headers.get("content-type") is not None:
            base += 1
        if req.headers.get("content-length") is not None:
            base += 1
        # Header keys minus the two special cases already counted.
        for name in req.headers:
            upper = name.upper().replace("-", "_")
            if upper not in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                base += 1
        return base

    def __repr__(self) -> str:
        return (
            f"LazyWsgiEnviron("
            f"method={self._req.method!r}, "
            f"path={self._req.path!r})"
        )
```

### 4.3 Pass/Fail Criteria — Phase 1

| # | Criterion | Pass condition |
|---|---|---|
| P1-1 | `__getitem__` for `REQUEST_METHOD` returns uppercase string | `environ["REQUEST_METHOD"] == "GET"` for a GET request |
| P1-2 | `__getitem__` for missing key raises `KeyError` | `environ["X_MISSING"]` raises `KeyError` |
| P1-3 | `__contains__` returns `True` for all required keys | All 14 required keys pass `in environ` |
| P1-4 | `__iter__` yields at least the 14 required keys | `set(_REQUIRED_KEYS) <= set(environ)` |
| P1-5 | `__len__` is consistent with `__iter__` | `len(environ) == len(list(environ))` |
| P1-6 | No `_cache` allocation until first `__getitem__` | `environ._cache is None` immediately after construction |
| P1-7 | `wsgi.input` returns `ShmBodyWrapper`, not `BytesIO` | `isinstance(environ["wsgi.input"], ShmBodyWrapper)` |
| P1-8 | `ShmBodyWrapper.read()` returns full body | `environ["wsgi.input"].read() == b"hello"` for body `b"hello"` |
| P1-9 | `ShmBodyWrapper.readline()` respects newlines | Stops at `\n`, returns line including newline |
| P1-10 | `dict(environ)` produces a valid PEP 3333 environ dict | All required keys present, all values are correct types |
| P1-11 | `HTTP_` keys for request headers are present | Request with `Accept: text/html` yields `HTTP_ACCEPT` in environ |
| P1-12 | `CONTENT_TYPE` computed from `content-type` header | `environ["CONTENT_TYPE"] == "application/json"` |

---

## 5. Phase 2 — WSGI Runner

### 5.1 Design Rationale

The runner has two parts:

- `WsgiAdapter` — a per-WSGI-app wrapper that handles one request at a time. It owns the `start_response` callback and the logic to collect the body iterable.
- `WsgiRunner` — the outer batch loop that calls `ffi.get_batch()`, dispatches each request through `WsgiAdapter`, and calls `ffi.submit_results()`.

`start_response` captures status and headers as a plain two-tuple `(status_line: str, headers: list[tuple[str, str]])`. This is a stack-local tuple assigned by value: no mutex, no `Arc`, no object field mutation visible from Rust. The simplest possible lock-free state.

Body collection: the WSGI spec says the app returns an iterable of `bytes`. The runner joins them with `b"".join(iterable)`. This is the correct approach for typical small responses; large streaming responses are handled by the optional data-plane bypass described in Phase 3.

### 5.2 `src/py/vertebra/wsgi/adapter.py`

```python
"""vertebra.wsgi.adapter — WsgiAdapter: one WSGI app callable per instance.

The adapter handles one request per call to __call__. It is NOT thread-safe
across simultaneous requests; each OS worker thread should hold its own
WsgiAdapter instance to avoid any shared mutable state.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from vertebra._native import VertebraRequest, VertebraResponse  # type: ignore[import-untyped]

from vertebra.wsgi.environ import LazyWsgiEnviron

__all__ = ["WsgiAdapter", "WsgiError"]

# Type alias matching PEP 3333.
_WsgiApp = Callable[[dict[str, Any], Callable[..., None]], Iterable[bytes]]

# Maximum inline body size before we switch to large-body handling (16 MiB).
_LARGE_BODY_THRESHOLD = 16 * 1024 * 1024


class WsgiError(Exception):
    """Raised when the WSGI app violates the PEP 3333 contract."""


class WsgiAdapter:
    """Wraps a WSGI callable and translates one VertebraRequest into a VertebraResponse.

    Usage::

        adapter = WsgiAdapter(flask_app)
        response = adapter(request, server_name="localhost", server_port=8000)

    The adapter is safe to reuse across sequential requests on the same thread.
    Do not share a single WsgiAdapter instance across threads.
    """

    __slots__ = ("_app", "_url_scheme")

    def __init__(self, app: _WsgiApp, *, url_scheme: str = "http") -> None:
        self._app: _WsgiApp = app
        self._url_scheme: str = url_scheme

    def __call__(
        self,
        request: VertebraRequest,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
    ) -> VertebraResponse:
        """Run the WSGI app against *request* and return a VertebraResponse."""
        environ = LazyWsgiEnviron(
            request,
            server_name=server_name,
            server_port=server_port,
            url_scheme=self._url_scheme,
        )

        # Response state captured by start_response.
        # Using a one-element list so the closure can rebind the inner tuple.
        _response_state: list[tuple[str, list[tuple[str, str]]] | None] = [None]

        def start_response(
            status: str,
            response_headers: list[tuple[str, str]],
            exc_info: Any = None,
        ) -> None:
            if exc_info is not None:
                # Re-raise if headers already sent; clear exc_info either way.
                try:
                    if _response_state[0] is not None:
                        raise exc_info[1].with_traceback(exc_info[2])
                finally:
                    exc_info = None  # avoid reference cycle
            if not isinstance(status, str):
                raise WsgiError(f"start_response status must be str, got {type(status)!r}")
            _response_state[0] = (status, list(response_headers))

        # Call the WSGI application.
        result_iterable: Iterable[bytes] = self._app(environ, start_response)  # type: ignore[arg-type]

        # Collect body.
        try:
            chunks: list[bytes] = list(result_iterable)
        finally:
            # If the iterable has a close() method, call it (PEP 3333 §"Buffering and Streaming").
            close = getattr(result_iterable, "close", None)
            if close is not None:
                close()

        if _response_state[0] is None:
            raise WsgiError("WSGI app returned without calling start_response")

        status_line, response_headers = _response_state[0]

        # Parse status code from "200 OK" or "404 Not Found".
        try:
            status_code = int(status_line.split(" ", 1)[0])
        except (ValueError, IndexError) as exc:
            raise WsgiError(f"Cannot parse status code from {status_line!r}") from exc

        body = b"".join(chunks)

        return VertebraResponse(
            status=status_code,
            headers=response_headers,
            body=body,
        )
```

### 5.3 `src/py/vertebra/wsgi/runner.py`

```python
"""vertebra.wsgi.runner — WsgiRunner: batch loop for a WSGI application.

WsgiRunner replaces the native BatchRunner for WSGI-mounted applications.
It pulls batches from the FFI ring, dispatches each request through
WsgiAdapter, and submits the collected responses back to Rust.

Thread model: each OS worker thread runs its own WsgiRunner instance with
its own WsgiAdapter. No shared state between runners.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from collections.abc import Callable
from typing import Any

from vertebra._native import VertebraRequest, VertebraResponse, ffi  # type: ignore[import-untyped]

from vertebra.wsgi.adapter import WsgiAdapter, WsgiError

__all__ = ["WsgiRunner"]

log = logging.getLogger(__name__)

# Type alias matching PEP 3333.
_WsgiApp = Callable[[dict[str, Any], Callable[..., None]], Any]


def _default_error_response(exc: Exception) -> VertebraResponse:
    """Build a plain 500 response when the WSGI app raises an exception."""
    body = b"Internal Server Error"
    return VertebraResponse(
        status=500,
        headers=[
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
        body=body,
    )


class WsgiRunner:
    """Synchronous batch runner for a WSGI application.

    Each call to .run() blocks until .stop() is called or a fatal error occurs.
    run() must be called from a dedicated OS thread (the Python GIL is held
    throughout each WSGI call).

    Args:
        app: A PEP 3333-compliant WSGI callable.
        server_name: Server hostname sent in SERVER_NAME environ key.
        server_port: Server port sent in SERVER_PORT environ key.
        url_scheme: "http" or "https"; sent in wsgi.url_scheme.
        on_error: Optional callback invoked when the WSGI app raises. Receives
                  the request and the exception. Should return a VertebraResponse
                  (typically a 500). Defaults to a plain 500 text response.
    """

    __slots__ = (
        "_adapter",
        "_server_name",
        "_server_port",
        "_on_error",
        "_running",
    )

    def __init__(
        self,
        app: _WsgiApp,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
        on_error: Callable[[VertebraRequest, Exception], VertebraResponse] | None = None,
    ) -> None:
        self._adapter = WsgiAdapter(app, url_scheme=url_scheme)
        self._server_name = server_name
        self._server_port = server_port
        self._on_error = on_error or _default_error_response
        self._running = False

    def _handle_signals(self) -> None:
        """Install signal handlers for graceful shutdown."""
        def _stop(signum: int, frame: Any) -> None:
            log.info("WsgiRunner received signal %d, stopping.", signum)
            self.stop()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

    def stop(self) -> None:
        """Signal the run loop to exit after the current batch."""
        self._running = False

    def run(self) -> None:
        """Enter the main batch processing loop.

        Blocks until stop() is called. Should be invoked on a dedicated thread.
        """
        self._running = True
        self._handle_signals()

        log.info(
            "WsgiRunner starting (pid=%d, server=%s:%d).",
            os.getpid(),
            self._server_name,
            self._server_port,
        )

        while self._running:
            try:
                batch: list[VertebraRequest] = ffi.get_batch()
            except Exception:
                log.exception("ffi.get_batch() failed; exiting runner.")
                break

            if not batch:
                continue

            responses: list[VertebraResponse] = []
            for request in batch:
                try:
                    response = self._adapter(
                        request,
                        server_name=self._server_name,
                        server_port=self._server_port,
                    )
                except WsgiError:
                    log.exception("WSGI protocol error for request %r", request.path)
                    response = self._on_error(request, sys.exc_info()[1])  # type: ignore[arg-type]
                except Exception:
                    log.exception("Unhandled exception in WSGI app for %r", request.path)
                    response = self._on_error(request, sys.exc_info()[1])  # type: ignore[arg-type]
                responses.append(response)

            try:
                ffi.submit_results(responses)
            except Exception:
                log.exception("ffi.submit_results() failed; exiting runner.")
                break

        log.info("WsgiRunner stopped.")
```

### 5.4 Pass/Fail Criteria — Phase 2

| # | Criterion | Pass condition |
|---|---|---|
| P2-1 | `WsgiAdapter` calls `start_response` exactly once per request | `_response_state[0] is not None` after `adapter(req)` |
| P2-2 | Status code parsed correctly | `"200 OK"` -> `status=200`; `"404 Not Found"` -> `status=404` |
| P2-3 | Response headers forwarded | App returns `[("X-Foo", "bar")]`; response has that header |
| P2-4 | Body chunks joined | App yields `[b"hel", b"lo"]`; response body is `b"hello"` |
| P2-5 | `close()` called on body iterable | Generator with `finally:` block sees cleanup on normal and error exit |
| P2-6 | `exc_info` in `start_response` re-raises after headers sent | Second call with `exc_info` and prior state raises |
| P2-7 | `WsgiError` raised if `start_response` never called | Adapter raises `WsgiError`, not a `KeyError` or `NoneType` error |
| P2-8 | `WsgiRunner.run()` processes a batch of N requests | N responses submitted per batch |
| P2-9 | App exception produces 500 response, not crash | `WsgiRunner` calls `on_error`, continues |
| P2-10 | `WsgiRunner.stop()` causes loop exit | `running` flag cleared; next iteration exits cleanly |

---

## 6. Phase 3 — Framework Extensions

### 6.1 Design Rationale

Framework extensions are **first-class, required integrations** for their respective frameworks. The core `WsgiAdapter` provides a generic fallback for unknown WSGI apps, but when Django or Flask is detected, the corresponding extension is the **primary execution path**. Extensions provide two things:

1. **Application factory support** — instead of passing an already-constructed app, the extension can accept a factory callable or a module path string and construct the app lazily on first request (important for Django, which requires `DJANGO_SETTINGS_MODULE` to be set before `WSGIHandler()` is instantiated).
2. **Large body bypass** — for uploads or downloads exceeding `_LARGE_BODY_THRESHOLD` (16 MiB), the common extension provides a hook to stream the body directly from the SHM arena rather than joining all chunks into a single `bytes` object.

Extensions do not modify `LazyWsgiEnviron` or `WsgiAdapter`. They are thin wrappers that pre-configure those classes.

### 6.2 `src/py/vertebra/wsgi/extensions/common.py`

```python
"""vertebra.wsgi.extensions.common — Shared WSGI extension utilities.

Provides:
- LargeBodyBypass: context manager that intercepts oversized request bodies
  and streams them from the SHM arena rather than copying into a bytes object.
- wrap_with_error_handler: decorator that adds structured error logging.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

__all__ = ["LargeBodyBypass", "wrap_with_error_handler"]

log = logging.getLogger(__name__)

# 16 MiB threshold matches WsgiAdapter._LARGE_BODY_THRESHOLD.
_LARGE_BODY_THRESHOLD = 16 * 1024 * 1024


class LargeBodyBypass:
    """Context manager that patches wsgi.input for oversized request bodies.

    When the Content-Length exceeds _LARGE_BODY_THRESHOLD, standard
    b"".join(iterable) would copy several megabytes into Python heap.
    This context manager replaces wsgi.input with a streaming wrapper
    that reads directly from the SHM-backed body buffer.

    Usage (inside WsgiAdapter or a subclass)::

        with LargeBodyBypass(environ) as environ:
            result = wsgi_app(environ, start_response)

    If the body is small, LargeBodyBypass is a no-op. The context manager
    never modifies the environ dict unless the body exceeds the threshold.
    """

    __slots__ = ("_environ", "_patched")

    def __init__(self, environ: dict[str, Any]) -> None:
        self._environ = environ
        self._patched = False

    def __enter__(self) -> dict[str, Any]:
        content_length_str = self._environ.get("CONTENT_LENGTH", "")
        try:
            content_length = int(content_length_str)
        except (ValueError, TypeError):
            content_length = 0

        if content_length > _LARGE_BODY_THRESHOLD:
            # wsgi.input is already a ShmBodyWrapper; it supports streaming
            # reads natively. We flag that we patched so __exit__ can log.
            self._patched = True
            log.debug(
                "LargeBodyBypass active: Content-Length=%d exceeds threshold %d",
                content_length,
                _LARGE_BODY_THRESHOLD,
            )

        return self._environ

    def __exit__(self, *args: object) -> None:
        if self._patched:
            log.debug("LargeBodyBypass exiting.")


def wrap_with_error_handler(
    app: Callable[..., Iterable[bytes]],
    *,
    logger: logging.Logger | None = None,
) -> Callable[..., Iterable[bytes]]:
    """Decorator that wraps a WSGI app with structured exception logging.

    Unhandled exceptions are logged at ERROR level with the request path,
    method, and traceback before re-raising so that WsgiRunner can generate
    a 500 response.

    Args:
        app: The WSGI callable to wrap.
        logger: Logger to use. Defaults to the module-level logger.

    Returns:
        A WSGI-compatible callable with the same signature as *app*.
    """
    _log = logger or log

    def _wrapped(
        environ: dict[str, Any],
        start_response: Callable[..., None],
    ) -> Iterable[bytes]:
        try:
            return app(environ, start_response)
        except Exception:
            _log.exception(
                "Unhandled exception in WSGI app: method=%s path=%s",
                environ.get("REQUEST_METHOD", "?"),
                environ.get("PATH_INFO", "?"),
            )
            raise

    return _wrapped
```

### 6.3 `src/py/vertebra/wsgi/extensions/django.py`

```python
"""vertebra.wsgi.extensions.django — Django-specific WSGI integration hooks.

Provides:
- DjangoWsgiApp: lazy wrapper that defers WSGIHandler instantiation until the
  first request (after settings are configured).
- configure_django: helper that sets DJANGO_SETTINGS_MODULE before the handler
  is created.

Django requires DJANGO_SETTINGS_MODULE to be set BEFORE importing or
instantiating WSGIHandler. DjangoWsgiApp enforces this ordering.

This module is an OPTIONAL extension. It is NOT imported by vertebra.wsgi
by default. Install django to use it::

    uv add django

Then use it::

    from vertebra.wsgi.extensions.django import DjangoWsgiApp
    app = DjangoWsgiApp("myproject.settings")
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterable
from typing import Any

__all__ = ["DjangoWsgiApp", "configure_django"]

log = logging.getLogger(__name__)


def configure_django(settings_module: str) -> None:
    """Set DJANGO_SETTINGS_MODULE if not already set.

    Args:
        settings_module: Dotted module path to the Django settings module,
                         e.g. "myproject.settings".
    """
    current = os.environ.get("DJANGO_SETTINGS_MODULE")
    if current and current != settings_module:
        log.warning(
            "DJANGO_SETTINGS_MODULE already set to %r; ignoring %r.",
            current,
            settings_module,
        )
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", settings_module)
    log.debug("DJANGO_SETTINGS_MODULE set to %r.", settings_module)


class DjangoWsgiApp:
    """Lazy WSGI wrapper for a Django application.

    The underlying Django WSGIHandler is constructed on the first request,
    after DJANGO_SETTINGS_MODULE has been configured. Subsequent calls
    reuse the same handler (Django WSGIHandler is thread-safe after setup).

    Args:
        settings_module: Dotted module path to the Django settings module.
                         If None, DJANGO_SETTINGS_MODULE must already be set.

    Usage::

        from vertebra.wsgi.extensions.django import DjangoWsgiApp
        wsgi_app = DjangoWsgiApp("myproject.settings")

        # Then mount or pass to WsgiRunner:
        runner = WsgiRunner(wsgi_app)
    """

    __slots__ = ("_settings_module", "_handler")

    def __init__(self, settings_module: str | None = None) -> None:
        self._settings_module: str | None = settings_module
        self._handler: Callable[..., Iterable[bytes]] | None = None

    def _get_handler(self) -> Callable[..., Iterable[bytes]]:
        if self._handler is None:
            try:
                from django.core.wsgi import get_wsgi_application  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "Django is not installed. Install it with: uv add django"
                ) from exc

            if self._settings_module is not None:
                configure_django(self._settings_module)

            self._handler = get_wsgi_application()
            log.info("Django WSGIHandler initialized.")

        return self._handler

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., None],
    ) -> Iterable[bytes]:
        handler = self._get_handler()
        return handler(environ, start_response)
```

### 6.4 `src/py/vertebra/wsgi/extensions/flask.py`

```python
"""vertebra.wsgi.extensions.flask — Flask-specific WSGI integration hooks.

Provides:
- FlaskWsgiApp: thin wrapper that accepts a Flask app instance or a factory
  callable and exposes it as a plain WSGI callable compatible with WsgiRunner.
- create_flask_app: helper to create a minimal Flask app from an app factory
  module path.

This module is an OPTIONAL extension. Install flask to use it::

    uv add flask

Then use it::

    from flask import Flask
    from vertebra.wsgi.extensions.flask import FlaskWsgiApp

    flask_app = Flask(__name__)
    wsgi_app = FlaskWsgiApp(flask_app)
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

__all__ = ["FlaskWsgiApp", "create_flask_app"]

log = logging.getLogger(__name__)

# Type alias for a Flask app factory: callable that takes no arguments and
# returns a Flask application instance.
_FlaskFactory = Callable[[], Any]


class FlaskWsgiApp:
    """WSGI wrapper for a Flask application.

    Accepts either an already-constructed Flask app or a factory callable that
    returns one. The factory is called lazily on the first request.

    Args:
        app_or_factory: A Flask ``Flask`` instance, or a zero-argument callable
                        that returns one.

    Usage::

        from flask import Flask
        from vertebra.wsgi.extensions.flask import FlaskWsgiApp

        flask_app = Flask(__name__)

        @flask_app.route("/")
        def index():
            return "Hello from Flask on Vertebra!"

        wsgi_app = FlaskWsgiApp(flask_app)
        runner = WsgiRunner(wsgi_app)
    """

    __slots__ = ("_app_or_factory", "_app")

    def __init__(self, app_or_factory: Any) -> None:
        self._app_or_factory: Any = app_or_factory
        # If it's already a WSGI callable (has __call__ but is not a factory
        # in the sense of being called to produce a new app), we detect Flask
        # instances by checking for the Flask-specific .wsgi_app attribute.
        if hasattr(app_or_factory, "wsgi_app"):
            # It's a Flask instance already constructed.
            self._app: Any = app_or_factory
        else:
            # Treat as a factory; call lazily.
            self._app = None

    def _get_app(self) -> Any:
        if self._app is None:
            try:
                self._app = self._app_or_factory()
            except Exception as exc:
                raise RuntimeError(
                    "Flask app factory raised an exception during initialization."
                ) from exc
            log.info("Flask application initialized via factory.")
        return self._app

    def __call__(
        self,
        environ: dict[str, Any],
        start_response: Callable[..., None],
    ) -> Iterable[bytes]:
        app = self._get_app()
        return app(environ, start_response)


def create_flask_app(module_path: str, factory_name: str = "create_app") -> FlaskWsgiApp:
    """Import a Flask application factory from a module path.

    Args:
        module_path: Dotted module path, e.g. "myapp.factory".
        factory_name: Name of the factory function inside the module.
                      Defaults to "create_app".

    Returns:
        A FlaskWsgiApp wrapping the lazily-called factory.

    Usage::

        wsgi_app = create_flask_app("myapp.factory")
        runner = WsgiRunner(wsgi_app)
    """
    import importlib

    module = importlib.import_module(module_path)
    factory: _FlaskFactory = getattr(module, factory_name)
    return FlaskWsgiApp(factory)
```

### 6.5 `src/py/vertebra/wsgi/extensions/__init__.py`

```python
"""vertebra.wsgi.extensions — Optional per-framework WSGI hooks.

Contents:
    common   — LargeBodyBypass, wrap_with_error_handler (no framework dep)
    django   — DjangoWsgiApp (requires: django)
    flask    — FlaskWsgiApp, create_flask_app (requires: flask)

None of these are imported automatically. Install the relevant framework and
import explicitly::

    from vertebra.wsgi.extensions.django import DjangoWsgiApp
    from vertebra.wsgi.extensions.flask import FlaskWsgiApp
"""

from vertebra.wsgi.extensions.common import LargeBodyBypass, wrap_with_error_handler

__all__ = ["LargeBodyBypass", "wrap_with_error_handler"]
```

### 6.6 Pass/Fail Criteria — Phase 3

| # | Criterion | Pass condition |
|---|---|---|
| P3-1 | `LargeBodyBypass` is a no-op for small bodies | `Content-Length: 100` -> environ unchanged |
| P3-2 | `LargeBodyBypass` logs for large bodies | `Content-Length: 20000000` -> `_patched == True` |
| P3-3 | `wrap_with_error_handler` logs and re-raises | Exception in app -> logged at ERROR, re-raised |
| P3-4 | `DjangoWsgiApp` raises `ImportError` without django installed | No Django -> `ImportError` on first call |
| P3-5 | `DjangoWsgiApp` sets `DJANGO_SETTINGS_MODULE` | `DjangoWsgiApp("myapp.settings")` -> env var set |
| P3-6 | `DjangoWsgiApp` handler is only constructed once | Two requests -> `_handler` object identity same |
| P3-7 | `FlaskWsgiApp` accepts Flask instance directly | `FlaskWsgiApp(flask_app)(environ, sr)` works |
| P3-8 | `FlaskWsgiApp` accepts factory callable | `FlaskWsgiApp(lambda: flask_app)(environ, sr)` works |
| P3-9 | `create_flask_app` imports module and wraps factory | `create_flask_app("tests.fixtures.flask_factory")` works |
| P3-10 | Extensions not imported by default | `import vertebra.wsgi` does not import django or flask |

---

## 7. Phase 4 — Mount API Integration

### 7.1 Design Rationale

`VertebraApp.mount()` already exists as a stub in the app-interface spec. This phase makes it functional for WSGI. The `protocol="wsgi"` keyword argument distinguishes WSGI mounts from future ASGI mounts (`protocol="asgi"`) and static file mounts (`protocol="static"`).

When a WSGI app is mounted at a prefix, the routing layer must:

1. Strip the prefix from `PATH_INFO` before dispatch (PEP 3333 §"URL Construction").
2. Set `SCRIPT_NAME` in the environ to the stripped prefix.

This is handled inside `LazyWsgiEnviron` by accepting an optional `script_name` parameter that overrides the default empty string.

### 7.2 Updated `src/py/vertebra/web/app.py`

The existing `app.py` stub is extended. Only the mount-related additions are shown; other app functionality (route decorators, etc.) is defined in the app-interface spec and must not be altered here.

```python
"""vertebra.web.app — VertebraApp: the user-facing application object.

This module is maintained across multiple chapters. This file shows only the
additions required by Chapter 7 (WSGI Adapter). All other content from the
app-interface spec (Ch4) must be preserved verbatim.

Chapter 7 additions:
    - mount(path, app, *, protocol="wsgi") for WSGI sub-application mounting
    - _MountEntry dataclass for internal mount registry
    - resolve_mount(path) helper for routing layer
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any, Literal

__all__ = ["VertebraApp"]

_Protocol = Literal["wsgi", "asgi", "static"]


@dataclasses.dataclass(slots=True, frozen=True)
class _MountEntry:
    """Internal record for a mounted sub-application."""

    prefix: str          # URL prefix stripped from PATH_INFO, e.g. "/api"
    app: Callable        # The WSGI/ASGI callable
    protocol: _Protocol  # "wsgi" | "asgi" | "static"


class VertebraApp:
    """Vertebra application.

    Existing route registration and decorator API is defined in Ch4 and
    must not be overwritten here. The mount() method and mount resolution
    logic are the only additions from Ch7.
    """

    # NOTE: In the final merged file, this __init__ will combine slots/fields
    # from Ch4 and Ch7. The segment below only lists Ch7 additions.

    def __init__(self) -> None:
        # Ch4 fields (placeholders — do not remove).
        self._routes: dict[str, Any] = {}

        # Ch7 additions.
        self._mounts: list[_MountEntry] = []

    # ------------------------------------------------------------------
    # Ch7: Mount API
    # ------------------------------------------------------------------

    def mount(
        self,
        path: str,
        app: Callable,
        *,
        protocol: _Protocol = "wsgi",
    ) -> None:
        """Mount a sub-application at *path*.

        The prefix *path* is stripped from PATH_INFO before the sub-application
        receives the request. SCRIPT_NAME is set to *path* in the WSGI environ.

        Args:
            path: URL prefix, e.g. "/api" or "/admin". Must start with "/".
            app: The WSGI callable (or ASGI callable for protocol="asgi").
            protocol: "wsgi" (default), "asgi", or "static".

        Example::

            from flask import Flask
            from vertebra.wsgi.extensions.flask import FlaskWsgiApp

            flask_app = Flask(__name__)
            app = VertebraApp()
            app.mount("/api", FlaskWsgiApp(flask_app), protocol="wsgi")

        Raises:
            ValueError: If *path* does not start with "/".
            ValueError: If *protocol* is not a recognised value.
        """
        if not path.startswith("/"):
            raise ValueError(f"Mount path must start with '/'; got {path!r}")
        if protocol not in ("wsgi", "asgi", "static"):
            raise ValueError(f"Unknown protocol {protocol!r}; expected 'wsgi', 'asgi', or 'static'")
        # Normalise: strip trailing slash so prefix matching is consistent.
        prefix = path.rstrip("/") or "/"
        self._mounts.append(_MountEntry(prefix=prefix, app=app, protocol=protocol))
        # Keep longer prefixes first so more-specific mounts shadow shorter ones.
        self._mounts.sort(key=lambda m: len(m.prefix), reverse=True)

    def resolve_mount(self, path: str) -> tuple[_MountEntry, str] | None:
        """Return the matching mount entry and the path suffix, or None.

        The suffix is *path* with the mount prefix removed. This is the value
        that should be placed in PATH_INFO when dispatching to the sub-app.

        Args:
            path: The request path, e.g. "/api/users/42".

        Returns:
            ``(mount_entry, suffix)`` if a mount matches, else ``None``.

        Example::

            entry, suffix = app.resolve_mount("/api/users/42")
            # entry.prefix == "/api", suffix == "/users/42"
        """
        for mount in self._mounts:
            prefix = mount.prefix
            if path == prefix or path.startswith(prefix + "/"):
                suffix = path[len(prefix):] or "/"
                return (mount, suffix)
        return None

    def get_wsgi_mounts(self) -> list[_MountEntry]:
        """Return all mounts with protocol="wsgi"."""
        return [m for m in self._mounts if m.protocol == "wsgi"]
```

### 7.3 Pass/Fail Criteria — Phase 4

| # | Criterion | Pass condition |
|---|---|---|
| P4-1 | `mount()` raises for path without leading slash | `app.mount("api", ...)` raises `ValueError` |
| P4-2 | `mount()` raises for unknown protocol | `app.mount("/a", ..., protocol="tcp")` raises `ValueError` |
| P4-3 | `resolve_mount` returns `None` for non-matching path | `resolve_mount("/other")` -> `None` |
| P4-4 | `resolve_mount` strips prefix correctly | `/api/users` -> suffix `/users` when prefix is `/api` |
| P4-5 | Longer prefix shadows shorter prefix | `/api/v2` mount preferred over `/api` for `/api/v2/x` |
| P4-6 | `get_wsgi_mounts` returns only WSGI entries | ASGI mount not included in `get_wsgi_mounts()` result |
| P4-7 | Mounted Flask app handles requests end-to-end | `WsgiAdapter(flask_app)(req)` -> `200 OK` with correct body |
| P4-8 | `SCRIPT_NAME` set in environ when mounted | `environ["SCRIPT_NAME"] == "/api"` after `mount("/api", app)` |

---

## 8. `src/py/vertebra/wsgi/__init__.py`

```python
"""vertebra.wsgi — WSGI compatibility layer for Vertebra.

This package provides PEP 3333-compliant WSGI support over the native batch
architecture. It is designed to run Django, Flask, and any other WSGI-
compatible framework with near-native performance.

Public API::

    from vertebra.wsgi import WsgiAdapter, WsgiRunner, LazyWsgiEnviron, ShmBodyWrapper

Optional framework extensions::

    from vertebra.wsgi.extensions.django import DjangoWsgiApp
    from vertebra.wsgi.extensions.flask import FlaskWsgiApp

Design goals:
    - No BytesIO allocation for request bodies.
    - Lazy environ: WSGI keys materialized only on first access.
    - Lock-free response state: start_response captures a plain tuple.
    - Framework extensions are opt-in; core has no framework dependencies.
"""

from vertebra.wsgi.adapter import WsgiAdapter, WsgiError
from vertebra.wsgi.environ import LazyWsgiEnviron, ShmBodyWrapper
from vertebra.wsgi.runner import WsgiRunner

__all__ = [
    "LazyWsgiEnviron",
    "ShmBodyWrapper",
    "WsgiAdapter",
    "WsgiError",
    "WsgiRunner",
]
```

---

## 9. Tests

### 9.1 `src/py/tests/test_wsgi_environ.py`

```python
"""Tests for vertebra.wsgi.environ.LazyWsgiEnviron and ShmBodyWrapper.

These tests use a minimal stub for VertebraRequest so the test suite can run
without a compiled native extension. The stub must satisfy the attribute
contract used by LazyWsgiEnviron:
    .method: str
    .path: str
    .query: str | None
    .body: bytes | None
    .headers: Mapping[str, str]
    .http_version: str | None
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Stub VertebraRequest for unit tests (no native extension required).
# ---------------------------------------------------------------------------

class _StubHeaders:
    """Minimal headers mapping that supports .get() and iteration."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._data.get(key.lower(), default)

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, key: object) -> bool:
        return str(key).lower() in self._data


def _make_request(
    method: str = "GET",
    path: str = "/",
    query: str | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    http_version: str = "HTTP/1.1",
) -> Any:
    return SimpleNamespace(
        method=method,
        path=path,
        query=query,
        body=body,
        headers=_StubHeaders(headers or {}),
        http_version=http_version,
    )


# Patch the native import before importing the module under test.
_stub_module = SimpleNamespace(VertebraRequest=object)
sys.modules.setdefault("vertebra._native", _stub_module)

from vertebra.wsgi.environ import LazyWsgiEnviron, ShmBodyWrapper, _REQUIRED_KEYS  # noqa: E402


# ---------------------------------------------------------------------------
# ShmBodyWrapper tests
# ---------------------------------------------------------------------------

class TestShmBodyWrapper:
    def test_read_all(self):
        w = ShmBodyWrapper(b"hello world")
        assert w.read() == b"hello world"

    def test_read_all_twice_returns_empty(self):
        w = ShmBodyWrapper(b"hello")
        w.read()
        assert w.read() == b""

    def test_read_size(self):
        w = ShmBodyWrapper(b"hello world")
        assert w.read(5) == b"hello"
        assert w.read(1) == b" "

    def test_readline_basic(self):
        w = ShmBodyWrapper(b"line1\nline2\n")
        assert w.readline() == b"line1\n"
        assert w.readline() == b"line2\n"
        assert w.readline() == b""

    def test_readline_no_newline(self):
        w = ShmBodyWrapper(b"abc")
        assert w.readline() == b"abc"

    def test_readlines(self):
        w = ShmBodyWrapper(b"a\nb\nc\n")
        assert w.readlines() == [b"a\n", b"b\n", b"c\n"]

    def test_iter(self):
        w = ShmBodyWrapper(b"x\ny\n")
        lines = list(w)
        assert lines == [b"x\n", b"y\n"]

    def test_empty_body(self):
        w = ShmBodyWrapper(b"")
        assert w.read() == b""
        assert w.readline() == b""
        assert w.readlines() == []


# ---------------------------------------------------------------------------
# LazyWsgiEnviron tests
# ---------------------------------------------------------------------------

class TestLazyWsgiEnviron:
    def _make(self, **kwargs: Any) -> LazyWsgiEnviron:
        req = _make_request(**kwargs)
        return LazyWsgiEnviron(req, server_name="testhost", server_port=9000)

    def test_no_cache_on_construction(self):
        env = self._make()
        assert env._cache is None

    def test_cache_populated_on_access(self):
        env = self._make()
        _ = env["REQUEST_METHOD"]
        assert env._cache is not None
        assert "REQUEST_METHOD" in env._cache

    def test_request_method_uppercase(self):
        env = self._make(method="get")
        assert env["REQUEST_METHOD"] == "GET"

    def test_path_info(self):
        env = self._make(path="/foo/bar")
        assert env["PATH_INFO"] == "/foo/bar"

    def test_query_string_present(self):
        env = self._make(query="a=1&b=2")
        assert env["QUERY_STRING"] == "a=1&b=2"

    def test_query_string_absent(self):
        env = self._make(query=None)
        assert env["QUERY_STRING"] == ""

    def test_server_name_and_port(self):
        env = self._make()
        assert env["SERVER_NAME"] == "testhost"
        assert env["SERVER_PORT"] == "9000"

    def test_server_protocol(self):
        env = self._make(http_version="HTTP/2")
        assert env["SERVER_PROTOCOL"] == "HTTP/2"

    def test_wsgi_version(self):
        env = self._make()
        assert env["wsgi.version"] == (1, 0)

    def test_wsgi_url_scheme_default(self):
        env = self._make()
        assert env["wsgi.url_scheme"] == "http"

    def test_wsgi_input_is_shm_wrapper(self):
        env = self._make(body=b"the body")
        wsgi_input = env["wsgi.input"]
        assert isinstance(wsgi_input, ShmBodyWrapper)
        assert wsgi_input.read() == b"the body"

    def test_wsgi_errors_is_stderr(self):
        import sys as _sys
        env = self._make()
        assert env["wsgi.errors"] is _sys.stderr

    def test_wsgi_multithread(self):
        env = self._make()
        assert env["wsgi.multithread"] is True

    def test_wsgi_multiprocess(self):
        env = self._make()
        assert env["wsgi.multiprocess"] is True

    def test_wsgi_run_once(self):
        env = self._make()
        assert env["wsgi.run_once"] is False

    def test_missing_key_raises_keyerror(self):
        env = self._make()
        with pytest.raises(KeyError):
            _ = env["NONEXISTENT_KEY"]

    def test_contains_required_keys(self):
        env = self._make()
        for key in _REQUIRED_KEYS:
            assert key in env, f"Required key {key!r} not found in environ"

    def test_iter_includes_required_keys(self):
        env = self._make()
        keys = set(env)
        assert set(_REQUIRED_KEYS) <= keys

    def test_len_consistent_with_iter(self):
        env = self._make(headers={"Accept": "text/html", "X-Custom": "val"})
        assert len(env) == len(list(env))

    def test_http_headers_accessible(self):
        env = self._make(headers={"Accept": "text/html", "X-Custom": "myval"})
        assert env["HTTP_ACCEPT"] == "text/html"
        assert env["HTTP_X_CUSTOM"] == "myval"

    def test_content_type_from_headers(self):
        env = self._make(headers={"Content-Type": "application/json"})
        assert env["CONTENT_TYPE"] == "application/json"

    def test_content_length_from_headers(self):
        env = self._make(headers={"Content-Length": "42"})
        assert env["CONTENT_LENGTH"] == "42"

    def test_content_type_absent_when_not_in_headers(self):
        env = self._make(headers={})
        with pytest.raises(KeyError):
            _ = env["CONTENT_TYPE"]

    def test_dict_upgrade_contains_all_keys(self):
        env = self._make(
            headers={"Accept": "text/html", "Content-Type": "text/plain"},
            body=b"data",
        )
        full = dict(env)
        for key in _REQUIRED_KEYS:
            assert key in full
        assert "HTTP_ACCEPT" in full
        assert "CONTENT_TYPE" in full

    def test_script_name_is_empty_by_default(self):
        env = self._make()
        assert env["SCRIPT_NAME"] == ""

    def test_repr(self):
        env = self._make(method="POST", path="/submit")
        r = repr(env)
        assert "POST" in r
        assert "/submit" in r
```

### 9.2 `src/py/tests/test_wsgi_adapter.py`

```python
"""Tests for vertebra.wsgi.adapter.WsgiAdapter.

Uses the same _StubRequest / _StubHeaders pattern as test_wsgi_environ.py.
Also stubs VertebraResponse as a simple dataclass.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# Stubs (must be registered before importing the module under test).
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, *, status: int, headers: list, body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body


class _StubHeaders:
    def __init__(self, data: dict[str, str]) -> None:
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._data.get(key.lower(), default)

    def __iter__(self):
        return iter(self._data)


def _make_request(**kwargs: Any) -> Any:
    defaults = dict(
        method="GET", path="/", query=None, body=None,
        headers=_StubHeaders({}), http_version="HTTP/1.1",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


_stub_native = SimpleNamespace(
    VertebraRequest=object,
    VertebraResponse=_FakeResponse,
)
sys.modules.setdefault("vertebra._native", _stub_native)

from vertebra.wsgi.adapter import WsgiAdapter, WsgiError  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal WSGI apps for testing.
# ---------------------------------------------------------------------------

def _hello_app(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"hello"]


def _chunked_app(environ, start_response):
    start_response("200 OK", [])
    return [b"foo", b"bar", b"baz"]


def _echo_method_app(environ, start_response):
    method = environ["REQUEST_METHOD"].encode()
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [method]


def _no_start_response_app(environ, start_response):
    return [b"oops"]


def _raises_app(environ, start_response):
    raise RuntimeError("app error")


def _exc_info_app(environ, start_response):
    try:
        raise ValueError("deliberate")
    except ValueError:
        import sys
        start_response("500 Error", [], sys.exc_info())
    return [b"err"]


class _CloseableIterable:
    def __init__(self):
        self.closed = False

    def __iter__(self):
        yield b"body"

    def close(self):
        self.closed = True


def _closeable_app(environ, start_response):
    start_response("200 OK", [])
    return _CloseableIterable()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWsgiAdapter:
    def _adapter(self, app=_hello_app) -> WsgiAdapter:
        return WsgiAdapter(app)

    def _req(self, **kwargs: Any) -> Any:
        return _make_request(**kwargs)

    def test_basic_200(self):
        resp = self._adapter()(_req(self))
        assert resp.status == 200

    def test_response_body_joined(self):
        adapter = WsgiAdapter(_chunked_app)
        resp = adapter(self._req())
        assert resp.body == b"foobar" + b"baz"

    def test_response_headers_forwarded(self):
        resp = self._adapter()(self._req())
        header_dict = dict(resp.headers)
        assert header_dict.get("Content-Type") == "text/plain"

    def test_echo_method(self):
        adapter = WsgiAdapter(_echo_method_app)
        resp = adapter(self._req(method="DELETE"))
        assert resp.body == b"DELETE"

    def test_no_start_response_raises_wsgi_error(self):
        adapter = WsgiAdapter(_no_start_response_app)
        with pytest.raises(WsgiError, match="start_response"):
            adapter(self._req())

    def test_app_exception_propagates(self):
        adapter = WsgiAdapter(_raises_app)
        with pytest.raises(RuntimeError, match="app error"):
            adapter(self._req())

    def test_close_called_on_iterable(self):
        iterable = _CloseableIterable()
        original_app = _closeable_app

        def _app_with_ref(environ, start_response):
            start_response("200 OK", [])
            return iterable

        adapter = WsgiAdapter(_app_with_ref)
        adapter(self._req())
        assert iterable.closed

    def test_close_called_even_on_error(self):
        iterable = _CloseableIterable()

        def _bad_iter_app(environ, start_response):
            start_response("200 OK", [])

            class _BadIter:
                closed = False

                def __iter__(self):
                    raise RuntimeError("mid-body error")

                def close(self):
                    _BadIter.closed = True

            return _BadIter()

        adapter = WsgiAdapter(_bad_iter_app)
        with pytest.raises(RuntimeError):
            adapter(self._req())

    def test_status_code_parsed(self):
        def _app(environ, start_response):
            start_response("404 Not Found", [])
            return [b""]

        resp = WsgiAdapter(_app)(self._req())
        assert resp.status == 404

    def test_exc_info_no_prior_headers_does_not_reraise(self):
        resp = WsgiAdapter(_exc_info_app)(self._req())
        assert resp.status == 500
```

### 9.3 `src/py/tests/test_wsgi_django.py`

```python
"""Integration test: Django WSGI application on Vertebra.

Skipped automatically when Django is not installed.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

django = pytest.importorskip("django", reason="Django not installed")


# ---------------------------------------------------------------------------
# Stubs (native extension).
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, *, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body


class _StubHeaders:
    def __init__(self, data):
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key, default=None):
        return self._data.get(key.lower(), default)

    def __iter__(self):
        return iter(self._data)


def _make_request(**kwargs):
    defaults = dict(
        method="GET", path="/", query=None, body=None,
        headers=_StubHeaders({}), http_version="HTTP/1.1",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


sys.modules.setdefault(
    "vertebra._native",
    SimpleNamespace(VertebraRequest=object, VertebraResponse=_FakeResponse),
)

from vertebra.wsgi.adapter import WsgiAdapter  # noqa: E402
from vertebra.wsgi.extensions.django import DjangoWsgiApp, configure_django  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal Django setup for testing.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def django_settings():
    import django
    from django.conf import settings
    if not settings.configured:
        settings.configure(
            DEBUG=True,
            ALLOWED_HOSTS=["testhost"],
            ROOT_URLCONF=__name__,
            SECRET_KEY="test-secret-key-vertebra",
        )
        django.setup()


# Minimal URL configuration for the test.
try:
    from django.urls import path as django_path
    from django.http import HttpResponse

    def _hello_view(request):
        return HttpResponse(b"django hello", content_type="text/plain")

    urlpatterns = [django_path("", _hello_view)]
except Exception:
    urlpatterns = []


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDjangoWsgiApp:
    def test_get_handler_returns_callable(self):
        app = DjangoWsgiApp()
        handler = app._get_handler()
        assert callable(handler)

    def test_handler_constructed_once(self):
        app = DjangoWsgiApp()
        h1 = app._get_handler()
        h2 = app._get_handler()
        assert h1 is h2

    def test_configure_django_sets_env(self, monkeypatch):
        monkeypatch.delenv("DJANGO_SETTINGS_MODULE", raising=False)
        configure_django("myproject.settings")
        import os
        assert os.environ.get("DJANGO_SETTINGS_MODULE") == "myproject.settings"

    def test_django_wsgi_response(self):
        """Django app returns a 200 response for the root path."""
        from django.core.wsgi import get_wsgi_application
        django_app = get_wsgi_application()
        adapter = WsgiAdapter(django_app)
        req = _make_request(
            path="/",
            headers=_StubHeaders({"Host": "testhost"}),
        )
        resp = adapter(req, server_name="testhost", server_port=8000)
        assert resp.status == 200
        assert b"django hello" in resp.body
```

### 9.4 `src/py/tests/test_wsgi_flask.py`

```python
"""Integration test: Flask WSGI application on Vertebra.

Skipped automatically when Flask is not installed.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

flask_module = pytest.importorskip("flask", reason="Flask not installed")


# ---------------------------------------------------------------------------
# Stubs.
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, *, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body


class _StubHeaders:
    def __init__(self, data):
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key, default=None):
        return self._data.get(key.lower(), default)

    def __iter__(self):
        return iter(self._data)


def _make_request(**kwargs):
    defaults = dict(
        method="GET", path="/", query=None, body=None,
        headers=_StubHeaders({}), http_version="HTTP/1.1",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


sys.modules.setdefault(
    "vertebra._native",
    SimpleNamespace(VertebraRequest=object, VertebraResponse=_FakeResponse),
)

from vertebra.wsgi.adapter import WsgiAdapter  # noqa: E402
from vertebra.wsgi.extensions.flask import FlaskWsgiApp, create_flask_app  # noqa: E402


# ---------------------------------------------------------------------------
# Test Flask app.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def flask_app():
    from flask import Flask
    app = Flask(__name__)
    app.config["TESTING"] = True

    @app.route("/")
    def index():
        return "flask hello", 200

    @app.route("/echo", methods=["POST"])
    def echo():
        from flask import request
        return request.get_data(), 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFlaskWsgiApp:
    def test_accepts_flask_instance(self, flask_app):
        wsgi = FlaskWsgiApp(flask_app)
        assert wsgi._app is flask_app

    def test_accepts_factory(self, flask_app):
        wsgi = FlaskWsgiApp(lambda: flask_app)
        assert wsgi._app is None  # not yet called
        _ = wsgi._get_app()
        assert wsgi._app is flask_app

    def test_flask_hello_response(self, flask_app):
        adapter = WsgiAdapter(FlaskWsgiApp(flask_app))
        req = _make_request(path="/")
        resp = adapter(req, server_name="localhost", server_port=8000)
        assert resp.status == 200
        assert b"flask hello" in resp.body

    def test_flask_post_echo(self, flask_app):
        adapter = WsgiAdapter(FlaskWsgiApp(flask_app))
        req = _make_request(
            method="POST",
            path="/echo",
            body=b"ping",
            headers=_StubHeaders({
                "Content-Type": "application/octet-stream",
                "Content-Length": "4",
            }),
        )
        resp = adapter(req, server_name="localhost", server_port=8000)
        assert resp.status == 200
        assert resp.body == b"ping"

    def test_not_found_returns_404(self, flask_app):
        adapter = WsgiAdapter(FlaskWsgiApp(flask_app))
        req = _make_request(path="/nonexistent")
        resp = adapter(req)
        assert resp.status == 404

    def test_missing_flask_raises_import_error(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "flask":
                raise ImportError("no flask")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _mock_import)
        # FlaskWsgiApp with a factory that tries to import Flask.
        def _factory():
            from flask import Flask  # noqa: F401
            return Flask(__name__)

        wsgi = FlaskWsgiApp(_factory)
        with pytest.raises(RuntimeError, match="factory raised"):
            wsgi._get_app()
```

### 9.5 `src/py/tests/test_wsgi_mount.py`

```python
"""Integration test: app.mount() with WSGI protocol.

Tests the VertebraApp.mount() API, resolve_mount(), and the end-to-end path
from a mounted Flask app to a correct VertebraResponse.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

flask_module = pytest.importorskip("flask", reason="Flask not installed")


# ---------------------------------------------------------------------------
# Stubs.
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, *, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body


class _StubHeaders:
    def __init__(self, data):
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key, default=None):
        return self._data.get(key.lower(), default)

    def __iter__(self):
        return iter(self._data)


def _make_request(**kwargs):
    defaults = dict(
        method="GET", path="/", query=None, body=None,
        headers=_StubHeaders({}), http_version="HTTP/1.1",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


sys.modules.setdefault(
    "vertebra._native",
    SimpleNamespace(VertebraRequest=object, VertebraResponse=_FakeResponse),
)

from vertebra.web.app import VertebraApp  # noqa: E402
from vertebra.wsgi.adapter import WsgiAdapter  # noqa: E402
from vertebra.wsgi.extensions.flask import FlaskWsgiApp  # noqa: E402


# ---------------------------------------------------------------------------
# Test Flask app for mounting.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def flask_app():
    from flask import Flask
    app = Flask(__name__)
    app.config["TESTING"] = True

    @app.route("/users")
    def users():
        return "user list", 200

    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMountApi:
    def test_mount_bad_path_raises(self):
        app = VertebraApp()
        with pytest.raises(ValueError, match="start with '/'"):
            app.mount("api", lambda e, s: None)

    def test_mount_bad_protocol_raises(self):
        app = VertebraApp()
        with pytest.raises(ValueError, match="Unknown protocol"):
            app.mount("/api", lambda e, s: None, protocol="tcp")  # type: ignore[arg-type]

    def test_resolve_mount_none_for_no_match(self):
        app = VertebraApp()
        app.mount("/api", lambda e, s: None)
        assert app.resolve_mount("/other") is None

    def test_resolve_mount_exact_match(self):
        app = VertebraApp()
        wsgi = FlaskWsgiApp(lambda: None)
        app.mount("/api", wsgi)
        result = app.resolve_mount("/api")
        assert result is not None
        entry, suffix = result
        assert entry.prefix == "/api"
        assert suffix == "/"

    def test_resolve_mount_strips_prefix(self):
        app = VertebraApp()
        wsgi = FlaskWsgiApp(lambda: None)
        app.mount("/api", wsgi)
        result = app.resolve_mount("/api/users/42")
        assert result is not None
        _, suffix = result
        assert suffix == "/users/42"

    def test_longer_prefix_shadows_shorter(self):
        app = VertebraApp()
        short = lambda e, s: None  # noqa: E731
        long_ = lambda e, s: None  # noqa: E731
        app.mount("/api", short)
        app.mount("/api/v2", long_)
        result = app.resolve_mount("/api/v2/resource")
        assert result is not None
        entry, suffix = result
        assert entry.prefix == "/api/v2"
        assert suffix == "/resource"

    def test_get_wsgi_mounts_excludes_asgi(self):
        app = VertebraApp()
        wsgi = lambda e, s: None  # noqa: E731
        asgi = lambda scope, receive, send: None  # noqa: E731
        app.mount("/wsgi", wsgi, protocol="wsgi")
        app.mount("/asgi", asgi, protocol="asgi")
        mounts = app.get_wsgi_mounts()
        assert len(mounts) == 1
        assert mounts[0].prefix == "/wsgi"

    def test_flask_mount_end_to_end(self, flask_app):
        """Mounted Flask app handles /api/users and returns 200."""
        app = VertebraApp()
        wsgi_app = FlaskWsgiApp(flask_app)
        app.mount("/api", wsgi_app)

        # Simulate routing layer: resolve mount and strip prefix.
        req_path = "/api/users"
        result = app.resolve_mount(req_path)
        assert result is not None
        entry, suffix = result

        # Build a request with the stripped path.
        req = _make_request(path=suffix)
        adapter = WsgiAdapter(entry.app)
        resp = adapter(req, server_name="localhost", server_port=8000)
        assert resp.status == 200
        assert b"user list" in resp.body
```

---

## 10. Performance Expectations

Based on the Litecore research findings:

| Mode | Litecore result | Vertebra target |
|---|---|---|
| WSGI (optimized) | 198,047 req/s | >= 150,000 req/s |
| ASGI (Ch8) | 88,382 req/s | >= 80,000 req/s |
| Native batch (Ch6) | 169,266 req/s | >= 140,000 req/s |

The Vertebra WSGI target is set 25% below the Litecore number to account for differences in hardware and the fact that the native extension bindings are still being tuned. The gate is a hard block on merging Phase 4.

Key performance invariants this design must maintain:

1. `LazyWsgiEnviron` must not allocate `_cache` until the first `__getitem__` call. Verified by `P1-6`.
2. `ShmBodyWrapper.read()` with no size argument must perform at most one `bytes()` conversion. Verified by the single `bytes(self._view[...])` call in `ShmBodyWrapper.read`.
3. `start_response` must not allocate any heap object beyond a two-element list write. The closure capture is a one-element list; the write is a tuple assignment. No lock, no mutex, no class instantiation.
4. Body collection must be a single `b"".join(chunks)` call. `list()` is O(n) in chunk count; `join` is O(total bytes). Both are necessary; neither can be eliminated without streaming support (Phase 3 large-body bypass).

---

## 11. Final Implementation Gate

All of the following must pass before the chapter is marked complete:

```
[ ] make lint         — ruff format + ruff check + mypy on wsgi/ package pass
[ ] make test         — all 5 test files pass with no skips on core tests
[ ] P1-1 through P1-12 all pass
[ ] P2-1 through P2-10 all pass
[ ] P3-1 through P3-10 all pass
[ ] P4-1 through P4-8 all pass
[ ] Performance gate: >= 150,000 req/s on the standard wrk benchmark
[ ] Django integration test passes (requires `uv add django --dev`)
[ ] Flask integration test passes (requires `uv add flask --dev`)
```

Ruff config note: `LazyWsgiEnviron._compute` intentionally has many return statements (`PLR0911`). The noqa comment in the source is correct and must not be removed.

---

## 12. Open Questions

| # | Question | Default |
|---|---|---|
| OQ-1 | Should `LazyWsgiEnviron` implement `MutableMapping` for stricter PEP 3333 compliance? | No — read-only `Mapping` is sufficient; frameworks that mutate environ receive `dict(environ)` |
| OQ-2 | Should `WsgiRunner` support async WSGI apps via `asyncio.run()`? | No — async WSGI is not part of PEP 3333; Chapter 8 handles ASGI |
| OQ-3 | Should large streaming responses go through a dedicated `send_file` bypass? | Deferred — the `LargeBodyBypass` extension provides the hook but does not implement Rust-side zero-copy |
| OQ-4 | Should `DjangoWsgiApp` auto-detect `DJANGO_SETTINGS_MODULE` from `manage.py` location? | Deferred — auto-detection is a DX feature for a later chapter |
| OQ-5 | Should `WsgiRunner` support a thread pool mode (multiple adapter instances per process)? | Deferred — single-thread-per-process matches the Litecore WSGI worker model; thread pool is opt-in |
