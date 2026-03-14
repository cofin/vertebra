# Chapter 8: ASGI Adapter

**Beads ID:** vertebra-w4i.8
**Flow ID:** asgi-adapter
**Saga:** Protocol Adapters
**Depends On:** Ch6 (native-ffi) — VertebraRequest proxy, batch runner FFI
**Consolidates:** asgi-adapter_20260314, asgi-core-adapter_20260314, asgi-extension-support_20260314
**Status:** Planned

---

## 1. Purpose and Scope

This chapter implements ASGI 3.0 compliance over the Vertebra native batch transport. When a user mounts a FastAPI, Litestar, or Starlette application via `app.mount("/api", asgi_app, protocol="asgi")`, the ASGI adapter intercepts the batch of matched requests, drives each one through the full ASGI protocol contract, and returns assembled `VertebraResponse` objects back to the batch runner.

ASGI is explicitly the **slow path**. This is not a bug; it is a structural consequence of protocol compliance. The ASGI spec mandates per-request `scope` dict construction, two Python-callable boundaries (`receive` and `send`), coroutine scheduling via `asyncio`, and dict-based message passing. Benchmarks from analogous implementations (litecore-asgi) show a stable ceiling around 88,000 req/s even with a heavily optimized bridge. WSGI reaches ~194,000 req/s over the same transport because its synchronous contract is far simpler.

The goal of this chapter is not to make ASGI as fast as WSGI. The goals are:

1. Correct ASGI 3.0 protocol compliance for HTTP connections.
2. A lean hot path that defers work until the app actually reads it.
3. `asyncio.TaskGroup` concurrency so a batch of N requests runs as N concurrent coroutines with a single wait point.
4. First-class framework extensions (Litestar PRIMARY, FastAPI secondary) that extract routes and call handlers directly — these are the primary integration path, not optional bypasses.
5. A mount API that is ergonomic and protocol-safe.

---

## 2. Performance Context

The research file `research_20260314_litecore_asgi_native_wsgi_gap/research.md` documents the structural reasons ASGI lags behind WSGI and raw native batch. Key findings for implementation decisions:

**Why the ~88k ceiling exists:**
- `AsgiScope` / `scope` dict is still a per-request Python mapping object.
- The app is an async callable; coroutine scheduling adds latency even for trivial handlers.
- `receive()` and `send()` are Python-callable boundaries per request.
- Messages are dict payloads shaped by the ASGI spec (`{"type": "http.request", "body": ...}`).

**What this chapter can and cannot fix:**
- Can: minimize allocation inside scope builder, defer header list construction, avoid redundant copies.
- Can: use `TaskGroup` so all N ASGI coroutines in a batch run concurrently within one await.
- Can: allow framework extensions to skip the generic adapter entirely for known route shapes.
- Cannot: remove the per-request `scope`/`receive`/`send` overhead; this is ASGI compliance.

**Performance gate:** A minimal ASGI echo app (`return Response("ok")`) must sustain >= 80,000 req/s in `wrk` on the same hardware used for the native batch gate. This is achievable with a clean implementation. It is not a regression target relative to litecore-asgi; it is a floor that proves the bridge is not pathologically slow.

---

## 3. Architecture

```
                         ┌─────────────────────────────────────┐
                         │          AsgiRunner.run_batch()      │
                         │                                      │
  Batch: [VertebraRequest, ...]                                 │
                         │                                      │
                         │   for req in batch:                  │
                         │     scope   = build_scope(req)       │
                         │     receive = ReceiveChannel(req)    │
                         │     send    = SendCollector()        │
                         │                                      │
                         │   async with TaskGroup() as tg:      │
                         │     for each (app,scope,rx,tx):      │
                         │       tg.create_task(                │
                         │         app(scope, rx, tx))          │
                         │                                      │
                         │   return [tx.response for tx in ...]│
                         └─────────────────────────────────────┘
                                         │
                         ┌───────────────┴──────────────────────┐
                         │         ASGI Protocol Layer          │
                         │                                      │
                         │  scope:   dict (ASGI 3.0 HTTP scope) │
                         │  receive: async callable             │
                         │           → http.request             │
                         │           → http.disconnect          │
                         │  send:    async callable             │
                         │           ← http.response.start      │
                         │           ← http.response.body       │
                         └──────────────────────────────────────┘
                                         │
                         ┌───────────────┴──────────────────────┐
                         │     Framework Extensions (required)  │
                         │  Primary integration path for known  │
                         │  frameworks — NOT optional bypasses   │
                         │                                      │
                         │  LitestarExtension (PRIMARY):        │
                         │    extract routes → call directly    │
                         │  FastApiExtension:                   │
                         │    extract routes → call directly    │
                         │  CommonExtension:                    │
                         │    JIT guard patterns                │
                         └──────────────────────────────────────┘
```

---

## 4. File Structure

```
src/py/vertebra/asgi/
├── __init__.py                  # Public exports
├── scope.py                     # build_scope(): VertebraRequest → ASGI scope dict
├── queues.py                    # ReceiveChannel, SendCollector
├── adapter.py                   # AsgiAdapter: drives app through scope/receive/send
├── runner.py                    # AsgiRunner: batch integration, TaskGroup orchestration
└── extensions/
    ├── __init__.py
    ├── common.py                # Shared JIT guard patterns
    ├── litestar.py              # Litestar route extraction, direct handler invocation (PRIMARY)
    └── fastapi.py               # FastAPI route extraction, OpenAPI passthrough

src/py/tests/
├── test_asgi_scope.py
├── test_asgi_queues.py
├── test_asgi_adapter.py
├── test_asgi_litestar.py
├── test_asgi_fastapi.py
└── test_asgi_mount.py

src/py/vertebra/web/app.py       # Modified: add protocol="asgi" to mount()
```

---

## 5. Phase 1: ASGI Scope Builder

### 5.1 Goal

`build_scope(request: VertebraRequest) -> dict` translates a `VertebraRequest` proxy into a fully compliant ASGI 3.0 HTTP Connection Scope. The scope is the primary context object the ASGI app reads to understand the incoming request.

### 5.2 ASGI 3.0 HTTP Scope Contract

Per the [ASGI HTTP Connection Scope specification](https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope):

Required fields:
- `"type"` — always `"http"`
- `"asgi"` — `{"version": "3.0", "spec_version": "2.3"}`
- `"http_version"` — `"1.1"` or `"2"` (string)
- `"method"` — uppercase HTTP method string
- `"headers"` — list of `[bytes, bytes]` pairs, lowercase names
- `"path"` — URL path, percent-decoded
- `"raw_path"` — raw bytes of the path including percent-encoding
- `"query_string"` — raw query string as bytes (no `?`)
- `"root_path"` — SCRIPT_NAME equivalent, typically `b""` or `""`
- `"scheme"` — `"http"` or `"https"`
- `"server"` — `(host, port)` tuple or `None`
- `"client"` — `(host, port)` tuple or `None`
- `"state"` — mutable dict for middleware to store per-request data
- `"extensions"` — optional dict

Optional but widely expected:
- `"path_params"` — dict populated if a router matched the path (non-ASGI extension, but FastAPI/Litestar expect it when driven via Starlette routing)

### 5.3 Lazy Headers

The headers field is the most expensive part of scope construction. The ASGI spec requires `list[tuple[bytes, bytes]]` with lowercase header names. Converting `VertebraHeaders` (Rust-backed lazy map) into this format requires iterating all headers and allocating bytes objects.

Strategy: Build headers only on first access. The scope dict stores a sentinel `_UNBUILT` for `"headers"`. A thin `_LazyScope` wrapper intercepts `__getitem__` and builds headers on the first read of `"headers"`. For apps that never read `scope["headers"]` (uncommon but possible), no allocation occurs.

In practice, most ASGI apps read headers. The lazy wrapper primarily benefits the extensions fast path where header access is avoided.

### 5.4 File: `src/py/vertebra/asgi/scope.py`

```python
"""ASGI 3.0 HTTP scope builder.

Translates a VertebraRequest proxy (Ch6 native-ffi) into a fully
compliant ASGI HTTP connection scope dict.

Design notes
------------
- Headers are deferred: the list[tuple[bytes, bytes]] allocation is skipped
  until the ASGI app actually reads scope["headers"]. Most apps read headers,
  so this is a micro-optimisation for extensions that bypass header access.
- http_version defaults to "1.1". When the native server signals HTTP/2 via
  VertebraRequest.http_version, "2" is used.
- server and client are (host, port) tuples sourced from the request proxy.
  They default to None if not available.
- path is percent-decoded; raw_path preserves the original bytes.
- query_string is raw bytes without the leading "?".
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    # VertebraRequest is a PyO3 native class; import is guarded for type checkers
    from vertebra._native import VertebraRequest  # type: ignore[import]

__all__ = ["build_scope", "LazyScope"]

_UNBUILT = object()


def _build_headers(request: "VertebraRequest") -> list[tuple[bytes, bytes]]:
    """Materialise headers from the lazy VertebraHeaders proxy.

    ASGI requires lowercase header names as bytes. VertebraHeaders provides
    items() yielding (str, str) pairs with original casing.
    """
    return [
        (name.lower().encode("latin-1"), value.encode("latin-1"))
        for name, value in request.headers.items()
    ]


class LazyScope(dict):  # type: ignore[type-arg]
    """ASGI scope dict that defers header list construction.

    Inherits from dict so it satisfies isinstance(scope, dict) checks in
    frameworks that inspect the scope type. The _request slot holds the
    VertebraRequest proxy until headers are first accessed, at which point
    the real list replaces the sentinel.
    """

    __slots__ = ("_request",)

    def __init__(self, base: dict[str, Any], request: "VertebraRequest") -> None:
        super().__init__(base)
        self._request = request

    def __getitem__(self, key: str) -> Any:
        value = super().__getitem__(key)
        if key == "headers" and value is _UNBUILT:
            value = _build_headers(self._request)
            super().__setitem__("headers", value)
        return value

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        try:
            return self[key]
        except KeyError:
            return default


def build_scope(request: "VertebraRequest", *, root_path: str = "") -> LazyScope:
    """Build an ASGI 3.0 HTTP connection scope from a VertebraRequest.

    Parameters
    ----------
    request:
        The native VertebraRequest proxy produced by the PyO3 batch bridge.
    root_path:
        SCRIPT_NAME-equivalent prefix for mounted sub-applications. Passed
        in by AsgiAdapter when the mount prefix is known.

    Returns
    -------
    LazyScope
        A dict subclass that satisfies the ASGI 3.0 HTTP scope contract.
        The ``headers`` field is deferred until first access.
    """
    # --- path and query -------------------------------------------------------
    raw_path_str: str = request.raw_path  # e.g. "/items/42?q=hello"
    # Split at first "?" to separate path from query.
    if "?" in raw_path_str:
        path_part, query_part = raw_path_str.split("?", 1)
    else:
        path_part, query_part = raw_path_str, ""

    raw_path_bytes: bytes = path_part.encode("latin-1")
    query_string_bytes: bytes = query_part.encode("latin-1")

    # percent-decoded path (VertebraRequest.path is already decoded)
    path: str = request.path

    # --- server / client ------------------------------------------------------
    server_host: str | None = getattr(request, "server_host", None)
    server_port: int | None = getattr(request, "server_port", None)
    server = (server_host, server_port) if server_host is not None else None

    client_host: str | None = getattr(request, "client_host", None)
    client_port: int | None = getattr(request, "client_port", None)
    client = (client_host, client_port) if client_host is not None else None

    # --- http version ---------------------------------------------------------
    http_version: str = getattr(request, "http_version", "1.1")

    # --- scheme ---------------------------------------------------------------
    scheme: str = getattr(request, "scheme", "http")

    base: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": http_version,
        "method": request.method.upper(),
        "headers": _UNBUILT,  # replaced on first access by LazyScope
        "path": path,
        "raw_path": raw_path_bytes,
        "query_string": query_string_bytes,
        "root_path": root_path,
        "scheme": scheme,
        "server": server,
        "client": client,
        "state": {},
        "extensions": {},
    }

    return LazyScope(base, request)
```

### 5.5 Tasks

- [ ] **Task 1.1** — Create `src/py/vertebra/asgi/` package directory.
- [ ] **Task 1.2** — Implement `src/py/vertebra/asgi/scope.py` exactly as specified above.
- [ ] **Task 1.3** — Populate `src/py/vertebra/asgi/__init__.py` (see Phase 1.6).
- [ ] **Task 1.4** — Write `src/py/tests/test_asgi_scope.py` (see Section 9.1).

### 5.6 Pass/Fail Criteria

- `build_scope(request)["type"]` == `"http"`.
- `build_scope(request)["asgi"]` == `{"version": "3.0", "spec_version": "2.3"}`.
- `build_scope(request)["method"]` is uppercase regardless of source casing.
- `scope["headers"]` is `list[tuple[bytes, bytes]]` with lowercase names after access.
- Before accessing `scope["headers"]`, internal sentinel is still `_UNBUILT` (headers not yet built).
- `scope["query_string"]` is `bytes` and does not include a leading `b"?"`.
- `scope["raw_path"]` is `bytes`.
- `scope["state"]` is a fresh mutable dict per request.
- All required ASGI 3.0 HTTP scope keys are present.

---

## 6. Phase 2: Async Queues & Signaling

### 6.1 Goal

The ASGI protocol requires two callables beyond the `scope` dict:

- `receive()`: async callable that the app calls to consume the request body and to learn when the client disconnected.
- `send()`: async callable that the app calls to emit response headers and body chunks.

These are the protocol boundaries that impose per-request overhead. The implementation must be as thin as possible: no asyncio.Queue overhead for the common single-body case, no lock contention.

### 6.2 ReceiveChannel

The HTTP request body is fully materialized in the `VertebraRequest` (the Rust batch bridge reads the full body before dispatching). This simplifies `receive()` considerably: there are exactly two events to yield.

1. First call: `{"type": "http.request", "body": <bytes>, "more_body": False}`
2. Second call: `{"type": "http.disconnect"}`
3. Any further call: `{"type": "http.disconnect"}` (idempotent; some apps poll)

No asyncio.Queue is needed. A simple integer state machine is sufficient.

**Important:** `receive` must be an `async def` callable per ASGI spec. The adapter passes `receive` (the bound method) directly to the app. The app calls `await receive()`.

### 6.3 SendCollector

The app calls `await send(message)` twice in the common non-streaming case:

1. `{"type": "http.response.start", "status": 200, "headers": [...]}`
2. `{"type": "http.response.body", "body": b"...", "more_body": False}`

For streaming responses, `http.response.body` may be called multiple times with `"more_body": True`, followed by a final call with `"more_body": False` (or no final call, which is treated as complete).

The `SendCollector` must:
- Capture status and headers from `http.response.start`.
- Accumulate body chunks from all `http.response.body` messages.
- Expose a `response` property that returns a `VertebraResponse`-compatible object once `http.response.start` has been received.

**VertebraResponse compatibility:** The batch runner expects objects with `.status` (int), `.headers` (list of (str, str) pairs), and `.body` (bytes). `SendCollector` produces a `_AsgiResponse` dataclass matching this interface.

### 6.4 File: `src/py/vertebra/asgi/queues.py`

```python
"""ASGI receive/send protocol callables.

ReceiveChannel and SendCollector implement the two async callable boundaries
required by the ASGI 3.0 HTTP protocol.

Design constraints
------------------
- ReceiveChannel uses an integer state machine rather than asyncio.Queue to
  avoid queue allocation and event overhead for the (overwhelmingly common)
  case of a single non-streaming request body.
- SendCollector accumulates body chunks in a list and joins them once. For
  responses with a single body chunk (the vast majority), the join is a
  single bytes() call on a one-element list.
- Both callables are designed to be awaited directly; they are async def
  methods wrapped as callable objects.
- _AsgiResponse is a plain dataclass that satisfies the VertebraResponse
  duck-type expected by the batch runner.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vertebra._native import VertebraRequest  # type: ignore[import]

__all__ = ["ReceiveChannel", "SendCollector"]

# ReceiveChannel state constants
_STATE_BODY = 0       # Next call returns http.request
_STATE_DISCONNECT = 1 # Next call (and all subsequent) returns http.disconnect


@dataclasses.dataclass
class _AsgiResponse:
    """Duck-type compatible with VertebraResponse for batch runner consumption."""

    status: int
    headers: list[tuple[str, str]]
    body: bytes


class ReceiveChannel:
    """Async callable that delivers ASGI http.request then http.disconnect.

    Usage::

        receive = ReceiveChannel(request)
        msg = await receive()  # {"type": "http.request", "body": b"..."}
        msg = await receive()  # {"type": "http.disconnect"}
    """

    __slots__ = ("_body", "_state")

    def __init__(self, request: "VertebraRequest") -> None:
        # Materialise body once at construction. VertebraRequest.body returns
        # bytes; it is already fully read by the Rust batch bridge before the
        # batch is dispatched to Python.
        body = request.body
        self._body: bytes = body if isinstance(body, bytes) else bytes(body)
        self._state: int = _STATE_BODY

    async def __call__(self) -> dict[str, Any]:
        if self._state == _STATE_BODY:
            self._state = _STATE_DISCONNECT
            return {
                "type": "http.request",
                "body": self._body,
                "more_body": False,
            }
        # _STATE_DISCONNECT and any subsequent calls
        return {"type": "http.disconnect"}


class SendCollector:
    """Async callable that collects ASGI send messages into a response.

    The app calls ``await send(message)`` with:

    1. ``{"type": "http.response.start", "status": ..., "headers": [...]}``
    2. One or more ``{"type": "http.response.body", "body": ..., "more_body": ...}``

    After the app completes, ``collector.response`` returns an ``_AsgiResponse``
    suitable for the batch runner.

    Raises
    ------
    RuntimeError
        If ``response`` is accessed before ``http.response.start`` is received,
        or if an unknown message type is sent.
    """

    __slots__ = ("_status", "_headers", "_body_chunks", "_started")

    def __init__(self) -> None:
        self._status: int = 0
        self._headers: list[tuple[str, str]] = []
        self._body_chunks: list[bytes] = []
        self._started: bool = False

    async def __call__(self, message: dict[str, Any]) -> None:
        msg_type: str = message["type"]

        if msg_type == "http.response.start":
            self._status = int(message["status"])
            raw_headers: list[Any] = message.get("headers", [])
            # ASGI headers are list[tuple[bytes, bytes]]; convert to str pairs.
            self._headers = [
                (name.decode("latin-1"), value.decode("latin-1"))
                for name, value in raw_headers
            ]
            self._started = True

        elif msg_type == "http.response.body":
            chunk: bytes = message.get("body", b"")
            if chunk:
                self._body_chunks.append(
                    chunk if isinstance(chunk, bytes) else bytes(chunk)
                )
            # more_body=True means more chunks follow; we just keep accumulating.

        else:
            raise RuntimeError(
                f"SendCollector received unexpected ASGI message type: {msg_type!r}"
            )

    @property
    def response(self) -> _AsgiResponse:
        """Assemble and return the collected response.

        May be called once the ASGI app coroutine has completed. If
        ``http.response.start`` was never received, raises RuntimeError.
        """
        if not self._started:
            raise RuntimeError(
                "ASGI app completed without sending http.response.start"
            )
        body = b"".join(self._body_chunks)
        return _AsgiResponse(
            status=self._status,
            headers=self._headers,
            body=body,
        )
```

### 6.5 Tasks

- [ ] **Task 2.1** — Implement `src/py/vertebra/asgi/queues.py` exactly as specified above.
- [ ] **Task 2.2** — Write `src/py/tests/test_asgi_queues.py` (see Section 9.2).

### 6.6 Pass/Fail Criteria

- First `await receive()` returns `{"type": "http.request", "body": <bytes>, "more_body": False}`.
- Second `await receive()` returns `{"type": "http.disconnect"}`.
- Third and subsequent `await receive()` calls return `{"type": "http.disconnect"}` (idempotent).
- `await send({"type": "http.response.start", "status": 200, "headers": []})` sets `_started = True`.
- `await send({"type": "http.response.body", "body": b"hello"})` accumulates body.
- `collector.response` before `http.response.start` raises `RuntimeError`.
- Multiple body chunks are concatenated correctly in `collector.response.body`.
- Headers in `collector.response.headers` are `list[tuple[str, str]]`.
- `await send({"type": "unknown"})` raises `RuntimeError`.

---

## 7. Phase 3: ASGI Runner

### 7.1 Goal

`AsgiAdapter` drives a single ASGI application through the protocol for a batch of requests. `AsgiRunner` integrates `AsgiAdapter` with the batch runner FFI layer from Ch6.

### 7.2 AsgiAdapter Design

The adapter holds a reference to the ASGI app callable. For each request in the batch it:

1. Calls `build_scope(request, root_path=self._mount_prefix)`.
2. Constructs a `ReceiveChannel` for that request.
3. Constructs a `SendCollector` for that request.
4. Schedules `app(scope, receive, send)` as a task.

All tasks for the batch run concurrently inside a single `asyncio.TaskGroup`. TaskGroup is preferred over `asyncio.gather()` because:
- Task group propagates exceptions immediately and cancels siblings; `gather` by default swallows them.
- Task group is the idiomatic Python 3.11+ API for structured concurrency.
- A single `async with TaskGroup()` clearly delimits the batch boundary.

After the TaskGroup block exits (all tasks complete or one raises), the adapter collects `collector.response` from each `SendCollector` and returns the list.

**Exception handling:** If any single ASGI task raises an unhandled exception, `TaskGroup` cancels remaining tasks and re-raises via `ExceptionGroup`. The runner catches `ExceptionGroup`, logs it, and returns 500 responses for all failed requests to prevent one bad request from crashing the batch.

### 7.3 File: `src/py/vertebra/asgi/adapter.py`

```python
"""AsgiAdapter: drives ASGI apps through the protocol for a request batch.

Core design
-----------
- One AsgiAdapter instance per mounted ASGI app. Reused across batches.
- Uses asyncio.TaskGroup (Python 3.11+) for structured concurrency.
- All N tasks in a batch run concurrently within one TaskGroup await.
- ExceptionGroup from TaskGroup is caught; failed requests return HTTP 500
  so one bad request cannot abort the entire batch.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from vertebra.asgi.queues import ReceiveChannel, SendCollector, _AsgiResponse
from vertebra.asgi.scope import build_scope

if TYPE_CHECKING:
    from vertebra._native import VertebraRequest  # type: ignore[import]

__all__ = ["AsgiAdapter"]

log = logging.getLogger(__name__)

_INTERNAL_ERROR_RESPONSE = _AsgiResponse(
    status=500,
    headers=[("content-type", "text/plain"), ("content-length", "21")],
    body=b"Internal Server Error",
)


class AsgiAdapter:
    """Drives an ASGI 3.0 application over a batch of VertebraRequests.

    Parameters
    ----------
    app:
        The ASGI application callable: ``async def app(scope, receive, send)``.
    mount_prefix:
        The path prefix at which this app is mounted. Passed as ``root_path``
        in the ASGI scope. Defaults to empty string.
    """

    __slots__ = ("_app", "_mount_prefix")

    def __init__(self, app: Any, mount_prefix: str = "") -> None:
        self._app = app
        self._mount_prefix = mount_prefix

    async def run_batch(
        self, requests: list["VertebraRequest"]
    ) -> list[_AsgiResponse]:
        """Run the ASGI app for every request in the batch concurrently.

        Returns a list of _AsgiResponse in the same order as ``requests``.
        Failed tasks produce HTTP 500 responses and are logged; they do not
        raise from this method.
        """
        n = len(requests)
        collectors: list[SendCollector] = [SendCollector() for _ in range(n)]
        # Pre-build scopes and receive channels outside the TaskGroup so that
        # scope construction errors are caught before any tasks start.
        scopes = [build_scope(req, root_path=self._mount_prefix) for req in requests]
        receives = [ReceiveChannel(req) for req in requests]

        # Track which tasks failed so we can substitute 500 responses.
        failed: set[int] = set()

        async def _run_one(idx: int) -> None:
            try:
                await self._app(scopes[idx], receives[idx], collectors[idx])
            except Exception:
                log.exception(
                    "ASGI app raised unhandled exception for request %d", idx
                )
                failed.add(idx)

        try:
            async with asyncio.TaskGroup() as tg:
                for i in range(n):
                    tg.create_task(_run_one(i))
        except* Exception as eg:
            # TaskGroup re-raises via ExceptionGroup if tasks raise outside
            # our inner try/except. This should not happen since _run_one
            # catches all exceptions, but guard anyway.
            log.exception("Unexpected ExceptionGroup from TaskGroup: %s", eg)

        results: list[_AsgiResponse] = []
        for i in range(n):
            if i in failed:
                results.append(_INTERNAL_ERROR_RESPONSE)
            else:
                try:
                    results.append(collectors[i].response)
                except RuntimeError:
                    log.error(
                        "ASGI app for request %d never sent http.response.start", i
                    )
                    results.append(_INTERNAL_ERROR_RESPONSE)

        return results
```

### 7.4 File: `src/py/vertebra/asgi/runner.py`

```python
"""AsgiRunner: integrates AsgiAdapter with the native batch runner (Ch6 FFI).

The runner is the glue between:
- The Ch6 batch runner loop, which calls Python with a list of VertebraRequest
  objects and expects a list of response-like objects back.
- The AsgiAdapter, which drives the ASGI app asynchronously.

The runner owns the event loop for its worker process. It runs
``adapter.run_batch()`` synchronously from the perspective of the batch
runner bridge by using ``asyncio.run()`` or, if a loop is already running
(e.g. during testing), by scheduling via ``loop.run_until_complete()``.

Worker startup
--------------
``AsgiRunner.start(app)`` is called once during worker initialisation
(the Ch6 worker shell equivalent for ASGI workers). It stores the adapter
and optionally warms up the event loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from vertebra.asgi.adapter import AsgiAdapter
from vertebra.asgi.queues import _AsgiResponse

if TYPE_CHECKING:
    from vertebra._native import VertebraRequest  # type: ignore[import]

__all__ = ["AsgiRunner"]

log = logging.getLogger(__name__)


class AsgiRunner:
    """Synchronous batch runner wrapper around AsgiAdapter.

    Designed to be used as the Python-side batch handler registered with
    the Ch6 FFI bridge for ASGI-mounted sub-applications.

    Usage::

        runner = AsgiRunner(fastapi_app, mount_prefix="/api")
        responses = runner(batch_of_requests)
    """

    __slots__ = ("_adapter", "_loop")

    def __init__(self, app: Any, mount_prefix: str = "") -> None:
        self._adapter = AsgiAdapter(app, mount_prefix=mount_prefix)
        # Create a dedicated event loop for this runner. Worker processes are
        # single-threaded; one loop per worker is correct.
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

    def __call__(
        self, requests: "list[VertebraRequest]"
    ) -> list[_AsgiResponse]:
        """Process a batch of requests and return responses.

        This is the synchronous entry point called by the Ch6 batch bridge.
        """
        return self._loop.run_until_complete(self._adapter.run_batch(requests))

    def close(self) -> None:
        """Shut down the event loop. Call during worker teardown."""
        self._loop.close()
```

### 7.5 Tasks

- [ ] **Task 3.1** — Implement `src/py/vertebra/asgi/adapter.py` exactly as specified above.
- [ ] **Task 3.2** — Implement `src/py/vertebra/asgi/runner.py` exactly as specified above.
- [ ] **Task 3.3** — Write `src/py/tests/test_asgi_adapter.py` (see Section 9.3).

### 7.6 Pass/Fail Criteria

- A minimal ASGI app returning status 200 and body `b"ok"` produces the correct `_AsgiResponse`.
- A batch of 5 requests all receive correct responses.
- A task that raises an unhandled exception produces a 500 response for that slot only; other slots receive their correct responses.
- `asyncio.TaskGroup` is used; no bare `asyncio.create_task()` calls outside a TaskGroup context.
- `AsgiRunner.__call__` is synchronous and drives the event loop to completion.
- `AsgiRunner.close()` shuts down the loop cleanly.

---

## 8. Phase 4: Framework Extensions

### 8.1 Goal

Framework extensions are **first-class, required integrations** — not optional performance bypasses. When a Litestar or FastAPI app is mounted, the corresponding extension is the **primary execution path**. It extracts routes and calls handlers directly, bypassing the generic ASGI `scope`/`receive`/`send` machinery for known route shapes.

**Litestar is the PRIMARY framework** — its extension must be complete and fully functional first. FastAPI extension follows.

The generic `AsgiAdapter` path is the fallback for any app that is NOT a known framework, or for specific routes within a known framework where direct invocation is not safe (e.g., routes with complex middleware chains).

### 8.2 Extension Architecture

Each extension:
1. Has a detection function: `can_accelerate(app) -> bool`.
2. Has a route extraction function: `extract_routes(app) -> dict[str, Callable]`.
3. Has a call-through function: `call_direct(handler, request) -> _AsgiResponse | None`, where `None` means "fall back to generic ASGI path".

The `AsgiAdapter` checks registered extensions before running the generic path:

```python
for ext in self._extensions:
    if ext.can_accelerate(self._app):
        result = await ext.try_handle(self._app, request)
        if result is not None:
            return result
# fall through to generic ASGI
```

Extensions are registered globally via `register_extension()`. This keeps `AsgiAdapter` unaware of specific frameworks.

### 8.3 Common Extension: JIT Guard Patterns

`common.py` provides shared utilities used by both framework extensions:

- `compile_guard_pattern(pattern: str) -> re.Pattern`: Compiles route guard patterns once and caches them. Used to match request paths without framework routing overhead.
- `is_static_route(path: str) -> bool`: Returns True if the path has no variable segments (no `{param}` or `:param`).
- `HeaderFilter`: A callable that checks a single required header value for fast-path eligibility (e.g., `Content-Type: application/json`).

### 8.4 File: `src/py/vertebra/asgi/extensions/common.py`

```python
"""Shared utilities for ASGI framework extensions.

Provides route guard pattern compilation and static-route detection.
These are used by framework extensions (litestar.py, fastapi.py) to identify
handlers that can be called without the full ASGI protocol machinery.
Litestar is the primary framework integration; FastAPI is secondary.
"""

from __future__ import annotations

import functools
import re
from typing import Any, Callable

__all__ = [
    "compile_guard_pattern",
    "is_static_route",
    "HeaderFilter",
    "register_extension",
    "get_extensions",
]

# Global extension registry. Populated by litestar.py and fastapi.py on import.
_EXTENSIONS: list[Any] = []


def register_extension(extension: Any) -> None:
    """Register a framework extension with AsgiAdapter's extension chain."""
    _EXTENSIONS.append(extension)


def get_extensions() -> list[Any]:
    """Return the list of registered extensions (read-only view)."""
    return list(_EXTENSIONS)


@functools.lru_cache(maxsize=512)
def compile_guard_pattern(pattern: str) -> re.Pattern[str]:
    """Compile and cache a route guard pattern.

    Parameters
    ----------
    pattern:
        A route path pattern, e.g. ``"/items/{item_id}"``. Curly-brace
        parameters are converted to named regex groups.
    """
    # Convert {param} → (?P<param>[^/]+) and :param → (?P<param>[^/]+)
    regex = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
    regex = re.sub(r":(\w+)", r"(?P<\1>[^/]+)", regex)
    return re.compile(rf"^{regex}$")


def is_static_route(path: str) -> bool:
    """Return True if the path contains no variable segments."""
    return "{" not in path and ":" not in path


class HeaderFilter:
    """Fast-path check for a required header name/value pair.

    Used by extensions to confirm that a request has a required header
    (e.g. ``Content-Type: application/json``) before attempting direct
    handler invocation.
    """

    __slots__ = ("_name_bytes", "_value_bytes")

    def __init__(self, name: str, value: str) -> None:
        self._name_bytes = name.lower().encode("latin-1")
        self._value_bytes = value.encode("latin-1")

    def matches(self, headers_bytes: list[tuple[bytes, bytes]]) -> bool:
        """Return True if the required header is present with the expected value."""
        for name, value in headers_bytes:
            if name == self._name_bytes and value == self._value_bytes:
                return True
        return False
```

### 8.5 Litestar Extension (PRIMARY)

Litestar is the **primary ASGI framework integration**. Its extension is not optional — it is the default execution path for all Litestar apps mounted on Vertebra. The extension extracts routes and calls handlers directly, bypassing the generic ASGI scope/receive/send machinery.

The extension directly invokes handlers that:
- Have no complex before/after middleware that modifies the ASGI scope.
- Return a simple `dict`, `str`, or `bytes` response.

For handlers with complex middleware chains, streaming responses, or custom response classes, the extension falls back to the generic ASGI adapter.

### 8.6 File: `src/py/vertebra/asgi/extensions/litestar.py`

```python
"""Litestar ASGI extension for Vertebra (PRIMARY framework integration).

This is the primary execution path for Litestar apps on Vertebra. It extracts
routes and calls handlers directly, bypassing the generic ASGI scope/receive/send
machinery for maximum performance.

Direct invocation conditions
----------------------------
- Litestar is installed (guarded by try/import).
- The handler has no complex middleware chain.
- The handler returns a dict, str, or bytes (simple response shapes).

Fallback
--------
For handlers with complex middleware, streaming, or custom response classes,
``try_handle`` returns None and AsgiAdapter uses the generic ASGI path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from vertebra.asgi.extensions.common import register_extension
from vertebra.asgi.queues import _AsgiResponse

if TYPE_CHECKING:
    from vertebra._native import VertebraRequest  # type: ignore[import]

log = logging.getLogger(__name__)

try:
    import litestar as _litestar_mod  # noqa: F401
    _LITESTAR_AVAILABLE = True
except ImportError:
    _LITESTAR_AVAILABLE = False


class LitestarExtension:
    """Vertebra extension for Litestar ASGI applications."""

    def can_accelerate(self, app: Any) -> bool:
        """Return True if app is a Litestar application instance."""
        if not _LITESTAR_AVAILABLE:
            return False
        try:
            from litestar import Litestar
            return isinstance(app, Litestar)
        except Exception:
            return False

    async def try_handle(
        self, app: Any, request: "VertebraRequest"
    ) -> _AsgiResponse | None:
        """Attempt direct handler invocation.

        Returns None if acceleration is not safe for this request, signalling
        the caller to fall through to the generic ASGI adapter.
        """
        # Direct handler invocation for simple handlers.
        # Falls back to None (generic ASGI) for handlers with complex middleware.
        try:
            from litestar import Litestar
            if not isinstance(app, Litestar):
                return None
            # Resolve the route handler for this request path
            route_handler, params = self._resolve_handler(app, request)
            if route_handler is None:
                return None
            # Call handler directly, bypassing ASGI scope/receive/send
            result = await route_handler(**params)
            # Convert to _AsgiResponse
            if isinstance(result, dict):
                import json as _json
                body = _json.dumps(result).encode("utf-8")
                return _AsgiResponse(status=200, headers=[("content-type", "application/json")], body=body)
            if isinstance(result, (str, bytes)):
                body = result.encode("utf-8") if isinstance(result, str) else result
                return _AsgiResponse(status=200, headers=[("content-type", "text/plain")], body=body)
            # Complex response type — fall back to generic ASGI
            return None
        except Exception:
            log.debug("Litestar direct invocation failed, falling back to ASGI", exc_info=True)
            return None

    def _resolve_handler(self, app: Any, request: "VertebraRequest") -> tuple[Any, dict[str, Any]]:
        """Resolve a route handler and path parameters for direct invocation.

        Returns (handler_fn, params) or (None, {}) if the handler cannot be
        safely invoked directly (complex middleware, streaming, etc.).
        """
        try:
            for route in getattr(app, "routes", []):
                handler = getattr(route, "route_handler", None)
                path = getattr(route, "path", None)
                if path == request.path and handler is not None:
                    fn = getattr(handler, "fn", None)
                    if fn is not None and not getattr(handler, "middleware", None):
                        return fn.value if hasattr(fn, "value") else fn, {}
        except Exception:
            pass
        return None, {}

    def extract_routes(self, app: Any) -> dict[str, Any]:
        """Extract route handler mapping from a Litestar app.

        Returns a dict mapping path patterns to handler callables.
        Empty dict if Litestar is not available or extraction fails.
        """
        if not _LITESTAR_AVAILABLE:
            return {}
        try:
            # Litestar exposes routes via app.routes (list of BaseRoute)
            routes: dict[str, Any] = {}
            for route in getattr(app, "routes", []):
                pattern = getattr(route, "path", None)
                handler = getattr(route, "route_handler", None)
                if pattern and handler:
                    routes[pattern] = handler
            return routes
        except Exception:
            log.debug("Litestar route extraction failed", exc_info=True)
            return {}


if _LITESTAR_AVAILABLE:
    register_extension(LitestarExtension())
```

### 8.7 FastAPI Extension

FastAPI is built on Starlette. Its router is accessible via `app.router`. For the Ghost rewrite, the extension provides the scaffold and route extraction; direct handler invocation is deferred to an optimisation pass once the generic ASGI path is validated.

### 8.8 File: `src/py/vertebra/asgi/extensions/fastapi.py`

```python
"""FastAPI ASGI extension for Vertebra.

Provides route extraction and OpenAPI passthrough for FastAPI applications.
Direct handler invocation (bypassing ASGI protocol) is scaffolded here and
will be activated in a follow-up optimisation pass once the generic ASGI
adapter is validated.

OpenAPI passthrough
-------------------
FastAPI's ``/openapi.json`` and ``/docs`` routes are detected and allowed
to pass through the generic ASGI path unchanged. These routes are low-traffic
and the OpenAPI response shape is complex; they are not candidates for the
direct acceleration path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from vertebra.asgi.extensions.common import register_extension
from vertebra.asgi.queues import _AsgiResponse

if TYPE_CHECKING:
    from vertebra._native import VertebraRequest  # type: ignore[import]

log = logging.getLogger(__name__)

try:
    import fastapi as _fastapi_mod  # noqa: F401
    _FASTAPI_AVAILABLE = True
except ImportError:
    _FASTAPI_AVAILABLE = False

# Paths that are always routed through generic ASGI (OpenAPI tooling).
_OPENAPI_PATHS: frozenset[str] = frozenset({
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
})


class FastApiExtension:
    """Vertebra extension for FastAPI ASGI applications."""

    def can_accelerate(self, app: Any) -> bool:
        """Return True if app is a FastAPI application instance."""
        if not _FASTAPI_AVAILABLE:
            return False
        try:
            from fastapi import FastAPI
            return isinstance(app, FastAPI)
        except Exception:
            return False

    async def try_handle(
        self, app: Any, request: "VertebraRequest"
    ) -> _AsgiResponse | None:
        """Attempt direct handler invocation.

        Returns None for all requests in the current implementation. Direct
        handler invocation will be activated in a follow-up optimisation pass.
        OpenAPI paths always return None to use the generic ASGI path.
        """
        if getattr(request, "path", "") in _OPENAPI_PATHS:
            return None
        # Direct invocation scaffold: return None to delegate to generic ASGI.
        return None

    def extract_routes(self, app: Any) -> dict[str, Any]:
        """Extract route handler mapping from a FastAPI app.

        Returns a dict mapping path patterns to APIRoute objects.
        """
        if not _FASTAPI_AVAILABLE:
            return {}
        try:
            from fastapi.routing import APIRoute
            routes: dict[str, Any] = {}
            for route in getattr(app.router, "routes", []):
                if isinstance(route, APIRoute):
                    routes[route.path] = route
            return routes
        except Exception:
            log.debug("FastAPI route extraction failed", exc_info=True)
            return {}

    def get_openapi_paths(self) -> frozenset[str]:
        """Return the set of paths that bypass acceleration (OpenAPI tooling)."""
        return _OPENAPI_PATHS


if _FASTAPI_AVAILABLE:
    register_extension(FastApiExtension())
```

### 8.9 File: `src/py/vertebra/asgi/extensions/__init__.py`

```python
"""ASGI framework extensions for Vertebra.

Extensions are first-class integrations, registered automatically on import.
Import this package to activate all available extensions.

Litestar is the PRIMARY framework integration and is loaded first.

Available extensions
--------------------
- LitestarExtension (requires ``litestar`` installed) — PRIMARY
- FastApiExtension (requires ``fastapi`` installed)
"""

from vertebra.asgi.extensions import litestar, fastapi  # noqa: F401  # litestar first

__all__ = ["litestar", "fastapi"]
```

### 8.10 Tasks

- [ ] **Task 4.1** — Implement `src/py/vertebra/asgi/extensions/__init__.py`.
- [ ] **Task 4.2** — Implement `src/py/vertebra/asgi/extensions/common.py`.
- [ ] **Task 4.3** — Implement `src/py/vertebra/asgi/extensions/litestar.py`.
- [ ] **Task 4.4** — Implement `src/py/vertebra/asgi/extensions/fastapi.py`.
- [ ] **Task 4.5** — Write `src/py/tests/test_asgi_fastapi.py` (see Section 9.4).
- [ ] **Task 4.6** — Write `src/py/tests/test_asgi_litestar.py` (see Section 9.5).

### 8.11 Pass/Fail Criteria

- `LitestarExtension().can_accelerate(litestar_app)` returns `True` when litestar is installed.
- `FastApiExtension().can_accelerate(fastapi_app)` returns `True` when fastapi is installed.
- `extract_routes(fastapi_app)` returns a non-empty dict for an app with at least one route.
- Extensions that raise internally (e.g. import error) never propagate to the adapter; they return `False`/`None`/`{}` silently.
- `compile_guard_pattern` is memoized: same pattern string returns the same compiled object.
- `is_static_route("/items")` is `True`; `is_static_route("/items/{id}")` is `False`.

---

## 9. Phase 5: Mount API Integration

### 9.1 Goal

Users mount ASGI apps via `app.mount("/api", fastapi_app, protocol="asgi")`. The `VertebraApp.mount()` method from Ch4 (app-interface) must be extended to recognise `protocol="asgi"`, create an `AsgiRunner`, and register it as the handler for all requests under the given prefix.

### 9.2 Changes to `src/py/vertebra/web/app.py`

The existing `mount()` signature is:

```python
def mount(self, path: str, app: Any) -> None:
```

This chapter extends it to:

```python
def mount(self, path: str, app: Any, *, protocol: str = "auto") -> None:
```

Where `protocol` can be:
- `"auto"` — detect from the app object (inspect for `__call__` signature matching ASGI or WSGI).
- `"asgi"` — force ASGI adapter.
- `"wsgi"` — force WSGI adapter (handled by Ch7).
- `"native"` — force native batch handler (handled by Ch6).

For `protocol="asgi"`, `mount()`:
1. Creates `AsgiRunner(app, mount_prefix=path)`.
2. Registers the runner as the batch handler for the path prefix in the native router.
3. Stores the runner in `self._mounts` for lifecycle management (runner.close() on shutdown).

### 9.3 ASGI Protocol Detection (auto mode)

Auto-detection checks whether the app is an async callable with a 3-argument signature matching `(scope, receive, send)`. This is done via `inspect.signature()` at mount time, not at request time.

```python
import inspect

def _detect_protocol(app: Any) -> str:
    """Detect whether app is ASGI, WSGI, or native."""
    try:
        sig = inspect.signature(app)
        params = list(sig.parameters.values())
        if len(params) == 3 and asyncio.iscoroutinefunction(app):
            return "asgi"
        elif len(params) == 2:
            return "wsgi"
    except (ValueError, TypeError):
        pass
    return "native"
```

### 9.4 Modified `src/py/vertebra/web/app.py` Fragment

Only the `mount` method is shown. The rest of `VertebraApp` is unchanged from Ch4.

```python
def mount(
    self,
    path: str,
    app: object,
    *,
    protocol: str = "auto",
) -> None:
    """Mount a sub-application at the given path prefix.

    Parameters
    ----------
    path:
        URL prefix for this sub-application, e.g. ``"/api"``.
    app:
        The sub-application callable. ASGI, WSGI, and native batch
        callables are all supported.
    protocol:
        Explicit protocol override. ``"auto"`` (default) detects from
        the app signature. Valid values: ``"auto"``, ``"asgi"``,
        ``"wsgi"``, ``"native"``.

    Raises
    ------
    ValueError
        If protocol is not one of the valid values.
    """
    import asyncio
    import inspect

    valid_protocols = {"auto", "asgi", "wsgi", "native"}
    if protocol not in valid_protocols:
        raise ValueError(
            f"protocol={protocol!r} is not valid. Choose from {valid_protocols}."
        )

    if protocol == "auto":
        protocol = _detect_protocol(app)

    if protocol == "asgi":
        from vertebra.asgi.runner import AsgiRunner

        runner = AsgiRunner(app, mount_prefix=path)
        self._mounts[path] = ("asgi", runner)
        self._router.register_prefix(path, runner)

    elif protocol == "wsgi":
        from vertebra.wsgi.runner import WsgiRunner  # Ch7

        runner = WsgiRunner(app, mount_prefix=path)
        self._mounts[path] = ("wsgi", runner)
        self._router.register_prefix(path, runner)

    elif protocol == "native":
        self._mounts[path] = ("native", app)
        self._router.register_prefix(path, app)

def _detect_protocol(app: object) -> str:
    """Detect whether an app is ASGI, WSGI, or native by inspecting its signature."""
    import asyncio
    import inspect

    try:
        sig = inspect.signature(app)  # type: ignore[arg-type]
        params = list(sig.parameters.values())
        if len(params) == 3 and asyncio.iscoroutinefunction(app):
            return "asgi"
        elif len(params) == 2:
            return "wsgi"
    except (ValueError, TypeError):
        pass
    return "native"
```

### 9.5 Tasks

- [ ] **Task 5.1** — Extend `src/py/vertebra/web/app.py` `mount()` method with `protocol` keyword argument.
- [ ] **Task 5.2** — Implement `_detect_protocol()` helper in `app.py`.
- [ ] **Task 5.3** — Write `src/py/tests/test_asgi_mount.py` (see Section 9.6).

### 9.6 Pass/Fail Criteria

- `app.mount("/api", fastapi_app, protocol="asgi")` succeeds without error.
- `app.mount("/api", fastapi_app)` auto-detects ASGI from the signature.
- `app.mount("/api", flask_app, protocol="asgi")` creates an `AsgiRunner` even if Flask is not genuinely ASGI-compliant (mount respects explicit protocol override).
- `app.mount("/api", fastapi_app, protocol="invalid")` raises `ValueError`.
- After mounting, requests to `/api/...` are routed to the `AsgiRunner`.
- `AsgiRunner.close()` is called during `VertebraApp` shutdown (lifecycle management).

---

## 10. Public Package: `src/py/vertebra/asgi/__init__.py`

```python
"""vertebra.asgi — ASGI 3.0 adapter for the Vertebra native batch transport.

Public API
----------
AsgiAdapter:
    Drives an ASGI app for a batch of requests using asyncio.TaskGroup.
AsgiRunner:
    Synchronous wrapper around AsgiAdapter for integration with the Ch6
    batch runner FFI bridge.
build_scope:
    Translates a VertebraRequest into an ASGI 3.0 HTTP connection scope.
ReceiveChannel:
    Async callable delivering http.request then http.disconnect.
SendCollector:
    Async callable collecting http.response.start / http.response.body
    messages into a VertebraResponse-compatible object.

Performance note
----------------
ASGI is the slow path. Expect ~80-88k req/s on a single worker for trivial
handlers. See docs/prd/asgi-performance.md for structural analysis.
"""

from vertebra.asgi.adapter import AsgiAdapter
from vertebra.asgi.queues import ReceiveChannel, SendCollector
from vertebra.asgi.runner import AsgiRunner
from vertebra.asgi.scope import LazyScope, build_scope

__all__ = [
    "AsgiAdapter",
    "AsgiRunner",
    "LazyScope",
    "ReceiveChannel",
    "SendCollector",
    "build_scope",
]
```

---

## 11. Test Suite

### 11.1 `src/py/tests/test_asgi_scope.py`

```python
"""Tests for vertebra.asgi.scope: ASGI 3.0 HTTP scope builder."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from vertebra.asgi.scope import build_scope, LazyScope, _UNBUILT


def _make_request(
    method: str = "GET",
    raw_path: str = "/items/42",
    path: str = "/items/42",
    query: str = "",
    body: bytes = b"",
    headers: dict[str, str] | None = None,
    http_version: str = "1.1",
    scheme: str = "http",
    server_host: str | None = "localhost",
    server_port: int | None = 8000,
    client_host: str | None = "127.0.0.1",
    client_port: int | None = 12345,
) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.raw_path = raw_path + (f"?{query}" if query else "")
    req.path = path
    req.body = body
    req.http_version = http_version
    req.scheme = scheme
    req.server_host = server_host
    req.server_port = server_port
    req.client_host = client_host
    req.client_port = client_port
    hdr_items = list((headers or {}).items())
    req.headers.items.return_value = hdr_items
    return req


class TestBuildScopeRequiredFields:
    def test_type_is_http(self) -> None:
        scope = build_scope(_make_request())
        assert scope["type"] == "http"

    def test_asgi_version(self) -> None:
        scope = build_scope(_make_request())
        assert scope["asgi"] == {"version": "3.0", "spec_version": "2.3"}

    def test_method_uppercased(self) -> None:
        scope = build_scope(_make_request(method="get"))
        assert scope["method"] == "GET"

    def test_path(self) -> None:
        scope = build_scope(_make_request(path="/items/42"))
        assert scope["path"] == "/items/42"

    def test_raw_path_is_bytes(self) -> None:
        scope = build_scope(_make_request())
        assert isinstance(scope["raw_path"], bytes)

    def test_query_string_is_bytes_no_question_mark(self) -> None:
        scope = build_scope(_make_request(raw_path="/search", query="q=hello"))
        assert scope["query_string"] == b"q=hello"

    def test_query_string_empty(self) -> None:
        scope = build_scope(_make_request(raw_path="/items"))
        assert scope["query_string"] == b""

    def test_http_version(self) -> None:
        scope = build_scope(_make_request(http_version="2"))
        assert scope["http_version"] == "2"

    def test_scheme(self) -> None:
        scope = build_scope(_make_request(scheme="https"))
        assert scope["scheme"] == "https"

    def test_server_tuple(self) -> None:
        scope = build_scope(_make_request(server_host="example.com", server_port=443))
        assert scope["server"] == ("example.com", 443)

    def test_server_none_when_host_absent(self) -> None:
        scope = build_scope(_make_request(server_host=None, server_port=None))
        assert scope["server"] is None

    def test_client_tuple(self) -> None:
        scope = build_scope(_make_request(client_host="10.0.0.1", client_port=54321))
        assert scope["client"] == ("10.0.0.1", 54321)

    def test_state_is_fresh_dict(self) -> None:
        scope1 = build_scope(_make_request())
        scope2 = build_scope(_make_request())
        assert scope1["state"] is not scope2["state"]
        assert isinstance(scope1["state"], dict)

    def test_root_path_default(self) -> None:
        scope = build_scope(_make_request())
        assert scope["root_path"] == ""

    def test_root_path_custom(self) -> None:
        scope = build_scope(_make_request(), root_path="/api")
        assert scope["root_path"] == "/api"

    def test_extensions_is_dict(self) -> None:
        scope = build_scope(_make_request())
        assert isinstance(scope["extensions"], dict)


class TestLazyScopeHeaders:
    def test_headers_not_built_before_access(self) -> None:
        req = _make_request(headers={"Content-Type": "application/json"})
        scope = build_scope(req)
        # Access internal dict without triggering __getitem__ override
        raw = dict.__getitem__(scope, "headers")
        assert raw is _UNBUILT

    def test_headers_built_on_access(self) -> None:
        req = _make_request(headers={"Content-Type": "application/json"})
        scope = build_scope(req)
        headers = scope["headers"]
        assert isinstance(headers, list)
        assert (b"content-type", b"application/json") in headers

    def test_headers_names_lowercased(self) -> None:
        req = _make_request(headers={"X-Custom-Header": "value"})
        scope = build_scope(req)
        names = [name for name, _ in scope["headers"]]
        assert b"x-custom-header" in names

    def test_headers_values_are_bytes(self) -> None:
        req = _make_request(headers={"Accept": "text/html"})
        scope = build_scope(req)
        for name, value in scope["headers"]:
            assert isinstance(name, bytes)
            assert isinstance(value, bytes)

    def test_scope_is_instance_of_dict(self) -> None:
        scope = build_scope(_make_request())
        assert isinstance(scope, dict)
```

### 11.2 `src/py/tests/test_asgi_queues.py`

```python
"""Tests for vertebra.asgi.queues: ReceiveChannel and SendCollector."""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import MagicMock

from vertebra.asgi.queues import ReceiveChannel, SendCollector


def _make_request(body: bytes = b"hello world") -> MagicMock:
    req = MagicMock()
    req.body = body
    return req


class TestReceiveChannel:
    def test_first_call_returns_http_request(self) -> None:
        channel = ReceiveChannel(_make_request(b"data"))
        msg = asyncio.get_event_loop().run_until_complete(channel())
        assert msg["type"] == "http.request"
        assert msg["body"] == b"data"
        assert msg["more_body"] is False

    def test_second_call_returns_disconnect(self) -> None:
        channel = ReceiveChannel(_make_request())
        asyncio.get_event_loop().run_until_complete(channel())
        msg = asyncio.get_event_loop().run_until_complete(channel())
        assert msg["type"] == "http.disconnect"

    def test_subsequent_calls_return_disconnect(self) -> None:
        channel = ReceiveChannel(_make_request())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(channel())
        for _ in range(5):
            msg = loop.run_until_complete(channel())
            assert msg["type"] == "http.disconnect"

    def test_empty_body(self) -> None:
        channel = ReceiveChannel(_make_request(b""))
        msg = asyncio.get_event_loop().run_until_complete(channel())
        assert msg["body"] == b""

    def test_body_is_bytes(self) -> None:
        channel = ReceiveChannel(_make_request(b"test"))
        msg = asyncio.get_event_loop().run_until_complete(channel())
        assert isinstance(msg["body"], bytes)


class TestSendCollector:
    def _run(self, coro):  # type: ignore[no-untyped-def]
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_response_raises_before_start(self) -> None:
        collector = SendCollector()
        with pytest.raises(RuntimeError, match="http.response.start"):
            _ = collector.response

    def test_status_captured(self) -> None:
        collector = SendCollector()
        self._run(collector({"type": "http.response.start", "status": 201, "headers": []}))
        self._run(collector({"type": "http.response.body", "body": b"", "more_body": False}))
        assert collector.response.status == 201

    def test_body_assembled(self) -> None:
        collector = SendCollector()
        self._run(collector({"type": "http.response.start", "status": 200, "headers": []}))
        self._run(collector({"type": "http.response.body", "body": b"hello", "more_body": True}))
        self._run(collector({"type": "http.response.body", "body": b" world", "more_body": False}))
        assert collector.response.body == b"hello world"

    def test_headers_converted_to_str_pairs(self) -> None:
        collector = SendCollector()
        self._run(collector({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }))
        self._run(collector({"type": "http.response.body", "body": b"{}"}))
        headers = collector.response.headers
        assert ("content-type", "application/json") in headers

    def test_unknown_message_type_raises(self) -> None:
        collector = SendCollector()
        with pytest.raises(RuntimeError, match="unexpected"):
            self._run(collector({"type": "http.unknown"}))

    def test_empty_body_chunk_not_appended(self) -> None:
        collector = SendCollector()
        self._run(collector({"type": "http.response.start", "status": 200, "headers": []}))
        self._run(collector({"type": "http.response.body", "body": b""}))
        self._run(collector({"type": "http.response.body", "body": b"content"}))
        assert collector.response.body == b"content"
```

### 11.3 `src/py/tests/test_asgi_adapter.py`

```python
"""Tests for vertebra.asgi.adapter: AsgiAdapter and AsgiRunner."""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import MagicMock

from vertebra.asgi.adapter import AsgiAdapter
from vertebra.asgi.runner import AsgiRunner


def _make_request(
    method: str = "GET",
    path: str = "/",
    body: bytes = b"",
) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.path = path
    req.raw_path = path
    req.body = body
    req.http_version = "1.1"
    req.scheme = "http"
    req.server_host = "localhost"
    req.server_port = 8000
    req.client_host = "127.0.0.1"
    req.client_port = 54321
    req.headers.items.return_value = []
    return req


async def simple_asgi_app(scope, receive, send):  # type: ignore[no-untyped-def]
    """Minimal ASGI app returning 200 OK."""
    assert scope["type"] == "http"
    await receive()
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({
        "type": "http.response.body",
        "body": b"ok",
        "more_body": False,
    })


async def raising_asgi_app(scope, receive, send):  # type: ignore[no-untyped-def]
    """ASGI app that always raises."""
    raise RuntimeError("intentional failure")


class TestAsgiAdapter:
    def test_single_request_success(self) -> None:
        adapter = AsgiAdapter(simple_asgi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request()])
            )
        finally:
            loop.close()
        assert len(results) == 1
        assert results[0].status == 200
        assert results[0].body == b"ok"

    def test_batch_of_five(self) -> None:
        adapter = AsgiAdapter(simple_asgi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request() for _ in range(5)])
            )
        finally:
            loop.close()
        assert len(results) == 5
        assert all(r.status == 200 for r in results)

    def test_failed_task_returns_500(self) -> None:
        adapter = AsgiAdapter(raising_asgi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request()])
            )
        finally:
            loop.close()
        assert len(results) == 1
        assert results[0].status == 500

    def test_mixed_batch_failure_isolation(self) -> None:
        """One failing request should not corrupt adjacent successful requests."""
        call_count = 0

        async def alternating_app(scope, receive, send):  # type: ignore[no-untyped-def]
            nonlocal call_count
            idx = call_count
            call_count += 1
            if idx % 2 == 1:
                raise RuntimeError("alternate failure")
            await receive()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        adapter = AsgiAdapter(alternating_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request() for _ in range(4)])
            )
        finally:
            loop.close()
        # Positions 0 and 2 succeed; positions 1 and 3 fail.
        assert results[0].status == 200
        assert results[1].status == 500
        assert results[2].status == 200
        assert results[3].status == 500

    def test_mount_prefix_in_scope(self) -> None:
        captured_root: list[str] = []

        async def capture_app(scope, receive, send):  # type: ignore[no-untyped-def]
            captured_root.append(scope["root_path"])
            await receive()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        adapter = AsgiAdapter(capture_app, mount_prefix="/api")
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(adapter.run_batch([_make_request()]))
        finally:
            loop.close()
        assert captured_root == ["/api"]


class TestAsgiRunner:
    def test_runner_calls_app(self) -> None:
        runner = AsgiRunner(simple_asgi_app)
        try:
            results = runner([_make_request()])
        finally:
            runner.close()
        assert results[0].status == 200

    def test_runner_close_is_idempotent(self) -> None:
        runner = AsgiRunner(simple_asgi_app)
        runner.close()
        # Calling close twice should not raise.
        # (Loop is already closed; no further run_until_complete calls expected.)
```

### 11.4 `src/py/tests/test_asgi_fastapi.py`

```python
"""Integration tests: FastAPI app running via AsgiAdapter.

Requires fastapi as a test dependency. Skipped if not installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="fastapi not installed")

from unittest.mock import MagicMock
import asyncio
import json

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from vertebra.asgi.adapter import AsgiAdapter


def _make_request(
    method: str = "GET",
    path: str = "/hello",
    body: bytes = b"",
) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.path = path
    req.raw_path = path
    req.body = body
    req.http_version = "1.1"
    req.scheme = "http"
    req.server_host = "localhost"
    req.server_port = 8000
    req.client_host = "127.0.0.1"
    req.client_port = 54321
    req.headers.items.return_value = []
    return req


@pytest.fixture()
def fastapi_app() -> FastAPI:
    app = FastAPI()

    @app.get("/hello")
    async def hello() -> dict[str, str]:
        return {"message": "hello from fastapi"}

    @app.get("/items/{item_id}")
    async def get_item(item_id: int) -> dict[str, int]:
        return {"item_id": item_id}

    return app


class TestFastApiViaAsgiAdapter:
    def test_hello_route(self, fastapi_app: FastAPI) -> None:
        adapter = AsgiAdapter(fastapi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request(path="/hello")])
            )
        finally:
            loop.close()
        assert results[0].status == 200
        body = json.loads(results[0].body)
        assert body == {"message": "hello from fastapi"}

    def test_not_found_returns_404(self, fastapi_app: FastAPI) -> None:
        adapter = AsgiAdapter(fastapi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request(path="/nonexistent")])
            )
        finally:
            loop.close()
        assert results[0].status == 404

    def test_batch_of_identical_requests(self, fastapi_app: FastAPI) -> None:
        adapter = AsgiAdapter(fastapi_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request(path="/hello") for _ in range(3)])
            )
        finally:
            loop.close()
        assert all(r.status == 200 for r in results)

    def test_fastapi_extension_can_accelerate(self, fastapi_app: FastAPI) -> None:
        from vertebra.asgi.extensions.fastapi import FastApiExtension
        ext = FastApiExtension()
        assert ext.can_accelerate(fastapi_app) is True

    def test_fastapi_extension_extract_routes(self, fastapi_app: FastAPI) -> None:
        from vertebra.asgi.extensions.fastapi import FastApiExtension
        ext = FastApiExtension()
        routes = ext.extract_routes(fastapi_app)
        assert "/hello" in routes
```

### 11.5 `src/py/tests/test_asgi_litestar.py`

```python
"""Integration tests: Litestar app running via AsgiAdapter.

Requires litestar as a test dependency. Skipped if not installed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("litestar", reason="litestar not installed")

from unittest.mock import MagicMock
import asyncio
import json

from litestar import Litestar, get

from vertebra.asgi.adapter import AsgiAdapter


def _make_request(
    method: str = "GET",
    path: str = "/hello",
    body: bytes = b"",
) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.path = path
    req.raw_path = path
    req.body = body
    req.http_version = "1.1"
    req.scheme = "http"
    req.server_host = "localhost"
    req.server_port = 8000
    req.client_host = "127.0.0.1"
    req.client_port = 54321
    req.headers.items.return_value = []
    return req


@pytest.fixture()
def litestar_app() -> Litestar:
    @get("/hello")
    async def hello_handler() -> dict[str, str]:
        return {"message": "hello from litestar"}

    return Litestar(route_handlers=[hello_handler])


class TestLitestarViaAsgiAdapter:
    def test_hello_route(self, litestar_app: Litestar) -> None:
        adapter = AsgiAdapter(litestar_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request(path="/hello")])
            )
        finally:
            loop.close()
        assert results[0].status == 200
        body = json.loads(results[0].body)
        assert body == {"message": "hello from litestar"}

    def test_not_found_returns_404(self, litestar_app: Litestar) -> None:
        adapter = AsgiAdapter(litestar_app)
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                adapter.run_batch([_make_request(path="/nonexistent")])
            )
        finally:
            loop.close()
        assert results[0].status == 404

    def test_litestar_extension_can_accelerate(self, litestar_app: Litestar) -> None:
        from vertebra.asgi.extensions.litestar import LitestarExtension
        ext = LitestarExtension()
        assert ext.can_accelerate(litestar_app) is True

    def test_litestar_extension_extract_routes(self, litestar_app: Litestar) -> None:
        from vertebra.asgi.extensions.litestar import LitestarExtension
        ext = LitestarExtension()
        routes = ext.extract_routes(litestar_app)
        # Litestar exposes at least the /hello route
        assert len(routes) >= 1
```

### 11.6 `src/py/tests/test_asgi_mount.py`

```python
"""Tests for app.mount() with protocol='asgi'."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_vertebra_app() -> MagicMock:
    """Create a minimal VertebraApp mock with mount() support."""
    # Import the real class if available; otherwise mock it.
    try:
        from vertebra.web.app import VertebraApp
        return VertebraApp()
    except ImportError:
        return MagicMock()


async def _simple_asgi_app(scope, receive, send):  # type: ignore[no-untyped-def]
    """Minimal 3-argument async callable (valid ASGI signature)."""
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"mounted"})


def _simple_wsgi_app(environ, start_response):  # type: ignore[no-untyped-def]
    """Minimal 2-argument sync callable (valid WSGI signature)."""
    start_response("200 OK", [])
    return [b"wsgi response"]


class TestMountProtocolDetection:
    def test_explicit_asgi_protocol(self) -> None:
        from vertebra.asgi.runner import AsgiRunner
        from vertebra.web.app import VertebraApp

        app = VertebraApp()
        with patch.object(app._router, "register_prefix") as mock_register:
            app.mount("/api", _simple_asgi_app, protocol="asgi")
            mock_register.assert_called_once()
            _, runner = app._mounts["/api"]
            assert isinstance(runner, AsgiRunner)

    def test_auto_detects_asgi_from_signature(self) -> None:
        from vertebra.asgi.runner import AsgiRunner
        from vertebra.web.app import VertebraApp

        app = VertebraApp()
        with patch.object(app._router, "register_prefix"):
            app.mount("/api", _simple_asgi_app)
            proto, runner = app._mounts["/api"]
            assert proto == "asgi"
            assert isinstance(runner, AsgiRunner)

    def test_auto_detects_wsgi_from_signature(self) -> None:
        from vertebra.web.app import VertebraApp

        app = VertebraApp()
        with patch.object(app._router, "register_prefix"):
            app.mount("/legacy", _simple_wsgi_app)
            proto, _ = app._mounts["/legacy"]
            assert proto == "wsgi"

    def test_invalid_protocol_raises_value_error(self) -> None:
        from vertebra.web.app import VertebraApp

        app = VertebraApp()
        with pytest.raises(ValueError, match="protocol"):
            app.mount("/api", _simple_asgi_app, protocol="grpc")

    def test_mounted_runner_receives_mount_prefix(self) -> None:
        from vertebra.asgi.runner import AsgiRunner
        from vertebra.web.app import VertebraApp

        app = VertebraApp()
        with patch.object(app._router, "register_prefix"):
            app.mount("/v1", _simple_asgi_app, protocol="asgi")
            _, runner = app._mounts["/v1"]
            assert isinstance(runner, AsgiRunner)
            assert runner._adapter._mount_prefix == "/v1"
```

---

## 12. Completion Gate

All of the following must pass before this chapter is marked complete:

| Gate | Command / Check |
|------|-----------------|
| Lint clean | `make lint` exits 0 |
| All tests pass | `make test` exits 0 |
| ASGI scope conformance | `test_asgi_scope.py` all tests green |
| ReceiveChannel/SendCollector protocol | `test_asgi_queues.py` all tests green |
| Adapter batch isolation | `test_asgi_adapter.py::TestAsgiAdapter::test_mixed_batch_failure_isolation` green |
| TaskGroup used in adapter | `grep -n "TaskGroup" src/py/vertebra/asgi/adapter.py` non-empty |
| FastAPI mountable | `test_asgi_fastapi.py::TestFastApiViaAsgiAdapter::test_hello_route` green |
| Litestar mountable | `test_asgi_litestar.py::TestLitestarViaAsgiAdapter::test_hello_route` green |
| Mount API | `test_asgi_mount.py` all tests green |
| Performance floor | `wrk -t4 -c100 -d10s http://localhost:8000/api/hello` sustains >= 80,000 req/s |

---

## 13. Known Limitations and Follow-Up Work

### 13.1 Streaming Responses

The current `SendCollector` accumulates all body chunks in memory and joins them after the ASGI coroutine completes. This is correct for the common case (single `http.response.body` chunk) and for moderate-sized streaming responses.

For large streaming responses (file downloads, server-sent events), this pattern:
- Holds the entire response body in memory before returning to the batch runner.
- Blocks the batch from completing until the last chunk is received.

A follow-up chapter should implement a streaming-aware `SendCollector` that can pass chunks back to the Rust transport incrementally. This requires the Ch6 FFI layer to support partial response submission.

### 13.2 WebSocket Support

The current implementation handles only `type="http"` scopes. ASGI also defines WebSocket connection scopes (`type="websocket"`). WebSocket support is out of scope for this chapter; it requires a persistent connection model that is incompatible with the batch-and-return architecture.

WebSocket support will be addressed in a separate spec once the HTTP data plane is stable.

### 13.3 HTTP/3 PUSH

HTTP/3 server push (ASGI extension `http.response.push`) is not implemented. The `extensions` dict in the scope is empty, which correctly signals to the ASGI app that push is not available.

### 13.4 Extension Direct Invocation Depth

The `LitestarExtension.try_handle()` must implement direct handler invocation for simple handlers (no complex middleware chains) as the primary path. `FastApiExtension.try_handle()` follows the same pattern. Both extensions must:

- Extract routes at mount time via `extract_routes()`
- Call simple handlers directly, bypassing ASGI scope/receive/send
- Fall back to generic ASGI only for routes with complex middleware, streaming, or DI requirements

Full middleware-chain replication and complex DI bypass are deferred to a follow-up optimisation pass.

### 13.5 Event Loop Per Worker

`AsgiRunner` creates one `asyncio.new_event_loop()` per instance. This is correct for a worker-per-process model. In a future embedded-worker model (single process, multiple runners), loop ownership needs to be revisited to avoid conflicts with the process-level event loop.

---

## 14. Dependencies

### Runtime

- `asyncio` — stdlib (Python 3.11+, for TaskGroup)
- `vertebra._native` — PyO3 extension (Ch6): `VertebraRequest`, `VertebraHeaders`

### Optional

- `fastapi` — required only for `test_asgi_fastapi.py` and `FastApiExtension` activation
- `litestar` — required only for `test_asgi_litestar.py` and `LitestarExtension` activation

### Dev / Test

- `pytest` — test runner
- `pytest-asyncio` — async test support (if needed for standalone async tests)

---

## 15. References

- ASGI 3.0 Specification: https://asgi.readthedocs.io/en/latest/specs/main.html
- ASGI HTTP Connection Scope: https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope
- PEP 3333 (WSGI): https://peps.python.org/pep-3333/
- Research: `.agent/research/research_20260314_litecore_asgi_native_wsgi_gap/research.md`
- Prior specs: `.agent/specs/asgi-adapter_20260314/spec.md`, `.agent/specs/asgi-core-adapter_20260314/spec.md`, `.agent/specs/asgi-extension-support_20260314/spec.md`
- Preceding chapter (WSGI Adapter): `.agent/specs/wsgi-adapter/spec.md`
- Ghost Framework Rewrite plan: `.agent/plans/ghost-framework-rewrite.md`
