"""Tests for the ASGI adapter (Ch8)."""

from typing import Any

import pytest

# --- Mock objects ---


class MockHeaders:
    def __init__(self, headers: "dict[str, str] | None" = None):
        self._headers = {k.lower(): v for k, v in (headers or {}).items()}

    def get(self, key: str, default: Any = None) -> Any:
        return self._headers.get(key.lower(), default)

    def __getitem__(self, key: str) -> str:
        v = self._headers.get(key.lower())
        if v is None:
            raise KeyError(key)
        return v

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key.lower() in self._headers

    def __iter__(self):
        return iter(self._headers)

    def __len__(self):
        return len(self._headers)

    def items(self):
        return list(self._headers.items())

    def keys(self):
        return list(self._headers.keys())


class MockRequest:
    def __init__(self, method="GET", path="/", query=None, body=None, handler_id=1, request_id=1, headers=None):
        self.method = method
        self.path = path
        self.query = query
        self.body = body or b""
        self.handler_id = handler_id
        self.request_id = request_id
        self.headers = MockHeaders(headers)


# --- Scope ---


def test_build_scope_type_is_http() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest())
    assert scope["type"] == "http"


def test_build_scope_asgi_version() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest())
    assert scope["asgi"] == {"version": "3.0", "spec_version": "2.3"}


def test_build_scope_method_uppercase() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest(method="get"))
    assert scope["method"] == "GET"


def test_build_scope_headers_lazy() -> None:
    from vertebra.server._asgi._scope import _UNBUILT, build_scope

    scope = build_scope(MockRequest(headers={"accept": "text/html"}))
    assert dict.__getitem__(scope, "headers") is _UNBUILT
    headers = scope["headers"]
    assert isinstance(headers, list)
    assert all(isinstance(h, tuple) for h in headers)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("a=1", b"a=1"),
        (None, b""),
    ],
    ids=["present", "empty"],
)
def test_build_scope_query_string(query: "str | None", expected: bytes) -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest(query=query))
    assert scope["query_string"] == expected


def test_build_scope_raw_path_bytes() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest(path="/hello"))
    assert scope["raw_path"] == b"/hello"


def test_build_scope_state_is_fresh_dict() -> None:
    from vertebra.server._asgi._scope import build_scope

    s1 = build_scope(MockRequest())
    s2 = build_scope(MockRequest())
    assert s1["state"] is not s2["state"]


def test_build_scope_all_required_keys() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest())
    required = {
        "type",
        "asgi",
        "http_version",
        "method",
        "headers",
        "path",
        "raw_path",
        "query_string",
        "root_path",
        "scheme",
        "server",
        "client",
        "state",
        "extensions",
    }
    assert required.issubset(set(scope.keys()))


def test_build_scope_root_path() -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest(), root_path="/api")
    assert scope["root_path"] == "/api"


# --- Queues ---


@pytest.mark.anyio
async def test_receive_channel_first_call_returns_body() -> None:
    from vertebra.server._asgi._queues import ReceiveChannel

    rc = ReceiveChannel(MockRequest(body=b"hello"))
    msg = await rc()
    assert msg["type"] == "http.request"
    assert msg["body"] == b"hello"
    assert msg["more_body"] is False


@pytest.mark.anyio
async def test_receive_channel_second_call_disconnect() -> None:
    from vertebra.server._asgi._queues import ReceiveChannel

    rc = ReceiveChannel(MockRequest())
    await rc()
    msg = await rc()
    assert msg["type"] == "http.disconnect"


@pytest.mark.anyio
async def test_receive_channel_subsequent_calls_disconnect() -> None:
    from vertebra.server._asgi._queues import ReceiveChannel

    rc = ReceiveChannel(MockRequest())
    await rc()
    for _ in range(5):
        msg = await rc()
        assert msg["type"] == "http.disconnect"


@pytest.mark.anyio
async def test_send_collector_captures_status() -> None:
    from vertebra.server._asgi._queues import SendCollector

    sc = SendCollector()
    await sc({"type": "http.response.start", "status": 200, "headers": []})
    await sc({"type": "http.response.body", "body": b"ok"})
    r = sc.response
    assert r.status == 200
    assert r.body == b"ok"


@pytest.mark.anyio
async def test_send_collector_accumulates_chunks() -> None:
    from vertebra.server._asgi._queues import SendCollector

    sc = SendCollector()
    await sc({"type": "http.response.start", "status": 200, "headers": []})
    await sc({"type": "http.response.body", "body": b"abc", "more_body": True})
    await sc({"type": "http.response.body", "body": b"de", "more_body": False})
    assert sc.response.body == b"abcde"


@pytest.mark.anyio
async def test_send_collector_response_before_start_raises() -> None:
    from vertebra.server._asgi._queues import SendCollector

    sc = SendCollector()
    with pytest.raises(RuntimeError, match="http.response.start"):
        _ = sc.response


@pytest.mark.anyio
async def test_send_collector_unknown_type_raises() -> None:
    from vertebra.server._asgi._queues import SendCollector

    sc = SendCollector()
    with pytest.raises(RuntimeError, match="unexpected"):
        await sc({"type": "http.weirdmessage"})


@pytest.mark.anyio
async def test_send_collector_header_conversion() -> None:
    from vertebra.server._asgi._queues import SendCollector

    sc = SendCollector()
    await sc(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/html")],
        }
    )
    await sc({"type": "http.response.body", "body": b""})
    r = sc.response
    # SendCollector stores headers as-is; VertebraResponse handles byte→str.
    assert r.headers == [(b"content-type", b"text/html")]


# --- Adapter ---


@pytest.mark.anyio
async def test_asgi_adapter_simple_app() -> None:
    from vertebra.server._asgi._adapter import AsgiAdapter

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    adapter = AsgiAdapter(app)
    results = await adapter.run_batch([MockRequest()])
    assert len(results) == 1
    assert results[0].status == 200
    assert results[0].body == b"ok"


@pytest.mark.anyio
async def test_asgi_adapter_batch_of_five() -> None:
    from vertebra.server._asgi._adapter import AsgiAdapter

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        body = scope["path"].encode()
        await send({"type": "http.response.body", "body": body})

    adapter = AsgiAdapter(app)
    requests = [MockRequest(path=f"/p{i}") for i in range(5)]
    results = await adapter.run_batch(requests)
    assert len(results) == 5
    for i, r in enumerate(results):
        assert r.status == 200
        assert r.body == f"/p{i}".encode()


@pytest.mark.anyio
async def test_asgi_adapter_failing_task_returns_500() -> None:
    from vertebra.server._asgi._adapter import AsgiAdapter

    async def app(scope, receive, send):
        if scope["path"] == "/fail":
            raise ValueError("boom")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    adapter = AsgiAdapter(app)
    requests = [MockRequest(path="/ok"), MockRequest(path="/fail"), MockRequest(path="/ok2")]
    results = await adapter.run_batch(requests)
    assert results[0].status == 200
    assert results[1].status == 500
    assert results[2].status == 200


def test_asgi_runner_synchronous_interface() -> None:
    from vertebra.server._asgi._runner import AsgiRunner

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"sync"})

    runner = AsgiRunner(app)
    try:
        results = runner([MockRequest()])
        assert results[0].status == 200
        assert results[0].body == b"sync"
    finally:
        runner.close()


def test_asgi_runner_shared_running_loop_uses_threadsafe_submission() -> None:
    import asyncio
    import threading

    from vertebra.server._asgi._runner import AsgiRunner

    ready = threading.Event()
    holder: dict[str, asyncio.AbstractEventLoop] = {}

    def run_loop() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        holder["loop"] = loop
        ready.set()
        loop.run_forever()
        loop.close()

    thread = threading.Thread(target=run_loop, daemon=True)
    thread.start()
    assert ready.wait(timeout=1.0)

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"shared"})

    runner = AsgiRunner(app, loop=holder["loop"])
    try:
        results = runner([MockRequest()])
        assert results[0].status == 200
        assert results[0].body == b"shared"
    finally:
        holder["loop"].call_soon_threadsafe(holder["loop"].stop)
        thread.join(timeout=1.0)


# --- Extensions ---


def test_asgi_extensions_compile_guard_pattern() -> None:
    from vertebra.server._asgi._extensions import compile_guard_pattern

    pat = compile_guard_pattern("/items/{item_id}")
    assert pat.match("/items/42")
    assert pat.match("/items/abc")
    assert not pat.match("/items/42/sub")


def test_asgi_extensions_is_static_route() -> None:
    from vertebra.server._asgi._extensions import is_static_route

    assert is_static_route("/hello")
    assert not is_static_route("/items/{id}")
    assert not is_static_route("/items/:id")


def test_asgi_extensions_header_filter() -> None:
    from vertebra.server._asgi._extensions import HeaderFilter

    f = HeaderFilter("content-type", "application/json")
    assert f.matches([(b"content-type", b"application/json")])
    assert not f.matches([(b"content-type", b"text/html")])


def test_asgi_extensions_register_and_get_extensions() -> None:
    from vertebra.server._asgi._extensions import _EXTENSIONS, get_extensions, register_extension

    orig_len = len(_EXTENSIONS)
    sentinel = object()
    register_extension(sentinel)
    exts = get_extensions()
    assert sentinel in exts
    _EXTENSIONS.pop()  # cleanup
    assert len(_EXTENSIONS) == orig_len


# --- Ch2: ASGI fast-path optimizations ---


def test_asgi_scope_optimizations_asgi_version_shared_across_scopes() -> None:
    """The asgi version dict should be the same object (not copied)."""
    from vertebra.server._asgi._scope import build_scope

    s1 = build_scope(MockRequest())
    s2 = build_scope(MockRequest())
    assert s1["asgi"] is s2["asgi"]


@pytest.mark.parametrize(
    "key",
    ["server", "client"],
)
def test_asgi_scope_optimizations_none_when_no_host(key: str) -> None:
    from vertebra.server._asgi._scope import build_scope

    scope = build_scope(MockRequest())
    assert scope[key] is None


@pytest.mark.anyio
async def test_send_collector_fast_path_single_chunk_no_list() -> None:
    """Single-chunk response should not allocate _extra_chunks list."""
    from vertebra.server._asgi._queues import SendCollector

    c = SendCollector()
    await c({"type": "http.response.start", "status": 200, "headers": []})
    await c({"type": "http.response.body", "body": b"hello"})
    assert c._extra_chunks is None  # noqa: SLF001
    assert c.response.body == b"hello"


@pytest.mark.anyio
async def test_send_collector_fast_path_multi_chunk_joins() -> None:
    """Multiple chunks should accumulate and join correctly."""
    from vertebra.server._asgi._queues import SendCollector

    c = SendCollector()
    await c({"type": "http.response.start", "status": 200, "headers": []})
    await c({"type": "http.response.body", "body": b"abc"})
    await c({"type": "http.response.body", "body": b"de"})
    assert c._extra_chunks is not None  # noqa: SLF001
    assert c.response.body == b"abcde"


@pytest.mark.anyio
async def test_gather_adapter_one_failure_does_not_cancel_others() -> None:
    from vertebra.server._asgi._adapter import AsgiAdapter

    call_count = 0

    async def app(scope, receive, send):
        nonlocal call_count
        call_count += 1
        if scope["path"] == "/fail":
            msg = "boom"
            raise RuntimeError(msg)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    adapter = AsgiAdapter(app)
    reqs = [
        MockRequest(path="/ok", request_id=1),
        MockRequest(path="/fail", request_id=2),
        MockRequest(path="/ok", request_id=3),
    ]
    results = await adapter.run_batch(reqs)

    assert call_count == 3
    assert results[0].status == 200
    assert results[1].status == 500
    assert results[2].status == 200
