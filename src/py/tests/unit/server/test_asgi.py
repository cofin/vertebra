"""Tests for vertebra.server._asgi — scope, queues, runner, and wire helpers."""

import asyncio
import threading
from typing import Any

import pytest

from vertebra._vertebra import AsgiScope, Request
from vertebra.server._asgi._queues import (
    AsgiProtocolError,
    AsgiReceive,
    AsgiResponse,
    AsgiSend,
)
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._asgi._wire import (
    HeaderFilter,
    compile_guard_pattern,
    is_static_route,
)


class MockRequest:
    """Minimal mock mimicking a Vertebra Request object."""

    def __init__(
        self,
        method: str = "GET",
        path: str = "/hello",
        raw_path: bytes | None = None,
        query: str | None = None,
        raw_query: bytes | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        request_id: int = 1,
        handler_id: int = 0,
        http_version: str = "1.1",
        scheme: str = "http",
        client: tuple[str, int] = ("127.0.0.1", 50000),
        server: tuple[str, int] = ("0.0.0.0", 8000),
    ) -> None:
        self.method = method
        self.path = path
        self.raw_path = raw_path if raw_path is not None else path.encode("latin-1")
        self.query = query
        self.raw_query = raw_query if raw_query is not None else (query.encode("latin-1") if query else b"")
        self.headers = headers or {}
        self.body = body
        self.request_id = request_id
        self.handler_id = handler_id
        self.http_version = http_version
        self.scheme = scheme
        self.client = client
        self.server = server


async def _noop_app(scope: Any, receive: Any, send: Any) -> None:
    del scope, receive, send


def _build_scope(req: Any, *, mount_prefix: str = "") -> AsgiScope:
    runner = AsgiRunner(_noop_app, mount_prefix=mount_prefix)
    return runner._build_scope(req)


def test_build_scope_basic_get() -> None:
    req = MockRequest(method="GET", path="/items", query="page=2")
    scope = _build_scope(req)
    assert scope["type"] == "http"
    assert scope["method"] == "GET"
    assert scope["path"] == "/items"
    assert scope["query_string"] == b"page=2"
    assert scope["asgi"] == {"version": "3.0", "spec_version": "2.4"}


def test_build_scope_raw_path_preserved() -> None:
    req = MockRequest(path="/café", raw_path=b"/caf%C3%A9")
    scope = _build_scope(req)
    assert scope["raw_path"] == b"/caf%C3%A9"


def test_build_scope_headers_encoded_as_byte_pairs() -> None:
    req = MockRequest(headers={"Content-Type": "application/json", "X-Custom": "val"})
    scope = _build_scope(req)
    hdrs = scope["headers"]
    assert isinstance(hdrs, list)
    assert (b"content-type", b"application/json") in hdrs
    assert (b"x-custom", b"val") in hdrs


def test_build_scope_lazy_headers_materialised_on_access() -> None:
    req = MockRequest(headers={"Host": "localhost"})
    scope = _build_scope(req)
    hdrs = scope["headers"]
    assert hdrs == [(b"host", b"localhost")]
    assert scope["headers"] == hdrs


def test_build_scope_lazy_headers_Resolved_By_get() -> None:
    req = MockRequest(headers={"Accept": "*/*"})
    scope = _build_scope(req)
    hdrs = scope.get("headers")
    assert hdrs == [(b"accept", b"*/*")]


def test_build_scope_lazy_headers_Resolved_By_items() -> None:
    req = MockRequest(headers={"X-Test": "1"})
    scope = _build_scope(req)
    items_dict = dict(scope.items())
    assert items_dict["headers"] == [(b"x-test", b"1")]


def test_build_scope_lazy_headers_Resolved_By_values() -> None:
    req = MockRequest(headers={"X-Val": "v"})
    scope = _build_scope(req)
    vals = list(scope.values())
    assert [(b"x-val", b"v")] in vals


def test_build_scope_lazy_headers_Resolved_By_copy() -> None:
    req = MockRequest(headers={"X-Copy": "c"})
    scope = _build_scope(req)
    copied = scope.copy()
    assert copied["headers"] == [(b"x-copy", b"c")]
    assert isinstance(copied, dict)


def test_build_scope_client_and_server_tuples() -> None:
    req = MockRequest(client=("10.0.0.1", 12345), server=("0.0.0.0", 9000))
    scope = _build_scope(req)
    assert scope["client"] == ("10.0.0.1", 12345)
    assert scope["server"] == ("0.0.0.0", 9000)


def test_build_scope_http_version_and_scheme_defaults() -> None:
    req = MockRequest()
    scope = _build_scope(req)
    assert scope["http_version"] == "1.1"
    assert scope["scheme"] == "http"


def test_build_scope_root_path_empty_by_default() -> None:
    req = MockRequest()
    scope = _build_scope(req)
    assert scope["root_path"] == ""


def test_build_scope_native_vertebra_request() -> None:
    """Verify AsgiRunner._build_scope works with a real Rust Request."""
    req = Request.from_parts(
        "POST",
        "/users",
        b'{"name":"alice"}',
        request_id=42,
        handler_id=7,
        headers=[("content-type", "application/json"), ("x-req-id", "abc")],
    )
    scope = _build_scope(req)
    assert scope["type"] == "http"
    assert scope["method"] == "POST"
    assert scope["path"] == "/users"
    assert scope["raw_path"] == b"/users"
    hdrs = scope["headers"]
    assert (b"content-type", b"application/json") in hdrs
    assert (b"x-req-id", b"abc") in hdrs


def test_asgi_receive_returns_body_then_blocks_until_disconnect() -> None:
    recv = AsgiReceive(b"hello")

    async def _run() -> tuple[dict, dict]:
        msg1 = await recv()
        disconnect_event = asyncio.Event()
        recv._disconnect_event = disconnect_event
        disconnect_event.set()
        msg2 = await recv()
        return msg1, msg2

    msg1, msg2 = asyncio.run(_run())
    assert msg1 == {"type": "http.request", "body": b"hello", "more_body": False}
    assert msg2 == {"type": "http.disconnect"}


def test_asgi_receive_empty_body_when_none() -> None:
    recv = AsgiReceive(b"")
    msg = asyncio.run(recv())
    assert msg == {"type": "http.request", "body": b"", "more_body": False}


def test_asgi_receive_waits_for_disconnect_event() -> None:
    """Second receive() blocks until disconnect_event is set."""
    ev = asyncio.Event()
    recv = AsgiReceive(b"data", disconnect_event=ev)

    async def _run() -> dict:
        await recv()

        async def _trigger() -> None:
            await asyncio.sleep(0.01)
            ev.set()

        task = asyncio.create_task(_trigger())
        msg = await recv()
        await task
        return msg

    msg = asyncio.run(_run())
    assert msg == {"type": "http.disconnect"}


def test_asgi_send_happy_path_single_chunk() -> None:
    send = AsgiSend(request_id=5)

    async def _run() -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"world",
                "more_body": False,
            }
        )

    asyncio.run(_run())
    resp = send.response
    assert resp.status == 200
    assert resp.body == b"world"
    assert resp.headers == [("content-type", "text/plain")]
    assert resp.request_id == 5


def test_asgi_send_multi_chunk_body_concatenation() -> None:
    send = AsgiSend()

    async def _run() -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"chunk1-", "more_body": True})
        await send({"type": "http.response.body", "body": b"chunk2", "more_body": False})

    asyncio.run(_run())
    assert send.response.body == b"chunk1-chunk2"


def test_asgi_send_body_before_start_raises() -> None:
    send = AsgiSend()
    with pytest.raises(AsgiProtocolError, match="before 'http.response.start'"):
        asyncio.run(send({"type": "http.response.body", "body": b"oops"}))


def test_asgi_send_duplicate_start_raises() -> None:
    send = AsgiSend()

    async def _run() -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.start", "status": 200, "headers": []})

    with pytest.raises(AsgiProtocolError, match="Duplicate"):
        asyncio.run(_run())


def test_asgi_send_body_after_complete_raises() -> None:
    send = AsgiSend()

    async def _run() -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"done", "more_body": False})
        await send({"type": "http.response.body", "body": b"extra"})

    with pytest.raises(AsgiProtocolError, match="already complete"):
        asyncio.run(_run())


def test_asgi_send_response_before_start_raises() -> None:
    send = AsgiSend()
    with pytest.raises(AsgiProtocolError, match="did not send"):
        _ = send.response


def test_asgi_send_unknown_message_type_raises() -> None:
    send = AsgiSend()

    async def _run() -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "websocket.accept"})

    with pytest.raises(AsgiProtocolError, match="Unexpected"):
        asyncio.run(_run())


def test_asgi_send_chunk_callback_invoked() -> None:
    chunks_seen: list[tuple[int, bytes]] = []
    send = AsgiSend(request_id=9, on_chunk=lambda rid, data: chunks_seen.append((rid, data)))

    async def _run() -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        await send({"type": "http.response.body", "body": b"b", "more_body": False})

    asyncio.run(_run())
    assert chunks_seen == [(9, b"a"), (9, b"b")]


def test_asgi_runner_simple_200_response() -> None:
    async def app(scope: dict, receive: Any, send: Any) -> None:
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-powered-by", b"vertebra")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.run_single(MockRequest(request_id=7)))
    assert resp.status == 200
    assert resp.body == b"ok"
    assert resp.request_id == 7


def test_asgi_runner_app_exception_before_start_returns_500() -> None:
    async def app(scope: dict, receive: Any, send: Any) -> None:
        raise ValueError("boom")

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.run_single(MockRequest(request_id=3)))
    assert resp.status == 500
    assert b"Internal Server Error" in resp.body
    assert resp.request_id == 3


def test_asgi_runner_app_exception_after_start_returns_500() -> None:
    async def app(scope: dict, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("mid-flight crash")

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.run_single(MockRequest(request_id=4)))
    assert resp.status == 500
    assert resp.request_id == 4


def test_asgi_runner_run_batch_concurrent() -> None:
    order: list[int] = []

    async def app(scope: dict, receive: Any, send: Any) -> None:
        path = scope["path"]
        idx = int(path.lstrip("/"))
        if idx == 0:
            await asyncio.sleep(0.02)
        order.append(idx)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": f"r{idx}".encode()})

    runner = AsgiRunner(app)
    reqs = [MockRequest(path="/0", request_id=10), MockRequest(path="/1", request_id=11)]
    resps = asyncio.run(runner.run_batch(reqs))
    assert len(resps) == 2
    assert resps[0].body == b"r0"
    assert resps[1].body == b"r1"
    assert order == [1, 0]


def test_asgi_runner_sync_batch_execution() -> None:
    async def app(scope: dict, receive: Any, send: Any) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 201, "headers": []})
        await send({"type": "http.response.body", "body": b"created"})

    runner = AsgiRunner(app)
    try:
        results = runner([MockRequest(request_id=1), MockRequest(request_id=2)])
        assert len(results) == 2
        assert all(isinstance(r, AsgiResponse) for r in results)
        assert results[0].status == 201
        assert results[1].body == b"created"
    finally:
        runner.close()


def test_asgi_runner_empty_batch_returns_empty() -> None:
    async def app(scope: dict, receive: Any, send: Any) -> None:
        pass

    runner = AsgiRunner(app)
    assert runner([]) == []
    runner.close()


def test_asgi_runner_custom_loop_not_closed_by_runner() -> None:
    loop = asyncio.new_event_loop()

    async def app(scope: dict, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hi"})

    runner = AsgiRunner(app, loop=loop)
    results = runner([MockRequest()])
    assert results[0].body == b"hi"
    runner.close()
    assert not loop.is_closed()
    loop.close()


def test_asgi_runner_Works_Inside_Running_Loop() -> None:
    """Runner uses run_coroutine_threadsafe when loop is already running."""
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()

    async def app(scope: dict, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"threaded"})

    try:
        runner = AsgiRunner(app, loop=loop)
        results = runner([MockRequest()])
        assert results[0].body == b"threaded"
    finally:
        loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=2)
        loop.close()


def test_asgi_wire_guard_pattern_compilation() -> None:
    pat = compile_guard_pattern("/users/{user_id}/posts/{post_id:int}")
    assert pat.match("/users/42/posts/99")
    assert not pat.match("/users/42")
    assert not pat.match("/users/42/posts/99/extra")


def test_asgi_wire_is_static_route() -> None:
    assert is_static_route("/health") is True
    assert is_static_route("/api/v1/items") is True
    assert is_static_route("/users/{id}") is False
    assert is_static_route("/files/{path:path}") is False


def test_asgi_wire_header_filter_required_and_blocked() -> None:
    hf = HeaderFilter(
        required={"authorization": "", "x-api-version": "2"},
        blocked={"x- banned"},
    )
    ok, code = hf.check({"Authorization": "Bearer tok", "X-Api-Version": "2"})
    assert ok is True
    assert code == 200

    ok, code = hf.check({"X-Api-Version": "2"})
    assert ok is False
    assert code == 401

    ok, code = hf.check({"Authorization": "Bearer tok", "X-Api-Version": "1"})
    assert ok is False
    assert code == 403

    ok, code = hf.check({"Authorization": "Bearer tok", "X-Api-Version": "2", "X- Banned": "yes"})
    assert ok is False
    assert code == 403


def test_build_scope_mount_prefix_strips_prefix_and_sets_root_path() -> None:
    req = MockRequest(path="/sub/items", raw_path=b"/sub/items")
    scope = _build_scope(req, mount_prefix="/sub")
    assert scope["path"] == "/items"
    assert scope["raw_path"] == b"/items"
    assert scope["root_path"] == "/sub"


def test_build_scope_mount_prefix_exact_prefix_becomes_slash() -> None:
    req = MockRequest(path="/sub", raw_path=b"/sub")
    scope = _build_scope(req, mount_prefix="/sub")
    assert scope["path"] == "/"
    assert scope["raw_path"] == b"/"
    assert scope["root_path"] == "/sub"


def test_asgi_runner_mount_prefix_propagated_to_scope() -> None:
    seen_scopes: list[Any] = []

    async def sub_app(scope: Any, receive: Any, send: Any) -> None:
        seen_scopes.append(scope)
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"mounted"})

    runner = AsgiRunner(sub_app, mount_prefix="/api/v2")
    try:
        results = runner([MockRequest(path="/api/v2/users", raw_path=b"/api/v2/users")])
        assert results[0].body == b"mounted"
        assert seen_scopes[0]["path"] == "/users"
        assert seen_scopes[0]["root_path"] == "/api/v2"
    finally:
        runner.close()


def test_asgi_runner_handle_path_convenience() -> None:
    """AsgiRunner.handle_path constructs a request from path and runs the app."""
    seen: list[dict] = []

    async def app(scope: dict, receive: Any, send: Any) -> None:
        msg = await receive()
        seen.append({"path": scope["path"], "method": scope["method"], "body": msg["body"]})
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-Echo", scope["path"].encode())],
            }
        )
        await send({"type": "http.response.body", "body": b"handled"})

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.handle_path("/test/path", method="POST", body=b"payload"))
    assert resp.status == 200
    assert resp.body == b"handled"
    assert seen[0]["path"] == "/test/path"
    assert seen[0]["method"] == "POST"
    assert seen[0]["body"] == b"payload"
