"""Tests for vertebra.server._asgi — scope, queues, runner, and wire helpers."""

import asyncio
import threading
from collections.abc import Awaitable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, cast

import pytest

from vertebra._vertebra import (
    STREAM_FLAG_ERROR,
    STREAM_FLAG_FINAL,
    STREAM_FLAG_HEADERS_SENT,
    STREAM_FLAG_STREAMING,
    STREAM_FLAG_TRAILERS,
    AsgiProtocol,
    AsgiScope,
    PyEmptyAwaitable,
    PyValueAwaitable,
    Request,
    _set_stream_sink,
    drive_asgi_batch,
    drive_native_batch,
    init_transport,
)
from vertebra.framework._router import get
from vertebra.server._asgi._queues import (
    AsgiProtocolError,
    AsgiReceive,
    AsgiResponse,
    AsgiSend,
)
from vertebra.server._asgi._runner import AsgiLifespan, AsgiRunner
from vertebra.server._asgi._wire import (
    HeaderFilter,
    compile_guard_pattern,
    is_static_route,
)
from vertebra.server._dispatch import compile_handler_spec
from vertebra.server._fastpath_core import (
    FASTPATH_ATTR,
    CompiledRouteBase,
    get_attached_fastpath,
    has_custom_exception_handlers,
    set_attached_fastpath,
)
from vertebra.server._loops import EventLoopPool
from vertebra.server.worker import _AsgiMountRunner


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
    hdrs = cast("list[tuple[bytes, bytes]]", scope["headers"])
    assert (b"content-type", b"application/json") in hdrs
    assert (b"x-req-id", b"abc") in hdrs


def test_asgi_receive_returns_body_then_blocks_until_disconnect() -> None:
    recv = AsgiReceive(b"hello")

    async def _run() -> tuple[dict[str, Any], dict[str, Any]]:
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

    async def _run() -> dict[str, Any]:
        await recv()

        async def _trigger() -> None:
            await asyncio.sleep(0)
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
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
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
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        raise ValueError("boom")

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.run_single(MockRequest(request_id=3)))
    assert resp.status == 500
    assert b"Internal Server Error" in resp.body
    assert resp.request_id == 3


def test_asgi_runner_app_exception_after_start_returns_500() -> None:
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("mid-flight crash")

    runner = AsgiRunner(app)
    resp = asyncio.run(runner.run_single(MockRequest(request_id=4)))
    assert resp.status == 500
    assert resp.request_id == 4


def test_asgi_runner_run_batch_concurrent() -> None:
    order: list[int] = []
    fast_done = asyncio.Event()

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope["path"]
        idx = int(path.lstrip("/"))
        if idx == 0:
            await asyncio.wait_for(fast_done.wait(), timeout=2.0)
        order.append(idx)
        if idx == 1:
            fast_done.set()
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
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
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
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        pass

    runner = AsgiRunner(app)
    assert runner([]) == []
    runner.close()


def test_asgi_runner_custom_loop_not_closed_by_runner() -> None:
    loop = asyncio.new_event_loop()

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
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

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
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
    seen: list[dict[str, Any]] = []

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
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


def test_rust_empty_and_value_awaitables_complete_without_suspension() -> None:
    """Verify PyEmptyAwaitable and PyValueAwaitable resolve synchronously and thread-safely."""
    shared_empty = PyEmptyAwaitable()
    shared_val = PyValueAwaitable({"ok": True})

    def _step_worker(_idx: int) -> tuple[object, object]:
        coro_empty = shared_empty.__await__()
        with pytest.raises(StopIteration) as empty_stop:
            next(coro_empty)
        coro_val = shared_val.__await__()
        with pytest.raises(StopIteration) as val_stop:
            next(coro_val)
        return (empty_stop.value.value, val_stop.value.value)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_step_worker, range(64)))

    assert all(r == (None, {"ok": True}) for r in results)


@pytest.mark.anyio
async def test_asgi_protocol_unary_multi_chunk_and_disconnect() -> None:
    """Verify AsgiProtocol state machine across unary, multi-chunk, and disconnect events."""
    proto = AsgiProtocol(b"inbound-body", request_id=41)
    first_msg = await proto.receive()
    assert first_msg == {"type": "http.request", "body": b"inbound-body", "more_body": False}

    await proto.send(
        {
            "type": "http.response.start",
            "status": 202,
            "headers": [(b"content-type", b"text/plain"), (b"x-mode", b"rust")],
        }
    )
    assert proto.started is True
    assert proto.complete is False
    await proto.send({"type": "http.response.body", "body": b"part-a|", "more_body": True})
    assert proto.complete is False
    await proto.send({"type": "http.response.body", "body": b"part-b", "more_body": False})
    assert proto.complete is True

    resp = proto.take_response()
    assert resp.status == 202
    assert resp.body == b"part-a|part-b"
    assert resp.request_id == 41
    assert (b"x-mode", b"rust") in resp.headers

    proto.notify_disconnect()
    disc_msg = await proto.receive()
    assert disc_msg == {"type": "http.disconnect"}


@pytest.mark.anyio
async def test_drive_asgi_batch_unary_zero_task_and_suspending_fallback() -> None:
    """Verify drive_asgi_batch executes unary handlers with zero tasks and resumes suspending coroutines."""
    init_transport()
    seen_bodies: list[bytes] = []
    loop = asyncio.get_running_loop()
    orig_create_task = loop.create_task
    task_creations: list[asyncio.Task[Any]] = []

    def tracked_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        t = orig_create_task(coro, **kwargs)
        task_creations.append(t)
        return t

    async def unary_app(scope: Any, receive: Any, send: Any) -> None:
        event = await receive()
        seen_bodies.append(event["body"])
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": scope["path"].encode("utf-8")})

    reqs = [
        Request.from_parts("POST", "/u1", b"b1", request_id=501),
        Request.from_parts("POST", "/u2", b"b2", request_id=502),
    ]
    create_task_attr = "create_task"
    setattr(loop, create_task_attr, tracked_create_task)
    try:
        inline_count = drive_asgi_batch(unary_app, reqs, loop_obj=loop)
    finally:
        setattr(loop, create_task_attr, orig_create_task)

    assert inline_count == 2
    assert len(task_creations) == 0
    assert seen_bodies == [b"b1", b"b2"]

    completed_async: list[str] = []
    release_sleep = asyncio.Event()

    async def mixed_app(scope: Any, receive: Any, send: Any) -> None:
        path = scope["path"]
        if path == "/boom":
            raise RuntimeError("boom")
        if path == "/sleep":
            await asyncio.wait_for(release_sleep.wait(), timeout=2.0)
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})
        completed_async.append(path)

    mixed_reqs = [
        Request.from_parts("GET", "/fast", b"", request_id=601),
        Request.from_parts("GET", "/sleep", b"", request_id=602),
        Request.from_parts("GET", "/boom", b"", request_id=603),
    ]
    task_creations.clear()
    setattr(loop, create_task_attr, tracked_create_task)
    try:
        inline_mixed = drive_asgi_batch(mixed_app, mixed_reqs, loop_obj=loop)
        assert inline_mixed == 2
        assert len(task_creations) == 1
        release_sleep.set()
        await asyncio.gather(*task_creations)
    finally:
        setattr(loop, create_task_attr, orig_create_task)

    assert completed_async == ["/fast", "/sleep"]


def test_asgi_lifespan_startup_shutdown_state_and_unsupported_app() -> None:
    """Verify AsgiLifespan executes startup/shutdown hooks, populates state, and ignores HTTP-only apps."""
    events: list[str] = []

    async def lifespan_app(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    events.append("startup")
                    scope["state"]["db_pool"] = "ready"
                    await send({"type": "lifespan.startup.complete"})
                elif msg["type"] == "lifespan.shutdown":
                    events.append("shutdown")
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        await receive()
        state_val = scope.get("state", {}).get("db_pool", "missing")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": state_val.encode("utf-8")})

    with AsgiLifespan(lifespan_app) as state:
        assert events == ["startup"]
        assert state == {"db_pool": "ready"}
        runner = AsgiRunner(lifespan_app, lifespan_state=state)
        try:
            res = runner([MockRequest(path="/check")])
            assert res[0].status == 200
            assert res[0].body == b"ready"
        finally:
            runner.close()

    assert events == ["startup", "shutdown"]

    async def failing_lifespan_app(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.failed", "message": "db offline"})

    with pytest.raises(RuntimeError, match="db offline"), AsgiLifespan(failing_lifespan_app):
        pass

    async def http_only_app(scope: Any, receive: Any, send: Any) -> None:
        assert scope["type"] == "http"
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lifespan_ctx = AsgiLifespan(http_only_app)
    with lifespan_ctx as empty_state:
        assert empty_state == {}
        assert lifespan_ctx.supported is False


@pytest.mark.anyio
async def test_asgi_scope_and_protocol_body_bytes_and_raw_response() -> None:
    """Verify AsgiScope.body_bytes, AsgiProtocol.body_bytes/__call__/send_raw_response, and AsgiSend.send_raw_response."""
    scope = AsgiScope.from_parts(method="POST", path="/echo", body=b"raw-body")
    assert scope.body_bytes() == b"raw-body"

    proto = AsgiProtocol(b"proto-body", request_id=99)
    assert proto.body_bytes() == b"proto-body"
    await cast(
        "Awaitable[None]", proto.send_raw_response(201, [(b"content-type", b"application/json")], b'{"ok":true}')
    )
    assert proto.started is True
    assert proto.complete is True
    resp = proto.take_response()
    assert resp.status == 201
    assert resp.body == b'{"ok":true}'
    assert (b"content-type", b"application/json") in resp.headers

    proto2 = AsgiProtocol(b"", request_id=100)
    await cast("Awaitable[None]", proto2({"type": "http.response.start", "status": 204, "headers": []}))
    await cast("Awaitable[None]", proto2({"type": "http.response.body", "body": b""}))
    assert proto2.take_response().status == 204

    send = AsgiSend(request_id=77)
    raw_pair = (b"x-fast", b"1")
    backing = bytearray(b"fast-zero-copy-body")
    await cast("Awaitable[None]", send.send_raw_response(202, [raw_pair], memoryview(backing)))
    assert send.started is True
    assert send.complete is True
    assert send.raw_headers == [raw_pair]
    assert send.raw_headers[0] is raw_pair
    assert send.response.status == 202
    assert isinstance(send.response.body, memoryview)
    assert send.response.zero_copy_buffer is not None
    assert send.response.zero_copy_buffer.obj is backing
    assert bytes(send.response.body) == b"fast-zero-copy-body"
    assert send.response.raw_headers == [raw_pair]
    assert send.response.headers == [("x-fast", "1")]


@pytest.mark.anyio
async def test_drive_native_batch_and_flattened_compile_handler_spec() -> None:
    """Verify flattened compile_handler_spec and drive_native_batch C-API PyIter_Send stepping."""

    @get("/users/{user_id:int}")
    async def parameterized_async(user_id: int, tag: str = "v1") -> dict[str, Any]:
        return {"user_id": user_id, "tag": tag}

    spec = compile_handler_spec(parameterized_async)
    req = Request.from_parts("GET", "/users/42?tag=fast", request_id=701, handler_id=1)
    coro = spec.invoker(req)
    try:
        assert coro.__qualname__ == parameterized_async.__qualname__
    finally:
        coro.close()

    init_transport()
    loop = asyncio.get_running_loop()
    orig_create_task = loop.create_task
    task_creations: list[asyncio.Task[Any]] = []

    def tracked_create_task(c: Any, **kwargs: Any) -> asyncio.Task[Any]:
        t = orig_create_task(c, **kwargs)
        task_creations.append(t)
        return t

    completed_tags: list[str] = []
    release_slow = asyncio.Event()

    async def fast_coro() -> dict[str, Any]:
        completed_tags.append("fast")
        return {"ok": True}

    async def slow_coro() -> bytes:
        await asyncio.wait_for(release_slow.wait(), timeout=2.0)
        completed_tags.append("slow")
        return b"slow-done"

    create_task_attr = "create_task"
    setattr(loop, create_task_attr, tracked_create_task)
    try:
        inline = drive_native_batch(
            [
                (710, fast_coro(), 200),
                (711, slow_coro(), 200),
            ],
            loop_obj=loop,
        )
        assert inline == 1
        assert len(task_creations) == 1
        release_slow.set()
        await asyncio.gather(*task_creations)
    finally:
        setattr(loop, create_task_attr, orig_create_task)

    assert completed_tags == ["fast", "slow"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("chunk_kind", "first_msg", "second_msg", "expected_first", "expected_second"),
    [
        (
            "body",
            {"type": "http.response.body", "body": b"alpha|", "more_body": True},
            {"type": "http.response.body", "body": b"omega", "more_body": False},
            b"alpha|",
            b"omega",
        ),
        (
            "zerocopysend",
            {"type": "http.response.zerocopysend", "buffer": memoryview(b"zc-1|"), "more_body": True},
            {"type": "http.response.zerocopysend", "buffer": memoryview(b"zc-2"), "more_body": False},
            b"zc-1|",
            b"zc-2",
        ),
        (
            "sse",
            {"type": "http.response.sse", "event": "tick", "data": "one", "more_body": True},
            {"type": "http.response.sse", "data": "", "more_body": False},
            b"event: tick\r\ndata: one\r\n\r\n",
            b"",
        ),
    ],
)
async def test_rust_asgi_protocol_flushes_progressive_chunks_before_gate_release(
    chunk_kind: str,
    first_msg: dict[str, Any],
    second_msg: dict[str, Any],
    expected_first: bytes,
    expected_second: bytes,
) -> None:
    """Verify Rust AsgiProtocol flushes body, zerocopysend, and sse chunks before generator gate release."""
    del chunk_kind
    init_transport()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    gate = asyncio.Event()
    first_flushed = asyncio.Event()
    terminal_flushed = asyncio.Event()

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        frames.append((req_id, status, flags, headers, body))
        if len(frames) == 1:
            first_flushed.set()
        if (flags & (STREAM_FLAG_FINAL | STREAM_FLAG_ERROR)) != 0:
            terminal_flushed.set()
        return True

    async def streaming_app(scope: Any, receive: Any, send: Any) -> None:
        del scope
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        await send(first_msg)
        await send({"type": "http.response.body", "body": b"", "more_body": True})
        await gate.wait()
        await send(second_msg)

    loop = asyncio.get_running_loop()
    _set_stream_sink(sink)
    try:
        drive_asgi_batch(
            streaming_app,
            [Request.from_parts("GET", "/events", b"", request_id=901)],
            loop_obj=loop,
        )
        await asyncio.wait_for(first_flushed.wait(), timeout=1.0)
        assert not gate.is_set()
        assert frames == [
            (
                901,
                200,
                STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                [(b"content-type", b"text/event-stream")],
                expected_first,
            )
        ]
        gate.set()
        await asyncio.wait_for(terminal_flushed.wait(), timeout=1.0)
        assert len(frames) == 2
        assert frames[1] == (
            901,
            200,
            STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
            [],
            expected_second,
        )
    finally:
        _set_stream_sink(None)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", ["trailers", "mid_stream_error", "disconnect"])
async def test_rust_asgi_protocol_trailers_error_and_disconnect(scenario: str) -> None:
    """Verify Rust AsgiProtocol defers FLAG_FINAL for trailers, emits FLAG_ERROR, and signals disconnect."""
    init_transport()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    terminal_flushed = asyncio.Event()
    accept_client = scenario != "disconnect"

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        frames.append((req_id, status, flags, headers, body))
        if (flags & (STREAM_FLAG_FINAL | STREAM_FLAG_ERROR)) != 0:
            terminal_flushed.set()
        return accept_client

    _set_stream_sink(sink)
    try:
        if scenario == "trailers":
            proto_trailers = AsgiProtocol(b"", request_id=910, direct_submit=True)
            await proto_trailers.send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"application/grpc")],
                    "trailers": True,
                }
            )
            await proto_trailers.send({"type": "http.response.body", "body": b"grpc-frame", "more_body": False})
            assert len(frames) == 1
            assert frames[0][2] == (STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT)
            assert frames[0][4] == b"grpc-frame"
            await proto_trailers.send(
                {
                    "type": "http.response.trailers",
                    "headers": [(b"grpc-status", b"0")],
                    "more_trailers": False,
                }
            )
            assert len(frames) == 2
            assert frames[1] == (
                910,
                200,
                STREAM_FLAG_STREAMING | STREAM_FLAG_TRAILERS | STREAM_FLAG_FINAL,
                [(b"grpc-status", b"0")],
                b"",
            )
        elif scenario == "mid_stream_error":
            loop = asyncio.get_running_loop()

            async def crashing_stream_app(scope: Any, receive: Any, send: Any) -> None:
                del scope, receive
                await send({"type": "http.response.start", "status": 200, "headers": []})
                await send({"type": "http.response.body", "body": b"partial", "more_body": True})
                await asyncio.sleep(0)
                msg = "stream failed mid-flight"
                raise RuntimeError(msg)

            drive_asgi_batch(
                crashing_stream_app,
                [Request.from_parts("GET", "/crash", b"", request_id=911)],
                loop_obj=loop,
            )
            await asyncio.wait_for(terminal_flushed.wait(), timeout=1.0)
            assert [f[2] for f in frames] == [
                STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
            ]
            assert frames[0][4] == b"partial"
        else:
            proto_disc = AsgiProtocol(b"", request_id=912, direct_submit=True)
            await proto_disc.receive()
            disc_fut = proto_disc.receive()
            await proto_disc.send({"type": "http.response.start", "status": 200, "headers": []})
            with pytest.raises(ConnectionResetError):
                await proto_disc.send({"type": "http.response.body", "body": b"drop", "more_body": True})
            disc_event = await asyncio.wait_for(disc_fut, timeout=1.0)
            assert disc_event == {"type": "http.disconnect"}
    finally:
        _set_stream_sink(None)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["multi_chunk", "empty_start_flush", "trailers", "unary_unchanged"],
)
async def test_python_asgi_send_stream_submitter_modes(scenario: str) -> None:
    """Verify Python AsgiSend streams progressively via stream_submitter without buffering."""
    emitted: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []

    def submitter(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> None:
        emitted.append((req_id, status, flags, headers, body))

    send = AsgiSend(request_id=70, stream_submitter=submitter)
    if scenario == "multi_chunk":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [("content-type", "text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"c1|", "more_body": True})
        assert len(emitted) == 1
        await send({"type": "http.response.body", "body": b"", "more_body": True})
        assert len(emitted) == 1
        await send({"type": "http.response.body", "body": b"c2", "more_body": False})
        assert send.streamed is True
        assert send.response.body == b""
        assert emitted == [
            (70, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, [(b"content-type", b"text/plain")], b"c1|"),
            (70, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b"c2"),
        ]
    elif scenario == "empty_start_flush":
        await send(
            {
                "type": "http.response.start",
                "status": 202,
                "headers": [(b"x-flush", b"1")],
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": True})
        assert emitted == [
            (70, 202, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, [(b"x-flush", b"1")], b""),
        ]
        await send({"type": "http.response.body", "body": b"done", "more_body": False})
        assert emitted[1] == (70, 202, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b"done")
    elif scenario == "trailers":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-proto", b"h2")],
                "trailers": True,
            }
        )
        await send({"type": "http.response.body", "body": b"payload", "more_body": False})
        assert len(emitted) == 1
        assert emitted[0][2] == (STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT)
        await send({"type": "http.response.trailers", "headers": [("x-digest", "sha256")]})
        assert emitted[1] == (
            70,
            200,
            STREAM_FLAG_STREAMING | STREAM_FLAG_TRAILERS | STREAM_FLAG_FINAL,
            [(b"x-digest", b"sha256")],
            b"",
        )
    else:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"unary", "more_body": False})
        assert send.streamed is False
        assert emitted == []
        assert send.response.body == b"unary"


@pytest.mark.anyio
@pytest.mark.parametrize("runner_kind", ["asgi_runner", "mount_runner"])
@pytest.mark.parametrize("outcome", ["ok", "mid_stream_error", "disconnect"])
async def test_python_asgi_execute_and_submit_streaming_and_errors(
    runner_kind: str,
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify AsgiRunner and _AsgiMountRunner stream fallback frames, emit FLAG_ERROR, and handle disconnect."""
    stream_calls: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    unary_calls: list[Any] = []
    disconnect_observed: list[dict[str, Any]] = []

    def fake_stream_submit(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> None:
        stream_calls.append((req_id, status, flags, headers, body))
        if outcome == "disconnect":
            raise ConnectionResetError("peer closed")

    monkeypatch.setattr("vertebra.server._asgi._runner._native_submit_stream_frame", fake_stream_submit)
    monkeypatch.setattr("vertebra.server._asgi._runner._native_submit_response", unary_calls.append)
    monkeypatch.setattr("vertebra.server.worker.submit_stream_frame", fake_stream_submit)
    monkeypatch.setattr("vertebra.server.worker.submit_response", unary_calls.append)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        del scope
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"x-m", b"1")]})
        try:
            await send({"type": "http.response.body", "body": b"first-", "more_body": True})
        except ConnectionResetError:
            disconnect_observed.append(await receive())
            raise
        if outcome == "mid_stream_error":
            msg = "boom after start"
            raise ValueError(msg)
        await send({"type": "http.response.body", "body": b"second", "more_body": False})

    req = MockRequest(path="/stream", request_id=950)
    if runner_kind == "asgi_runner":
        runner = AsgiRunner(app)
        await runner._execute_and_submit(req, 950)
    else:
        pool = EventLoopPool(size=2)
        try:
            mount_runner = _AsgiMountRunner(app, pool)
            await mount_runner._execute_and_submit(req)
        finally:
            pool.stop()

    assert unary_calls == []
    if outcome == "ok":
        assert [c[2] for c in stream_calls] == [
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
        ]
        assert b"".join(c[4] for c in stream_calls) == b"first-second"
    elif outcome == "mid_stream_error":
        assert [c[2] for c in stream_calls] == [
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
        ]
    else:
        assert len(stream_calls) == 1
        assert disconnect_observed == [{"type": "http.disconnect"}]


def test_asgi_lifespan_startup_timeout_cancels_and_awaits_task() -> None:
    """AsgiLifespan.startup cancels and awaits the stalled lifespan task when startup times out."""
    task_cancelled = asyncio.Event()

    async def stalled_lifespan(scope: dict[str, Any], receive: Any, _send: Any) -> None:
        if scope["type"] != "lifespan":
            return
        msg = await receive()
        assert msg["type"] == "lifespan.startup"
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    async def _run_check() -> None:
        lifespan = AsgiLifespan(stalled_lifespan, timeout=0.05)
        with pytest.raises(TimeoutError):
            await lifespan.startup()
        assert task_cancelled.is_set()
        assert lifespan._task is not None
        assert lifespan._task.done()

    asyncio.run(_run_check())


class _SlottedDictStateApp:
    __slots__ = ("state",)

    def __init__(self) -> None:
        self.state: dict[str, Any] = {}


class _SlottedAttrStateApp:
    __slots__ = ("state",)

    def __init__(self) -> None:
        self.state = SimpleNamespace()


class _SlottedStatelessApp:
    __slots__ = ()


def _custom_exception_handler(_req: Any, _exc: Any) -> None:
    return None


@pytest.mark.parametrize("target_kind", ["dict_attr", "slotted_dict_state", "slotted_attr_state"])
def test_attached_fastpath_helpers_store_and_read_single_attr(target_kind: str) -> None:
    """set_attached_fastpath and get_attached_fastpath use __vertebra_fastpath__ across app and state."""
    sentinel = SimpleNamespace(dispatch=lambda *_args, **_kwargs: None)
    if target_kind == "dict_attr":
        app: Any = SimpleNamespace()
    elif target_kind == "slotted_dict_state":
        app = _SlottedDictStateApp()
    else:
        app = _SlottedAttrStateApp()

    set_attached_fastpath(app, sentinel)
    assert get_attached_fastpath(app) is sentinel
    if target_kind == "dict_attr":
        assert getattr(app, FASTPATH_ATTR) is sentinel
    elif target_kind == "slotted_dict_state":
        assert app.state[FASTPATH_ATTR] is sentinel
    else:
        assert getattr(app.state, FASTPATH_ATTR) is sentinel


def test_attached_fastpath_rejects_stateless_slots_and_legacy_aliases() -> None:
    """Slotted stateless apps raise TypeError and legacy alias attributes are ignored."""
    stateless = _SlottedStatelessApp()
    sentinel = SimpleNamespace(dispatch=lambda *_args, **_kwargs: None)
    with pytest.raises(TypeError, match="cannot attach"):
        set_attached_fastpath(stateless, sentinel)
    assert get_attached_fastpath(stateless) is None

    legacy_app = SimpleNamespace(
        fastpath=sentinel,
        _vertebra_fastpath=sentinel,
        state={"vertebra_fastpath_bypass": sentinel},
        extensions={"vertebra": SimpleNamespace(fastpath=sentinel)},
    )
    assert get_attached_fastpath(legacy_app) is None
    assert AsgiRunner._resolve_fastpath(legacy_app) is None


def test_compiled_route_base_defaults_and_custom_exception_handlers() -> None:
    """CompiledRouteBase provides shared fields and has_custom_exception_handlers filters builtin prefixes."""
    route = CompiledRouteBase(handler_id=7)
    assert route.handler_id == 7
    assert route.is_async is False
    assert route.status_code == 200
    assert route.param_converters == ()

    builtin_fn = SimpleNamespace(__module__="starlette.exceptions")
    assert has_custom_exception_handlers(None, ("starlette.",)) is False
    assert has_custom_exception_handlers({}, ("starlette.",)) is False
    assert has_custom_exception_handlers({Exception: builtin_fn}, ("starlette.",)) is False
    assert has_custom_exception_handlers({Exception: _custom_exception_handler}, ("starlette.",)) is True
