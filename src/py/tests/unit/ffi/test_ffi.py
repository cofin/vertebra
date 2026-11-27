"""Tests for the native FFI bridge (Ch6)."""

import asyncio
import gc
import runpy
import sys
import threading
import time
import weakref
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import msgspec
import pytest

from vertebra._vertebra import (
    STREAM_FLAG_ERROR,
    STREAM_FLAG_FINAL,
    STREAM_FLAG_HEADERS_SENT,
    STREAM_FLAG_STREAMING,
    STREAM_FLAG_TRAILERS,
    AsgiScopeIterator,
    Headers,
    HeadersIterator,
    ReadyAwaitable,
    Request,
    RequestBatch,
    RequestBatchIterator,
    Response,
    RsgiHttpProtocol,
    RsgiTransport,
    TaskAwaitable,
    TaskPanicError,
    WsgiEnviron,
    WsgiEnvironIterator,
    _set_stream_sink,
    _WsgiStartResponse,
    cancel_request_sleep,
    drive_native_batch,
    hydrate_one_vectorcall,
    init_transport,
    max_stream_frame_payload,
    prepare_request_sleep,
    submit_raw_response,
    submit_response,
    submit_stream_frame,
)
from vertebra.framework._app import App, _make_handler_id
from vertebra.framework._router import handler_id
from vertebra.server import worker as server_worker_module
from vertebra.server._dispatch import (
    BatchDispatchPlan,
    compile_handler_spec,
    dispatch_batch,
)
from vertebra.server._loops import EventLoopPool
from vertebra.supervisor import _worker as worker_module
from vertebra.utils.module_loader import import_app


def test_vertebra_response_construct_basic() -> None:
    r = Response(status=200, body=b"ok")
    assert r.status == 200
    assert bytes(r.body) == b"ok"
    assert r.request_id == 0


def test_vertebra_response_construct_empty_body() -> None:
    r = Response(status=404)
    assert r.status == 404
    assert bytes(r.body) == b""


def test_vertebra_response_construct_with_headers_dict() -> None:
    r = Response(
        status=200,
        body=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status == 200
    hdrs = list(r.headers)
    assert len(hdrs) == 1
    assert hdrs[0][0] == b"content-type"
    assert hdrs[0][1] == b"text/plain"


def test_vertebra_response_construct_with_headers_list() -> None:
    r = Response(
        status=200,
        headers=[("x-custom", "value")],
    )
    hdrs = list(r.headers)
    assert hdrs[0][0] == b"x-custom"


def test_vertebra_response_request_id_set() -> None:
    r = Response(status=200, request_id=42)
    assert r.request_id == 42


def test_vertebra_response_request_id_mutated_in_place() -> None:
    r = Response(status=200, body=b"ok", headers=[("x-test", "1")])
    assert r.request_id == 0
    r.request_id = 99
    assert r.request_id == 99
    assert list(r.headers) == [(b"x-test", b"1")]


def test_vertebra_response_repr() -> None:
    r = Response(status=200, body=b"hi", request_id=7)
    rep = repr(r)
    assert "200" in rep
    assert "7" in rep


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "/hello", 13629070520106334972),
        ("POST", "/api/users", 5612694501581302581),
        ("GET", "/", 14915177997962173262),
        ("DELETE", "/items/{id}", 9853804240848949641),
    ],
)
def test_handler_id_cross_language(method: str, path: str, expected: int) -> None:
    """Verify Python handler IDs match Rust reference values."""
    assert _make_handler_id(method, path) == expected


def test_handler_id_cross_language_routing_module_matches() -> None:
    assert handler_id("GET", "/hello") == _make_handler_id("GET", "/hello")


def test_worker_import_app_valid() -> None:
    app = import_app("vertebra.framework._app:App")
    assert app is App


def test_worker_import_app_invalid_format() -> None:
    with pytest.raises(ValueError, match="module:attr"):
        import_app("invalid_no_colon")


def test_worker_main_shm_transport_initialized_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """SHM workers must not initialize the native transport twice."""
    init_calls: list[str] = []
    stop_event = threading.Event()

    def fake_init_transport(*, mode: str = "inprocess", **_: object) -> None:
        init_calls.append(mode)
        if len(init_calls) > 1:
            msg = "transport initialized twice"
            raise RuntimeError(msg)

    def fake_get_batch(timeout_ms: int = 100) -> None:
        del timeout_ms
        stop_event.set()
        return None

    fake_app = SimpleNamespace(_handlers={}, mounts=[])
    orig_worker_cls = server_worker_module.Worker

    def make_worker(handlers: Any, **kwargs: Any) -> Any:
        kwargs["stop_event"] = stop_event
        return orig_worker_cls(handlers, **kwargs)

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: fake_app)
    monkeypatch.setattr(server_worker_module, "Worker", make_worker)
    monkeypatch.setattr(server_worker_module, "init_transport", fake_init_transport)
    monkeypatch.setattr(server_worker_module, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(server_worker_module, "get_batch", fake_get_batch)
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake.module:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    worker_module.main()

    assert init_calls == ["shm"]


def test_worker_main_clears_executor_on_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Worker lazily allocates ThreadPoolExecutor only when needed and shuts it down on exit."""
    init_calls: list[int] = []
    shutdown_calls: list[bool] = []
    stop_event = threading.Event()
    stop_event.set()

    def blocking_handler() -> bytes:
        return b"ok"

    inline_only_app = App()

    @inline_only_app.get("/fast")
    def fast_handler() -> bytes:
        return b"ok"

    threaded_app = App()
    threaded_app.get("/block", sync_to_thread=True)(blocking_handler)

    orig_worker_cls = server_worker_module.Worker

    class FakeExecutor:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            init_calls.append(1)

        def shutdown(self, *, wait: bool = False) -> None:
            shutdown_calls.append(wait)

    def make_worker(handlers: Any, **kwargs: Any) -> Any:
        kwargs["stop_event"] = stop_event
        return orig_worker_cls(handlers, **kwargs)

    monkeypatch.setattr(server_worker_module, "Worker", make_worker)
    monkeypatch.setattr(server_worker_module, "init_transport", lambda **kwargs: None)
    monkeypatch.setattr(server_worker_module, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(server_worker_module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake.module:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: inline_only_app)
    worker_module.main()
    assert init_calls == []
    assert shutdown_calls == []

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: threaded_app)
    worker_module.main()
    assert len(init_calls) == 1
    assert len(shutdown_calls) == 1
    assert not hasattr(server_worker_module, "_wsgi_executor")


def test_worker_module_executes_main_when_run_as_script(monkeypatch: pytest.MonkeyPatch) -> None:
    """python -m vertebra.supervisor._worker must invoke main()."""
    fake_app = SimpleNamespace(_handlers={}, mounts=[])
    fake_app_module: Any = ModuleType("fake_worker_app")
    fake_app_module.app = fake_app

    run_worker_calls: list[tuple[object, dict[str, object]]] = []

    monkeypatch.setitem(sys.modules, "fake_worker_app", fake_app_module)
    monkeypatch.delitem(sys.modules, "vertebra.supervisor._worker", raising=False)
    monkeypatch.setattr(
        server_worker_module,
        "run_worker",
        lambda handlers, **kwargs: run_worker_calls.append((handlers, kwargs)),
    )
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake_worker_app:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    runpy.run_module("vertebra.supervisor._worker", run_name="__main__")

    assert len(run_worker_calls) == 1
    assert run_worker_calls[0][0] == {}


def test_submit_response_exists() -> None:
    init_transport()
    resp = Response(status=200, body=b"hello", request_id=123)
    submit_response(resp)


def test_non_blocking_dispatch() -> None:
    init_transport()
    scheduler = EventLoopPool(size=1)

    async def slow_handler() -> bytes:
        await asyncio.sleep(0.1)
        return b"slow"

    class Req:
        def __init__(self, rid: int) -> None:
            self.request_id = rid
            self.handler_id = 1

    spec = compile_handler_spec(slow_handler)
    assert isinstance(hash(spec), int)
    batch = [Req(1), Req(2)]
    partitioned = BatchDispatchPlan(async_native=[(0, batch[0], spec), (1, batch[1], spec)])
    untyped_plan: Any = partitioned
    with pytest.raises(AttributeError):
        untyped_plan.async_native = []

    start = time.monotonic()
    dispatch_batch(partitioned, {1: spec}, [], Response, scheduler, async_mode=True)
    elapsed = time.monotonic() - start

    assert elapsed < 0.05
    scheduler.stop()


def test_dekker_request_sleep_helpers_roundtrip() -> None:
    """Verify prepare_request_sleep and cancel_request_sleep FFI helpers execute cleanly."""
    init_transport()
    should_sleep = prepare_request_sleep()
    assert isinstance(should_sleep, bool)
    assert should_sleep is True
    cancel_request_sleep()


def test_async_bridge_exports_preserved() -> None:
    """Verify async_bridge.rs exports ReadyAwaitable, TaskAwaitable, and TaskPanicError on vertebra._vertebra."""
    assert issubclass(TaskPanicError, RuntimeError)
    assert hasattr(TaskAwaitable, "__await__")
    assert hasattr(ReadyAwaitable, "__await__")
    assert hasattr(ReadyAwaitable, "__iter__")
    assert hasattr(ReadyAwaitable, "__next__")


@pytest.mark.parametrize(
    "invalid_flags",
    [
        0,
        STREAM_FLAG_FINAL,
        STREAM_FLAG_HEADERS_SENT,
        STREAM_FLAG_TRAILERS,
        STREAM_FLAG_ERROR,
    ],
)
def test_submit_stream_frame_rejects_missing_streaming_flag(invalid_flags: int) -> None:
    """Verify submit_stream_frame raises ValueError when STREAM_FLAG_STREAMING is absent."""
    init_transport()
    with pytest.raises(ValueError, match="STREAM_FLAG_STREAMING"):
        submit_stream_frame(1, 200, invalid_flags, None, b"chunk")


def test_submit_stream_frame_missing_sink_raises_runtime_error() -> None:
    """Verify submit_stream_frame raises RuntimeError when no stream sink is registered."""
    init_transport()
    _set_stream_sink(None)
    with pytest.raises(RuntimeError, match="stream"):
        submit_stream_frame(
            1,
            200,
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            [(b"content-type", b"text/plain")],
            b"chunk",
        )


@pytest.mark.parametrize(
    ("headers_in", "body_in", "flags_in", "accept", "expected_headers"),
    [
        (
            [("content-type", "text/event-stream"), (b"x-stream", b"1")],
            memoryview(b"data: hello\r\n\r\n"),
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            True,
            [(b"content-type", b"text/event-stream"), (b"x-stream", b"1")],
        ),
        (
            None,
            b"final-chunk",
            STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
            True,
            [],
        ),
        (
            [(b"x-trailer", b"ok")],
            b"",
            STREAM_FLAG_STREAMING | STREAM_FLAG_TRAILERS | STREAM_FLAG_FINAL,
            False,
            [(b"x-trailer", b"ok")],
        ),
    ],
)
def test_submit_stream_frame_embedded_delivery_and_closed_client(
    headers_in: Any,
    body_in: bytes | memoryview,
    flags_in: int,
    accept: bool,
    expected_headers: list[tuple[bytes, bytes]],
) -> None:
    """Verify embedded stream sink delivery and ConnectionResetError on StreamDelivery::Closed."""
    init_transport()
    delivered: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        delivered.append((req_id, status, flags, headers, body))
        return accept

    _set_stream_sink(sink)
    try:
        if accept:
            submit_stream_frame(55, 200, flags_in, headers_in, body_in)
            assert delivered == [(55, 200, flags_in, expected_headers, bytes(body_in))]
        else:
            with pytest.raises(ConnectionResetError):
                submit_stream_frame(55, 200, flags_in, headers_in, body_in)
            assert len(delivered) == 1
    finally:
        _set_stream_sink(None)


def test_submit_stream_frame_splits_payload_above_max_stream_frame_payload() -> None:
    """Verify payloads exceeding max_stream_frame_payload split cleanly across consecutive frames."""
    init_transport()
    max_payload = max_stream_frame_payload()
    assert max_payload > 65536
    payload = b"x" * (max_payload * 2 + 29)
    delivered: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        delivered.append((req_id, status, flags, headers, body))
        return True

    _set_stream_sink(sink)
    try:
        submit_stream_frame(
            88,
            200,
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT | STREAM_FLAG_FINAL,
            [("content-type", "application/octet-stream")],
            payload,
        )
        assert len(delivered) == 3
        assert delivered[0][2] == (STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT)
        assert delivered[0][3] == [(b"content-type", b"application/octet-stream")]
        assert delivered[1][2] == STREAM_FLAG_STREAMING
        assert delivered[1][3] == []
        assert delivered[2][2] == (STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL)
        assert delivered[2][3] == []
        assert b"".join(item[4] for item in delivered) == payload

        delivered.clear()
        encoded_slots = submit_raw_response(
            89,
            200,
            [("x-raw", "stream")],
            b"abcdef",
            flags=STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
            slot_capacity=64,
        )
        assert len(encoded_slots) >= 1
        assert delivered[0][0] == 89
        assert (delivered[0][2] & STREAM_FLAG_HEADERS_SENT) != 0
        assert (delivered[-1][2] & STREAM_FLAG_FINAL) != 0
    finally:
        _set_stream_sink(None)


def test_msgspec_decoder_cache_collects_dynamic_struct_class() -> None:
    """Verify dynamic msgspec.Struct types cached by msgspec_bridge collect after gc.collect()."""

    def _decode_dynamic() -> weakref.ref[type[Any]]:
        class DynamicPayload(msgspec.Struct):
            value: int

        req = Request.from_parts("POST", "/dynamic", b'{"value": 42}')
        decoded = req.decode_msgspec(DynamicPayload, "json")
        assert decoded.value == 42
        hydrated = hydrate_one_vectorcall(DynamicPayload, (7,))
        assert hydrated.value == 7
        return weakref.ref(DynamicPayload)

    cls_ref = _decode_dynamic()
    gc.collect()
    assert cls_ref() is None

    generic_req = Request.from_parts("POST", "/generic", b"[1, 2, 3]")
    assert generic_req.decode_msgspec(list[int], "json") == [1, 2, 3]


def test_stepped_coroutine_breaks_self_referential_cycle_on_gc() -> None:
    """Verify SteppedCoroutine cyclic GC hooks collect unfinished self-referential tasks."""

    class _CycleMarker:
        coro: Any = None

    def _spawn_cyclic_stepped_coroutine() -> weakref.ref[_CycleMarker]:
        loop = asyncio.new_event_loop()
        marker = _CycleMarker()
        marker_ref = weakref.ref(marker)

        async def _runner() -> None:
            fut = loop.create_future()

            async def _self_referential_handler() -> bytes:
                _ = marker
                await fut
                return b"unreachable"

            coro = _self_referential_handler()
            marker.coro = coro
            tasks_before = asyncio.all_tasks(loop)
            drive_native_batch([(77, coro)])
            for task in asyncio.all_tasks(loop) - tasks_before:
                cast("Any", task)._log_destroy_pending = False

        try:
            loop.run_until_complete(_runner())
        finally:
            loop.close()
        return marker_ref

    marker_ref = _spawn_cyclic_stepped_coroutine()
    gc.collect()
    assert marker_ref() is None


@pytest.mark.parametrize(
    ("offset", "length", "expected_body"),
    [
        (2, 4, b"cdef"),
        (2**64 - 4, 16, None),
    ],
)
def test_response_from_file_checks_range_overflow(
    tmp_path: Path,
    offset: int,
    length: int,
    expected_body: bytes | None,
) -> None:
    """Verify Response.from_file reads valid ranges and raises OverflowError on u64 overflow."""
    payload_path = tmp_path / "range_payload.bin"
    payload_path.write_bytes(b"abcdefgh")
    if expected_body is None:
        with pytest.raises(OverflowError):
            Response.from_file(str(payload_path), offset=offset, length=length)
    else:
        resp = Response.from_file(str(payload_path), offset=offset, length=length)
        assert resp.body == expected_body


@pytest.mark.parametrize("as_memoryview", [False, True])
def test_submit_raw_response_copies_writable_buffer_before_mutation(as_memoryview: bool) -> None:
    """Verify submit_raw_response copies writable bytearray payloads so post-submit edits cannot leak."""
    init_transport()
    mutable = bytearray(b"initial-mutable-body")
    body_arg: bytearray | memoryview = memoryview(mutable) if as_memoryview else mutable
    frames = submit_raw_response(901, 200, [("x-mode", "mutable")], body_arg)
    mutable[:] = b"mutated-after-submit"
    assert len(frames) == 1
    assert b"initial-mutable-body" in frames[0]
    assert b"mutated-after-submit" not in frames[0]


@pytest.mark.parametrize(
    "native_cls",
    [
        Request,
        Headers,
        HeadersIterator,
        RequestBatch,
        RequestBatchIterator,
        AsgiScopeIterator,
        WsgiEnviron,
        WsgiEnvironIterator,
        _WsgiStartResponse,
        RsgiTransport,
        ReadyAwaitable,
    ],
)
def test_internal_native_classes_reject_direct_instantiation(native_cls: type[Any]) -> None:
    """Verify internal native classes without constructors raise TypeError on instantiation."""
    with pytest.raises(TypeError):
        native_cls()


@pytest.mark.parametrize("as_memoryview", [False, True])
def test_rsgi_protocol_response_bytes_accepts_buffer(as_memoryview: bool) -> None:
    """Verify RsgiHttpProtocol.response_bytes accepts bytearray and memoryview buffers."""
    mutable = bytearray(b"rsgi-buffer-payload")
    body_arg: bytearray | memoryview = memoryview(mutable) if as_memoryview else mutable
    proto = RsgiHttpProtocol(body=b"", request_id=915)
    proto.response_bytes(200, [("content-type", "text/plain")], body_arg)
    mutable[:] = b"mutated-rsgi-buffer"
    resp = proto.take_response()
    assert resp.status == 200
    assert resp.request_id == 915
    assert resp.body == b"rsgi-buffer-payload"
    assert resp.headers == [(b"content-type", b"text/plain")]
