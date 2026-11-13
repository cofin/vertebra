"""Unit tests for the ASGI SSE framing helper and native App streaming dispatch."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from vertebra._vertebra import (
    STREAM_FLAG_ERROR,
    STREAM_FLAG_FINAL,
    STREAM_FLAG_HEADERS_SENT,
    STREAM_FLAG_STREAMING,
    Request,
    Response,
    _set_stream_sink,
    drive_native_batch,
    init_transport,
)
from vertebra.server._asgi._sse import SseEvent, SseResponse, extract_last_event_id
from vertebra.server._dispatch import (
    BatchDispatchPlan,
    _await_async_item,
    _submit_coerced,
    compile_handler_spec,
    dispatch_batch,
)
from vertebra.server._loops import EventLoopPool
from vertebra.storage import AsyncObjectStore, MemoryConfig, SyncObjectStore
from vertebra.storage._streaming import StreamingStorageResponse
from vertebra.utils.serializers import decode_json, encode_json_bytes


def test_sse_event_encoding_and_header_sanitization() -> None:
    """Verify RFC 8895 multi-line data framing and control character sanitization."""
    event = SseEvent(
        data="alpha\r\nbeta\ngamma",
        event="token\r\ninjected",
        id="101\nfake",
        retry=2500,
        comment="keepalive",
    )
    encoded = event.encode().decode("utf-8")
    assert encoded == (
        ": keepalive\r\n"
        "id: 101fake\r\n"
        "event: tokeninjected\r\n"
        "retry: 2500\r\n"
        "data: alpha\r\n"
        "data: beta\r\n"
        "data: gamma\r\n\r\n"
    )
    assert SseEvent.__struct_config__.array_like is False
    assert SseEvent.__struct_config__.cache_hash is True
    assert isinstance(decode_json(encode_json_bytes(event)), dict)
    assert isinstance(hash(event), int)


def test_extract_last_event_id_from_asgi_scope() -> None:
    """Verify case-insensitive extraction of Last-Event-ID from ASGI scope headers."""
    scope: dict[str, Any] = {
        "type": "http",
        "headers": [
            (b"accept", b"text/event-stream"),
            (b"Last-Event-ID", b"evt-77"),
        ],
    }
    assert extract_last_event_id(scope) == "evt-77"


@pytest.mark.anyio
async def test_sse_response_emits_asgi_extension_and_fallback_body_frames() -> None:
    """Verify SseResponse emits http.response.sse when supported and http.response.body otherwise."""

    async def event_stream() -> AsyncIterator[SseEvent]:
        yield SseEvent(data="hello", id="1", event="greeting")
        yield SseEvent(data="world", id="2", event="greeting")

    sent_ext: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send_ext(message: dict[str, Any]) -> None:
        sent_ext.append(message)

    scope_with_ext: dict[str, Any] = {
        "type": "http",
        "headers": [],
        "extensions": {"http.response.sse": {}},
    }
    response = SseResponse(event_stream())
    await response(scope_with_ext, receive, send_ext)

    assert sent_ext[0]["type"] == "http.response.start"
    assert sent_ext[0]["status"] == 200
    header_map = dict(sent_ext[0]["headers"])
    assert header_map[b"content-type"] == b"text/event-stream; charset=utf-8"
    assert header_map[b"cache-control"] == b"no-cache, no-transform"
    assert header_map[b"x-accel-buffering"] == b"no"

    assert sent_ext[1]["type"] == "http.response.sse"
    assert sent_ext[1]["id"] == "1"
    assert sent_ext[1]["event"] == "greeting"
    assert sent_ext[1]["data"] == "hello"
    assert sent_ext[1]["more_body"] is True

    assert sent_ext[2]["type"] == "http.response.sse"
    assert sent_ext[2]["id"] == "2"
    assert sent_ext[2]["more_body"] is True

    assert sent_ext[3] == {"type": "http.response.sse", "data": "", "more_body": False}

    sent_body: list[dict[str, Any]] = []

    async def send_body(message: dict[str, Any]) -> None:
        sent_body.append(message)

    scope_without_ext: dict[str, Any] = {
        "type": "http",
        "headers": [],
    }
    response_fallback = SseResponse(event_stream())
    await response_fallback(scope_without_ext, receive, send_body)

    assert sent_body[0]["type"] == "http.response.start"
    assert sent_body[1]["type"] == "http.response.body"
    assert sent_body[1]["body"] == b"id: 1\r\nevent: greeting\r\ndata: hello\r\n\r\n"
    assert sent_body[1]["more_body"] is True
    assert sent_body[2]["type"] == "http.response.body"
    assert sent_body[2]["body"] == b"id: 2\r\nevent: greeting\r\ndata: world\r\n\r\n"
    assert sent_body[2]["more_body"] is True
    assert sent_body[3] == {"type": "http.response.body", "body": b"", "more_body": False}


@pytest.mark.anyio
@pytest.mark.parametrize("stream_kind", ["sync", "async"])
@pytest.mark.parametrize("scenario", ["multi_chunk", "empty", "mid_stream_error", "disconnect"])
async def test_native_dispatch_tuple_streaming_modes(stream_kind: str, scenario: str) -> None:
    """Verify _dispatch.py streams sync and async (status, headers, iterable) tuples progressively."""
    init_transport()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    closed_cleanly: list[bool] = []

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        frames.append((req_id, status, flags, headers, body))
        return not (scenario == "disconnect" and (flags & STREAM_FLAG_HEADERS_SENT) == 0)

    def sync_stream() -> Iterator[bytes]:
        try:
            if scenario == "empty":
                return
            yield b"data: one\n\n"
            if scenario == "mid_stream_error":
                msg = "sync stream boom"
                raise RuntimeError(msg)
            yield b""
            yield b"data: two\n\n"
        finally:
            closed_cleanly.append(True)

    async def async_stream() -> AsyncIterator[bytes]:
        try:
            if scenario == "empty":
                return
            yield b"data: one\n\n"
            if scenario == "mid_stream_error":
                msg = "async stream boom"
                raise RuntimeError(msg)
            yield b""
            yield b"data: two\n\n"
        finally:
            closed_cleanly.append(True)

    unary_calls: list[Any] = []
    headers_in = {"content-type": "text/event-stream", "cache-control": "no-cache"}
    expected_hdrs = [(b"content-type", b"text/event-stream"), (b"cache-control", b"no-cache")]

    _set_stream_sink(sink)
    try:
        if stream_kind == "sync":
            _submit_coerced(
                (200, headers_in, sync_stream()),
                777,
                Response,
                unary_calls.append,
            )
        else:

            async def async_handler() -> tuple[int, dict[str, str], AsyncIterator[bytes]]:
                return 200, headers_in, async_stream()

            await _await_async_item(
                async_handler(),
                777,
                None,
                None,
                Response,
                unary_calls.append,
                None,
            )
    finally:
        _set_stream_sink(None)

    assert unary_calls == []
    assert closed_cleanly == [True]
    if scenario == "multi_chunk":
        assert frames == [
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, expected_hdrs, b""),
            (777, 200, STREAM_FLAG_STREAMING, [], b"data: one\n\n"),
            (777, 200, STREAM_FLAG_STREAMING, [], b"data: two\n\n"),
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b""),
        ]
    elif scenario == "empty":
        assert frames == [
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, expected_hdrs, b""),
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b""),
        ]
    elif scenario == "mid_stream_error":
        assert frames == [
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, expected_hdrs, b""),
            (777, 200, STREAM_FLAG_STREAMING, [], b"data: one\n\n"),
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR, [], b""),
        ]
    else:
        assert frames == [
            (777, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT, expected_hdrs, b""),
            (777, 200, STREAM_FLAG_STREAMING, [], b"data: one\n\n"),
        ]


@pytest.mark.anyio
async def test_drive_native_batch_streams_async_generator_before_gate_release() -> None:
    """Verify dispatch_batch + drive_native_batch streams native App SSE before generator release."""
    init_transport()
    gate = asyncio.Event()
    first_chunk_event = asyncio.Event()
    terminal_flushed = asyncio.Event()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        frames.append((req_id, status, flags, headers, body))
        if body == b"data: first\n\n":
            first_chunk_event.set()
        if (flags & (STREAM_FLAG_FINAL | STREAM_FLAG_ERROR)) != 0:
            terminal_flushed.set()
        return True

    async def sse_endpoint() -> tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]:
        async def gen() -> AsyncIterator[bytes]:
            yield b"data: first\n\n"
            await gate.wait()
            yield b"data: second\n\n"

        return 200, [("content-type", "text/event-stream")], gen()

    spec = compile_handler_spec(sse_endpoint)
    req = Request.from_parts("GET", "/native-sse", b"", request_id=790, handler_id=1)
    plan = BatchDispatchPlan(async_native=[(0, req, spec)])
    loop = asyncio.get_running_loop()

    _set_stream_sink(sink)
    try:
        dispatch_batch(plan, {1: spec}, [], Response, loop, async_mode=True)
        await asyncio.wait_for(first_chunk_event.wait(), timeout=1.0)
        assert not gate.is_set()
        assert frames == [
            (
                790,
                200,
                STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                [(b"content-type", b"text/event-stream")],
                b"",
            ),
            (790, 200, STREAM_FLAG_STREAMING, [], b"data: first\n\n"),
        ]
        gate.set()
        await asyncio.wait_for(terminal_flushed.wait(), timeout=1.0)
        assert frames[-2] == (790, 200, STREAM_FLAG_STREAMING, [], b"data: second\n\n")
        assert frames[-1] == (790, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b"")
    finally:
        _set_stream_sink(None)


def test_stopped_inline_loop_and_suspended_native_async_stream_complete_fully() -> None:
    """Verify drive_until_complete and inline_single_loop EventLoopPool finish async streams without hanging."""
    init_transport()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    _set_stream_sink(lambda rid, st, fl, hd, bd: frames.append((rid, st, fl, hd, bd)) or True)
    pool = EventLoopPool(size=1, inline_single_loop=True)
    try:

        async def suspended_sse() -> tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]:
            await asyncio.sleep(0)

            async def gen() -> AsyncIterator[bytes]:
                await asyncio.sleep(0)
                yield b"data: step-1\n\n"
                await asyncio.sleep(0)
                yield b"data: step-2\n\n"

            return 200, [("content-type", "text/event-stream")], gen()

        drive_native_batch([(991, suspended_sse(), 200)], loop_obj=pool.primary_loop)
        assert [f[4] for f in frames if f[0] == 991] == [b"", b"data: step-1\n\n", b"data: step-2\n\n", b""]
        assert (frames[-1][2] & STREAM_FLAG_FINAL) != 0

        frames.clear()

        def sync_returning_async_stream() -> tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]:
            async def gen() -> AsyncIterator[bytes]:
                await asyncio.sleep(0)
                yield b"sync-to-async"

            return 200, [("x-mode", "inline")], gen()

        _submit_coerced(
            sync_returning_async_stream(),
            992,
            Response,
            lambda _r: None,
            scheduler=pool,
        )
        assert [f[4] for f in frames if f[0] == 992] == [b"", b"sync-to-async", b""]
        assert (frames[-1][2] & STREAM_FLAG_FINAL) != 0
    finally:
        _set_stream_sink(None)
        pool.stop()


@pytest.mark.anyio
async def test_native_special_returns_support_storage_response_and_body_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify StreamingStorageResponse (sync + async) and .body_iterator stream objects dispatch cleanly."""
    init_transport()
    monkeypatch.setattr(SyncObjectStore, "_ensure_native_module", classmethod(lambda _cls: object()))
    monkeypatch.setattr(AsyncObjectStore, "_ensure_native_module", classmethod(lambda _cls: object()))

    class StubSyncNativeStore:
        def get_buffer(self, _key: str) -> bytes:
            return b"storage-payload"

        def head(self, _key: str) -> Any:
            return type("Meta", (), {"size": len(b"storage-payload")})()

    class StubAsyncNativeStore:
        async def get_buffer(self, _key: str) -> bytes:
            await asyncio.sleep(0)
            return b"storage-payload"

        async def head(self, _key: str) -> Any:
            return type("Meta", (), {"size": len(b"storage-payload")})()

    sync_store = SyncObjectStore(MemoryConfig(), _native_instance=StubSyncNativeStore())
    async_store = AsyncObjectStore(MemoryConfig(), _native_instance=StubAsyncNativeStore())

    submitted_responses: list[Response] = []
    sync_desc = StreamingStorageResponse(
        store=sync_store,
        key="hello.txt",
        total_size=len(b"storage-payload"),
        content_type="text/plain",
    )
    _submit_coerced(sync_desc, 995, Response, submitted_responses.append, submit_raw=lambda *_: None)
    assert len(submitted_responses) == 1
    assert bytes(submitted_responses[0].body) == b"storage-payload"

    submitted_responses.clear()
    async_desc = StreamingStorageResponse(
        store=async_store,
        key="hello.txt",
        total_size=len(b"storage-payload"),
        content_type="text/plain",
    )

    async def async_storage_handler() -> StreamingStorageResponse:
        return async_desc

    await _await_async_item(
        async_storage_handler(),
        996,
        None,
        None,
        Response,
        submitted_responses.append,
        lambda *_: None,
    )
    assert len(submitted_responses) == 1
    assert bytes(submitted_responses[0].body) == b"storage-payload"

    class BodyIteratorResponse:
        status_code = 202
        headers = [("x-iter", "yes")]

        def __init__(self) -> None:
            self.body_iterator = iter([b"chunk-a", b"chunk-b"])

    stream_frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    _submit_coerced(
        BodyIteratorResponse(),
        997,
        Response,
        submitted_responses.append,
        stream_submitter=lambda rid, st, fl, hd, bd: stream_frames.append((rid, st, fl, hd, bd)),
    )
    assert [f[4] for f in stream_frames] == [b"", b"chunk-a", b"chunk-b", b""]
    assert stream_frames[0][1] == 202
