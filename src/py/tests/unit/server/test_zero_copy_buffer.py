"""Unit tests for zero-copy Py_buffer response submission and Request.body_view."""

from __future__ import annotations

import gc

import pytest

from vertebra._storage import StorageBuffer
from vertebra._vertebra import (
    ArrowCursor as RustArrowCursor,
)
from vertebra._vertebra import (
    Request,
    Response,
    init_transport,
    submit_batch_raw,
    submit_raw_response,
    submit_response_raw,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.server._fastpath import (
    ARROW_STREAM_MEDIA_TYPE,
    serialize_fastpath_special_result,
)
from vertebra.storage import AsyncObjectStore, StreamingStorageResponse


def test_request_body_view_returns_readonly_memoryview() -> None:
    """Verify Request.body_view exposes a read-only memoryview matching Request.body."""
    payload = b'{"event":"zero-copy","count":42}'
    req = Request.from_parts("POST", "/ingest", payload)

    view = req.body_view
    assert isinstance(view, memoryview)
    assert view.readonly is True
    assert bytes(view) == payload
    assert req.body == payload

    with pytest.raises(TypeError):
        view[0] = 0x7F


def test_request_body_view_empty_body_returns_empty_memoryview() -> None:
    """Verify Request.body_view returns an empty read-only memoryview when body is empty."""
    req = Request.from_parts("GET", "/empty", b"")
    view = req.body_view
    assert isinstance(view, memoryview)
    assert view.readonly is True
    assert len(view) == 0
    assert req.body is None


def test_response_from_buffer_accepts_bytes_bytearray_and_memoryview() -> None:
    """Verify Response.from_buffer reads contiguous bytes, bytearray, and sliced memoryview."""
    raw_bytearray = bytearray(b"prefix-payload-suffix")
    sliced_view = memoryview(raw_bytearray)[7:14]

    resp_mv = Response.from_buffer(
        201,
        sliced_view,
        headers=[("content-type", "text/plain")],
        request_id=55,
    )
    assert resp_mv.status == 201
    assert resp_mv.body == b"payload"
    assert resp_mv.request_id == 55
    assert resp_mv.headers == [(b"content-type", b"text/plain")]

    resp_ba = Response.from_buffer(200, raw_bytearray, request_id=56)
    assert resp_ba.body == b"prefix-payload-suffix"

    resp_bytes = Response.from_buffer(200, b"direct-bytes", request_id=57)
    assert resp_bytes.body == b"direct-bytes"


def test_submit_raw_response_unary_and_auto_chunked_streaming() -> None:
    """Verify submit_raw_response encodes unary slots and chunks >16 KiB buffers with FLAG_STREAMING."""
    small_view = memoryview(b"compact-unary-payload")
    frames = submit_raw_response(
        request_id=101,
        status=200,
        headers=[("content-type", "application/octet-stream")],
        body=small_view,
        flags=2,
        slot_capacity=16384,
    )
    assert len(frames) == 1

    large_payload = bytearray(i % 251 for i in range(256 * 1024))
    stream_frames = submit_raw_response(
        request_id=202,
        status=200,
        headers=[("content-type", "application/octet-stream"), ("x-stream", "true")],
        body=memoryview(large_payload),
        flags=2,
        slot_capacity=16384,
    )
    assert len(stream_frames) > 16

    reassembled = bytearray()
    for idx, frame in enumerate(stream_frames):
        req_id = int.from_bytes(frame[0:8], "little")
        status = int.from_bytes(frame[8:10], "little")
        flags = int.from_bytes(frame[10:12], "little")
        header_count = int.from_bytes(frame[12:14], "little")
        assert req_id == 202
        assert status == 200
        assert (flags & 1) != 0

        if idx == 0:
            assert (flags & (1 << 4)) != 0
            assert header_count == 2
        else:
            assert header_count == 0

        if idx == len(stream_frames) - 1:
            assert (flags & (1 << 1)) != 0
        else:
            assert (flags & (1 << 1)) == 0

        offset = 16
        for _ in range(header_count):
            klen = int.from_bytes(frame[offset : offset + 2], "little")
            offset += 2 + klen
            vlen = int.from_bytes(frame[offset : offset + 2], "little")
            offset += 2 + vlen
        blen = int.from_bytes(frame[offset : offset + 4], "little")
        offset += 4
        reassembled.extend(frame[offset : offset + blen])

    assert bytes(reassembled) == bytes(large_payload)


def test_request_buffer_protocol_and_anchored_lifetime() -> None:
    """Verify Request implements buffer protocol and anchors body_view lifetime."""
    payload = b"anchored-zero-copy-payload-0123456789"
    req = Request.from_parts("POST", "/anchor", payload)

    direct_view = memoryview(req)
    assert direct_view.readonly is True
    assert bytes(direct_view) == payload

    view = req.body_view
    assert view.readonly is True
    assert view.obj is req

    del direct_view
    del req
    gc.collect()

    assert bytes(view) == payload


def test_submit_response_raw_and_batch_raw_accept_buffers() -> None:
    """Verify submit_response_raw and submit_batch_raw accept bytes, bytearray, and memoryview."""
    init_transport()
    req = Request.from_parts("POST", "/echo", b"echo-buffer-view")

    submit_response_raw(1001, 200, req.body_view)
    submit_response_raw(1002, 201, bytearray(b"mutable-buffer"), memoryview(b""))
    submit_batch_raw(
        [
            (1003, 200, b"bytes-item"),
            (1004, 200, bytearray(b"bytearray-item")),
            (1005, 200, req.body_view),
            (1006, 202, req.body_view, memoryview(b"")),
        ]
    )


def test_serialize_fastpath_special_result_returns_zero_copy_arrow_memoryview() -> None:
    """Verify serialize_fastpath_special_result returns ArrowCursor buffer_view without bytes(...) copy."""
    backing = bytearray(b"ARROW1\x00\x00zero-copy-ipc-stream")
    py_cursor = ArrowCursor(backing, rows=[(1,)], columns=["id"])
    serialized = serialize_fastpath_special_result(py_cursor)
    assert serialized is not None
    status, headers, raw_view = serialized
    assert status == 200
    assert (b"content-type", ARROW_STREAM_MEDIA_TYPE) in headers
    assert (b"content-length", str(len(backing)).encode("ascii")) in headers
    assert isinstance(raw_view, memoryview)
    assert raw_view.obj is backing

    shm_cursor = RustArrowCursor.write_ipc_to_shm("/vtb-test-zc-arrow", bytes(backing), 1)
    shm_serialized = serialize_fastpath_special_result(shm_cursor)
    assert shm_serialized is not None
    assert isinstance(shm_serialized[2], memoryview)
    assert bytes(shm_serialized[2]) == bytes(backing)

    class ImpostorArrowCursor:
        def buffer_view(self) -> memoryview:
            return memoryview(b"impostor")

    assert serialize_fastpath_special_result(ImpostorArrowCursor()) is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("range_header", "status", "content_range", "expected"),
    [
        (None, 200, None, bytes(range(100))),
        ("items=0-5", 200, None, bytes(range(100))),
        ("bytes=0-15", 206, "bytes 0-15/100", bytes(range(16))),
        ("bytes=80-", 206, "bytes 80-99/100", bytes(range(80, 100))),
        ("bytes=-12", 206, "bytes 88-99/100", bytes(range(88, 100))),
        ("bytes=60-999", 206, "bytes 60-99/100", bytes(range(60, 100))),
        ("bytes=-999", 206, "bytes 0-99/100", bytes(range(100))),
        ("bytes=100-110", 416, "bytes */100", b""),
        ("bytes=40-10", 416, "bytes */100", b""),
        ("bytes=-0", 416, "bytes */100", b""),
        ("bytes=0-2,4-6", 416, "bytes */100", b""),
        ("bytes=wat", 416, "bytes */100", b""),
    ],
)
async def test_storage_response_async_ranges(
    range_header: str | None, status: int, content_range: str | None, expected: bytes
) -> None:
    """Native memory storage produces exact HTTP range bodies through both async seams."""
    store = AsyncObjectStore("memory://")
    await store.put("video.mp4", bytes(range(100)), content_type="video/mp4")
    response = await StreamingStorageResponse.from_async_store(
        store, "video.mp4", range_header=range_header, chunk_size=7
    )
    chunks = [chunk async for chunk in response.aiter_chunks()]
    assert b"".join(bytes(chunk) for chunk in chunks) == expected
    if status == 200:
        assert all(isinstance(chunk, StorageBuffer) for chunk in chunks)
    assert all(len(chunk) <= 7 for chunk in chunks)
    resolved_status, header_pairs, body = await response.to_async_fastpath_tuple()
    headers = dict(header_pairs)
    assert resolved_status == status
    assert bytes(body) == expected
    assert headers[b"accept-ranges"] == b"bytes"
    assert headers[b"content-length"] == str(len(expected)).encode()
    assert headers[b"content-type"] == b"video/mp4"
    assert headers.get(b"content-range") == (content_range.encode() if content_range else None)
    etag = (await store.head("video.mp4")).etag
    assert etag is not None
    assert headers[b"etag"] == etag.encode()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("range_header", "expected_status", "expected_body"),
    [(None, 200, b"0123456789"), ("bytes=2-5", 206, b"2345"), ("bytes=10-", 416, b"")],
)
async def test_async_storage_handler_submits_native_fastpath_frames(
    range_header: str | None, expected_status: int, expected_body: bytes
) -> None:
    """An explicit async handler resolves storage before native FastPath buffer submission."""
    store = AsyncObjectStore("memory://")
    await store.put("body", b"0123456789")

    async def handler() -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview]:
        response = await StreamingStorageResponse.from_async_store(store, "body", range_header=range_header)
        return await response.to_async_fastpath_tuple()

    status, headers, body = await handler()
    if expected_status == 200:
        assert isinstance(body, memoryview)
        assert isinstance(body.obj, StorageBuffer)
    native = Response.from_buffer(status, body, headers=headers, request_id=303)
    assert native.status == expected_status
    assert native.body == expected_body
    assert dict(native.headers)[b"content-length"] == str(len(expected_body)).encode()
    text_headers = [(key.decode("latin-1"), value.decode("latin-1")) for key, value in headers]
    frames = submit_raw_response(request_id=303, status=status, headers=text_headers, body=body, flags=2)
    assert len(frames) == 1
    frame = frames[0]
    assert int.from_bytes(frame[0:8], "little") == 303
    assert int.from_bytes(frame[8:10], "little") == expected_status
    header_count = int.from_bytes(frame[12:14], "little")
    offset = 16
    wire_headers: dict[bytes, bytes] = {}
    for _ in range(header_count):
        key_length = int.from_bytes(frame[offset : offset + 2], "little")
        offset += 2
        key = frame[offset : offset + key_length]
        offset += key_length
        value_length = int.from_bytes(frame[offset : offset + 2], "little")
        offset += 2
        wire_headers[key] = frame[offset : offset + value_length]
        offset += value_length
    body_length = int.from_bytes(frame[offset : offset + 4], "little")
    assert frame[offset + 4 : offset + 4 + body_length] == expected_body
    assert wire_headers == dict(headers)
