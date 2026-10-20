"""Unit tests for the ASGI SSE framing helper and http.response.sse extension."""

from collections.abc import AsyncIterator
from typing import Any

import pytest

from vertebra.server._asgi._sse import SseEvent, SseResponse, extract_last_event_id


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
