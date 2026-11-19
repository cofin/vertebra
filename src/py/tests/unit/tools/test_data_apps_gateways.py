"""Unit tests for RSGI, ASGI, and WSGI TechEmpower data benchmark apps."""

from __future__ import annotations

import asyncio
import io
import sys
from typing import Any

import pytest
from tools.benchmarks.apps._data_backend import (
    JSON_HELLO_PAYLOAD,
    WORLD_ROW_COUNT,
    DataBackend,
    DataBackendConfig,
)
from tools.benchmarks.apps.data_asgi import (
    asgi_app,
    create_asgi_data_app,
    create_vertebra_asgi_app,
    vertebra_asgi_app,
)
from tools.benchmarks.apps.data_rsgi import (
    create_rsgi_data_app,
    create_vertebra_rsgi_app,
    rsgi_app,
    vertebra_rsgi_app,
)
from tools.benchmarks.apps.data_wsgi import (
    create_vertebra_wsgi_app,
    create_wsgi_data_app,
    vertebra_wsgi_app,
    wsgi_app,
)

from vertebra.framework._app import App
from vertebra.utils.serializers import decode_json


class _FakeRSGIScope:
    """Minimal RSGI v1.6 HTTP scope stub for in-process testing."""

    def __init__(
        self,
        path: str,
        *,
        method: str = "GET",
        query_string: str = "",
        proto: str = "http",
    ) -> None:
        self.proto = proto
        self.method = method
        self.path = path
        self.query_string = query_string


class _FakeRSGIProtocol:
    """Minimal RSGI v1.6 protocol recorder for in-process testing."""

    def __init__(self, body: bytes = b"") -> None:
        self._body = body
        self.status: int | None = None
        self.headers: list[tuple[str, str]] = []
        self.body: bytes = b""

    async def __call__(self) -> bytes:
        return self._body

    def response_bytes(self, status: int, headers: list[tuple[str, str]], body: bytes) -> None:
        self.status = status
        self.headers = list(headers)
        self.body = body


def _make_memory_backend(*, strict_free_threading: bool = False) -> DataBackend:
    """Create an isolated in-memory DataBackend for offline unit tests."""
    return DataBackend(
        DataBackendConfig(
            db_backend="memory",
            cache_backend="memory",
            storage_backend="memory",
            storage_bucket="vertebra-gateway-test",
            strict_free_threading=strict_free_threading,
        )
    )


async def _call_rsgi(
    app: Any,
    path: str,
    *,
    method: str = "GET",
    query_string: str = "",
    body: bytes = b"",
    proto: str = "http",
) -> _FakeRSGIProtocol:
    """Invoke an RSGI callable and return the recorded protocol response."""
    scope = _FakeRSGIScope(path, method=method, query_string=query_string, proto=proto)
    protocol = _FakeRSGIProtocol(body=body)
    await app(scope, protocol)
    return protocol


async def _call_asgi_http(
    app: Any,
    path: str,
    *,
    method: str = "GET",
    query_string: bytes = b"",
    body_chunks: list[bytes] | None = None,
) -> tuple[int, dict[bytes, bytes], bytes]:
    """Invoke an ASGI callable for an HTTP request and return status, headers, and body."""
    chunks = [b""] if body_chunks is None else list(body_chunks)
    chunk_index = 0

    async def _receive() -> dict[str, Any]:
        nonlocal chunk_index
        if chunk_index < len(chunks):
            chunk = chunks[chunk_index]
            chunk_index += 1
            return {"type": "http.request", "body": chunk, "more_body": chunk_index < len(chunks)}
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list[dict[str, Any]] = []

    async def _send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string,
    }
    await app(scope, _receive, _send)

    start = sent[0]
    body = b"".join(msg.get("body", b"") for msg in sent[1:])
    headers = {k.lower(): v for k, v in start["headers"]}
    return int(start["status"]), headers, body


def _call_wsgi(
    app: Any,
    path: str,
    *,
    method: str = "GET",
    query_string: str = "",
    body: bytes = b"",
    include_content_length: bool = True,
) -> tuple[str, dict[str, str], bytes]:
    """Invoke a WSGI callable and return status string, lowercase headers, and body."""
    captured_status = ""
    captured_headers: list[tuple[str, str]] = []

    def _start_response(status: str, headers: list[tuple[str, str]]) -> None:
        nonlocal captured_status, captured_headers
        captured_status = status
        captured_headers = list(headers)

    environ: dict[str, Any] = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query_string,
        "wsgi.input": io.BytesIO(body),
    }
    if include_content_length:
        environ["CONTENT_LENGTH"] = str(len(body))

    chunks = app(environ, _start_response)
    headers = {k.lower(): v for k, v in captured_headers}
    return captured_status, headers, b"".join(chunks)


def test_rsgi_data_routes_and_object_storage() -> None:
    """Exercise all TechEmpower, object-storage, health, and error routes on RSGI."""
    backend = _make_memory_backend()
    app = create_rsgi_data_app(backend)

    async def _run() -> None:
        json_res = await _call_rsgi(app, "/json")
        assert json_res.status == 200
        assert json_res.body == JSON_HELLO_PAYLOAD

        db_res = await _call_rsgi(app, "/db")
        assert db_res.status == 200
        db_row = decode_json(db_res.body)
        assert 1 <= db_row["id"] <= WORLD_ROW_COUNT
        assert 1 <= db_row["randomNumber"] <= WORLD_ROW_COUNT

        queries_res = await _call_rsgi(app, "/queries", query_string="queries=5")
        assert queries_res.status == 200
        assert len(decode_json(queries_res.body)) == 5

        updates_res = await _call_rsgi(app, "/updates", query_string="queries=3")
        assert updates_res.status == 200
        assert len(decode_json(updates_res.body)) == 3

        cached_res = await _call_rsgi(app, "/cached-queries", query_string="count=4")
        assert cached_res.status == 200
        assert len(decode_json(cached_res.body)) == 4

        put_res = await _call_rsgi(app, "/object/k1", method="PUT", body=b"rsgi-payload")
        assert put_res.status == 200
        assert decode_json(put_res.body) == {"key": "k1", "size": 12}

        get_res = await _call_rsgi(app, "/object/k1", method="GET")
        assert get_res.status == 200
        assert get_res.body == b"rsgi-payload"

        nested_put = await _call_rsgi(app, "/object/bench/4k.bin", method="PUT", body=b"x" * 64)
        assert nested_put.status == 200
        assert decode_json(nested_put.body) == {"key": "bench/4k.bin", "size": 64}

        nested_get = await _call_rsgi(app, "/object/bench/4k.bin", method="GET")
        assert nested_get.status == 200
        assert nested_get.body == b"x" * 64

        missing_res = await _call_rsgi(app, "/object/missing", method="GET")
        assert missing_res.status == 404

        method_not_allowed = await _call_rsgi(app, "/object/k1", method="DELETE")
        assert method_not_allowed.status == 405

        health_res = await _call_rsgi(app, "/health")
        assert health_res.status == 200
        assert health_res.body == b"ok"

        unknown_res = await _call_rsgi(app, "/does-not-exist")
        assert unknown_res.status == 404

        ws_res = await _call_rsgi(app, "/json", proto="ws")
        assert ws_res.status is None

    asyncio.run(_run())
    backend.close()


@pytest.mark.parametrize(
    ("query_string", "expected_len"),
    [
        ("queries=20", 20),
        ("queries=invalid", 1),
        ("queries=999", 500),
        ("", 1),
    ],
)
def test_gateway_query_clamping(query_string: str, expected_len: int) -> None:
    """Verify RSGI, ASGI, and WSGI identically clamp query count parameters."""
    backend = _make_memory_backend()
    rsgi_callable = create_rsgi_data_app(backend)
    asgi_callable = create_asgi_data_app(backend)
    wsgi_callable = create_wsgi_data_app(backend)

    async def _run_async() -> None:
        rsgi_res = await _call_rsgi(rsgi_callable, "/queries", query_string=query_string)
        assert rsgi_res.status == 200
        assert len(decode_json(rsgi_res.body)) == expected_len

        asgi_status, _, asgi_body = await _call_asgi_http(
            asgi_callable,
            "/queries",
            query_string=query_string.encode("ascii"),
        )
        assert asgi_status == 200
        assert len(decode_json(asgi_body)) == expected_len

    asyncio.run(_run_async())

    wsgi_status, _, wsgi_body = _call_wsgi(wsgi_callable, "/queries", query_string=query_string)
    assert wsgi_status == "200 OK"
    assert len(decode_json(wsgi_body)) == expected_len
    backend.close()


def test_asgi_lifespan_and_data_routes() -> None:
    """Exercise ASGI lifespan startup/shutdown and all data/object-storage routes."""
    backend = _make_memory_backend()
    app = create_asgi_data_app(backend)

    async def _run() -> None:
        events = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
        sent_lifespan: list[dict[str, Any]] = []

        async def _lifespan_receive() -> dict[str, Any]:
            return next(events)

        async def _lifespan_send(message: dict[str, Any]) -> None:
            sent_lifespan.append(message)

        await app({"type": "lifespan"}, _lifespan_receive, _lifespan_send)
        assert sent_lifespan == [
            {"type": "lifespan.startup.complete"},
            {"type": "lifespan.shutdown.complete"},
        ]

        status, headers, body = await _call_asgi_http(app, "/json")
        assert status == 200
        assert headers[b"content-type"] == b"application/json"
        assert body == JSON_HELLO_PAYLOAD

        status, _, body = await _call_asgi_http(app, "/db")
        assert status == 200
        row = decode_json(body)
        assert 1 <= row["id"] <= WORLD_ROW_COUNT

        status, _, body = await _call_asgi_http(app, "/queries", query_string=b"queries=5")
        assert status == 200
        assert len(decode_json(body)) == 5

        status, _, body = await _call_asgi_http(app, "/updates", query_string=b"queries=3")
        assert status == 200
        assert len(decode_json(body)) == 3

        status, _, body = await _call_asgi_http(app, "/cached-queries", query_string=b"count=4")
        assert status == 200
        assert len(decode_json(body)) == 4

        status, _, body = await _call_asgi_http(
            app,
            "/object/bench/4k.bin",
            method="PUT",
            body_chunks=[b"chunk-a-", b"chunk-b"],
        )
        assert status == 200
        assert decode_json(body) == {"key": "bench/4k.bin", "size": 15}

        status, headers, body = await _call_asgi_http(app, "/object/bench/4k.bin", method="GET")
        assert status == 200
        assert headers[b"content-type"] == b"application/octet-stream"
        assert body == b"chunk-a-chunk-b"

        status, _, _ = await _call_asgi_http(app, "/object/missing", method="GET")
        assert status == 404

        status, _, _ = await _call_asgi_http(app, "/object/bench/4k.bin", method="DELETE")
        assert status == 405

        status, _, body = await _call_asgi_http(app, "/health")
        assert status == 200
        assert body == b"ok"

        status, _, _ = await _call_asgi_http(app, "/unknown")
        assert status == 404

    asyncio.run(_run())
    backend.close()


def test_asgi_lifespan_strict_free_threading_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ASGI lifespan startup reports failure when strict free-threading rejects GIL."""
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    backend = _make_memory_backend(strict_free_threading=True)
    app = create_asgi_data_app(backend)

    async def _run() -> None:
        sent: list[dict[str, Any]] = []

        async def _receive() -> dict[str, Any]:
            return {"type": "lifespan.startup"}

        async def _send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app({"type": "lifespan"}, _receive, _send)
        assert len(sent) == 1
        assert sent[0]["type"] == "lifespan.startup.failed"
        assert "GIL disabled" in sent[0]["message"]

    asyncio.run(_run())
    backend.close()


def test_wsgi_data_routes_and_object_storage() -> None:
    """Exercise all TechEmpower, object-storage, health, and error routes on WSGI."""
    backend = _make_memory_backend()
    app = create_wsgi_data_app(backend)

    status, headers, body = _call_wsgi(app, "/json")
    assert status == "200 OK"
    assert headers["content-type"] == "application/json"
    assert body == JSON_HELLO_PAYLOAD

    status, _, body = _call_wsgi(app, "/db")
    assert status == "200 OK"
    row = decode_json(body)
    assert 1 <= row["id"] <= WORLD_ROW_COUNT

    status, _, body = _call_wsgi(app, "/queries", query_string="queries=5")
    assert status == "200 OK"
    assert len(decode_json(body)) == 5

    status, _, body = _call_wsgi(app, "/updates", query_string="queries=3")
    assert status == "200 OK"
    assert len(decode_json(body)) == 3

    status, _, body = _call_wsgi(app, "/cached-queries", query_string="count=4")
    assert status == "200 OK"
    assert len(decode_json(body)) == 4

    status, _, body = _call_wsgi(app, "/object/k1", method="PUT", body=b"wsgi-body")
    assert status == "200 OK"
    assert decode_json(body) == {"key": "k1", "size": 9}

    status, headers, body = _call_wsgi(app, "/object/k1", method="GET")
    assert status == "200 OK"
    assert headers["content-type"] == "application/octet-stream"
    assert body == b"wsgi-body"

    status, _, body = _call_wsgi(
        app,
        "/object/bench/4k.bin",
        method="PUT",
        body=b"streamed-without-length",
        include_content_length=False,
    )
    assert status == "200 OK"
    assert decode_json(body) == {"key": "bench/4k.bin", "size": 23}

    status, _, body = _call_wsgi(app, "/object/bench/4k.bin", method="GET")
    assert status == "200 OK"
    assert body == b"streamed-without-length"

    status, _, _ = _call_wsgi(app, "/object/missing", method="GET")
    assert status == "404 Not Found"

    status, _, _ = _call_wsgi(app, "/object/k1", method="DELETE")
    assert status == "405 Method Not Allowed"

    status, _, body = _call_wsgi(app, "/health")
    assert status == "200 OK"
    assert body == b"ok"

    status, _, _ = _call_wsgi(app, "/unknown")
    assert status == "404 Not Found"

    backend.close()


@pytest.mark.parametrize(
    ("app_instance", "expected_protocol", "raw_callable"),
    [
        (vertebra_rsgi_app, "rsgi", rsgi_app),
        (vertebra_asgi_app, "asgi", asgi_app),
        (vertebra_wsgi_app, "wsgi", wsgi_app),
    ],
)
def test_vertebra_gateway_wrappers_mount_protocols(
    app_instance: App,
    expected_protocol: str,
    raw_callable: Any,
) -> None:
    """Verify module-level Vertebra App wrappers mount the expected protocol at root."""
    assert isinstance(app_instance, App)
    assert callable(raw_callable)
    assert len(app_instance.mounts) == 1
    prefix, mounted_app, protocol = app_instance.mounts[0]
    assert prefix in {"", "/"}
    assert callable(mounted_app)
    assert protocol == expected_protocol


@pytest.mark.parametrize(
    ("factory", "expected_protocol"),
    [
        (create_vertebra_rsgi_app, "rsgi"),
        (create_vertebra_asgi_app, "asgi"),
        (create_vertebra_wsgi_app, "wsgi"),
    ],
)
def test_vertebra_gateway_factories_accept_custom_backend(
    factory: Any,
    expected_protocol: str,
) -> None:
    """Verify Vertebra gateway factories wire a custom DataBackend onto mounted apps."""
    backend = _make_memory_backend()
    try:
        backend.put_object_sync("custom-key", b"injected-payload")
        app_instance = factory(backend)
        assert isinstance(app_instance, App)
        assert len(app_instance.mounts) == 1
        mounted_callable = app_instance.mounts[0][1]
        assert app_instance.mounts[0][2] == expected_protocol

        if expected_protocol == "rsgi":
            protocol_resp = asyncio.run(_call_rsgi(mounted_callable, "/object/custom-key"))
            assert protocol_resp.status == 200
            assert protocol_resp.body == b"injected-payload"
        elif expected_protocol == "asgi":
            status, _headers, body = asyncio.run(_call_asgi_http(mounted_callable, "/object/custom-key"))
            assert status == 200
            assert body == b"injected-payload"
        else:
            status_str, _headers, body = _call_wsgi(mounted_callable, "/object/custom-key")
            assert status_str.startswith("200")
            assert body == b"injected-payload"
    finally:
        backend.close()
