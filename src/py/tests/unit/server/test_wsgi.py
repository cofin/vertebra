"""Unit tests for the WSGI 1.0 (PEP 3333) server adapter."""

import io
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import Request, WsgiEnviron, WsgiInput, init_transport
from vertebra.server._wsgi._adapter import WsgiAdapter, WsgiError
from vertebra.server._wsgi._frameworks import (
    FlaskWsgiApp,
    LargeBodyBypass,
    resolve_wsgi_file_wrapper,
    wrap_with_error_handler,
)


def test_wsgi_input_read_all() -> None:
    stream = WsgiInput(b"hello world")
    assert stream.read() == b"hello world"
    assert stream.read() == b""


def test_wsgi_input_read_chunked() -> None:
    stream = WsgiInput(b"abcdefgh")
    assert stream.read(3) == b"abc"
    assert stream.read(3) == b"def"
    assert stream.read(3) == b"gh"
    assert stream.read(3) == b""


def test_wsgi_input_readline() -> None:
    stream = WsgiInput(b"line1\nline2\nline3")
    assert stream.readline() == b"line1\n"
    assert stream.readline() == b"line2\n"
    assert stream.readline() == b"line3"
    assert stream.readline() == b""


def test_wsgi_input_readline_with_size() -> None:
    stream = WsgiInput(b"longline\n")
    assert stream.readline(4) == b"long"
    assert stream.readline(10) == b"line\n"


def test_wsgi_input_readlines() -> None:
    stream = WsgiInput(b"a\nb\nc")
    assert stream.readlines() == [b"a\n", b"b\n", b"c"]


def test_wsgi_input_iteration() -> None:
    stream = WsgiInput(b"x\ny\n")
    assert list(stream) == [b"x\n", b"y\n"]


def test_wsgi_input_empty() -> None:
    stream = WsgiInput()
    assert stream.read() == b""
    assert stream.readline() == b""
    assert stream.readlines() == []


def test_wsgi_environ_mandatory_pep3333_keys() -> None:
    env = WsgiEnviron.from_parts(
        method="POST",
        path="/users/42",
        query_string="active=1",
        headers=[
            (b"content-type", b"application/json"),
            (b"content-length", b"13"),
            (b"x-custom-header", b"val"),
        ],
        body=b'{"name":"Jo"}',
        server_name="example.com",
        server_port=8080,
        url_scheme="https",
    )
    assert env["REQUEST_METHOD"] == "POST"
    assert env["SCRIPT_NAME"] == ""
    assert env["PATH_INFO"] == "/users/42"
    assert env["QUERY_STRING"] == "active=1"
    assert env["CONTENT_TYPE"] == "application/json"
    assert env["CONTENT_LENGTH"] == "13"
    assert env["SERVER_NAME"] == "example.com"
    assert env["SERVER_PORT"] == "8080"
    assert env["SERVER_PROTOCOL"] == "HTTP/1.1"
    assert env["HTTP_X_CUSTOM_HEADER"] == "val"
    assert env["wsgi.version"] == (1, 0)
    assert env["wsgi.url_scheme"] == "https"
    assert env["wsgi.multithread"] is True
    assert env["wsgi.multiprocess"] is False
    assert env["wsgi.run_once"] is False
    assert env["wsgi.input"].read() == b'{"name":"Jo"}'


def test_wsgi_environ_dict_mutation_and_iteration() -> None:
    env = WsgiEnviron.from_parts(method="GET", path="/")
    env["custom.key"] = "hello"
    assert "custom.key" in env
    assert env["custom.key"] == "hello"
    assert env.get("missing", "fallback") == "fallback"

    env.setdefault("custom.key", "ignored")
    assert env["custom.key"] == "hello"

    env.setdefault("new.key", "set")
    assert env["new.key"] == "set"

    assert env.pop("new.key") == "set"
    assert "new.key" not in env

    copied = env.copy()
    assert copied["REQUEST_METHOD"] == "GET"
    assert copied["custom.key"] == "hello"
    assert len(env) >= 13
    assert "REQUEST_METHOD" in list(env.keys())


def test_wsgi_environ_from_request() -> None:
    req = Request.from_parts(
        "PUT",
        "/items",
        body=b"data",
        headers=[("content-type", "text/plain"), ("authorization", "Bearer tok")],
    )
    env = req.wsgi_environ(server_name="api.local", server_port=9000)
    assert env["REQUEST_METHOD"] == "PUT"
    assert env["PATH_INFO"] == "/items"
    assert env["CONTENT_TYPE"] == "text/plain"
    assert env["HTTP_AUTHORIZATION"] == "Bearer tok"
    assert env["SERVER_NAME"] == "api.local"
    assert env["SERVER_PORT"] == "9000"
    assert env["wsgi.input"].read() == b"data"


class MockRequest:
    """Minimal request double for WSGI adapter tests."""

    def __init__(
        self,
        method: str = "GET",
        path: str = "/hello",
        query: str | None = None,
        headers: Any = None,
        body: bytes | None = None,
        request_id: int = 7,
    ) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.headers = headers or {}
        self.body = body
        self.request_id = request_id


def test_wsgi_adapter_200_ok() -> None:
    def simple_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        body = environ["wsgi.input"].read()
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"echo: " + body]

    adapter = WsgiAdapter(simple_app)
    resp = adapter.handle_request(MockRequest(body=b"hello"))
    assert resp.status == 200
    assert resp.body == b"echo: hello"
    assert resp.request_id == 7


def test_wsgi_adapter_custom_status_and_headers() -> None:
    def created_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("201 Created", [("X-Custom", "val")])
        return [b"created"]

    adapter = WsgiAdapter(created_app)
    resp = adapter.handle_request(MockRequest())
    assert resp.status == 201
    assert (b"x-custom", b"val") in resp.headers


def test_wsgi_adapter_write_callable() -> None:
    def write_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        write = start_response("200 OK", [("Content-Type", "text/plain")])
        write(b"part1-")
        write(b"part2")
        return [b"-part3"]

    adapter = WsgiAdapter(write_app)
    resp = adapter.handle_request(MockRequest())
    assert resp.body == b"part1-part2-part3"


def test_wsgi_adapter_missing_start_response_raises() -> None:
    def broken_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        return [b"oops"]

    adapter = WsgiAdapter(broken_app)
    with pytest.raises(WsgiError, match="start_response"):
        adapter.handle_request(MockRequest())


def test_wsgi_adapter_invalid_status_string_raises() -> None:
    def bad_status_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("not_a_code", [])
        return [b""]

    adapter = WsgiAdapter(bad_status_app)
    with pytest.raises(WsgiError):
        adapter.handle_request(MockRequest())


def test_wsgi_adapter_exc_info_before_headers_sent() -> None:
    def recovery_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("200 OK", [("Content-Type", "text/plain")])
        try:
            raise ValueError("changed mind")
        except ValueError:
            start_response("503 Service Unavailable", [], sys.exc_info())
        return [b"maintenance"]

    adapter = WsgiAdapter(recovery_app)
    resp = adapter.handle_request(MockRequest())
    assert resp.status == 503
    assert resp.body == b"maintenance"


def test_wsgi_adapter_exc_info_after_headers_sent_reraises() -> None:
    def late_error_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("200 OK", [])
        yield b"partial"
        try:
            raise ValueError("too late")
        except ValueError:
            start_response("500 Error", [], sys.exc_info())

    adapter = WsgiAdapter(late_error_app)
    with pytest.raises(ValueError, match="too late"):
        adapter.handle_request(MockRequest())


def test_wsgi_adapter_iterator_close_called() -> None:
    class ClosableIter:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self) -> Any:
            yield b"body"

        def close(self) -> None:
            self.closed = True

    it = ClosableIter()

    def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("200 OK", [])
        return it

    adapter = WsgiAdapter(app)
    resp = adapter.handle_request(MockRequest())
    assert resp.body == b"body"
    assert it.closed


def test_wsgi_adapter_file_wrapper_fast_path() -> None:
    f = io.BytesIO(b"file content here")

    def file_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        wrapper = environ["wsgi.file_wrapper"]
        return wrapper(f, 16)

    adapter = WsgiAdapter(file_app)
    resp = adapter.handle_request(MockRequest())
    assert resp.body == b"file content here"
    assert f.closed


def test_wsgi_extensions_large_body_bypass() -> None:
    environ = {
        "CONTENT_LENGTH": str(2 * 1024 * 1024),
        "wsgi.input": WsgiInput(b"x" * 100),
    }
    with LargeBodyBypass(environ) as e:
        assert e["vertebra.large_body"] is True
        assert isinstance(e["wsgi.input"], WsgiInput)


def test_large_body_bypass_streams_from_slab_without_full_read() -> None:
    """Verify LargeBodyBypass keeps wsgi.input bound to the zero-copy WsgiInput stream without pre-reading."""
    payload = bytes(range(256))
    captured_chunks: list[bytes] = []
    captured_stream: list[Any] = []

    def streaming_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        stream = environ["wsgi.input"]
        captured_stream.append(stream)
        captured_chunks.append(stream.read(32))
        captured_chunks.append(stream.read(32))
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        return captured_chunks

    wrapped = LargeBodyBypass(streaming_app, threshold=64)
    req = Request.from_parts(
        "POST",
        "/upload",
        body=payload,
        headers=[("content-length", str(len(payload)))],
    )
    environ = req.wsgi_environ()
    status_holder: list[str] = []

    def start_response(status: str, headers: list[tuple[str, str]], exc_info: Any = None) -> Callable[[bytes], None]:
        del headers, exc_info
        status_holder.append(status)
        return lambda _b: None

    body_iter = wrapped(environ, start_response)
    assert status_holder == ["200 OK"]
    assert environ["vertebra.large_body"] is True
    assert len(captured_stream) == 1
    stream_obj = captured_stream[0]
    assert isinstance(stream_obj, WsgiInput)
    assert b"".join(body_iter) == payload[:64]
    assert stream_obj.read(32) == payload[64:96]


def test_wsgi_file_wrapper_uses_from_file_without_header_injection(tmp_path: Path) -> None:
    """Verify WsgiAdapter serves WsgiFileWrapper via Response.from_file without x-vertebra-sendfile headers."""
    asset = tmp_path / "payload.bin"
    expected = b"zero-copy-file-wrapper-payload"
    asset.write_bytes(expected)

    def file_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        start_response("200 OK", [("Content-Type", "application/octet-stream")])
        fh = asset.open("rb")
        wrapper = environ.get("wsgi.file_wrapper", resolve_wsgi_file_wrapper)
        return wrapper(fh, 4096)

    adapter = WsgiAdapter(file_app)
    req = Request.from_parts("GET", "/download", request_id=42)
    resp = adapter.handle_request(req)
    assert resp.status == 200
    assert resp.body == expected
    assert resp.request_id == 42
    header_names = {name.lower() for name, _ in resp.headers}
    assert b"x-vertebra-sendfile" not in header_names


def test_wsgi_extensions_error_handler_catches_unhandled() -> None:
    def crashing_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        raise RuntimeError("boom")

    safe_app = wrap_with_error_handler(crashing_app, debug=True)
    adapter = WsgiAdapter(safe_app)
    resp = adapter.handle_request(MockRequest())
    assert resp.status == 500
    assert b"RuntimeError: boom" in resp.body


def test_wsgi_extensions_flask_wrapper() -> None:
    class FakeFlask:
        def wsgi_app(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"from flask"]

    wrapped = FlaskWsgiApp(FakeFlask())
    adapter = WsgiAdapter(wrapped)
    resp = adapter.handle_request(MockRequest())
    assert resp.status == 200
    assert resp.body == b"from flask"


def test_wsgi_mount_prefix_stripping() -> None:
    seen_env: dict[str, Any] = {}

    def sub_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        seen_env["SCRIPT_NAME"] = environ["SCRIPT_NAME"]
        seen_env["PATH_INFO"] = environ["PATH_INFO"]
        start_response("200 OK", [])
        return [b"ok"]

    adapter = WsgiAdapter(sub_app, mount_prefix="/legacy")
    resp = adapter.handle_request(MockRequest(path="/legacy/orders"))
    assert resp.status == 200
    assert seen_env["SCRIPT_NAME"] == "/legacy"
    assert seen_env["PATH_INFO"] == "/orders"


def test_wsgi_environ_combines_duplicate_headers_consistently() -> None:
    """Verify WsgiEnviron combines duplicate HTTP headers with ', ' both before and after key iteration."""
    req = Request.from_parts(
        "GET",
        "/items",
        headers=[
            ("accept", "text/html"),
            ("accept", "application/json"),
            ("x-custom", "first"),
            ("x-custom", "second"),
        ],
    )
    env = req.wsgi_environ()
    assert env["HTTP_ACCEPT"] == "text/html, application/json"
    assert env["HTTP_X_CUSTOM"] == "first, second"
    assert len(env) >= 2
    keys = list(env.keys())
    assert "HTTP_ACCEPT" in keys
    assert "HTTP_X_CUSTOM" in keys
    assert env["HTTP_ACCEPT"] == "text/html, application/json"
    assert env["HTTP_X_CUSTOM"] == "first, second"


def test_wsgi_errors_reflects_reassigned_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify wsgi.errors dynamically looks up sys.stderr on each access."""
    req = Request.from_parts("GET", "/log")
    env = req.wsgi_environ()
    buf = io.StringIO()
    monkeypatch.setattr(sys, "stderr", buf)
    errors_stream = env["wsgi.errors"]
    errors_stream.write("wsgi error line\n")
    assert buf.getvalue() == "wsgi error line\n"


def test_wsgi_spawn_batch_drives_multiple_requests_and_root_path() -> None:
    """Verify WsgiAdapter.spawn_batch drives single-chunk, multi-chunk, and write() apps with root_path."""
    init_transport()
    seen: list[tuple[str, str, bytes]] = []

    def batch_wsgi_app(environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        script_name = environ["SCRIPT_NAME"]
        path_info = environ["PATH_INFO"]
        body = environ["wsgi.input"].read()
        seen.append((script_name, path_info, body))
        if path_info == "/write":
            writer = start_response("201 Created", [("X-Mode", "write")])
            writer(b"via-")
            writer(b"writer")
            return []
        if path_info == "/multi":
            start_response("200 OK", [("X-Mode", "multi")])
            return (chunk for chunk in [b"part1-", b"part2"])
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"single-chunk"]

    requests = [
        Request.from_parts("POST", "/legacy/single", body=b"abc"),
        Request.from_parts("POST", "/legacy/write", body=b"def"),
        Request.from_parts("GET", "/legacy/multi"),
    ]
    adapter = WsgiAdapter(batch_wsgi_app, mount_prefix="/legacy")
    adapter.spawn_batch(requests)

    assert seen == [
        ("/legacy", "/single", b"abc"),
        ("/legacy", "/write", b"def"),
        ("/legacy", "/multi", b""),
    ]


def test_wsgi_environ_request_and_scan_in_place_headers() -> None:
    """Verify WsgiEnviron.request() and scan-in-place HTTP_* header lookup without smuggling."""
    env = WsgiEnviron.from_parts(
        method="POST",
        path="/items",
        headers=[
            (b"Content-Type", b"application/json"),
            (b"X-Custom-Token", b"tok-1"),
            (b"X-Custom-Token", b"tok-2"),
            (b"X_Spoofed_Token", b"bad"),
        ],
        body=b'{"id": 7}',
    )
    assert "CONTENT_TYPE" in env
    assert env["CONTENT_TYPE"] == "application/json"
    assert "HTTP_X_CUSTOM_TOKEN" in env
    assert env["HTTP_X_CUSTOM_TOKEN"] == "tok-1, tok-2"
    assert "HTTP_X_SPOOFED_TOKEN" not in env
    req = env.request()
    assert req.method == "POST"
    assert req.path == "/items"
    assert req.decode_msgspec() == {"id": 7}


def test_wsgi_start_response_send_raw_response_in_batch() -> None:
    """Verify _WsgiStartResponse.send_raw_response writes status, raw headers, and body in a single call."""
    init_transport()
    seen_methods: list[bool] = []

    def raw_wsgi_app(environ: dict[str, Any], start_response: Any) -> Iterable[bytes]:
        del environ
        has_raw = hasattr(start_response, "send_raw_response")
        seen_methods.append(has_raw)
        return start_response.send_raw_response(
            201,
            [(b"content-type", b"application/json"), (b"x-wsgi-fast", b"1")],
            b'{"wsgi":true}',
        )

    adapter = WsgiAdapter(raw_wsgi_app)
    resp = adapter.handle_request(Request.from_parts("GET", "/raw", request_id=88))
    assert seen_methods == [True]
    assert resp.status == 201
    assert resp.body == b'{"wsgi":true}'
    assert (b"content-type", b"application/json") in resp.headers
    assert (b"x-wsgi-fast", b"1") in resp.headers
