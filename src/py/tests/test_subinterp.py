"""Tests for vertebra.server._ipc._subinterp — sub-interpreter WSGI bridge."""

from vertebra.server._ipc._ring_reader import RawRequest, decode_headers
from vertebra.server._ipc._subinterp import build_wsgi_environ, call_wsgi_app, interpreters_available


def _make_request(
    *,
    method: str = "GET",
    path: str = "/hello",
    query: str | None = None,
    headers_raw: bytes = b"",
    body: bytes = b"",
) -> RawRequest:
    return RawRequest(
        method=method,
        http_version="1.1",
        handler_id=42,
        correlation_id=1,
        path=path,
        query=query,
        headers_raw=headers_raw,
        body=body,
    )


def _encode_headers(pairs: list[tuple[str, str]]) -> bytes:
    buf = bytearray()
    for name, value in pairs:
        buf.append(len(name))
        buf.extend(name.encode())
        buf.extend(len(value).to_bytes(2, "little"))
        buf.extend(value.encode())
    return bytes(buf)


class TestInterpretersAvailable:
    def test_returns_bool(self):
        result = interpreters_available()
        assert isinstance(result, bool)


class TestBuildWsgiEnviron:
    def test_basic_get(self):
        req = _make_request(path="/hello")
        environ = build_wsgi_environ(req, [])

        assert environ["REQUEST_METHOD"] == "GET"
        assert environ["PATH_INFO"] == "/hello"
        assert environ["QUERY_STRING"] == ""
        assert environ["SERVER_NAME"] == "localhost"
        assert environ["SERVER_PORT"] == "8000"
        assert environ["SERVER_PROTOCOL"] == "HTTP/1.1"
        assert environ["wsgi.version"] == (1, 0)
        assert environ["wsgi.url_scheme"] == "http"

    def test_with_query_string(self):
        req = _make_request(path="/search", query="q=test&page=1")
        environ = build_wsgi_environ(req, [])
        assert environ["QUERY_STRING"] == "q=test&page=1"

    def test_percent_encoded_path(self):
        req = _make_request(path="/hello%20world")
        environ = build_wsgi_environ(req, [])
        assert environ["PATH_INFO"] == "/hello world"

    def test_headers_mapped_to_cgi(self):
        headers = [("host", "example.com"), ("accept", "text/html")]
        req = _make_request()
        environ = build_wsgi_environ(req, headers)
        assert environ["HTTP_HOST"] == "example.com"
        assert environ["HTTP_ACCEPT"] == "text/html"

    def test_content_type_special_handling(self):
        headers = [("content-type", "application/json")]
        req = _make_request()
        environ = build_wsgi_environ(req, headers)
        assert environ["CONTENT_TYPE"] == "application/json"
        assert "HTTP_CONTENT_TYPE" not in environ

    def test_content_length_special_handling(self):
        headers = [("content-length", "42")]
        req = _make_request()
        environ = build_wsgi_environ(req, headers)
        assert environ["CONTENT_LENGTH"] == "42"
        assert "HTTP_CONTENT_LENGTH" not in environ

    def test_body_as_wsgi_input(self):
        req = _make_request(body=b"request body data")
        environ = build_wsgi_environ(req, [])
        assert environ["wsgi.input"].read() == b"request body data"

    def test_custom_server_name_port(self):
        req = _make_request()
        environ = build_wsgi_environ(req, [], server_name="0.0.0.0", server_port=9000)
        assert environ["SERVER_NAME"] == "0.0.0.0"
        assert environ["SERVER_PORT"] == "9000"

    def test_headers_from_raw_bytes(self):
        """End-to-end: encode headers → decode → build environ."""
        raw = _encode_headers([("host", "example.com"), ("content-type", "text/plain")])
        headers = decode_headers(raw)
        req = _make_request(headers_raw=raw)
        environ = build_wsgi_environ(req, headers)
        assert environ["HTTP_HOST"] == "example.com"
        assert environ["CONTENT_TYPE"] == "text/plain"


class TestCallWsgiApp:
    def test_hello_world_app(self):
        def app(environ, start_response):
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"Hello, World!"]

        req = _make_request()
        environ = build_wsgi_environ(req, [])
        status, headers, body = call_wsgi_app(app, environ)

        assert status == 200
        assert body == b"Hello, World!"
        assert ("Content-Type", "text/plain") in headers

    def test_app_reads_body(self):
        def app(environ, start_response):
            body = environ["wsgi.input"].read()
            start_response("200 OK", [])
            return [body]

        req = _make_request(method="POST", body=b"input data")
        environ = build_wsgi_environ(req, [])
        status, _, body = call_wsgi_app(app, environ)

        assert status == 200
        assert body == b"input data"

    def test_app_reads_query(self):
        def app(environ, start_response):
            qs = environ["QUERY_STRING"]
            start_response("200 OK", [])
            return [qs.encode()]

        req = _make_request(query="key=value")
        environ = build_wsgi_environ(req, [])
        _, _, body = call_wsgi_app(app, environ)
        assert body == b"key=value"

    def test_app_exception_returns_500(self):
        def app(environ, start_response):
            msg = "boom"
            raise RuntimeError(msg)

        req = _make_request()
        environ = build_wsgi_environ(req, [])
        status, _, body = call_wsgi_app(app, environ)

        assert status == 500
        assert b"boom" in body

    def test_app_no_start_response_returns_500(self):
        def app(environ, start_response):
            return [b"forgot to call start_response"]

        req = _make_request()
        environ = build_wsgi_environ(req, [])
        status, _, body = call_wsgi_app(app, environ)

        assert status == 500
        assert b"start_response" in body

    def test_multiple_body_chunks(self):
        def app(environ, start_response):
            start_response("200 OK", [])
            return [b"chunk1", b"chunk2", b"chunk3"]

        req = _make_request()
        environ = build_wsgi_environ(req, [])
        _, _, body = call_wsgi_app(app, environ)
        assert body == b"chunk1chunk2chunk3"

    def test_close_called_on_result(self):
        closed = []

        class ClosableIterator:
            def __iter__(self):
                yield b"data"

            def close(self):
                closed.append(True)

        def app(environ, start_response):
            start_response("200 OK", [])
            return ClosableIterator()

        req = _make_request()
        environ = build_wsgi_environ(req, [])
        call_wsgi_app(app, environ)
        assert closed == [True]
