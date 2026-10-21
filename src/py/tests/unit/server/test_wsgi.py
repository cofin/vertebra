"""Tests for the WSGI adapter (Ch7)."""

from typing import Any

import pytest

from vertebra.server._wsgi._environ import BodyStream, WsgiEnviron


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


class MockRequest:
    def __init__(self, method="GET", path="/", query=None, body=None, handler_id=1, request_id=1, headers=None):
        self.method = method
        self.path = path
        self.query = query
        self.body = body
        self.handler_id = handler_id
        self.request_id = request_id
        self.headers = MockHeaders(headers)


def test_body_stream_read_all() -> None:
    w = BodyStream(b"hello world")
    assert w.read() == b"hello world"
    assert w.read() == b""


def test_body_stream_read_sized() -> None:
    w = BodyStream(b"abcde")
    assert w.read(3) == b"abc"
    assert w.read(10) == b"de"
    assert w.read() == b""


def test_body_stream_readline() -> None:
    w = BodyStream(b"line1\nline2\n")
    assert w.readline() == b"line1\n"
    assert w.readline() == b"line2\n"
    assert w.readline() == b""


def test_body_stream_readlines() -> None:
    w = BodyStream(b"a\nb\nc\n")
    lines = w.readlines()
    assert lines == [b"a\n", b"b\n", b"c\n"]


def test_body_stream_iter() -> None:
    w = BodyStream(b"x\ny\n")
    assert list(w) == [b"x\n", b"y\n"]


def test_body_stream_empty_body() -> None:
    w = BodyStream(b"")
    assert w.read() == b""
    assert w.readline() == b""


def test_wsgi_environ_request_method() -> None:
    env = WsgiEnviron(MockRequest(method="GET"))
    assert env["REQUEST_METHOD"] == "GET"


def test_wsgi_environ_path_info() -> None:
    env = WsgiEnviron(MockRequest(path="/hello"))
    assert env["PATH_INFO"] == "/hello"


def test_wsgi_environ_query_string() -> None:
    env = WsgiEnviron(MockRequest(query="a=1&b=2"))
    assert env["QUERY_STRING"] == "a=1&b=2"


def test_wsgi_environ_query_string_empty() -> None:
    env = WsgiEnviron(MockRequest())
    assert env["QUERY_STRING"] == ""


def test_wsgi_environ_server_name_and_port() -> None:
    env = WsgiEnviron(MockRequest(), server_name="example.com", server_port=443)
    assert env["SERVER_NAME"] == "example.com"
    assert env["SERVER_PORT"] == "443"


def test_wsgi_environ_wsgi_version() -> None:
    env = WsgiEnviron(MockRequest())
    assert env["wsgi.version"] == (1, 0)


def test_wsgi_environ_wsgi_input_is_shm_wrapper() -> None:
    env = WsgiEnviron(MockRequest(body=b"test body"))
    inp = env["wsgi.input"]
    assert isinstance(inp, BodyStream)
    assert inp.read() == b"test body"


def test_wsgi_environ_cache_pre_seeded() -> None:
    env = WsgiEnviron(MockRequest())
    assert env._cache is not None
    assert "REQUEST_METHOD" in env._cache


def test_wsgi_environ_cache_grows_on_dynamic_access() -> None:
    env = WsgiEnviron(MockRequest())
    initial_size = len(env._cache)
    _ = env["wsgi.input"]
    assert len(env._cache) > initial_size


def test_wsgi_environ_missing_key_raises() -> None:
    env = WsgiEnviron(MockRequest())
    with pytest.raises(KeyError):
        env["NONEXISTENT_KEY"]


def test_wsgi_environ_contains_required_keys() -> None:
    env = WsgiEnviron(MockRequest())
    assert "REQUEST_METHOD" in env
    assert "wsgi.version" in env
    assert "NONEXISTENT" not in env


def test_wsgi_environ_http_headers() -> None:
    env = WsgiEnviron(MockRequest(headers={"Accept": "text/html", "X-Custom": "val"}))
    assert env["HTTP_ACCEPT"] == "text/html"
    assert env["HTTP_X_CUSTOM"] == "val"


def test_wsgi_environ_content_type_from_header() -> None:
    env = WsgiEnviron(MockRequest(headers={"Content-Type": "application/json"}))
    assert env["CONTENT_TYPE"] == "application/json"


def test_wsgi_environ_iter_and_len_consistent() -> None:
    env = WsgiEnviron(MockRequest(headers={"Accept": "text/html"}))
    keys = list(env)
    assert len(env) == len(keys)


def test_wsgi_environ_dict_conversion() -> None:
    env = WsgiEnviron(MockRequest(body=b"body"))
    d = dict(env)
    assert d["REQUEST_METHOD"] == "GET"
    assert "wsgi.input" in d


def test_wsgi_adapter_simple_app() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter

    def simple_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"hello"]

    adapter = WsgiAdapter(simple_app)
    request = MockRequest()
    response = adapter(request)
    assert response.status == 200
    assert bytes(response.body) == b"hello"


def test_wsgi_adapter_status_parsing() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter

    def app(environ, start_response):
        start_response("404 Not Found", [])
        return [b"not found"]

    adapter = WsgiAdapter(app)
    response = adapter(MockRequest())
    assert response.status == 404


def test_wsgi_adapter_body_chunks_joined() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter

    def app(environ, start_response):
        start_response("200 OK", [])
        return [b"abc", b"de"]

    adapter = WsgiAdapter(app)
    response = adapter(MockRequest())
    assert bytes(response.body) == b"abcde"


def test_wsgi_adapter_close_called_on_iterable() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter

    closed = []

    class ClosableIter:
        def __iter__(self):
            yield b"data"

        def close(self):
            closed.append(True)

    def app(environ, start_response):
        start_response("200 OK", [])
        return ClosableIter()

    adapter = WsgiAdapter(app)
    adapter(MockRequest())
    assert closed == [True]


def test_wsgi_adapter_no_start_response_raises() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter, WsgiError

    def bad_app(environ, start_response):
        return [b"oops"]

    adapter = WsgiAdapter(bad_app)
    with pytest.raises(WsgiError, match="start_response"):
        adapter(MockRequest())


def test_wsgi_adapter_response_headers_forwarded() -> None:
    from vertebra.server._wsgi._adapter import WsgiAdapter

    def app(environ, start_response):
        start_response("200 OK", [("X-Foo", "bar")])
        return [b"ok"]

    adapter = WsgiAdapter(app)
    response = adapter(MockRequest())
    hdrs = list(response.headers)
    assert any(h[0] == b"X-Foo" and h[1] == b"bar" for h in hdrs)


def test_wsgi_extensions_large_body_bypass_noop() -> None:
    from vertebra.server._wsgi._extensions import LargeBodyBypass

    environ = {"CONTENT_LENGTH": "100"}
    with LargeBodyBypass(environ) as e:
        assert e is environ


def test_wsgi_extensions_wrap_with_error_handler() -> None:
    from vertebra.server._wsgi._extensions import wrap_with_error_handler

    def good_app(environ, start_response):
        return [b"ok"]

    wrapped = wrap_with_error_handler(good_app)
    result = wrapped({}, lambda *a: None)
    assert list(result) == [b"ok"]


def test_wsgi_extensions_flask_wsgi_app_with_instance() -> None:
    from vertebra.server._wsgi._extensions import FlaskWsgiApp

    class FakeFlask:
        wsgi_app = True

        def __call__(self, environ, start_response):
            return [b"flask"]

    app = FlaskWsgiApp(FakeFlask())
    result = list(app({}, lambda *a: None))
    assert result == [b"flask"]


def test_wsgi_extensions_flask_wsgi_app_with_factory() -> None:
    from vertebra.server._wsgi._extensions import FlaskWsgiApp

    class FakeFlask:
        def __call__(self, environ, start_response):
            return [b"from factory"]

    app = FlaskWsgiApp(lambda: FakeFlask())
    result = list(app({}, lambda *a: None))
    assert result == [b"from factory"]
