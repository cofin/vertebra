"""Tests for the WSGI adapter (Ch7)."""

from typing import Any

import pytest

from vertebra.server._wsgi._environ import BodyStream, WsgiEnviron

# --- Mock objects ---


class MockHeaders:
    def __init__(self, headers: dict[str, str] | None = None):
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


# --- BodyStream ---


class TestBodyStream:
    def test_read_all(self) -> None:
        w = BodyStream(b"hello world")
        assert w.read() == b"hello world"
        assert w.read() == b""

    def test_read_sized(self) -> None:
        w = BodyStream(b"abcde")
        assert w.read(3) == b"abc"
        assert w.read(10) == b"de"
        assert w.read() == b""

    def test_readline(self) -> None:
        w = BodyStream(b"line1\nline2\n")
        assert w.readline() == b"line1\n"
        assert w.readline() == b"line2\n"
        assert w.readline() == b""

    def test_readlines(self) -> None:
        w = BodyStream(b"a\nb\nc\n")
        lines = w.readlines()
        assert lines == [b"a\n", b"b\n", b"c\n"]

    def test_iter(self) -> None:
        w = BodyStream(b"x\ny\n")
        assert list(w) == [b"x\n", b"y\n"]

    def test_empty_body(self) -> None:
        w = BodyStream(b"")
        assert w.read() == b""
        assert w.readline() == b""


# --- WsgiEnviron ---


class TestWsgiEnviron:
    def test_request_method(self) -> None:
        env = WsgiEnviron(MockRequest(method="GET"))
        assert env["REQUEST_METHOD"] == "GET"

    def test_path_info(self) -> None:
        env = WsgiEnviron(MockRequest(path="/hello"))
        assert env["PATH_INFO"] == "/hello"

    def test_query_string(self) -> None:
        env = WsgiEnviron(MockRequest(query="a=1&b=2"))
        assert env["QUERY_STRING"] == "a=1&b=2"

    def test_query_string_empty(self) -> None:
        env = WsgiEnviron(MockRequest())
        assert env["QUERY_STRING"] == ""

    def test_server_name_and_port(self) -> None:
        env = WsgiEnviron(MockRequest(), server_name="example.com", server_port=443)
        assert env["SERVER_NAME"] == "example.com"
        assert env["SERVER_PORT"] == "443"

    def test_wsgi_version(self) -> None:
        env = WsgiEnviron(MockRequest())
        assert env["wsgi.version"] == (1, 0)

    def test_wsgi_input_is_shm_wrapper(self) -> None:
        env = WsgiEnviron(MockRequest(body=b"test body"))
        inp = env["wsgi.input"]
        assert isinstance(inp, BodyStream)
        assert inp.read() == b"test body"

    def test_cache_none_before_access(self) -> None:
        env = WsgiEnviron(MockRequest())
        assert env._cache is None  # noqa: SLF001

    def test_cache_populated_after_access(self) -> None:
        env = WsgiEnviron(MockRequest())
        _ = env["REQUEST_METHOD"]
        assert env._cache is not None  # noqa: SLF001

    def test_missing_key_raises(self) -> None:
        env = WsgiEnviron(MockRequest())
        with pytest.raises(KeyError):
            env["NONEXISTENT_KEY"]

    def test_contains_required_keys(self) -> None:
        env = WsgiEnviron(MockRequest())
        assert "REQUEST_METHOD" in env
        assert "wsgi.version" in env
        assert "NONEXISTENT" not in env

    def test_http_headers(self) -> None:
        env = WsgiEnviron(MockRequest(headers={"Accept": "text/html", "X-Custom": "val"}))
        assert env["HTTP_ACCEPT"] == "text/html"
        assert env["HTTP_X_CUSTOM"] == "val"

    def test_content_type_from_header(self) -> None:
        env = WsgiEnviron(MockRequest(headers={"Content-Type": "application/json"}))
        assert env["CONTENT_TYPE"] == "application/json"

    def test_iter_and_len_consistent(self) -> None:
        env = WsgiEnviron(MockRequest(headers={"Accept": "text/html"}))
        keys = list(env)
        assert len(env) == len(keys)

    def test_dict_conversion(self) -> None:
        env = WsgiEnviron(MockRequest(body=b"body"))
        d = dict(env)
        assert d["REQUEST_METHOD"] == "GET"
        assert "wsgi.input" in d


# --- WsgiAdapter ---


class TestWsgiAdapter:
    def test_simple_app(self) -> None:
        from vertebra.server._wsgi._adapter import WsgiAdapter

        def simple_app(environ, start_response):
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"hello"]

        adapter = WsgiAdapter(simple_app)
        request = MockRequest()
        response = adapter(request)
        assert response.status == 200
        assert bytes(response.body) == b"hello"

    def test_status_parsing(self) -> None:
        from vertebra.server._wsgi._adapter import WsgiAdapter

        def app(environ, start_response):
            start_response("404 Not Found", [])
            return [b"not found"]

        adapter = WsgiAdapter(app)
        response = adapter(MockRequest())
        assert response.status == 404

    def test_body_chunks_joined(self) -> None:
        from vertebra.server._wsgi._adapter import WsgiAdapter

        def app(environ, start_response):
            start_response("200 OK", [])
            return [b"abc", b"de"]

        adapter = WsgiAdapter(app)
        response = adapter(MockRequest())
        assert bytes(response.body) == b"abcde"

    def test_close_called_on_iterable(self) -> None:
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

    def test_no_start_response_raises(self) -> None:
        from vertebra.server._wsgi._adapter import WsgiAdapter, WsgiError

        def bad_app(environ, start_response):
            return [b"oops"]

        adapter = WsgiAdapter(bad_app)
        with pytest.raises(WsgiError, match="start_response"):
            adapter(MockRequest())

    def test_response_headers_forwarded(self) -> None:
        from vertebra.server._wsgi._adapter import WsgiAdapter

        def app(environ, start_response):
            start_response("200 OK", [("X-Foo", "bar")])
            return [b"ok"]

        adapter = WsgiAdapter(app)
        response = adapter(MockRequest())
        hdrs = list(response.headers)
        assert any(h[0] == "X-Foo" and h[1] == "bar" for h in hdrs)


# --- Extensions ---


class TestWsgiExtensions:
    def test_large_body_bypass_noop(self) -> None:
        from vertebra.server._wsgi._extensions import LargeBodyBypass

        environ = {"CONTENT_LENGTH": "100"}
        with LargeBodyBypass(environ) as e:
            assert e is environ

    def test_wrap_with_error_handler(self) -> None:
        from vertebra.server._wsgi._extensions import wrap_with_error_handler

        def good_app(environ, start_response):
            return [b"ok"]

        wrapped = wrap_with_error_handler(good_app)
        result = wrapped({}, lambda *a: None)
        assert list(result) == [b"ok"]

    def test_flask_wsgi_app_with_instance(self) -> None:
        from vertebra.server._wsgi._extensions import FlaskWsgiApp

        class FakeFlask:
            wsgi_app = True

            def __call__(self, environ, start_response):
                return [b"flask"]

        app = FlaskWsgiApp(FakeFlask())
        result = list(app({}, lambda *a: None))
        assert result == [b"flask"]

    def test_flask_wsgi_app_with_factory(self) -> None:
        from vertebra.server._wsgi._extensions import FlaskWsgiApp

        class FakeFlask:
            def __call__(self, environ, start_response):
                return [b"from factory"]

        app = FlaskWsgiApp(lambda: FakeFlask())
        result = list(app({}, lambda *a: None))
        assert result == [b"from factory"]
