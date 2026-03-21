"""Tests for the ASGI adapter (Ch8)."""

from typing import Any

import pytest

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

    def keys(self):
        return list(self._headers.keys())


class MockRequest:
    def __init__(self, method="GET", path="/", query=None, body=None, handler_id=1, request_id=1, headers=None):
        self.method = method
        self.path = path
        self.query = query
        self.body = body or b""
        self.handler_id = handler_id
        self.request_id = request_id
        self.headers = MockHeaders(headers)


# --- Scope ---


class TestBuildScope:
    def test_type_is_http(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        assert scope["type"] == "http"

    def test_asgi_version(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        assert scope["asgi"] == {"version": "3.0", "spec_version": "2.3"}

    def test_method_uppercase(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest(method="get"))
        assert scope["method"] == "GET"

    def test_headers_lazy(self) -> None:
        from vertebra.server._asgi._scope import _UNBUILT, build_scope

        scope = build_scope(MockRequest(headers={"accept": "text/html"}))
        assert dict.__getitem__(scope, "headers") is _UNBUILT
        headers = scope["headers"]
        assert isinstance(headers, list)
        assert all(isinstance(h, tuple) for h in headers)

    def test_query_string_bytes(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest(query="a=1"))
        assert scope["query_string"] == b"a=1"

    def test_query_string_empty(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        assert scope["query_string"] == b""

    def test_raw_path_bytes(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest(path="/hello"))
        assert scope["raw_path"] == b"/hello"

    def test_state_is_fresh_dict(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        s1 = build_scope(MockRequest())
        s2 = build_scope(MockRequest())
        assert s1["state"] is not s2["state"]

    def test_all_required_keys(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        required = {
            "type",
            "asgi",
            "http_version",
            "method",
            "headers",
            "path",
            "raw_path",
            "query_string",
            "root_path",
            "scheme",
            "server",
            "client",
            "state",
            "extensions",
        }
        assert required.issubset(set(scope.keys()))

    def test_root_path(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest(), root_path="/api")
        assert scope["root_path"] == "/api"


# --- Queues ---


class TestReceiveChannel:
    @pytest.mark.anyio
    async def test_first_call_returns_body(self) -> None:
        from vertebra.server._asgi._queues import ReceiveChannel

        rc = ReceiveChannel(MockRequest(body=b"hello"))
        msg = await rc()
        assert msg["type"] == "http.request"
        assert msg["body"] == b"hello"
        assert msg["more_body"] is False

    @pytest.mark.anyio
    async def test_second_call_disconnect(self) -> None:
        from vertebra.server._asgi._queues import ReceiveChannel

        rc = ReceiveChannel(MockRequest())
        await rc()
        msg = await rc()
        assert msg["type"] == "http.disconnect"

    @pytest.mark.anyio
    async def test_subsequent_calls_disconnect(self) -> None:
        from vertebra.server._asgi._queues import ReceiveChannel

        rc = ReceiveChannel(MockRequest())
        await rc()
        for _ in range(5):
            msg = await rc()
            assert msg["type"] == "http.disconnect"


class TestSendCollector:
    @pytest.mark.anyio
    async def test_captures_status(self) -> None:
        from vertebra.server._asgi._queues import SendCollector

        sc = SendCollector()
        await sc({"type": "http.response.start", "status": 200, "headers": []})
        await sc({"type": "http.response.body", "body": b"ok"})
        r = sc.response
        assert r.status == 200
        assert r.body == b"ok"

    @pytest.mark.anyio
    async def test_accumulates_chunks(self) -> None:
        from vertebra.server._asgi._queues import SendCollector

        sc = SendCollector()
        await sc({"type": "http.response.start", "status": 200, "headers": []})
        await sc({"type": "http.response.body", "body": b"abc", "more_body": True})
        await sc({"type": "http.response.body", "body": b"de", "more_body": False})
        assert sc.response.body == b"abcde"

    @pytest.mark.anyio
    async def test_response_before_start_raises(self) -> None:
        from vertebra.server._asgi._queues import SendCollector

        sc = SendCollector()
        with pytest.raises(RuntimeError, match="http.response.start"):
            _ = sc.response

    @pytest.mark.anyio
    async def test_unknown_type_raises(self) -> None:
        from vertebra.server._asgi._queues import SendCollector

        sc = SendCollector()
        with pytest.raises(RuntimeError, match="unexpected"):
            await sc({"type": "http.weirdmessage"})

    @pytest.mark.anyio
    async def test_header_conversion(self) -> None:
        from vertebra.server._asgi._queues import SendCollector

        sc = SendCollector()
        await sc(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/html")],
            }
        )
        await sc({"type": "http.response.body", "body": b""})
        r = sc.response
        assert r.headers == [("content-type", "text/html")]


# --- Adapter ---


class TestAsgiAdapter:
    @pytest.mark.anyio
    async def test_simple_app(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        adapter = AsgiAdapter(app)
        results = await adapter.run_batch([MockRequest()])
        assert len(results) == 1
        assert results[0].status == 200
        assert results[0].body == b"ok"

    @pytest.mark.anyio
    async def test_batch_of_five(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            body = scope["path"].encode()
            await send({"type": "http.response.body", "body": body})

        adapter = AsgiAdapter(app)
        requests = [MockRequest(path=f"/p{i}") for i in range(5)]
        results = await adapter.run_batch(requests)
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.status == 200
            assert r.body == f"/p{i}".encode()

    @pytest.mark.anyio
    async def test_failing_task_returns_500(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        async def app(scope, receive, send):
            if scope["path"] == "/fail":
                raise ValueError("boom")
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        adapter = AsgiAdapter(app)
        requests = [MockRequest(path="/ok"), MockRequest(path="/fail"), MockRequest(path="/ok2")]
        results = await adapter.run_batch(requests)
        assert results[0].status == 200
        assert results[1].status == 500
        assert results[2].status == 200


class TestAsgiRunner:
    def test_synchronous_interface(self) -> None:
        from vertebra.server._asgi._runner import AsgiRunner

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"sync"})

        runner = AsgiRunner(app)
        try:
            results = runner([MockRequest()])
            assert results[0].status == 200
            assert results[0].body == b"sync"
        finally:
            runner.close()


# --- Extensions ---


class TestAsgiExtensions:
    def test_compile_guard_pattern(self) -> None:
        from vertebra.server._asgi._extensions import compile_guard_pattern

        pat = compile_guard_pattern("/items/{item_id}")
        assert pat.match("/items/42")
        assert pat.match("/items/abc")
        assert not pat.match("/items/42/sub")

    def test_is_static_route(self) -> None:
        from vertebra.server._asgi._extensions import is_static_route

        assert is_static_route("/hello")
        assert not is_static_route("/items/{id}")
        assert not is_static_route("/items/:id")

    def test_header_filter(self) -> None:
        from vertebra.server._asgi._extensions import HeaderFilter

        f = HeaderFilter("content-type", "application/json")
        assert f.matches([(b"content-type", b"application/json")])
        assert not f.matches([(b"content-type", b"text/html")])

    def test_register_and_get_extensions(self) -> None:
        from vertebra.server._asgi._extensions import _EXTENSIONS, get_extensions, register_extension

        orig_len = len(_EXTENSIONS)
        sentinel = object()
        register_extension(sentinel)
        exts = get_extensions()
        assert sentinel in exts
        _EXTENSIONS.pop()  # cleanup
        assert len(_EXTENSIONS) == orig_len


# --- Ch2: ASGI fast-path optimizations ---


class TestAsgiScopeOptimizations:
    """Verify scope construction optimizations (Ch2: asgi-fast-path)."""

    def test_asgi_version_shared_across_scopes(self) -> None:
        """The asgi version dict should be the same object (not copied)."""
        from vertebra.server._asgi._scope import build_scope

        s1 = build_scope(MockRequest())
        s2 = build_scope(MockRequest())
        assert s1["asgi"] is s2["asgi"]

    def test_server_none_when_no_host(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        assert scope["server"] is None

    def test_client_none_when_no_host(self) -> None:
        from vertebra.server._asgi._scope import build_scope

        scope = build_scope(MockRequest())
        assert scope["client"] is None


class TestSendCollectorFastPath:
    """Verify SendCollector single-chunk optimization."""

    @pytest.mark.anyio
    async def test_single_chunk_no_list(self) -> None:
        """Single-chunk response should not allocate _extra_chunks list."""
        from vertebra.server._asgi._queues import SendCollector

        c = SendCollector()
        await c({"type": "http.response.start", "status": 200, "headers": []})
        await c({"type": "http.response.body", "body": b"hello"})
        assert c._extra_chunks is None  # noqa: SLF001
        assert c.response.body == b"hello"

    @pytest.mark.anyio
    async def test_multi_chunk_joins(self) -> None:
        """Multiple chunks should accumulate and join correctly."""
        from vertebra.server._asgi._queues import SendCollector

        c = SendCollector()
        await c({"type": "http.response.start", "status": 200, "headers": []})
        await c({"type": "http.response.body", "body": b"abc"})
        await c({"type": "http.response.body", "body": b"de"})
        assert c._extra_chunks is not None  # noqa: SLF001
        assert c.response.body == b"abcde"


class TestGatherAdapter:
    """Verify gather()-based adapter preserves error isolation."""

    @pytest.mark.anyio
    async def test_one_failure_does_not_cancel_others(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        call_count = 0

        async def app(scope, receive, send):
            nonlocal call_count
            call_count += 1
            if scope["path"] == "/fail":
                msg = "boom"
                raise RuntimeError(msg)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        adapter = AsgiAdapter(app)
        reqs = [
            MockRequest(path="/ok", request_id=1),
            MockRequest(path="/fail", request_id=2),
            MockRequest(path="/ok", request_id=3),
        ]
        results = await adapter.run_batch(reqs)

        assert call_count == 3
        assert results[0].status == 200
        assert results[1].status == 500
        assert results[2].status == 200
