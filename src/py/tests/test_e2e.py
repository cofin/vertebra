"""End-to-end integration tests for the batch runner with mount dispatch."""

from typing import Any

import pytest

from vertebra.server._asgi._queues import AsgiResponse

# --- Mocks ---


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
    def __init__(
        self,
        method: str = "GET",
        path: str = "/",
        query: str | None = None,
        body: bytes | None = None,
        handler_id: int = 1,
        request_id: int = 1,
        headers: dict[str, str] | None = None,
    ):
        self.method = method
        self.path = path
        self.query = query
        self.body = body or b""
        self.handler_id = handler_id
        self.request_id = request_id
        self.headers = MockHeaders(headers)


class MockResponse:
    def __init__(
        self,
        status: int = 200,
        body: bytes = b"ok",
        request_id: int = 0,
        headers: Any = None,
    ):
        self.status = status
        self.body = body
        self.request_id = request_id
        self.headers = headers or []


# --- Phase 1: AsgiResponse request_id ---


class TestAsgiResponseRequestId:
    def test_default_request_id_is_zero(self) -> None:
        resp = AsgiResponse(status=200, headers=[], body=b"ok")
        assert resp.request_id == 0

    def test_explicit_request_id(self) -> None:
        resp = AsgiResponse(status=200, headers=[], body=b"ok", request_id=42)
        assert resp.request_id == 42

    @pytest.mark.anyio
    async def test_adapter_propagates_request_id(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        async def app(scope: dict, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        adapter = AsgiAdapter(app)
        results = await adapter.run_batch([MockRequest(request_id=99)])
        assert results[0].request_id == 99

    @pytest.mark.anyio
    async def test_adapter_propagates_request_id_on_failure(self) -> None:
        from vertebra.server._asgi._adapter import AsgiAdapter

        async def bad_app(scope: dict, receive: Any, send: Any) -> None:
            raise ValueError("boom")

        adapter = AsgiAdapter(bad_app)
        results = await adapter.run_batch([MockRequest(request_id=77)])
        assert results[0].request_id == 77
        assert results[0].status == 500


# --- Phase 2: Mount Dispatch ---


class TestProtocolDetection:
    def test_detect_wsgi(self) -> None:
        from vertebra.server._native import _detect_protocol

        def wsgi_app(environ: dict, start_response: Any) -> list:
            return []

        assert _detect_protocol(wsgi_app) == "wsgi"

    def test_detect_asgi(self) -> None:
        from vertebra.server._native import _detect_protocol

        async def asgi_app(scope: dict, receive: Any, send: Any) -> None:
            pass

        assert _detect_protocol(asgi_app) == "asgi"

    def test_detect_class_wsgi(self) -> None:
        from vertebra.server._native import _detect_protocol

        class WsgiApp:
            def __call__(self, environ: dict, start_response: Any) -> list:
                return []

        assert _detect_protocol(WsgiApp()) == "wsgi"

    def test_detect_class_asgi(self) -> None:
        from vertebra.server._native import _detect_protocol

        class AsgiApp:
            async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
                pass

        assert _detect_protocol(AsgiApp()) == "asgi"


class TestMountDispatch:
    def _dispatch_single(self, req: Any, handlers: dict, mount_runners: list, vr_cls: type) -> Any:
        """Helper: dispatch a single request through the batch API."""
        import asyncio

        from vertebra.server._native import _dispatch_batch, _partition_batch

        loop = asyncio.new_event_loop()
        try:
            partitioned = _partition_batch([req], handlers, mount_runners)
            results = _dispatch_batch(partitioned, handlers, mount_runners, vr_cls, loop)
            return results[0]
        finally:
            loop.close()

    def test_dispatch_native_handler(self) -> None:
        def handler(req: Any) -> MockResponse:
            return MockResponse(status=200, body=b"native", request_id=req.request_id)

        req = MockRequest(handler_id=1, request_id=10)
        result = self._dispatch_single(req, {1: handler}, [], MockResponse)
        assert result.status == 200
        assert result.body == b"native"

    def test_dispatch_wsgi_mount(self) -> None:
        class FakeWsgiAdapter:
            def __call__(self, request: Any, **kwargs: Any) -> MockResponse:
                return MockResponse(status=200, body=b"wsgi", request_id=request.request_id)

        req = MockRequest(handler_id=999, path="/flask/hello", request_id=20)
        mount_runners = [("/flask", FakeWsgiAdapter(), "wsgi")]
        result = self._dispatch_single(req, {}, mount_runners, MockResponse)
        assert result.status == 200
        assert result.body == b"wsgi"

    def test_dispatch_asgi_mount(self) -> None:
        class FakeAsgiRunner:
            def __call__(self, requests: list) -> list:
                return [AsgiResponse(status=200, headers=[], body=b"asgi", request_id=0)]

        req = MockRequest(handler_id=999, path="/api/items", request_id=30)
        mount_runners = [("/api", FakeAsgiRunner(), "asgi")]
        result = self._dispatch_single(req, {}, mount_runners, MockResponse)
        assert result.status == 200
        assert result.body == b"asgi"
        assert result.request_id == 30

    def test_dispatch_no_match_returns_404(self) -> None:
        req = MockRequest(handler_id=999, path="/unknown", request_id=40)
        result = self._dispatch_single(req, {}, [], MockResponse)
        assert result.status == 404

    def test_native_handler_takes_priority(self) -> None:
        """Native handler_id match beats mount prefix match."""

        def handler(req: Any) -> MockResponse:
            return MockResponse(status=200, body=b"native wins", request_id=req.request_id)

        class FakeWsgiAdapter:
            def __call__(self, request: Any, **kwargs: Any) -> MockResponse:
                return MockResponse(status=200, body=b"wsgi loses")

        req = MockRequest(handler_id=1, path="/flask/hello", request_id=50)
        mount_runners = [("/flask", FakeWsgiAdapter(), "wsgi")]
        result = self._dispatch_single(req, {1: handler}, mount_runners, MockResponse)
        assert result.body == b"native wins"


class TestBuildMountAdapters:
    def test_auto_detection_wsgi(self) -> None:
        from vertebra.server._native import _build_mount_adapters

        def wsgi_app(environ: dict, start_response: Any) -> list:
            return []

        result = _build_mount_adapters([("/wsgi", wsgi_app, "auto")])
        assert result[0][2] == "wsgi"

    def test_auto_detection_asgi(self) -> None:
        from vertebra.server._native import _build_mount_adapters

        async def asgi_app(scope: dict, receive: Any, send: Any) -> None:
            pass

        result = _build_mount_adapters([("/asgi", asgi_app, "auto")])
        assert result[0][2] == "asgi"

    def test_explicit_protocol_preserved(self) -> None:
        from vertebra.server._native import _build_mount_adapters

        result = _build_mount_adapters([("/x", lambda: None, "wsgi")])
        assert result[0][2] == "wsgi"


class TestAsgiEventLoopSharing:
    def test_shared_loop_passed_to_runner(self) -> None:
        import asyncio

        from vertebra.server._asgi._runner import AsgiRunner

        async def app(scope: dict, receive: Any, send: Any) -> None:
            pass

        loop = asyncio.new_event_loop()
        try:
            runner = AsgiRunner(app, loop=loop)
            assert runner._loop is loop
            assert not runner._owns_loop
        finally:
            loop.close()

    def test_runner_creates_own_loop_by_default(self) -> None:
        from vertebra.server._asgi._runner import AsgiRunner

        async def app(scope: dict, receive: Any, send: Any) -> None:
            pass

        runner = AsgiRunner(app)
        assert runner._owns_loop
        runner.close()

    def test_close_only_closes_owned_loop(self) -> None:
        import asyncio

        from vertebra.server._asgi._runner import AsgiRunner

        async def app(scope: dict, receive: Any, send: Any) -> None:
            pass

        loop = asyncio.new_event_loop()
        try:
            runner = AsgiRunner(app, loop=loop)
            runner.close()
            # Shared loop should still be open
            assert not loop.is_closed()
        finally:
            loop.close()


class TestWsgiEnvironHttpVersion:
    def test_default_http_version(self) -> None:
        from vertebra.server._wsgi._environ import WsgiEnviron

        req = MockRequest()
        env = WsgiEnviron(req)
        # Default falls back to HTTP/1.1 since MockRequest has no http_version
        assert env["SERVER_PROTOCOL"] == "HTTP/1.1"

    def test_custom_http_version(self) -> None:
        from vertebra.server._wsgi._environ import WsgiEnviron

        req = MockRequest()
        req.http_version = "2"
        env = WsgiEnviron(req)
        assert env["SERVER_PROTOCOL"] == "2"
