"""Offline unit tests for framework baseline data benchmark apps."""

from __future__ import annotations

import asyncio
import importlib
import io
from collections.abc import Callable
from typing import Any

import pytest
import tools.benchmarks.apps.data_frameworks as data_frameworks
from tools.benchmarks.apps._data_backend import (
    JSON_HELLO_PAYLOAD,
    WORLD_ROW_COUNT,
    DataBackend,
    DataBackendConfig,
)
from tools.benchmarks.apps.data_frameworks import (
    SUPPORTED_FRAMEWORK_BASELINES,
    build_django_bolt_command,
    create_django_app,
    create_django_bolt_app,
    create_django_wsgi_app,
    create_fastapi_app,
    create_flask_app,
    create_litestar_app,
    create_spikard_data_app,
)

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

_FRAMEWORK_FACTORIES: dict[str, Callable[[DataBackend | None], Any]] = {
    "django_bolt": create_django_bolt_app,
    "spikard": create_spikard_data_app,
    "litestar": create_litestar_app,
    "fastapi": create_fastapi_app,
    "django": create_django_app,
    "django_wsgi": create_django_wsgi_app,
    "flask": create_flask_app,
}


async def _call_asgi(
    app: Any,
    method: str,
    path: str,
    *,
    query_string: bytes = b"",
    body: bytes = b"",
) -> tuple[int, bytes]:
    """Invoke an ASGI 3.0 HTTP application in-process and collect status and body."""
    headers: list[tuple[bytes, bytes]] = [(b"host", b"testserver")]
    if body:
        headers.append((b"content-length", str(len(body)).encode("ascii")))
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query_string,
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    sent_body = False
    response_done = asyncio.Event()
    status_code = 500
    chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        nonlocal sent_body
        if not sent_body:
            sent_body = True
            return {"type": "http.request", "body": body, "more_body": False}
        await response_done.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                chunks.append(bytes(chunk))
            if not message.get("more_body", False):
                response_done.set()

    await app(scope, receive, send)
    return status_code, b"".join(chunks)


def _assert_standardized_responses(
    invoke: Callable[[str, str, bytes, bytes], tuple[int, bytes]],
) -> None:
    """Assert TechEmpower and object-storage routes via a framework-neutral invoker."""
    status, body = invoke("GET", "/json", b"", b"")
    assert status == 200
    assert body == JSON_HELLO_PAYLOAD

    status, body = invoke("GET", "/db", b"", b"")
    assert status == 200
    row = decode_json(body)
    assert isinstance(row, dict)
    assert 1 <= row["id"] <= WORLD_ROW_COUNT
    assert 1 <= row["randomNumber"] <= WORLD_ROW_COUNT

    status, body = invoke("GET", "/queries", b"queries=3", b"")
    assert status == 200
    rows = decode_json(body)
    assert isinstance(rows, list)
    assert len(rows) == 3

    status, body = invoke("GET", "/updates", b"queries=2", b"")
    assert status == 200
    updated = decode_json(body)
    assert isinstance(updated, list)
    assert len(updated) == 2

    status, body = invoke("GET", "/cached-queries", b"count=4", b"")
    assert status == 200
    cached = decode_json(body)
    assert isinstance(cached, list)
    assert len(cached) == 4

    status, _ = invoke("GET", "/object/missing-key", b"", b"")
    assert status == 404

    payload = b"framework-object-payload"
    status, body = invoke("PUT", "/object/k1", b"", payload)
    assert status == 200
    assert decode_json(body) == {"key": "k1", "size": len(payload)}

    status, body = invoke("GET", "/object/k1", b"", b"")
    assert status == 200
    assert body == payload

    status, body = invoke("GET", "/health", b"", b"")
    assert status == 200
    assert body == b"ok"


def _extract_spikard_handlers(app: Any) -> dict[str, Callable[..., Any]]:
    """Extract registered handler callables from a Spikard ``App`` by function name."""
    handlers: dict[str, Callable[..., Any]] = {}
    for _kind, _args, adapter in app._app._registrations:
        for cell in adapter.__closure__ or ():
            value = cell.cell_contents
            handler = getattr(value, "handler", None)
            if callable(handler):
                handlers[handler.__name__] = handler
    return handlers


def test_module_imports_cleanly_without_optional_frameworks() -> None:
    """Verify data_frameworks imports cleanly and exposes all 6 baseline identifiers."""
    reloaded = importlib.reload(data_frameworks)
    assert reloaded.SUPPORTED_FRAMEWORK_BASELINES == (
        "django_bolt",
        "spikard",
        "litestar",
        "fastapi",
        "django",
        "django_wsgi",
        "flask",
    )
    assert SUPPORTED_FRAMEWORK_BASELINES == reloaded.SUPPORTED_FRAMEWORK_BASELINES
    for name in SUPPORTED_FRAMEWORK_BASELINES:
        assert callable(_FRAMEWORK_FACTORIES[name])


@pytest.mark.parametrize(
    ("framework", "expected_package", "expected_group"),
    [
        ("django_bolt", "django-bolt", "bench-comparators"),
        ("spikard", "spikard", "bench"),
        ("litestar", "litestar", "bench-comparators"),
        ("fastapi", "fastapi", "bench-comparators"),
        ("django", "django", "bench-comparators"),
        ("django_wsgi", "django", "bench-comparators"),
        ("flask", "flask", "bench-comparators"),
    ],
)
def test_missing_framework_raises_actionable_import_error(
    monkeypatch: pytest.MonkeyPatch,
    framework: str,
    expected_package: str,
    expected_group: str,
) -> None:
    """Verify each factory raises ImportError naming the package and uv group when absent."""
    monkeypatch.setattr(data_frameworks, "import_optional", lambda _name: None)
    factory = _FRAMEWORK_FACTORIES[framework]
    with pytest.raises(ImportError) as exc_info:
        factory(None)
    message = str(exc_info.value)
    assert expected_package in message
    assert f"uv sync --group {expected_group}" in message


@pytest.mark.parametrize(
    ("host", "port", "workers", "expected_processes"),
    [
        ("127.0.0.1", 8140, 4, "4"),
        ("0.0.0.0", 9000, 1, "1"),
        ("127.0.0.1", 8140, 0, "1"),
    ],
)
def test_build_django_bolt_command_flags(
    host: str,
    port: int,
    workers: int,
    expected_processes: str,
) -> None:
    """Verify build_django_bolt_command emits runbolt with host, port, and process flags."""
    cmd = build_django_bolt_command(host, port, workers)
    assert "runbolt" in cmd
    host_idx = cmd.index("--host")
    port_idx = cmd.index("--port")
    proc_idx = cmd.index("--processes")
    assert cmd[host_idx + 1] == host
    assert cmd[port_idx + 1] == str(port)
    assert cmd[proc_idx + 1] == expected_processes


@pytest.mark.parametrize("framework", SUPPORTED_FRAMEWORK_BASELINES)
def test_installed_framework_factories_serve_standardized_routes(framework: str) -> None:
    """Verify every installed framework factory serves standardized benchmark routes offline."""
    module_name = "django" if framework == "django_wsgi" else framework
    if import_optional(module_name) is None:
        pytest.skip(f"Optional framework {framework!r} is not installed in this environment")

    backend = DataBackend(DataBackendConfig(db_backend="memory", storage_backend="memory"))
    try:
        app = _FRAMEWORK_FACTORIES[framework](backend)
        if framework in {"litestar", "fastapi", "django"}:

            def _invoke_asgi(method: str, path: str, query: bytes, body: bytes) -> tuple[int, bytes]:
                return asyncio.run(_call_asgi(app, method, path, query_string=query, body=body))

            _assert_standardized_responses(_invoke_asgi)
        elif framework == "django_wsgi":

            def _invoke_django_wsgi(method: str, path: str, query: bytes, body: bytes) -> tuple[int, bytes]:
                captured_status = "500 Error"

                def _start_response(status: str, _headers: list[tuple[str, str]], _exc_info: Any = None) -> None:
                    nonlocal captured_status
                    captured_status = status

                environ = {
                    "REQUEST_METHOD": method,
                    "PATH_INFO": path,
                    "QUERY_STRING": query.decode("ascii"),
                    "SERVER_NAME": "127.0.0.1",
                    "SERVER_PORT": "8151",
                    "wsgi.version": (1, 0),
                    "wsgi.url_scheme": "http",
                    "wsgi.input": io.BytesIO(body),
                    "wsgi.errors": io.StringIO(),
                    "wsgi.multithread": False,
                    "wsgi.multiprocess": False,
                    "wsgi.run_once": False,
                    "CONTENT_LENGTH": str(len(body)),
                }
                chunks = list(app(environ, _start_response))
                return int(captured_status.split(" ", 1)[0]), b"".join(chunks)

            _assert_standardized_responses(_invoke_django_wsgi)
        elif framework == "flask":
            client = app.test_client()

            def _invoke_wsgi(method: str, path: str, query: bytes, body: bytes) -> tuple[int, bytes]:
                target = f"{path}?{query.decode('ascii')}" if query else path
                resp = client.open(target, method=method, data=body)
                return resp.status_code, resp.get_data()

            _assert_standardized_responses(_invoke_wsgi)
        elif framework == "spikard":
            handlers = _extract_spikard_handlers(app)
            assert len(handlers) == 8

            async def _invoke_spikard_async(
                method: str,
                path: str,
                query: bytes,
                body: bytes,
            ) -> tuple[int, bytes]:
                if path == "/json":
                    resp = await handlers["json_endpoint"]()
                elif path == "/db":
                    resp = await handlers["db_endpoint"]()
                elif path == "/queries":
                    q = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                    resp = await handlers["queries_endpoint"](queries=q)
                elif path == "/updates":
                    q = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                    resp = await handlers["updates_endpoint"](queries=q)
                elif path == "/cached-queries":
                    c = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                    resp = await handlers["cached_queries_endpoint"](count=c)
                elif path.startswith("/object/") and method == "GET":
                    key = path.removeprefix("/object/")
                    resp = await handlers["get_object_endpoint"](key=key)
                elif path.startswith("/object/") and method == "PUT":
                    key = path.removeprefix("/object/")
                    resp = await handlers["put_object_endpoint"](key=key, body=body)
                elif path == "/health":
                    resp = await handlers["health_endpoint"]()
                else:
                    raise AssertionError(f"Unexpected route {method} {path}")
                content = resp.content or ""
                raw = content if isinstance(content, bytes) else content.encode("latin1")
                return int(resp.status_code), raw

            def _invoke_spikard(method: str, path: str, query: bytes, body: bytes) -> tuple[int, bytes]:
                return asyncio.run(_invoke_spikard_async(method, path, query, body))

            _assert_standardized_responses(_invoke_spikard)
        else:
            pytest.skip("django_bolt routes are verified by test_django_bolt_routes_with_stub_api")
    finally:
        backend.close()


def test_django_bolt_routes_with_stub_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify create_django_bolt_app registers all standardized routes and handlers."""
    registered: dict[tuple[str, str], Any] = {}

    class _FakeBoltAPI:
        def get(self, path: str) -> Any:
            def _decorator(fn: Any) -> Any:
                registered[("GET", path)] = fn
                return fn

            return _decorator

        def put(self, path: str) -> Any:
            def _decorator(fn: Any) -> Any:
                registered[("PUT", path)] = fn
                return fn

            return _decorator

    class _FakeResponse:
        def __init__(self, body: Any, *, status_code: int = 200, media_type: str = "application/json") -> None:
            self.body = body
            self.status_code = status_code
            self.media_type = media_type

    class _FakeRequest:
        def __init__(self, raw_body: bytes) -> None:
            self._body = raw_body

        async def body(self) -> bytes:
            return self._body

    fake_bolt_mod = type("FakeBoltMod", (), {"BoltAPI": _FakeBoltAPI, "Response": _FakeResponse})()
    monkeypatch.setattr(
        "tools.benchmarks.apps.data_frameworks._require_framework",
        lambda module_name, *_args: fake_bolt_mod if module_name == "django_bolt" else None,
    )
    monkeypatch.setattr("tools.benchmarks.apps.data_frameworks._ensure_django_settings", lambda **_kwargs: None)

    backend = DataBackend(
        DataBackendConfig(
            db_backend="memory",
            cache_backend="memory",
            storage_backend="memory",
            storage_bucket="vertebra-framework-test",
        )
    )
    try:
        app = create_django_bolt_app(backend)
        assert isinstance(app, _FakeBoltAPI)
        assert len(registered) == 8

        async def _invoke_bolt_async(method: str, path: str, query: bytes, body: bytes) -> tuple[int, bytes]:
            if path == "/json":
                resp = await registered[("GET", "/json")]()
            elif path == "/db":
                resp = await registered[("GET", "/db")]()
            elif path == "/queries":
                q = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                resp = await registered[("GET", "/queries")](queries=q)
            elif path == "/updates":
                q = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                resp = await registered[("GET", "/updates")](queries=q)
            elif path == "/cached-queries":
                c = query.decode("ascii").split("=", 1)[1] if b"=" in query else None
                resp = await registered[("GET", "/cached-queries")](count=c)
            elif path.startswith("/object/") and method == "GET":
                key = path.removeprefix("/object/")
                resp = await registered[("GET", "/object/{key}")](key=key)
            elif path.startswith("/object/") and method == "PUT":
                key = path.removeprefix("/object/")
                resp = await registered[("PUT", "/object/{key}")](key=key, request=_FakeRequest(body))
            elif path == "/health":
                resp = await registered[("GET", "/health")]()
            else:
                raise AssertionError(f"Unexpected route {method} {path}")
            raw = resp.body if isinstance(resp.body, bytes) else str(resp.body).encode("latin1")
            return int(resp.status_code), raw

        _assert_standardized_responses(lambda m, p, q, b: asyncio.run(_invoke_bolt_async(m, p, q, b)))
    finally:
        backend.close()


def test_lazy_module_app_attributes_resolve_and_reject_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PEP 562 lazy attributes delegate to framework factories and reject unknown names."""
    sentinel = object()
    monkeypatch.setitem(data_frameworks._LAZY_APP_FACTORIES, "litestar_app", lambda: sentinel)
    assert data_frameworks.litestar_app is sentinel
    with pytest.raises(AttributeError):
        _ = data_frameworks.nonexistent_app_attr
