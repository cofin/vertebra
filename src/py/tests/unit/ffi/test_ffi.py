"""Tests for the native FFI bridge (Ch6)."""

import asyncio
import runpy
import sys
import threading
import time
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from vertebra._vertebra import (
    Response,
    cancel_request_sleep,
    init_transport,
    prepare_request_sleep,
    submit_response,
)
from vertebra.framework._app import App, _make_handler_id
from vertebra.framework._router import handler_id
from vertebra.server import worker as server_worker_module
from vertebra.server._dispatch import (
    BatchDispatchPlan,
    compile_handler_spec,
    dispatch_batch,
)
from vertebra.server._loops import EventLoopPool
from vertebra.supervisor import _worker as worker_module
from vertebra.utils.module_loader import import_app


def test_vertebra_response_construct_basic() -> None:
    r = Response(status=200, body=b"ok")
    assert r.status == 200
    assert bytes(r.body) == b"ok"
    assert r.request_id == 0


def test_vertebra_response_construct_empty_body() -> None:
    r = Response(status=404)
    assert r.status == 404
    assert bytes(r.body) == b""


def test_vertebra_response_construct_with_headers_dict() -> None:
    r = Response(
        status=200,
        body=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status == 200
    hdrs = list(r.headers)
    assert len(hdrs) == 1
    assert hdrs[0][0] == b"content-type"
    assert hdrs[0][1] == b"text/plain"


def test_vertebra_response_construct_with_headers_list() -> None:
    r = Response(
        status=200,
        headers=[("x-custom", "value")],
    )
    hdrs = list(r.headers)
    assert hdrs[0][0] == b"x-custom"


def test_vertebra_response_request_id_set() -> None:
    r = Response(status=200, request_id=42)
    assert r.request_id == 42


def test_vertebra_response_request_id_mutated_in_place() -> None:
    r = Response(status=200, body=b"ok", headers=[("x-test", "1")])
    assert r.request_id == 0
    r.request_id = 99
    assert r.request_id == 99
    assert list(r.headers) == [(b"x-test", b"1")]


def test_vertebra_response_repr() -> None:
    r = Response(status=200, body=b"hi", request_id=7)
    rep = repr(r)
    assert "200" in rep
    assert "7" in rep


@pytest.mark.parametrize(
    ("method", "path", "expected"),
    [
        ("GET", "/hello", 13629070520106334972),
        ("POST", "/api/users", 5612694501581302581),
        ("GET", "/", 14915177997962173262),
        ("DELETE", "/items/{id}", 9853804240848949641),
    ],
)
def test_handler_id_cross_language(method: str, path: str, expected: int) -> None:
    """Verify Python handler IDs match Rust reference values."""
    assert _make_handler_id(method, path) == expected


def test_handler_id_cross_language_routing_module_matches() -> None:
    assert handler_id("GET", "/hello") == _make_handler_id("GET", "/hello")


def test_worker_import_app_valid() -> None:
    app = import_app("vertebra.framework._app:App")
    assert app is App


def test_worker_import_app_invalid_format() -> None:
    with pytest.raises(ValueError, match="module:attr"):
        import_app("invalid_no_colon")


def test_worker_main_shm_transport_initialized_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """SHM workers must not initialize the native transport twice."""
    init_calls: list[str] = []
    stop_event = threading.Event()

    def fake_init_transport(*, mode: str = "inprocess", **_: object) -> None:
        init_calls.append(mode)
        if len(init_calls) > 1:
            msg = "transport initialized twice"
            raise RuntimeError(msg)

    def fake_get_batch(timeout_ms: int = 100) -> None:
        del timeout_ms
        stop_event.set()
        return None

    fake_app = SimpleNamespace(_handlers={}, mounts=[])
    orig_worker_cls = server_worker_module.Worker

    def make_worker(handlers: Any, **kwargs: Any) -> Any:
        kwargs["stop_event"] = stop_event
        return orig_worker_cls(handlers, **kwargs)

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: fake_app)
    monkeypatch.setattr(server_worker_module, "Worker", make_worker)
    monkeypatch.setattr(server_worker_module, "init_transport", fake_init_transport)
    monkeypatch.setattr(server_worker_module, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(server_worker_module, "get_batch", fake_get_batch)
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake.module:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    worker_module.main()

    assert init_calls == ["shm"]


def test_worker_main_clears_executor_on_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Worker lazily allocates ThreadPoolExecutor only when needed and shuts it down on exit."""
    init_calls: list[int] = []
    shutdown_calls: list[bool] = []
    stop_event = threading.Event()
    stop_event.set()

    def blocking_handler() -> bytes:
        return b"ok"

    inline_only_app = App()

    @inline_only_app.get("/fast")
    def fast_handler() -> bytes:
        return b"ok"

    threaded_app = App()
    threaded_app.get("/block", sync_to_thread=True)(blocking_handler)

    orig_worker_cls = server_worker_module.Worker

    class FakeExecutor:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            init_calls.append(1)

        def shutdown(self, *, wait: bool = False) -> None:
            shutdown_calls.append(wait)

    def make_worker(handlers: Any, **kwargs: Any) -> Any:
        kwargs["stop_event"] = stop_event
        return orig_worker_cls(handlers, **kwargs)

    monkeypatch.setattr(server_worker_module, "Worker", make_worker)
    monkeypatch.setattr(server_worker_module, "init_transport", lambda **kwargs: None)
    monkeypatch.setattr(server_worker_module, "get_request_notify_fd", lambda: -1)
    monkeypatch.setattr(server_worker_module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake.module:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: inline_only_app)
    worker_module.main()
    assert init_calls == []
    assert shutdown_calls == []

    monkeypatch.setattr(worker_module, "import_app", lambda app_path: threaded_app)
    worker_module.main()
    assert len(init_calls) == 1
    assert len(shutdown_calls) == 1
    assert not hasattr(server_worker_module, "_wsgi_executor")


def test_worker_module_executes_main_when_run_as_script(monkeypatch: pytest.MonkeyPatch) -> None:
    """python -m vertebra.supervisor._worker must invoke main()."""
    fake_app = SimpleNamespace(_handlers={}, mounts=[])
    fake_app_module: Any = ModuleType("fake_worker_app")
    fake_app_module.app = fake_app

    run_worker_calls: list[tuple[object, dict[str, object]]] = []

    monkeypatch.setitem(sys.modules, "fake_worker_app", fake_app_module)
    monkeypatch.delitem(sys.modules, "vertebra.supervisor._worker", raising=False)
    monkeypatch.setattr(
        server_worker_module,
        "run_worker",
        lambda handlers, **kwargs: run_worker_calls.append((handlers, kwargs)),
    )
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake_worker_app:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    runpy.run_module("vertebra.supervisor._worker", run_name="__main__")

    assert len(run_worker_calls) == 1
    assert run_worker_calls[0][0] == {}


def test_submit_response_exists() -> None:
    init_transport()
    resp = Response(status=200, body=b"hello", request_id=123)
    submit_response(resp)


def test_non_blocking_dispatch() -> None:
    init_transport()
    scheduler = EventLoopPool(size=1)

    async def slow_handler() -> bytes:
        await asyncio.sleep(0.1)
        return b"slow"

    class Req:
        def __init__(self, rid: int) -> None:
            self.request_id = rid
            self.handler_id = 1

    spec = compile_handler_spec(slow_handler)
    assert isinstance(hash(spec), int)
    batch = [Req(1), Req(2)]
    partitioned = BatchDispatchPlan(async_native=[(0, batch[0], spec), (1, batch[1], spec)])
    with pytest.raises(AttributeError):
        partitioned.async_native = []

    start = time.monotonic()
    dispatch_batch(partitioned, {1: spec}, [], Response, scheduler, async_mode=True)
    elapsed = time.monotonic() - start

    assert elapsed < 0.05
    scheduler.stop()


def test_dekker_request_sleep_helpers_roundtrip() -> None:
    """Verify prepare_request_sleep and cancel_request_sleep FFI helpers execute cleanly."""
    init_transport()
    should_sleep = prepare_request_sleep()
    assert isinstance(should_sleep, bool)
    assert should_sleep is False
    cancel_request_sleep()
