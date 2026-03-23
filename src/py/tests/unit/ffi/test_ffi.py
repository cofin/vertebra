"""Tests for the native FFI bridge (Ch6)."""

import runpy
import sys
from types import ModuleType, SimpleNamespace

import pytest

from vertebra._vertebra import (  # type: ignore[import-untyped]
    VertebraResponse,
)

# -- VertebraResponse --------------------------------------------------------


def test_vertebra_response_construct_basic() -> None:
    r = VertebraResponse(status=200, body=b"ok")
    assert r.status == 200
    assert bytes(r.body) == b"ok"
    assert r.request_id == 0


def test_vertebra_response_construct_empty_body() -> None:
    r = VertebraResponse(status=404)
    assert r.status == 404
    assert bytes(r.body) == b""


def test_vertebra_response_construct_with_headers_dict() -> None:
    r = VertebraResponse(
        status=200,
        body=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status == 200
    # headers returns list of tuples
    hdrs = list(r.headers)
    assert len(hdrs) == 1
    assert hdrs[0][0] == "content-type"
    assert hdrs[0][1] == "text/plain"


def test_vertebra_response_construct_with_headers_list() -> None:
    r = VertebraResponse(
        status=200,
        headers=[("x-custom", "value")],
    )
    hdrs = list(r.headers)
    assert hdrs[0][0] == "x-custom"


def test_vertebra_response_request_id_set() -> None:
    r = VertebraResponse(status=200, request_id=42)
    assert r.request_id == 42


def test_vertebra_response_repr() -> None:
    r = VertebraResponse(status=200, body=b"hi", request_id=7)
    rep = repr(r)
    assert "200" in rep
    assert "7" in rep


# -- VertebraHeaders ---------------------------------------------------------
# VertebraHeaders is not directly constructable from Python;
# it is tested indirectly via VertebraResponse round-trips above.


# -- HandlerIdCrossLanguage --------------------------------------------------


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
    from vertebra.framework._app import _make_handler_id

    assert _make_handler_id(method, path) == expected


def test_handler_id_cross_language_routing_module_matches() -> None:
    from vertebra.framework._app import _make_handler_id
    from vertebra.framework._router import handler_id

    assert handler_id("GET", "/hello") == _make_handler_id("GET", "/hello")


# -- WorkerImport ------------------------------------------------------------


def test_worker_import_app_valid() -> None:
    from vertebra.supervisor._worker import _import_app

    app = _import_app("vertebra.framework._app:VertebraApp")
    from vertebra.framework._app import VertebraApp

    assert app is VertebraApp


def test_worker_import_app_invalid_format() -> None:
    from vertebra.supervisor._worker import _import_app

    with pytest.raises(ValueError, match="module:attr"):
        _import_app("invalid_no_colon")


def test_worker_main_shm_transport_initialized_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """SHM workers must not initialize the native transport twice."""
    from vertebra.server import _native as native_module
    from vertebra.supervisor import _worker as worker_module

    init_calls: list[str] = []
    state = {"mode": "inprocess", "shm_loop_calls": 0}

    fake_ext = ModuleType("vertebra._vertebra")

    class FakeVertebraResponse:
        def __init__(self, status: int = 200, body: bytes = b"", request_id: int = 0) -> None:
            self.status = status
            self.body = body
            self.request_id = request_id
            self.headers: list[tuple[str, str]] = []

    def fake_init_transport(*, mode: str = "inprocess", **_: object) -> None:
        init_calls.append(mode)
        if len(init_calls) > 1:
            msg = "transport initialized twice"
            raise RuntimeError(msg)
        state["mode"] = mode

    def fake_get_request_notify_fd() -> int:
        return 11 if state["mode"] == "shm" else -1

    fake_ext.VertebraResponse = FakeVertebraResponse
    fake_ext.init_transport = fake_init_transport
    fake_ext.get_request_notify_fd = fake_get_request_notify_fd
    fake_ext.get_batch = lambda timeout_ms=100: None
    fake_ext.submit_results = lambda responses: None

    class FakeScheduler:
        def __init__(self) -> None:
            self.loop = object()

        def stop(self) -> None:
            return None

    class FakeExecutor:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        def shutdown(self, wait: bool = False) -> None:  # noqa: FBT001, ARG002
            return None

    async def fake_shm_worker_loop(*args: object, **kwargs: object) -> None:
        state["shm_loop_calls"] += 1

    fake_app = SimpleNamespace(_handlers={}, mounts=[])

    monkeypatch.setitem(sys.modules, "vertebra._vertebra", fake_ext)
    monkeypatch.setattr(worker_module, "_import_app", lambda app_path: fake_app)
    monkeypatch.setattr(native_module, "_install_signal_handlers", lambda: None)
    monkeypatch.setattr(native_module, "CallbackScheduler", FakeScheduler)
    monkeypatch.setattr(native_module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(native_module, "_shm_worker_loop", fake_shm_worker_loop)
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake.module:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    worker_module.main()

    assert init_calls == ["shm"]
    assert state["shm_loop_calls"] == 1


def test_worker_module_executes_main_when_run_as_script(monkeypatch: pytest.MonkeyPatch) -> None:
    """python -m vertebra.supervisor._worker must invoke main()."""
    from vertebra.server import _native as native_module

    fake_ext = ModuleType("vertebra._vertebra")
    fake_ext.init_transport = lambda *args, **kwargs: None

    fake_app = SimpleNamespace(_handlers={}, mounts=[])
    fake_app_module = ModuleType("fake_worker_app")
    fake_app_module.app = fake_app

    run_worker_calls: list[tuple[object, dict[str, object]]] = []

    monkeypatch.setitem(sys.modules, "vertebra._vertebra", fake_ext)
    monkeypatch.setitem(sys.modules, "fake_worker_app", fake_app_module)
    monkeypatch.delitem(sys.modules, "vertebra.supervisor._worker", raising=False)
    monkeypatch.setattr(
        native_module,
        "run_worker",
        lambda handlers, **kwargs: run_worker_calls.append((handlers, kwargs)),
    )
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_APP", "fake_worker_app:app")
    monkeypatch.setenv("VERTEBRA_WORKERS", "1")

    runpy.run_module("vertebra.supervisor._worker", run_name="__main__")

    assert len(run_worker_calls) == 1
    assert run_worker_calls[0][0] == {}
