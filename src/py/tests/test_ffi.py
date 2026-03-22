"""Tests for the native FFI bridge (Ch6)."""

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
