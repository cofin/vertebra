"""Tests for the native FFI bridge (Ch6)."""

import pytest

from vertebra._vertebra import (  # type: ignore[import-untyped]
    VertebraResponse,
)


class TestVertebraResponse:
    def test_construct_basic(self) -> None:
        r = VertebraResponse(status=200, body=b"ok")
        assert r.status == 200
        assert bytes(r.body) == b"ok"
        assert r.request_id == 0

    def test_construct_empty_body(self) -> None:
        r = VertebraResponse(status=404)
        assert r.status == 404
        assert bytes(r.body) == b""

    def test_construct_with_headers_dict(self) -> None:
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

    def test_construct_with_headers_list(self) -> None:
        r = VertebraResponse(
            status=200,
            headers=[("x-custom", "value")],
        )
        hdrs = list(r.headers)
        assert hdrs[0][0] == "x-custom"

    def test_request_id_set(self) -> None:
        r = VertebraResponse(status=200, request_id=42)
        assert r.request_id == 42

    def test_repr(self) -> None:
        r = VertebraResponse(status=200, body=b"hi", request_id=7)
        rep = repr(r)
        assert "200" in rep
        assert "7" in rep


class TestVertebraHeaders:
    """VertebraHeaders is normally only created from VertebraRequest.headers,
    but we can test it via VertebraResponse round-trip or direct import."""

    pass  # VertebraHeaders is not directly constructable from Python


class TestHandlerIdCrossLanguage:
    """Verify Python handler IDs match Rust reference values."""

    def test_get_hello(self) -> None:
        from vertebra.framework._app import _make_handler_id

        assert _make_handler_id("GET", "/hello") == 13629070520106334972

    def test_post_api_users(self) -> None:
        from vertebra.framework._app import _make_handler_id

        assert _make_handler_id("POST", "/api/users") == 5612694501581302581

    def test_get_root(self) -> None:
        from vertebra.framework._app import _make_handler_id

        assert _make_handler_id("GET", "/") == 14915177997962173262

    def test_delete_items_id(self) -> None:
        from vertebra.framework._app import _make_handler_id

        assert _make_handler_id("DELETE", "/items/{id}") == 9853804240848949641

    def test_routing_module_matches(self) -> None:
        from vertebra.framework._app import _make_handler_id
        from vertebra.framework._router import handler_id

        assert handler_id("GET", "/hello") == _make_handler_id("GET", "/hello")


class TestWorkerImport:
    def test_import_app_valid(self) -> None:
        from vertebra.supervisor._worker import _import_app

        app = _import_app("vertebra.framework._app:VertebraApp")
        from vertebra.framework._app import VertebraApp

        assert app is VertebraApp

    def test_import_app_invalid_format(self) -> None:
        from vertebra.supervisor._worker import _import_app

        with pytest.raises(ValueError, match="module:attr"):
            _import_app("invalid_no_colon")
