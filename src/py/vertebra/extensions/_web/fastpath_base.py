"""Shared Starlette/FastAPI request fallback, body preseeding, and response buffering helpers."""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl

from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json

__all__ = (
    "BUFFERED_RESPONSE_TYPES",
    "FallbackStarletteRequest",
    "get_starlette_request_cls",
    "preseed_request_body",
    "serialize_buffered_response",
)


def _build_buffered_response_types() -> frozenset[type]:
    """Collect exact Starlette, FastAPI, and Vertebra buffered Response classes when installed."""
    candidates: list[type] = []
    starlette_mod = import_optional("starlette.responses")
    if starlette_mod is not None:
        for name in ("Response", "HTMLResponse", "PlainTextResponse", "JSONResponse", "RedirectResponse"):
            cls = getattr(starlette_mod, name, None)
            if isinstance(cls, type):
                candidates.append(cls)
    fastapi_mod = import_optional("fastapi.responses")
    if fastapi_mod is not None:
        for name in ("ORJSONResponse", "UJSONResponse"):
            cls = getattr(fastapi_mod, name, None)
            if isinstance(cls, type):
                candidates.append(cls)
    vertebra_fastapi_mod = import_optional("vertebra.extensions.fastapi.responses")
    if vertebra_fastapi_mod is not None:
        cls = getattr(vertebra_fastapi_mod, "MsgspecJSONResponse", None)
        if isinstance(cls, type):
            candidates.append(cls)
    return frozenset(candidates)


BUFFERED_RESPONSE_TYPES: frozenset[type] = _build_buffered_response_types()


def serialize_buffered_response(
    response: Any,
    *,
    default_status: int = 200,
) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
    """Serialize an exact-type buffered response without copying its body buffer."""
    if type(response) not in BUFFERED_RESPONSE_TYPES:
        return None
    if getattr(response, "background", None) is not None:
        return None
    raw_headers = getattr(response, "raw_headers", None)
    resp_body = getattr(response, "body", None)
    if isinstance(raw_headers, list) and isinstance(resp_body, (bytes, memoryview)):
        status_code = int(getattr(response, "status_code", default_status))
        return status_code, list(raw_headers), resp_body
    return None


class FallbackStarletteRequest:
    """Lightweight Starlette ``Request`` fallback used when ``starlette`` is not installed."""

    __slots__ = ("_body", "_json", "_receive", "_send", "scope")

    def __init__(self, scope: Any, receive: Any = None, send: Any = None) -> None:
        self.scope = scope
        self._receive = receive
        self._send = send
        self._body: bytes = b""
        self._json: Any = None

    @property
    def method(self) -> str:
        return str(self.scope.get("method", "GET"))

    @property
    def path_params(self) -> dict[str, Any]:
        params: dict[str, Any] = self.scope.get("path_params") or {}
        return params

    @property
    def query_params(self) -> dict[str, str]:
        raw = self.scope.get("query_string", b"")
        text = raw.decode("latin-1") if isinstance(raw, bytes) else str(raw)
        if not text:
            return {}
        return dict(parse_qsl(text, keep_blank_values=True))

    async def body(self) -> bytes:
        return self._body

    async def json(self) -> Any:
        if self._json is None:
            self._json = decode_json(self._body)
        return self._json


def get_starlette_request_cls() -> Any:
    """Resolve ``starlette.requests.Request`` when installed, or return a lightweight fallback."""
    req_mod = import_optional("starlette.requests")
    if req_mod is not None and hasattr(req_mod, "Request"):
        return req_mod.Request
    return FallbackStarletteRequest


def preseed_request_body(scope: Any, request: Any, body: bytes | None = None) -> None:
    """Pre-populate Starlette ``Request._body`` via C-API ``PyObject_GenericSetAttr`` when available."""
    preseed_fn = getattr(scope, "preseed_request_body", None)
    if callable(preseed_fn):
        preseed_fn(request, body)
        return
    resolved = body if body is not None else b""
    request._body = resolved
