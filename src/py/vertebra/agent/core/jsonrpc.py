"""JSON-RPC 2.0 router, envelope structures, and dispatch logic.

This module provides single and batch JSON-RPC 2.0 processing, strict notification
suppression, standardized error handling, and sensitive error masking for agentic protocols.
"""

import inspect
import logging
from collections.abc import Callable, Coroutine
from typing import Any

import anyio
import msgspec

from vertebra.utils.serializers import decode_json, encode_json_bytes

try:
    from vertebra import _vertebra

    _HAS_NATIVE: bool = hasattr(_vertebra, "classify_jsonrpc_frame")
except ImportError:
    _HAS_NATIVE = False

logger = logging.getLogger(__name__)

PARSE_ERROR: int = -32700
INVALID_REQUEST: int = -32600
METHOD_NOT_FOUND: int = -32601
INVALID_PARAMS: int = -32602
INTERNAL_ERROR: int = -32603

FRAME_KIND_NOTIFICATION: int = 0
FRAME_KIND_SINGLE_REQUEST: int = 1
FRAME_KIND_BATCH_REQUEST: int = 2
FRAME_KIND_RESPONSE: int = 3


class JsonRpcError(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """JSON-RPC 2.0 error object."""

    code: int
    message: str
    data: Any = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the error payload to a serializable dictionary."""
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            payload["data"] = self.data
        return payload


class JsonRpcRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True, forbid_unknown_fields=True):
    """JSON-RPC 2.0 request or notification envelope."""

    method: str
    jsonrpc: str = "2.0"
    params: Any = None
    id: str | int | None = None

    @property
    def is_notification(self) -> bool:
        """Return True when the envelope represents a notification."""
        return self.id is None


class JsonRpcResponse(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """JSON-RPC 2.0 response envelope."""

    jsonrpc: str = "2.0"
    id: str | int | None = None
    result: Any = None
    error: JsonRpcError | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the response payload to a serializable dictionary."""
        payload: dict[str, Any] = {"jsonrpc": self.jsonrpc, "id": self.id}
        if self.error is not None:
            payload["error"] = self.error.to_dict()
        else:
            payload["result"] = self.result
        return payload


class JsonRpcErrorException(Exception):
    """Exception indicating a structured JSON-RPC error response."""

    def __init__(
        self,
        error: JsonRpcError,
        request_id: str | int | None = None,
    ) -> None:
        super().__init__(error.message)
        self.error = error
        self.request_id = request_id


type HandlerCallable = Callable[..., Coroutine[Any, Any, Any] | Any]


class JsonRpcRouter:
    """Dispatches JSON-RPC 2.0 single and batch requests to registered handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerCallable] = {}

    def register(self, method: str, handler: HandlerCallable) -> None:
        """Register a handler function for a JSON-RPC method."""
        self._handlers[method] = handler

    def has_method(self, method: str) -> bool:
        """Return True when a handler is registered for the specified method."""
        return method in self._handlers

    async def _invoke_handler(
        self,
        handler: HandlerCallable,
        params: Any,
        context: Any,
    ) -> Any:
        """Invoke a handler with params and optional context injection."""
        sig = inspect.signature(handler)
        kwargs: dict[str, Any] = {}
        accepts_context = "context" in sig.parameters or any(
            p.annotation.__name__ == "AgentContext"
            for p in sig.parameters.values()
            if hasattr(p.annotation, "__name__")
        )

        if accepts_context and context is not None:
            for p_name, p in sig.parameters.items():
                if p_name == "context" or (
                    hasattr(p.annotation, "__name__") and p.annotation.__name__ == "AgentContext"
                ):
                    kwargs[p_name] = context

        if params is None:
            result = handler(**kwargs) if kwargs else handler()
        elif isinstance(params, dict):
            call_kwargs = dict(params)
            call_kwargs.update(kwargs)
            try:
                result = handler(**call_kwargs)
            except TypeError:
                if "params" in sig.parameters:
                    call_kwargs_fallback = {"params": params}
                    if accepts_context and context is not None:
                        call_kwargs_fallback["context"] = context
                    result = handler(**call_kwargs_fallback)
                elif len(sig.parameters) == 1 and not kwargs:
                    result = handler(params)
                elif len(sig.parameters) == 2 and kwargs:
                    result = handler(params, **kwargs)
                else:
                    raise
        elif isinstance(params, (list, tuple)):
            result = handler(*params, **kwargs) if kwargs else handler(*params)
        else:
            result = handler(params, **kwargs) if kwargs else handler(params)

        if inspect.isawaitable(result):
            return await result
        return result

    async def dispatch(
        self,
        request: JsonRpcRequest,
        context: Any = None,
    ) -> dict[str, Any] | None:
        """Dispatch a single JSON-RPC request and return the response dictionary or None."""
        handler = self._handlers.get(request.method)
        if handler is None:
            if request.is_notification:
                return None
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {
                    "code": METHOD_NOT_FOUND,
                    "message": f"Method not found: {request.method}",
                },
            }

        try:
            result = await self._invoke_handler(handler, request.params, context)
            if request.is_notification:
                return None
            return {"jsonrpc": "2.0", "id": request.id, "result": result}
        except JsonRpcErrorException as exc:
            if request.is_notification:
                return None
            return {
                "jsonrpc": "2.0",
                "id": request.id if exc.request_id is None else exc.request_id,
                "error": exc.error.to_dict(),
            }
        except Exception as exc:
            logger.exception("JSON-RPC handler raised unhandled exception for method %s", request.method)
            if request.is_notification:
                return None
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {
                    "code": INTERNAL_ERROR,
                    "message": "Internal error",
                    "data": {"error": type(exc).__name__},
                },
            }

    async def dispatch_payload(
        self,
        raw_bytes: bytes,
        context: Any = None,
    ) -> bytes | None:
        """Parse raw payload bytes, dispatch requests, and return encoded response bytes."""
        if not raw_bytes or not raw_bytes.strip():
            error_resp = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": PARSE_ERROR, "message": "Parse error: empty payload"},
            }
            return encode_json_bytes(error_resp)

        try:
            parsed = decode_json(raw_bytes)
        except Exception:
            error_resp = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": PARSE_ERROR, "message": "Parse error: invalid JSON"},
            }
            return encode_json_bytes(error_resp)

        if isinstance(parsed, list):
            if not parsed:
                error_resp = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": INVALID_REQUEST, "message": "Invalid request: empty batch"},
                }
                return encode_json_bytes(error_resp)

            batch_results: list[dict[str, Any] | None] = [None] * len(parsed)

            async def process_entry(index: int, item: Any) -> None:
                if not isinstance(item, dict) or item.get("jsonrpc") != "2.0" or "method" not in item:
                    item_id = item.get("id") if isinstance(item, dict) else None
                    batch_results[index] = {
                        "jsonrpc": "2.0",
                        "id": item_id,
                        "error": {"code": INVALID_REQUEST, "message": "Invalid request envelope"},
                    }
                    return

                req = JsonRpcRequest(
                    method=str(item["method"]),
                    jsonrpc=str(item.get("jsonrpc", "2.0")),
                    params=item.get("params"),
                    id=item.get("id"),
                )
                batch_results[index] = await self.dispatch(req, context)

            async with anyio.create_task_group() as tg:
                for idx, entry in enumerate(parsed):
                    tg.start_soon(process_entry, idx, entry)

            valid_responses = [res for res in batch_results if res is not None]
            if not valid_responses:
                return None
            return encode_json_bytes(valid_responses)

        if isinstance(parsed, dict):
            if parsed.get("jsonrpc") != "2.0" or "method" not in parsed:
                item_id = parsed.get("id")
                error_resp = {
                    "jsonrpc": "2.0",
                    "id": item_id,
                    "error": {"code": INVALID_REQUEST, "message": "Invalid request envelope"},
                }
                return encode_json_bytes(error_resp)

            req = JsonRpcRequest(
                method=str(parsed["method"]),
                jsonrpc=str(parsed.get("jsonrpc", "2.0")),
                params=parsed.get("params"),
                id=parsed.get("id"),
            )
            resp = await self.dispatch(req, context)
            if resp is None:
                return None
            return encode_json_bytes(resp)

        error_resp = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": INVALID_REQUEST, "message": "Invalid request: expected object or array"},
        }
        return encode_json_bytes(error_resp)


__all__ = [
    "FRAME_KIND_BATCH_REQUEST",
    "FRAME_KIND_NOTIFICATION",
    "FRAME_KIND_RESPONSE",
    "FRAME_KIND_SINGLE_REQUEST",
    "INTERNAL_ERROR",
    "INVALID_PARAMS",
    "INVALID_REQUEST",
    "METHOD_NOT_FOUND",
    "PARSE_ERROR",
    "JsonRpcError",
    "JsonRpcErrorException",
    "JsonRpcRequest",
    "JsonRpcResponse",
    "JsonRpcRouter",
]
