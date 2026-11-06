"""Streamable HTTP, ASGI, and WSGI route handlers for Model Context Protocol (MCP) 2026-07-28.

This module provides MCPHttpHandler, AsgiMcpApp, and WsgiMcpApp delegating HTTP
request validation, Origin checking, ETag 304 Not Modified caching, and JSON-RPC
dispatch to MCPCore.
"""

from collections.abc import AsyncIterable, Callable, Mapping
from typing import Any

from vertebra._vertebra import Request, Response, WsgiEnviron
from vertebra.agent.core.context import AgentContext
from vertebra.agent.mcp.client import MCP_NAME_FIELDS, decode_mcp_header_value
from vertebra.agent.mcp.models import MCPConfig, McpHttpRequest, McpHttpResponse
from vertebra.agent.mcp.service import CACHEABLE_METHODS, MCPCore
from vertebra.utils.module_loader import import_optional
from vertebra.utils.portal import Portal, PortalProvider

__all__ = [
    "CACHEABLE_METHODS",
    "MCP_NAME_FIELDS",
    "AsgiMcpApp",
    "MCPHttpHandler",
    "WsgiMcpApp",
    "decode_mcp_header_value",
]


class MCPHttpHandler:
    """Streamable HTTP route handler delegating validation, caching, and dispatch to MCPCore."""

    def __init__(self, service: MCPCore) -> None:
        self.service = service
        self.config: MCPConfig = service.config

    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> tuple[int, dict[str, str], bytes | AsyncIterable[bytes]]:
        """Process an incoming HTTP request through MCPCore.handle."""
        response = await self.service.handle(
            McpHttpRequest(
                method=method,
                path=path,
                headers=headers,
                body=body,
                context=context,
            )
        )
        return response.status, response.headers, response.body


class AsgiMcpApp:
    """Shared ASGI 3.0 application and Starlette/FastAPI endpoint wrapper over MCPCore."""

    def __init__(self, core: MCPCore) -> None:
        self.core = core
        self.config: MCPConfig = core.config

    async def __call__(
        self,
        scope: Mapping[str, Any] | Any,
        receive: Callable[..., Any],
        send: Callable[..., Any],
    ) -> None:
        """Handle an ASGI HTTP request and write the response to ``send``."""
        method = str(scope.get("method", "POST") if hasattr(scope, "get") else scope["method"]).upper()
        path = str(scope.get("path", self.config.path) if hasattr(scope, "get") else scope["path"])
        headers = self._extract_scope_headers(scope)
        body = await self._read_scope_body(scope, receive)
        resp = await self.core.handle(
            McpHttpRequest(
                method=method,
                path=path,
                headers=headers,
                body=body,
            )
        )
        raw_headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in resp.headers.items()]
        raw_sender = getattr(send, "send_raw_response", None)
        if callable(raw_sender):
            await raw_sender(resp.status, raw_headers, resp.body)
            return
        await send({"type": "http.response.start", "status": resp.status, "headers": raw_headers})
        await send({"type": "http.response.body", "body": resp.body, "more_body": False})

    async def handle_starlette_request(self, request: Any) -> Any:
        """Handle a Starlette or FastAPI ``Request`` and return a framework response."""
        method = str(getattr(request, "method", "POST") or "POST").upper()
        url_obj = getattr(request, "url", None)
        scope = getattr(request, "scope", None)
        path = str(
            getattr(url_obj, "path", None)
            or (scope.get("path") if isinstance(scope, Mapping) else None)
            or self.config.path
        )
        req_headers = getattr(request, "headers", None)
        if isinstance(req_headers, Mapping):
            headers = {str(k): str(v) for k, v in req_headers.items()}
        elif isinstance(scope, Mapping):
            headers = self._extract_scope_headers(scope)
        else:
            headers = {}

        body_fn = getattr(request, "body", None)
        if callable(body_fn):
            raw_body = bytes(await body_fn())
        elif scope is not None and hasattr(scope, "body_bytes"):
            raw_body = bytes(scope.body_bytes())
        else:
            raw_body = b""

        resp = await self.core.handle(
            McpHttpRequest(
                method=method,
                path=path,
                headers=headers,
                body=raw_body,
            )
        )
        responses_mod = import_optional("starlette.responses")
        if responses_mod is not None and hasattr(responses_mod, "Response"):
            return responses_mod.Response(
                content=resp.body,
                status_code=resp.status,
                headers=resp.headers,
                media_type=resp.headers.get("content-type", "application/json"),
            )
        return resp.body

    @staticmethod
    def _extract_scope_headers(scope: Any) -> dict[str, str]:
        raw = scope.get("headers") if hasattr(scope, "get") else None
        if raw is None:
            return {}
        if isinstance(raw, Mapping):
            return {str(k): str(v) for k, v in raw.items()}
        headers: dict[str, str] = {}
        for item in raw:
            if isinstance(item, tuple) and len(item) == 2:
                k, v = item
                key_str = k.decode("latin-1") if isinstance(k, (bytes, bytearray)) else str(k)
                val_str = v.decode("latin-1") if isinstance(v, (bytes, bytearray)) else str(v)
                headers[key_str] = val_str
        return headers

    @staticmethod
    async def _read_scope_body(scope: Any, receive: Callable[..., Any]) -> bytes:
        if hasattr(scope, "body_bytes"):
            return bytes(scope.body_bytes())
        chunks: list[bytes] = []
        while True:
            event = await receive()
            chunk = event.get("body", b"")
            if chunk:
                chunks.append(bytes(chunk))
            if not event.get("more_body", False):
                break
        return b"".join(chunks)


class WsgiMcpApp:
    """Shared PEP 3333 WSGI application and synchronous Request adapter over MCPCore.

    All synchronous calls reuse a persistent background event loop via ``PortalProvider.default()``
    rather than spinning up a new ``asyncio.run`` loop per request.
    """

    def __init__(self, core: MCPCore, *, portal: Portal | None = None) -> None:
        self.core = core
        self.config: MCPConfig = core.config
        self._portal = portal

    def handle_http(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
        context: AgentContext | None = None,
    ) -> McpHttpResponse:
        """Execute ``MCPCore.handle`` synchronously on the persistent ``Portal``."""
        portal = self._portal if self._portal is not None else PortalProvider.default()
        return portal.call(
            self.core.handle,
            McpHttpRequest(
                method=method,
                path=path,
                headers=headers,
                body=body,
                context=context,
            ),
        )

    def handle_vertebra_request(
        self,
        request: Request | bytes,
        *,
        default_method: str = "POST",
        default_path: str | None = None,
    ) -> Response:
        """Execute an MCP HTTP request from a native ``Request`` or raw ``bytes``."""
        resolved_path = default_path or self.config.path
        if isinstance(request, Request):
            method = request.method.upper()
            path = request.path or resolved_path
            headers = request.headers.as_dict()
            body = bytes(request.body or b"")
            req_id = int(request.request_id)
        else:
            method = default_method.upper()
            path = resolved_path
            headers = {"content-type": "application/json"}
            body = bytes(request)
            req_id = 0
        resp = self.handle_http(method, path, headers, body)
        return Response(
            status=resp.status,
            headers=list(resp.headers.items()),
            body=resp.body,
            request_id=req_id,
        )

    def handle_flask_view(self, request: Request | Any = None) -> Response | tuple[bytes, int, dict[str, str]]:
        """Handle a Flask view invocation via either Vertebra FastPath or ``flask.request``."""
        if isinstance(request, Request):
            return self.handle_vertebra_request(request)
        if request is not None and hasattr(request, "body"):
            method = str(getattr(request, "method", "POST") or "POST").upper()
            path = str(getattr(request, "path", self.config.path) or self.config.path)
            raw_headers = getattr(request, "headers", {})
            headers = (
                raw_headers.as_dict()
                if hasattr(raw_headers, "as_dict")
                else {str(k): str(v) for k, v in dict(raw_headers).items()}
            )
            resp = self.handle_http(method, path, headers, bytes(request.body or b""))
            return resp.body, resp.status, resp.headers

        flask_mod = import_optional("flask")
        if flask_mod is not None and hasattr(flask_mod, "request"):
            fl_req = flask_mod.request
            method = str(getattr(fl_req, "method", "POST") or "POST").upper()
            path = str(getattr(fl_req, "path", self.config.path) or self.config.path)
            headers = {str(k): str(v) for k, v in getattr(fl_req, "headers", {}).items()}
            raw = bytes(fl_req.get_data() or b"")
        else:
            method = "POST"
            path = self.config.path
            headers = {"content-type": "application/json"}
            raw = b"{}"
        resp = self.handle_http(method, path, headers, raw)
        return resp.body, resp.status, resp.headers

    def __call__(
        self,
        environ: WsgiEnviron | Mapping[str, Any],
        start_response: Callable[..., Any],
    ) -> list[bytes]:
        """Handle a PEP 3333 WSGI request and invoke ``start_response``."""
        if isinstance(environ, WsgiEnviron):
            vtb_resp = self.handle_vertebra_request(environ.request())
            hdrs = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in vtb_resp.headers]
            status_line = f"{vtb_resp.status} OK" if vtb_resp.status < 400 else f"{vtb_resp.status} ERROR"
            start_response(status_line, hdrs)
            return [bytes(vtb_resp.body)]

        method = str(environ.get("REQUEST_METHOD", "POST") or "POST").upper()
        path = str(environ.get("PATH_INFO", self.config.path) or self.config.path)
        headers = self._extract_wsgi_headers(environ)
        wsgi_in = environ.get("wsgi.input")
        raw = bytes(wsgi_in.read()) if wsgi_in is not None and hasattr(wsgi_in, "read") else b""
        resp = self.handle_http(method, path, headers, raw)
        status_line = f"{resp.status} OK" if resp.status < 400 else f"{resp.status} ERROR"
        start_response(status_line, list(resp.headers.items()))
        return [resp.body]

    @staticmethod
    def _extract_wsgi_headers(environ: Mapping[str, Any]) -> dict[str, str]:
        headers: dict[str, str] = {}
        content_type = environ.get("CONTENT_TYPE")
        if content_type:
            headers["content-type"] = str(content_type)
        content_length = environ.get("CONTENT_LENGTH")
        if content_length:
            headers["content-length"] = str(content_length)
        for key, value in environ.items():
            if key.startswith("HTTP_") and value is not None:
                header_name = key[5:].replace("_", "-").lower()
                headers[header_name] = str(value)
        return headers
