"""RSGI v1.6 application adapter for Vertebra."""

import asyncio
import inspect
from collections.abc import Callable, Iterable
from typing import Any

from vertebra._vertebra import (
    Request as _NativeRequest,
)
from vertebra._vertebra import (
    RequestBatch,
    Response,
    RsgiHttpProtocol,
    RsgiScope,
    drive_rsgi_batch,
    submit_response,
)
from vertebra.server._protocol import InterfaceMode, detect_interface, is_rsgi_app

__all__ = (
    "InterfaceMode",
    "RsgiAdapter",
    "detect_interface",
    "is_rsgi_app",
)


class RsgiAdapter:
    """Adapter that drives an RSGI v1.6 application callable `(scope, protocol)`."""

    __slots__ = ("_app", "_client", "_is_async", "_mount_prefix", "_scheme", "_server", "_target")

    def __init__(
        self,
        app: Callable[..., Any],
        *,
        mount_prefix: str = "",
        server: str = "127.0.0.1:8000",
        client: str = "127.0.0.1:0",
        scheme: str = "http",
    ) -> None:
        self._app = app
        self._target: Callable[..., Any] = getattr(app, "__rsgi__", app)
        self._mount_prefix = mount_prefix.rstrip("/")
        self._server = server
        self._client = client
        self._scheme = scheme
        call_attr = type(app).__dict__.get("__call__")
        self._is_async = (
            inspect.iscoroutinefunction(self._target)
            or inspect.iscoroutinefunction(app)
            or inspect.iscoroutinefunction(call_attr)
        )

    async def run_single(self, request: Any) -> Response:
        """Execute a single HTTP request against the RSGI application."""
        request_id = int(getattr(request, "request_id", 0))
        if isinstance(request, _NativeRequest):
            scope = request.rsgi_scope(
                scheme=self._scheme,
                server=self._server,
                client=self._client,
                root_path=self._mount_prefix,
            )
            protocol = request.rsgi_protocol()
        else:
            raw_path: str = getattr(request, "path", "/")
            if self._mount_prefix and raw_path.startswith(self._mount_prefix):
                path = raw_path[len(self._mount_prefix) :] or "/"
            else:
                path = raw_path

            method: str = getattr(request, "method", "GET")
            query: str = getattr(request, "query", None) or ""
            http_version: str = getattr(request, "http_version", "1.1")
            raw_body: bytes = getattr(request, "body", None) or b""

            hdr_pairs: list[tuple[str, str]] = []
            req_headers = getattr(request, "headers", None)
            if req_headers is not None:
                items = req_headers.items() if hasattr(req_headers, "items") else req_headers
                for k, v in items:
                    ks = k.decode("latin-1") if isinstance(k, bytes) else str(k)
                    vs = v.decode("latin-1") if isinstance(v, bytes) else str(v)
                    hdr_pairs.append((ks, vs))

            authority = next((v for k, v in hdr_pairs if k.lower() == "host"), None)
            scope = RsgiScope(
                proto="http",
                http_version=http_version,
                server=self._server,
                client=self._client,
                scheme=self._scheme,
                method=method,
                path=path,
                query_string=query,
                headers=hdr_pairs,
                authority=authority,
            )
            protocol = RsgiHttpProtocol(body=raw_body, request_id=request_id)

        try:
            res = self._target(scope, protocol)
            if inspect.isawaitable(res):
                await res
            return protocol.take_response()
        except Exception:
            if Response is not None:
                return Response(
                    500,
                    b"Internal Server Error",
                    [("content-type", "text/plain")],
                    request_id,
                )
            raise

    def run_batch(self, requests: list[Any], loop: asyncio.AbstractEventLoop | None = None) -> list[Response]:
        """Execute a batch of requests against the RSGI application."""

        async def _gather() -> list[Response]:
            return list(await asyncio.gather(*(self.run_single(r) for r in requests)))

        if loop is not None and loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(_gather(), loop)
            return fut.result()
        return asyncio.run(_gather())

    def spawn_batch(
        self,
        requests: RequestBatch | Iterable[Any],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> int:
        """Drive a batch of RSGI HTTP requests via Rust drive_rsgi_batch or fallback tasks."""
        if isinstance(requests, RequestBatch):
            if len(requests) == 0:
                return 0
            return drive_rsgi_batch(
                self._target,
                requests,
                loop_obj=loop,
                root_path=self._mount_prefix,
                scheme=self._scheme,
                server=self._server,
                client=self._client,
            )

        req_list = requests if isinstance(requests, list | tuple) else list(requests)
        if not req_list:
            return 0
        if isinstance(req_list[0], _NativeRequest):
            return drive_rsgi_batch(
                self._target,
                req_list,
                loop_obj=loop,
                root_path=self._mount_prefix,
                scheme=self._scheme,
                server=self._server,
                client=self._client,
            )

        if loop is not None:
            for req in req_list:
                asyncio.run_coroutine_threadsafe(self._execute_and_submit(req), loop)
        return 0

    async def handle_http(
        self,
        *,
        method: str = "GET",
        path: str = "/",
        query_string: str = "",
        headers: list[tuple[str, str]] | None = None,
        body: bytes = b"",
        request_id: int = 0,
    ) -> Response:
        """Convenience helper to invoke the RSGI app directly with explicit HTTP fields."""
        full_path = f"{path}?{query_string}" if query_string else path
        raw_headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in (headers or ())]
        req = _NativeRequest.from_parts(
            method,
            full_path,
            body=body,
            request_id=request_id,
            headers=raw_headers or None,
        )
        return await self.run_single(req)

    async def _execute_and_submit(self, request: Any) -> None:
        resp = await self.run_single(request)
        submit_response(resp)
