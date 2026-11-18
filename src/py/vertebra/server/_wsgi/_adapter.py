"""WsgiAdapter: executes PEP 3333 WSGI callables on the Rust-backed environment."""

from collections.abc import Callable, Iterable
from typing import Any, cast

from vertebra._vertebra import (
    Request,
    RequestBatch,
    Response,
    WsgiEnviron,
    WsgiFileWrapper,
    drive_wsgi_batch,
    run_wsgi_callable,
    submit_response,
)
from vertebra.server._fastpath import attach_framework_fastpath

__all__ = (
    "WsgiAdapter",
    "WsgiError",
)


class WsgiError(RuntimeError):
    """Raised when a WSGI application violates PEP 3333."""


class WsgiAdapter:
    """Adapts a PEP 3333 WSGI callable to Vertebra's request/response pipeline."""

    __slots__ = ("_app", "_mount_prefix")

    def __init__(
        self,
        app: Callable[..., Iterable[bytes]] | Any,
        *,
        mount_prefix: str = "",
    ) -> None:
        self._mount_prefix = mount_prefix.rstrip("/")
        wrapped = attach_framework_fastpath(app, mount_prefix=self._mount_prefix, protocol="wsgi")
        wsgi_app = getattr(wrapped, "wsgi_app", None)
        if callable(wsgi_app) and (not callable(wrapped) or getattr(wsgi_app, "__vertebra_fastpath_attached__", False)):
            wrapped = wsgi_app
        self._app: Callable[..., Iterable[bytes]] = cast("Callable[..., Iterable[bytes]]", wrapped)

    @property
    def __vertebra_fastpath_attached__(self) -> bool:
        """Return True when a zero-copy FastPath wrapper is attached to this adapter."""
        return bool(getattr(self._app, "__vertebra_fastpath_attached__", False))

    @property
    def app(self) -> Callable[..., Iterable[bytes]]:
        """Return the underlying PEP 3333 WSGI callable."""
        return self._app

    @property
    def mount_prefix(self) -> str:
        """Return the normalized mount prefix for ``SCRIPT_NAME``."""
        return self._mount_prefix

    def __call__(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> Response:
        """Invoke ``handle_request`` when ``WsgiAdapter`` is called directly."""
        return self.handle_request(
            request,
            server_name=server_name,
            server_port=server_port,
            url_scheme=url_scheme,
        )

    def spawn_batch(
        self,
        requests: RequestBatch | Iterable[Any],
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> int:
        """Execute a batch of WSGI requests in Rust and submit all responses in one batch call."""
        if isinstance(requests, RequestBatch):
            if len(requests) == 0:
                return 0
            return drive_wsgi_batch(
                self._app,
                requests,
                root_path=self._mount_prefix,
                server_name=server_name,
                server_port=server_port,
                url_scheme=url_scheme,
            )

        req_list = requests if isinstance(requests, list | tuple) else list(requests)
        if not req_list:
            return 0
        if isinstance(req_list[0], Request):
            return drive_wsgi_batch(
                self._app,
                req_list,
                root_path=self._mount_prefix,
                server_name=server_name,
                server_port=server_port,
                url_scheme=url_scheme,
            )

        count = 0
        for req in req_list:
            req_id = int(getattr(req, "request_id", 0))
            try:
                resp = self.handle_request(
                    req,
                    server_name=server_name,
                    server_port=server_port,
                    url_scheme=url_scheme,
                )
                submit_response(resp)
            except Exception:
                submit_response(Response.error(500, req_id))
            count += 1
        return count

    def handle_request(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> Response:
        """Execute the WSGI application for ``request`` and return a ``Response``."""
        request_id: int = getattr(request, "request_id", 0)
        if isinstance(request, Request):
            environ = request.wsgi_environ(
                server_name=server_name,
                server_port=server_port,
                url_scheme=url_scheme,
            )
            if self._mount_prefix:
                self._apply_mount_prefix(environ)
            try:
                return run_wsgi_callable(self._app, environ, request_id)
            except (RuntimeError, ValueError) as exc:
                raise WsgiError(str(exc)) from exc
        environ = self._build_environ(
            request,
            server_name=server_name,
            server_port=server_port,
            url_scheme=url_scheme,
        )
        environ["wsgi.file_wrapper"] = WsgiFileWrapper
        return self._run_callable(environ, request_id)

    def run_environ(
        self,
        environ: WsgiEnviron,
        request_id: int = 0,
    ) -> Response:
        """Execute the WSGI application against an existing ``WsgiEnviron``."""
        if self._mount_prefix:
            self._apply_mount_prefix(environ)
        return self._run_callable(environ, request_id)

    def _apply_mount_prefix(self, environ: WsgiEnviron) -> None:
        """Adjust SCRIPT_NAME and PATH_INFO on a WsgiEnviron for mounted apps."""
        path_info = str(environ.get("PATH_INFO", "/"))
        if path_info.startswith(self._mount_prefix):
            stripped = path_info[len(self._mount_prefix) :] or "/"
            environ["SCRIPT_NAME"] = self._mount_prefix
            environ["PATH_INFO"] = stripped

    def _build_environ(
        self,
        request: Any,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
    ) -> WsgiEnviron:
        """Construct a Rust ``WsgiEnviron`` from a Python request double."""
        method: str = getattr(request, "method", "GET")
        path: str = getattr(request, "path", "/")
        query: str = getattr(request, "query", None) or ""
        body_raw: bytes | None = getattr(request, "body", None)
        raw_headers = getattr(request, "headers", None)
        header_pairs: list[tuple[bytes, bytes]] = []
        if raw_headers is not None:
            items = raw_headers.items() if hasattr(raw_headers, "items") else raw_headers
            for k, v in items:
                kb = k.encode("latin-1") if isinstance(k, str) else bytes(k)
                vb = v.encode("latin-1") if isinstance(v, str) else bytes(v)
                header_pairs.append((kb, vb))
        environ = WsgiEnviron.from_parts(
            method=method,
            path=path,
            query_string=query,
            headers=header_pairs,
            body=body_raw,
            server_name=server_name,
            server_port=int(server_port),
            url_scheme=url_scheme,
        )
        if self._mount_prefix:
            self._apply_mount_prefix(environ)
        return environ

    def _run_callable(
        self,
        environ: WsgiEnviron,
        request_id: int,
    ) -> Response:
        """Invoke the WSGI callable and collect status, headers, and body."""
        status_code: int | None = None
        response_headers: list[tuple[str, str]] = []
        headers_sent = False
        write_chunks: list[bytes] = []

        def start_response(
            status: str,
            headers: list[tuple[str, str]],
            exc_info: tuple[type[BaseException] | None, BaseException | None, Any] | None = None,
        ) -> Callable[[bytes], None]:
            nonlocal status_code, response_headers, headers_sent

            if exc_info is not None:
                try:
                    if headers_sent and exc_info[1] is not None:
                        raise exc_info[1].with_traceback(exc_info[2])
                finally:
                    exc_info = None
            elif status_code is not None:
                msg = "start_response called a second time without exc_info"
                raise WsgiError(msg)

            parts = status.split(" ", 1)
            try:
                code = int(parts[0])
            except (ValueError, IndexError) as exc:
                msg = f"Invalid WSGI status string: {status!r}"
                raise WsgiError(msg) from exc

            if code < 100 or code > 999:
                msg = f"WSGI status code out of range: {code}"
                raise WsgiError(msg)

            status_code = code
            response_headers = [(str(k).lower(), str(v)) for k, v in headers]

            def write(data: bytes) -> None:
                nonlocal headers_sent
                headers_sent = True
                if data:
                    write_chunks.append(bytes(data))

            return write

        result_iter = self._app(environ, start_response)

        if isinstance(result_iter, WsgiFileWrapper):
            file_path = result_iter.path
            if file_path is not None and status_code is not None:
                result_iter.close()
                return Response.from_file(
                    file_path,
                    status=status_code,
                    headers=response_headers,
                    request_id=request_id,
                )
            if status_code is not None:
                raw_bytes = bytes(result_iter.read_all_bytes() or b"")
                result_iter.close()
                return Response(
                    status=status_code,
                    body=raw_bytes,
                    headers=response_headers,
                    request_id=request_id,
                )

        body_parts: list[bytes] = []
        try:
            for chunk in result_iter:
                if status_code is None:
                    msg = "WSGI application yielded body bytes before calling start_response"
                    raise WsgiError(msg)
                headers_sent = True
                if chunk:
                    body_parts.append(bytes(chunk))
        finally:
            close = getattr(result_iter, "close", None)
            if callable(close):
                close()

        if status_code is None:
            msg = "WSGI application returned without calling start_response"
            raise WsgiError(msg)

        all_chunks = write_chunks + body_parts
        body = b"".join(all_chunks)
        return Response(
            status=status_code,
            body=body,
            headers=response_headers,
            request_id=request_id,
        )
