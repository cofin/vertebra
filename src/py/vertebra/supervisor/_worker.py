"""Vertebra worker process entrypoint.

This module is the entry point for each Python worker process. It is
invoked by the Rust supervisor with environment variables that describe
the transport channel to connect to.

Transport modes:
    inprocess (default): Uses BatchBridge (crossbeam channels) for in-process workers.
    shm: Uses SPSC ring pairs via inherited FDs for forked workers.

Environment variables (set by supervisor):
    VERTEBRA_TRANSPORT_MODE: "inprocess" or "shm" (default: "inprocess")
    VERTEBRA_APP:            App import path (e.g. "myapp:app")
    VERTEBRA_WORKER_ID:      Integer worker index (for logging)
    VERTEBRA_NATIVE:         "1" (default) for native dispatch or "0" for ASGI/gateway mount
    VERTEBRA_INTERFACE:      "auto" (default), "asgi", "rsgi", or "wsgi"
"""

import inspect
import logging
import os
from collections.abc import Callable
from typing import Any

from vertebra._vertebra import Response
from vertebra.framework._app import App
from vertebra.server._dispatch import _coerce_response
from vertebra.server._gil_audit import GilAuditReport
from vertebra.server._protocol import InterfaceMode, detect_interface
from vertebra.server.worker import run_worker
from vertebra.utils.module_loader import import_app

__all__ = ("main", "resolve_worker_targets")


def resolve_worker_targets(
    app: Any,
    *,
    native: bool = True,
    interface: str = "auto",
) -> tuple[dict[int, Callable[..., Any]], list[tuple[str, Any, str]]]:
    """Resolve native handler mapping and sub-application mounts without double-mounting App instances."""
    normalized_interface: InterfaceMode
    if interface == "asgi":
        normalized_interface = "asgi"
    elif interface == "rsgi":
        normalized_interface = "rsgi"
    elif interface == "wsgi":
        normalized_interface = "wsgi"
    else:
        normalized_interface = "auto"
    if isinstance(app, App):
        if native:
            return dict(app._handlers), list(app.mounts)
        if not app._handlers and app.mounts and not callable(app):
            return {}, list(app.mounts)
        asgi_target = app if callable(app) else _build_app_asgi_adapter(app)
        protocol = "asgi" if normalized_interface == "auto" else normalized_interface
        wrapped = App()
        wrapped.mount("/", asgi_target, protocol=protocol)
        return {}, list(wrapped.mounts)

    resolved_protocol = (
        "asgi"
        if not native and normalized_interface == "auto"
        else detect_interface(app, configured=normalized_interface)
    )
    wrapped = App()
    wrapped.mount("/", app, protocol=resolved_protocol)
    return {}, list(wrapped.mounts)


def main() -> None:
    """Initialize a worker process and start the batch runner loop."""
    transport_mode = os.environ.get("VERTEBRA_TRANSPORT_MODE", "inprocess")
    app_path = os.environ["VERTEBRA_APP"]
    worker_id = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))

    logger = logging.getLogger(f"vertebra.worker.{worker_id}")
    logger.info("starting, app=%r, mode=%s", app_path, transport_mode)

    before_extensions = GilAuditReport.snapshot_loaded_extensions()
    app = import_app(app_path)

    raw_native = os.environ.get("VERTEBRA_NATIVE", "1").strip().lower()
    native = raw_native not in {"0", "false", "no", "off"}
    interface = os.environ.get("VERTEBRA_INTERFACE", "auto").strip().lower()
    handlers, mounts = resolve_worker_targets(app, native=native, interface=interface)

    run_worker(
        handlers,
        mounts=mounts,
        batch_timeout_ms=100,
        before_extensions=before_extensions,
    )


def _build_app_asgi_adapter(app: App) -> Callable[..., Any]:
    """Wrap a native App container in an ASGI 3.0 callable when VERTEBRA_NATIVE=0."""
    route_map: dict[tuple[str, str], Callable[..., Any]] = {
        (route.method.upper(), route.path): route.handler for route in app.routes
    }

    async def _asgi_adapter(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            return
        method = str(scope.get("method", "GET")).upper()
        path = str(scope.get("path", "/"))
        handler = route_map.get((method, path))
        if handler is None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [(b"content-type", b"text/plain")],
                }
            )
            await send({"type": "http.response.body", "body": b"not found", "more_body": False})
            return

        chunks: list[bytes] = []
        while True:
            message = await receive()
            chunk = message.get("body", b"")
            if chunk:
                chunks.append(bytes(chunk))
            if not message.get("more_body", False):
                break
        body_bytes = b"".join(chunks)

        sig = inspect.signature(handler)
        result = handler(body_bytes) if "body" in sig.parameters else handler()
        if inspect.isawaitable(result):
            result = await result
        coerced = _coerce_response(result, 0, Response)
        raw_headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in coerced.headers]
        await send(
            {
                "type": "http.response.start",
                "status": coerced.status,
                "headers": raw_headers,
            }
        )
        await send({"type": "http.response.body", "body": coerced.body, "more_body": False})

    return _asgi_adapter


if __name__ == "__main__":
    main()
