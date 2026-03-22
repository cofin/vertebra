"""WsgiRunner: batch loop for a WSGI application."""

import logging
import os
import signal
import sys
from collections.abc import Callable
from typing import Any

from vertebra.server._wsgi._adapter import WsgiAdapter, WsgiError

__all__ = ["WsgiRunner"]

log = logging.getLogger(__name__)

_WsgiApp = Callable[[dict[str, Any], Callable[..., None]], Any]


class WsgiRunner:
    """Synchronous batch runner for a WSGI application."""

    __slots__ = ("_adapter", "_server_name", "_server_port", "_on_error", "_running")

    def __init__(
        self,
        app: _WsgiApp,
        *,
        server_name: str = "localhost",
        server_port: int = 8000,
        url_scheme: str = "http",
        on_error: Callable[..., Any] | None = None,
    ) -> None:
        self._adapter = WsgiAdapter(app, url_scheme=url_scheme)
        self._server_name = server_name
        self._server_port = server_port
        self._on_error = on_error or self._default_error
        self._running = False

    @staticmethod
    def _default_error(*_args: Any) -> Any:
        from vertebra._vertebra import VertebraResponse

        return VertebraResponse(status=500, body=b"Internal Server Error")

    def _handle_signals(self) -> None:
        def _stop(signum: int, frame: Any) -> None:  # noqa: ARG001
            log.info("WsgiRunner received signal %d, stopping.", signum)
            self.stop()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

    def stop(self) -> None:
        self._running = False

    def run(self) -> None:
        """Enter the main batch processing loop."""
        from vertebra._vertebra import get_batch, submit_results

        self._running = True
        self._handle_signals()

        log.info("WsgiRunner starting (pid=%d, server=%s:%d).", os.getpid(), self._server_name, self._server_port)

        while self._running:
            try:
                batch = get_batch()
            except Exception:
                log.exception("ffi.get_batch() failed; exiting runner.")
                break

            if not batch:
                continue

            responses = []
            for request in batch:
                try:
                    response = self._adapter(request, server_name=self._server_name, server_port=self._server_port)
                except (WsgiError, Exception):
                    log.exception("Error in WSGI app for %r", request.path)
                    response = self._on_error(request, sys.exc_info()[1])
                responses.append(response)

            try:
                submit_results(responses)
            except Exception:
                log.exception("ffi.submit_results() failed; exiting runner.")
                break

        log.info("WsgiRunner stopped.")
