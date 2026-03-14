"""Worker process utilities for Vertebra."""

from __future__ import annotations

import logging
import signal as _signal
import threading

logger = logging.getLogger("vertebra.worker")


def init_worker_signals(stop_event: threading.Event) -> None:
    """Register SIGTERM and SIGINT handlers that set ``stop_event``.

    Safe to call in the main thread of a worker process. On Windows,
    SIGTERM is not available; only SIGINT is registered there.
    """

    def _handler(signum: int, frame: object) -> None:
        logger.info("Signal %d received — requesting stop", signum)
        stop_event.set()

    if hasattr(_signal, "SIGTERM"):
        _signal.signal(_signal.SIGTERM, _handler)
    _signal.signal(_signal.SIGINT, _handler)


__all__ = ("init_worker_signals",)
