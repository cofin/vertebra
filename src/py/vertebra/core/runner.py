"""Synchronous batch runner for Vertebra workers.

This module implements the core execution loop that polls the native FFI layer
for request batches and dispatches them to Python handlers.

Status: STUB — ffi.get_batch() always returns None until Ch6 wires the real FFI.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger("vertebra.runner")


class _StubFFI:
    """Stand-in for the native FFI layer until Ch6."""

    def get_batch(self) -> Any:
        """Return None until native bindings are wired."""
        return None


ffi = _StubFFI()


def run_loop(
    stop_event: threading.Event,
    poll_interval: float = 0.001,
) -> None:
    """Run the synchronous batch dispatch loop.

    Polls ``ffi.get_batch()`` in a tight loop. When a batch is returned,
    dispatches it to ``_dispatch()``. Sleeps ``poll_interval`` seconds between
    empty polls to avoid busy-waiting.
    """
    logger.info("Runner loop starting (stub mode)")

    while not stop_event.is_set():
        batch = ffi.get_batch()

        if batch is None:
            time.sleep(poll_interval)
            continue

        try:
            _dispatch(batch)
        except Exception:
            logger.exception("Unhandled error in batch dispatch")

    logger.info("Runner loop exiting cleanly")


def _dispatch(batch: Any) -> None:
    """Dispatch a request batch to Python handlers.

    Stub: no-op. Ch6 replaces this with the full FFI dispatch pipeline.
    """
    logger.debug("_dispatch called with batch: %r (stub — no-op)", batch)


__all__ = ("_StubFFI", "_dispatch", "ffi", "run_loop")
