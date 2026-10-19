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
    VERTEBRA_RING_NAME:      Named MPMC ring to connect to
    VERTEBRA_SLOT_SIZE:      Ring slot size in bytes
    VERTEBRA_SLOTS:          Number of ring slots
    VERTEBRA_BATCH_SIZE:     Maximum batch size (default 64)
"""

import logging
import os

from vertebra.framework._app import App
from vertebra.server._native import run_worker
from vertebra.utils.module_loader import import_app

__all__ = ("import_app", "main")


def main() -> None:
    """Initialize a worker process and start the batch runner loop."""
    transport_mode = os.environ.get("VERTEBRA_TRANSPORT_MODE", "inprocess")
    app_path = os.environ["VERTEBRA_APP"]
    worker_id = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))

    logger = logging.getLogger(f"vertebra.worker.{worker_id}")
    logger.info("starting, app=%r, mode=%s", app_path, transport_mode)

    app = import_app(app_path)

    if os.environ.get("VERTEBRA_NATIVE") == "0":
        logger.info("wrapping raw app in ASGI adapter")
        wrapped = App()
        wrapped.mount("/", app, protocol="asgi")
        app = wrapped

    handlers = app._handlers
    mounts = list(app.mounts) if hasattr(app, "mounts") else []
    worker_count = int(os.environ.get("VERTEBRA_WORKERS", "1"))

    run_worker(handlers, mounts=mounts, batch_timeout_ms=100, worker_count=worker_count)


if __name__ == "__main__":
    main()
