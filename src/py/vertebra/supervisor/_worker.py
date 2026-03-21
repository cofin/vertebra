"""Vertebra worker process entrypoint.

This module is the entry point for each Python worker process. It is
invoked by the Rust supervisor with environment variables that describe
the shared-memory channel to connect to.

Environment variables (set by supervisor):
    VERTEBRA_RING_NAME:   Named MPMC ring to connect to
    VERTEBRA_SLOT_SIZE:   Ring slot size in bytes
    VERTEBRA_SLOTS:       Number of ring slots
    VERTEBRA_BATCH_SIZE:  Maximum batch size (default 64)
    VERTEBRA_APP:         App import path (e.g. "myapp:app")
    VERTEBRA_WORKER_ID:   Integer worker index (for logging)
"""

import importlib
import os
import sys
from typing import Any


def _import_app(app_path: str) -> Any:
    """Import an application object from a dotted path.

    Accepts "module:attr" format (e.g. "myapp.server:app").
    """
    if ":" not in app_path:
        msg = f"VERTEBRA_APP must be in 'module:attr' format, got: {app_path!r}"
        raise ValueError(msg)
    module_path, attr = app_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr)


def main() -> None:
    """Initialize a worker process and start the batch runner loop."""
    ring_name = os.environ["VERTEBRA_RING_NAME"]
    slot_size = int(os.environ["VERTEBRA_SLOT_SIZE"])
    slots = int(os.environ["VERTEBRA_SLOTS"])
    batch_size = int(os.environ.get("VERTEBRA_BATCH_SIZE", "64"))
    app_path = os.environ["VERTEBRA_APP"]
    worker_id = int(os.environ.get("VERTEBRA_WORKER_ID", "0"))

    print(  # noqa: T201
        f"[vertebra worker {worker_id}] starting, app={app_path!r}, ring={ring_name!r}",
        file=sys.stderr,
    )

    app = _import_app(app_path)

    from vertebra._vertebra import init_transport  # type: ignore[import-untyped]

    init_transport(
        ring_name=ring_name,
        slot_size=slot_size,
        slots=slots,
        batch_size=batch_size,
    )

    print(  # noqa: T201
        f"[vertebra worker {worker_id}] connected to ring {ring_name!r}",
        file=sys.stderr,
    )

    handlers = app._handlers  # noqa: SLF001
    mounts = list(app.mounts) if hasattr(app, "mounts") else []

    worker_count = int(os.environ.get("VERTEBRA_WORKERS", "1"))

    from vertebra.server._native import run_worker

    run_worker(handlers, mounts=mounts, batch_timeout_ms=100, worker_count=worker_count)


__all__ = ("_import_app", "main")
