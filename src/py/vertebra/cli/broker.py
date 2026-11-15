"""Lazy CLI commands for shared-memory broker supervision and live telemetry."""

from __future__ import annotations

import os
import signal
import sys
import threading
from typing import Any

import click

from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json

__all__ = ("broker",)


@click.group("broker")
def broker() -> None:
    """Manage the shared-memory database connection broker host."""


@broker.command("serve")
@click.option("--shm-name", default=None, help="Named POSIX SHM broker segment.")
@click.option("--socket-path", default=None, help="Unix domain socket path for authenticated FD attach.")
@click.option(
    "--secret",
    default=None,
    envvar="VERTEBRA_BROKER_SECRET",
    help="Attach secret (reads VERTEBRA_BROKER_SECRET when omitted).",
)
@click.option("--pool-size", type=int, default=20, show_default=True, help="Primary pool slot capacity.")
@click.option("--max-overflow", type=int, default=10, show_default=True, help="Burst overflow slot capacity.")
@click.option("--stmt-capacity", type=int, default=512, show_default=True, help="Statement cache slot capacity.")
@click.option(
    "--shutdown-timeout",
    type=float,
    default=10.0,
    show_default=True,
    help="Bounded shutdown drain timeout in seconds.",
)
def serve(
    shm_name: str | None,
    socket_path: str | None,
    secret: str | None,
    pool_size: int,
    max_overflow: int,
    stmt_capacity: int,
    shutdown_timeout: float,
) -> None:
    """Run the shared-memory connection broker host until SIGINT or SIGTERM."""
    host_mod = ensure_module("vertebra.extensions.broker.host")
    transport_mod = ensure_module("vertebra.extensions.broker.transport")

    cfg = transport_mod.BrokerConfig(
        pool_size=pool_size,
        max_overflow=max_overflow,
        statement_cache_capacity=stmt_capacity,
        shm_name=shm_name,
        role="owner",
    )
    secret_bytes = secret.encode("utf-8") if secret is not None else None
    host = host_mod.BrokerHost(cfg, socket_path=socket_path, secret=secret_bytes)
    stop_event = threading.Event()

    def _on_signal(_signum: int, _frame: Any) -> None:
        stop_event.set()

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    host.start()
    try:
        sys.stdout.write(
            f"BrokerHost listening on pid={os.getpid()} shm={shm_name or 'anonymous'} "
            f"socket={socket_path or 'disabled'}\n"
        )
        sys.stdout.flush()
        while not stop_event.wait(timeout=0.5):
            pass
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        report = host.stop(timeout=shutdown_timeout)
        sys.stdout.write(f"BrokerHost stopped: completed={report.completed} closed_pools={report.closed_pools}\n")
        sys.stdout.flush()


@broker.command("status")
@click.option("--shm-name", required=True, help="Named POSIX SHM broker segment to inspect.")
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit broker stats as JSON.")
def status(shm_name: str, as_json: bool) -> None:
    """Inspect live shared-memory lease and quarantine statistics."""
    native_mod = ensure_module("vertebra._vertebra")
    native = native_mod.LeaseBroker.open_named(shm_name)
    stats = native.stats()
    payload = {
        "pool_size": stats.pool_size,
        "max_overflow": stats.max_overflow,
        "active_leases": stats.active_leases,
        "primary_in_use": stats.primary_in_use,
        "overflow_in_use": stats.overflow_in_use,
        "waiting_threads": stats.waiting_threads,
        "quarantined": stats.quarantined,
        "total_acquired": stats.total_acquired,
        "total_released": stats.total_released,
        "total_expired": stats.total_expired,
        "total_timeouts": stats.total_timeouts,
        "total_reclaimed": stats.total_reclaimed,
        "total_quarantined": stats.total_quarantined,
        "total_disposed": stats.total_disposed,
    }
    if as_json:
        sys.stdout.write(encode_json(payload) + "\n")
    else:
        sys.stdout.write(
            f"Broker {shm_name}: active={stats.active_leases} quarantined={stats.quarantined} "
            f"disposed={stats.total_disposed} waiting={stats.waiting_threads}\n"
        )
    sys.stdout.flush()
