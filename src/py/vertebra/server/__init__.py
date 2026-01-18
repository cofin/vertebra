"""Server integration interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from vertebra.server.worker import AsgiWorker, WorkerConfig


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """Configuration for the Bun-backed server integration."""

    host: str = "127.0.0.1"
    port: int = 8000
    workers: int = 1


@runtime_checkable
class ServerBackend(Protocol):
    """Server backend contract (implemented in PRD-3)."""

    def serve(self, config: ServerConfig) -> None:
        """Start serving using the provided configuration."""

    def shutdown(self) -> None:
        """Stop the server and release resources."""


def get_server_backend() -> ServerBackend:
    """Return the configured server backend."""
    msg = "Server backend not configured yet"
    raise RuntimeError(msg)


__all__ = ("AsgiWorker", "ServerBackend", "ServerConfig", "WorkerConfig", "get_server_backend")
