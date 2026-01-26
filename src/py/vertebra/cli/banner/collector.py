"""Status collection from Bun server.

The StatusCollector fetches component status from the broker via IPC.
This provides real-time health information for the banner display
without requiring httpx or any network dependencies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal, Self

from vertebra.messaging.status_client import StatusClient

logger = logging.getLogger(__name__)

# Status level type
StatusLevel = Literal["ready", "starting", "degraded", "error"]


@dataclass(slots=True)
class ComponentStatus:
    """Status of a single server component."""

    status: StatusLevel
    version: str | None = None
    uptime_ms: int | None = None
    details: dict[str, Any] | None = None

    # Component-specific fields
    clients: int | None = None  # Broker
    pools: dict[str, int] | None = None  # Broker
    channels: int | None = None  # IPC
    latency_p99_us: int | None = None  # IPC


@dataclass(slots=True)
class ServerStatus:
    """Aggregated status from all server components."""

    bun: ComponentStatus
    broker: ComponentStatus
    ipc: ComponentStatus


class StatusCollector:
    """Collects status from the Bun server via IPC.

    Uses shared-memory IPC to query the broker for component health
    information to display in the banner. No network dependencies required.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8000, timeout: float = 1.0) -> None:
        """Initialize the status collector.

        Args:
            host: Server host address (unused, kept for API compatibility)
            port: Server port number (unused, kept for API compatibility)
            timeout: Request timeout in seconds
        """
        # host/port kept for backwards compatibility but not used with IPC
        self._host = host
        self._port = port
        self.timeout = timeout
        self._status_client: StatusClient | None = None

    def _get_status_client(self) -> StatusClient:
        """Get or create the IPC status client."""
        if self._status_client is None:
            self._status_client = StatusClient()
        return self._status_client

    def fetch(self) -> ServerStatus | None:
        """Fetch server status via IPC.

        Returns:
            ServerStatus if successful, None on failure
        """
        try:
            client = self._get_status_client()
            timeout_ms = int(self.timeout * 1000)
            data = client.query_status(timeout_ms=timeout_ms)
            if data is None:
                return None
            return self._parse_status(data)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError):
            logger.debug("Failed to fetch server status via IPC", exc_info=True)
            return None

    async def fetch_async(self) -> ServerStatus | None:
        """Fetch server status asynchronously.

        Note: Uses synchronous IPC under the hood since the status query
        is very fast (< 1ms). The async interface is kept for API compatibility.

        Returns:
            ServerStatus if successful, None on failure
        """
        # Status queries are fast enough that we can use sync IPC
        # wrapped in run_in_executor would add more overhead than benefit
        return self.fetch()

    def _parse_status(self, data: dict[str, Any]) -> ServerStatus:
        """Parse raw status response into ServerStatus object."""
        return ServerStatus(
            bun=self._parse_component(data.get("bun", {})),
            broker=self._parse_component(data.get("broker", {})),
            ipc=self._parse_component(data.get("ipc", {})),
        )

    def _parse_component(self, data: dict[str, Any]) -> ComponentStatus:
        """Parse component status from raw data."""
        return ComponentStatus(
            status=data.get("status", "unknown"),
            version=data.get("version"),
            uptime_ms=data.get("uptime_ms"),
            details=data.get("details"),
            clients=data.get("clients"),
            pools=data.get("pools"),
            channels=data.get("channels"),
            latency_p99_us=data.get("latency_p99_us"),
        )

    def close(self) -> None:
        """Clean up resources (no-op for IPC client)."""
        # StatusClient is stateless, no cleanup needed
        self._status_client = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
