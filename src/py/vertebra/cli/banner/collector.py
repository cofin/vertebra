"""Status collection from Bun server.

The StatusCollector fetches component status from the Bun server's
/internal/status endpoint. This provides real-time health information
for the banner display.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal, Self

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
    """Collects status from the Bun server.

    Makes HTTP requests to the /internal/status endpoint to gather
    component health information for display in the banner.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8000, timeout: float = 1.0) -> None:
        """Initialize the status collector.

        Args:
            host: Server host address
            port: Server port number
            timeout: Request timeout in seconds
        """
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout
        self._client: Any = None

    def _get_client(self) -> Any:
        """Lazily import and create httpx client."""
        if self._client is None:
            try:
                import httpx

                self._client = httpx.Client(timeout=self.timeout)
            except ImportError:
                logger.debug("httpx not available for status collection")
                return None
        return self._client

    def fetch(self) -> ServerStatus | None:
        """Fetch server status synchronously.

        Returns:
            ServerStatus if successful, None on failure
        """
        client = self._get_client()
        if client is None:
            return None

        try:
            response = client.get(f"{self.base_url}/internal/status")
            response.raise_for_status()
            data = response.json()
            return self._parse_status(data)
        except (OSError, ValueError, TypeError, KeyError):
            # OSError: connection failures, ValueError/TypeError/KeyError: JSON parsing issues
            logger.debug("Failed to fetch server status", exc_info=True)
            return None

    async def fetch_async(self) -> ServerStatus | None:
        """Fetch server status asynchronously.

        Returns:
            ServerStatus if successful, None on failure
        """
        try:
            import httpx

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.base_url}/internal/status")
                response.raise_for_status()
                data = response.json()
                return self._parse_status(data)
        except ImportError:
            logger.debug("httpx not available for async status collection")
            return None
        except (OSError, ValueError, TypeError, KeyError):
            # OSError: connection failures, ValueError/TypeError/KeyError: JSON parsing issues
            logger.debug("Failed to fetch server status async", exc_info=True)
            return None

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
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
