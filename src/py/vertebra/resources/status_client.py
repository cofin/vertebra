"""Lightweight status client for CLI banner display.

This module provides a minimal IPC client for querying server status
without requiring httpx or any network dependencies.
"""

from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING, Any

from vertebra._vertebra import SpscRing
from vertebra.serializers import MessagePackCodec

if TYPE_CHECKING:
    from vertebra.resources.broker import BrokerClientConfig


class StatusClient:
    """Lightweight client for querying server status via IPC.

    Unlike the full BrokerClient, this client performs a single status query
    and cleans up immediately. It's designed for CLI usage where we need
    a quick status check without long-lived connections.
    """

    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        """Initialize status client.

        Args:
            config: Broker configuration. Uses defaults if not provided.
        """
        if config is None:
            from vertebra.resources.broker import BrokerClientConfig

            config = BrokerClientConfig()
        self._config = config
        self._codec = MessagePackCodec()

    def query_status(self, timeout_ms: int | None = None) -> dict[str, Any] | None:
        """Query server status via IPC.

        Returns:
            Status dict with bun/broker/ipc sections, or None on failure.
        """
        timeout = timeout_ms or self._config.timeout_ms
        client_id = uuid.uuid4().hex

        control: SpscRing | None = None
        request: SpscRing | None = None
        response: SpscRing | None = None

        try:
            # Open control ring (must already exist from broker)
            control_name = self._config.control_name or f"/{self._config.name}.control"
            control = SpscRing.open_named(control_name, self._config.slot_size, self._config.slots)

            # Create client rings for this query
            req_name = f"/{self._config.name}.client.{client_id}.req"
            resp_name = f"/{self._config.name}.client.{client_id}.resp"
            request = SpscRing.create_named(req_name, self._config.slot_size, self._config.slots)
            response = SpscRing.create_named(resp_name, self._config.slot_size, self._config.slots)

            # Register with broker
            register_payload = {
                "id": 1,
                "kind": "register_client",
                "client_id": client_id,
                "req_ring": req_name,
                "resp_ring": resp_name,
                "weight": 1,
            }
            control.send_timeout(self._codec.encode(register_payload), 0, timeout)

            # Wait for registration ack
            ack_data = response.recv_timeout(self._config.slot_size, timeout)
            if not ack_data:
                return None
            ack = self._codec.decode(ack_data)
            if not isinstance(ack, dict) or not ack.get("ok"):
                return None

            # Send status request
            status_payload = {"id": 2, "kind": "status"}
            request.send_timeout(self._codec.encode(status_payload), 0, timeout)

            # Receive status response
            status_data = response.recv_timeout(self._config.slot_size, timeout)
            if not status_data:
                return None
            status = self._codec.decode(status_data)
            if not isinstance(status, dict) or not status.get("ok"):
                return None

            result = status.get("result")
            return result if isinstance(result, dict) else None

        except (OSError, RuntimeError, ValueError):
            # IPC failures - broker may not be running
            return None
        finally:
            # Clean up rings
            with contextlib.suppress(Exception):
                if request is not None:
                    request.close()
            with contextlib.suppress(Exception):
                if response is not None:
                    response.close()
            # Note: don't close control - it's the broker's ring


def query_status(timeout_ms: int = 2000) -> dict[str, Any] | None:
    """Query server status via IPC (convenience function).

    Args:
        timeout_ms: Timeout in milliseconds

    Returns:
        Status dict with bun/broker/ipc sections, or None on failure.
    """
    client = StatusClient()
    return client.query_status(timeout_ms=timeout_ms)


__all__ = ("StatusClient", "query_status")
