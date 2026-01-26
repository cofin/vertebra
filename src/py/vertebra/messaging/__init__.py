"""Messaging module for Vertebra.

Provides:
- transport: IPC transport layer (channels, async transport)
- protocol: Message protocol definitions (headers, state)
- broker: Broker client for shared-memory IPC
- pool: Broker pool abstractions
- provider: Shared-memory provider host
- status_client: Lightweight status client for CLI
"""

from vertebra.messaging import broker, pool, protocol, provider, status_client, transport

__all__ = ["broker", "pool", "protocol", "provider", "status_client", "transport"]
