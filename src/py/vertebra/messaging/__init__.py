"""Messaging module for Vertebra.

Provides:
- transport: IPC transport layer (channels, async transport)
- protocol: Message protocol definitions (headers, state)
- broker: Broker client for shared-memory IPC
"""

from vertebra.messaging import broker, protocol, transport

__all__ = ["broker", "protocol", "transport"]
