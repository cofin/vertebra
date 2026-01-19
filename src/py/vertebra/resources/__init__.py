"""Internal resource pool clients for broker IPC.

The resources package exposes the internal `BrokerClient` and pool
abstractions used by the Python runtime to talk to the Bun broker over
shared-memory IPC. These classes are internal implementation details
and are separate from the public SQLSpec adapters.
"""

from __future__ import annotations

from vertebra.resources.broker import BrokerClient, BrokerClientConfig, BrokerClientError
from vertebra.resources.pool import BrokerClientPool, BrokerLease, BrokerPool, BrokerPoolError
from vertebra.resources.provider import ProviderConfig, ProviderHandler, ProviderHost

__all__ = (
    "BrokerClient",
    "BrokerClientConfig",
    "BrokerClientError",
    "BrokerClientPool",
    "BrokerLease",
    "BrokerPool",
    "BrokerPoolError",
    "ProviderConfig",
    "ProviderHandler",
    "ProviderHost",
)
