"""Internal resource pool clients for broker IPC.

The resources package exposes the internal `AsyncBrokerClient` and pool
abstractions used by the Python runtime to talk to the Bun broker over
shared-memory IPC. These classes are internal implementation details
and are separate from the public SQLSpec adapters.
"""

from __future__ import annotations

from vertebra.resources.broker import AsyncBrokerClient, BrokerClientConfig, BrokerClientError, SyncBrokerClient
from vertebra.resources.pool import (
    AsyncBrokerClientPool,
    AsyncBrokerLease,
    AsyncBrokerPool,
    BrokerPoolError,
    SyncBrokerClientPool,
    SyncBrokerLease,
    SyncBrokerPool,
)
from vertebra.resources.provider import ProviderConfig, ProviderHandler, ProviderHost

__all__ = (
    "AsyncBrokerClient",
    "AsyncBrokerClientPool",
    "AsyncBrokerLease",
    "AsyncBrokerPool",
    "BrokerClientConfig",
    "BrokerClientError",
    "BrokerPoolError",
    "ProviderConfig",
    "ProviderHandler",
    "ProviderHost",
    "SyncBrokerClient",
    "SyncBrokerClientPool",
    "SyncBrokerLease",
    "SyncBrokerPool",
)
