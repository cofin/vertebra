"""Backwards-compatible re-export from vertebra.common.exceptions.

This module is deprecated. Import from vertebra.common.exceptions instead.
"""

from vertebra.common.exceptions import (
    BrokerError,
    ChannelError,
    ConfigurationError,
    IPCError,
    MissingDependencyError,
    PoolError,
    ResourceError,
    SerializationError,
    StorageError,
    TypeGenerationError,
    VertebraError,
)

__all__ = (
    "BrokerError",
    "ChannelError",
    "ConfigurationError",
    "IPCError",
    "MissingDependencyError",
    "PoolError",
    "ResourceError",
    "SerializationError",
    "StorageError",
    "TypeGenerationError",
    "VertebraError",
)
