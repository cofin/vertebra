"""Internal helpers for Vertebra runtime."""

from vertebra._internal.errors import MissingDependencyError
from vertebra._internal.serialization import MessagePackCodec

__all__ = (
    "MessagePackCodec",
    "MissingDependencyError",
)
