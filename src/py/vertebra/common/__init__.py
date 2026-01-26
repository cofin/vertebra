"""Common foundational modules for Vertebra.

This package contains exceptions, protocols, and serializers that define
the core contracts shared across all Vertebra feature packages.

Dependency Flow:
    Feature packages -> core -> common

This package should have NO dependencies on other Vertebra packages
(except vertebra.utils for dependency detection).
"""

from vertebra.common import exceptions, protocols, serializers

__all__ = [
    "exceptions",
    "protocols",
    "serializers",
]
