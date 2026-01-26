"""Common foundational modules for Vertebra.

This package contains base types, utilities, serializers, exceptions, and protocols
that are shared across all Vertebra feature packages.

Dependency Flow:
    Feature packages -> core -> common

This package should have NO dependencies on other Vertebra packages.
"""

from vertebra.common import exceptions, protocols, serializers, utils

__all__ = [
    "exceptions",
    "protocols",
    "serializers",
    "utils",
]
