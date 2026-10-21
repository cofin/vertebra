"""Re-export shim for vertebra.build.packaging.onefile."""

from vertebra.build.packaging.onefile import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    ONEFILE_TRAILER_STRUCT,
    OnefileBuilder,
)

__all__ = (
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "ONEFILE_TRAILER_STRUCT",
    "OnefileBuilder",
)
