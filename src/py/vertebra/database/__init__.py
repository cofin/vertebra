"""Database module for Vertebra.

Provides:
- backends: Low-level database driver abstractions
- interfaces: SQLSpec integration (coming from extensions/sqlspec)
"""

from vertebra.database import backends

__all__ = ["backends"]
