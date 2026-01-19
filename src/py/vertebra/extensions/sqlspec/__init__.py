"""SQLSpec integrations for Vertebra."""

from vertebra._internal.dependencies import SQLSPEC_INSTALLED
from vertebra._internal.errors import MissingDependencyError

if not SQLSPEC_INSTALLED:
    raise MissingDependencyError(package="sqlspec", install_package="vertebra[sqlspec]")

__all__ = ()
