"""SQLSpec integrations for Vertebra."""

from vertebra.exceptions import MissingDependencyError
from vertebra.utils.dependencies import SQLSPEC_INSTALLED

if not SQLSPEC_INSTALLED:
    raise MissingDependencyError(package="sqlspec", install_package="vertebra[sqlspec]")

__all__ = ()
