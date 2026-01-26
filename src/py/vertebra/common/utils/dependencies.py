"""Optional dependency availability flags for framework/extension dependencies."""

from vertebra.common.utils.module_loader import dependency_flag

# Framework/Extension Dependencies
ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")
FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
SQLALCHEMY_INSTALLED = dependency_flag("sqlalchemy")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
WATCHDOG_INSTALLED = dependency_flag("watchdog")

__all__ = (
    "ADVANCED_ALCHEMY_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLALCHEMY_INSTALLED",
    "SQLSPEC_INSTALLED",
    "WATCHDOG_INSTALLED",
)
