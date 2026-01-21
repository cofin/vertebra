"""Optional dependency availability flags."""

from vertebra.utils.module_loader import dependency_flag

FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
SQLSPEC_INSTALLED = dependency_flag("sqlspec")
WATCHDOG_INSTALLED = dependency_flag("watchdog")

__all__ = ("FSSPEC_INSTALLED", "LITESTAR_INSTALLED", "SQLSPEC_INSTALLED", "WATCHDOG_INSTALLED")
