"""SQLite Litestar store module."""

from vertebra.utils.module_loader import ensure_dependency

# sqlspec is implicitly required via parent imports
# Explicitly check for litestar which is a compound dependency
ensure_dependency("litestar")

from vertebra.extensions.sqlspec.sqlite.litestar.store import SqliteLitestarStore  # noqa: E402

__all__ = ("SqliteLitestarStore",)
