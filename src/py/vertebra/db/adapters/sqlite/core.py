"""SQLite WAL pragma builder and core helpers."""

from __future__ import annotations

import contextlib

from vertebra.utils.module_loader import import_optional

__all__ = (
    "VALID_SQLITE_SYNCHRONOUS",
    "SQLiteCore",
)

VALID_SQLITE_SYNCHRONOUS = frozenset({"OFF", "NORMAL", "FULL", "EXTRA"})


class SQLiteCore:
    """PRAGMA builders and helpers for SQLite 3.45+ WAL connections."""

    @staticmethod
    def initialization_pragmas(
        *,
        wal_mode: bool = True,
        synchronous: str = "NORMAL",
        mmap_size: int = 268_435_456,
        busy_timeout_ms: int = 5000,
        foreign_keys: bool = True,
    ) -> list[str]:
        """Build the startup PRAGMA statements for a SQLite connection."""
        native_mod = import_optional("vertebra._vertebra")
        if native_mod is not None:
            native_fn = getattr(native_mod, "sqlite_initialization_pragmas", None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    res = native_fn(
                        wal_mode,
                        synchronous,
                        mmap_size,
                        busy_timeout_ms,
                        foreign_keys,
                    )
                    if isinstance(res, list) and all(isinstance(p, str) for p in res):
                        return list(res)

        pragmas = [f"PRAGMA busy_timeout = {busy_timeout_ms}"]
        if wal_mode:
            pragmas.append("PRAGMA journal_mode = WAL")
        pragmas.append(f"PRAGMA synchronous = {synchronous}")
        if mmap_size > 0:
            pragmas.append(f"PRAGMA mmap_size = {mmap_size}")
        pragmas.append(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
        return pragmas
