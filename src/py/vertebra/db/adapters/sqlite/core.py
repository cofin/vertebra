"""SQLite WAL pragma builder and core helpers."""

from __future__ import annotations

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
        if synchronous not in VALID_SQLITE_SYNCHRONOUS:
            raise ValueError(
                f"Invalid synchronous mode '{synchronous}', expected one of {sorted(VALID_SQLITE_SYNCHRONOUS)}"
            )
        pragmas = [f"PRAGMA busy_timeout = {busy_timeout_ms}"]
        if wal_mode:
            pragmas.append("PRAGMA journal_mode = WAL")
        pragmas.append(f"PRAGMA synchronous = {synchronous}")
        if mmap_size > 0:
            pragmas.append(f"PRAGMA mmap_size = {mmap_size}")
        pragmas.append(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
        return pragmas
