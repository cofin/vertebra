"""SQLite 3.45+ WAL configuration with memory-mapped I/O and SHM broker leasing."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import msgspec

from vertebra.db.driver import BaseDatabaseConfig
from vertebra.db.sqlite.core import VALID_SQLITE_SYNCHRONOUS, SQLiteCore

__all__ = ("SQLiteConfig",)


class SQLiteConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for SQLite 3.45+ with WAL mode, memory-mapped I/O, and SHM broker leasing."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = 3

    path: str = ":memory:"
    """Filesystem path to the SQLite database file or `":memory:"`."""
    wal_mode: bool = True
    """When True, enable `PRAGMA journal_mode = WAL` for concurrent readers alongside a single writer."""
    synchronous: str = "NORMAL"
    """SQLite `PRAGMA synchronous` durability level (`"OFF"`, `"NORMAL"`, `"FULL"`, or `"EXTRA"`)."""
    mmap_size: int = 268_435_456
    """Maximum memory-mapped I/O window in bytes (`PRAGMA mmap_size`, default 256 MiB)."""
    busy_timeout_ms: int = 5000
    """Lock contention wait timeout in milliseconds (`PRAGMA busy_timeout`)."""
    max_readers: int = 8
    """Maximum number of concurrent read connections leased across workers."""
    foreign_keys: bool = True
    """When True, enforce referential integrity via `PRAGMA foreign_keys = ON`."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""
    database: str | Path | None = None
    """Optional alias for `path` accepting a `pathlib.Path` or string."""
    enable_jsonb: bool = True
    """When True, use SQLite 3.45+ binary `JSONB` functions when available."""

    def __post_init__(self) -> None:
        if self.database is not None and self.path == ":memory:":
            msgspec.structs.force_setattr(self, "path", str(self.database))
        if self.database is None:
            msgspec.structs.force_setattr(self, "database", self.path)
        if self.busy_timeout_ms < 0:
            msg = f"busy_timeout_ms must be non-negative (got {self.busy_timeout_ms})"
            raise ValueError(msg)
        sync_upper = self.synchronous.upper()
        if sync_upper not in VALID_SQLITE_SYNCHRONOUS:
            msg = f"Invalid synchronous mode: {self.synchronous!r}"
            raise ValueError(msg)
        msgspec.structs.force_setattr(self, "synchronous", sync_upper)

    @property
    def is_in_memory(self) -> bool:
        """Return True if the target database is in-memory."""
        raw = str(self.path)
        return raw == ":memory:" or "mode=memory" in raw

    def initialization_pragmas(self) -> list[str]:
        """Return the list of PRAGMA statements executed upon connection initialization."""
        return SQLiteCore.initialization_pragmas(
            wal_mode=self.wal_mode,
            synchronous=self.synchronous,
            mmap_size=self.mmap_size,
            busy_timeout_ms=self.busy_timeout_ms,
            foreign_keys=self.foreign_keys,
        )

    def startup_pragmas(self) -> list[str]:
        """Return startup PRAGMA statements for connection initialization."""
        return self.initialization_pragmas()
