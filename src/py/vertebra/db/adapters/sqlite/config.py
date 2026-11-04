"""SQLite 3.45+ WAL configuration with memory-mapped I/O and SHM broker leasing."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import msgspec

from vertebra.db.adapters.sqlite.core import VALID_SQLITE_SYNCHRONOUS, SQLiteCore
from vertebra.db.driver._common import BaseDatabaseConfig

__all__ = ("SQLiteConfig",)


class SQLiteConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for SQLite 3.45+ with WAL mode, memory-mapped I/O, and SHM broker leasing."""

    dialect: ClassVar[str] = "sqlite"
    dialect_tag: ClassVar[int] = 3
    url_schemes: ClassVar[tuple[str, ...]] = ("sqlite",)
    driver_module: ClassVar[str] = "vertebra.db.adapters.sqlite.driver"
    sync_driver_name: ClassVar[str] = "SyncSQLiteDriver"
    async_driver_name: ClassVar[str] = "AsyncSQLiteDriver"

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

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `SQLiteConfig` from a `sqlite://` URI."""
        parsed = urlparse(url)
        if parsed.path in ("", "/", "/:memory:", ":memory:"):
            db_path = ":memory:"
        elif parsed.path.startswith("//"):
            db_path = parsed.path[1:]
        elif os.path.isabs(parsed.path) and os.path.exists(os.path.dirname(parsed.path)):
            db_path = parsed.path
        else:
            db_path = parsed.path.lstrip("/")
        if parsed.query and (
            db_path.startswith("file:") or "mode=memory" in parsed.query or "cache=shared" in parsed.query
        ):
            prefix = db_path if db_path.startswith("file:") else f"file:{db_path}"
            db_path = f"{prefix}?{parsed.query}"
        return cls(path=db_path, database=db_path, **overrides)

    def pool_key(self) -> tuple[Any, ...] | None:
        """Return a deterministic shared-pool fingerprint, or `None` for isolated `:memory:` DBs."""
        raw_path = str(self.path or self.database or ":memory:")
        if self.is_in_memory and "cache=shared" not in raw_path:
            return None
        norm_path = raw_path if (raw_path == ":memory:" or "mode=memory" in raw_path) else os.path.abspath(raw_path)
        return ("sqlite", norm_path, bool(self.wal_mode))
