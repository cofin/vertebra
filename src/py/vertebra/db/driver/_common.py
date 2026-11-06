"""Common base configuration and driver mixin for Vertebra database drivers."""

from __future__ import annotations

import contextlib
import os
import zlib
from collections.abc import Sequence
from typing import Any, ClassVar, Final, Self

from vertebra.db.arrow import (
    _hydrate_rows_with_schema,
    write_arrow_shm,
)
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import ensure_module

__all__ = (
    "BIGQUERY_DIALECT_TAG",
    "DATAFUSION_DIALECT_TAG",
    "DUCKDB_DIALECT_TAG",
    "MSSQL_DIALECT_TAG",
    "MYSQL_DIALECT_TAG",
    "ORACLE_DIALECT_TAG",
    "POSTGRES_DIALECT_TAG",
    "REDIS_DIALECT_TAG",
    "SPANGRES_DIALECT_TAG",
    "SPANNER_GOOGLESQL_DIALECT_TAG",
    "SQLITE_DIALECT_TAG",
    "VALKEY_DIALECT_TAG",
    "BaseDatabaseConfig",
    "CommonDriverMixin",
)

POSTGRES_DIALECT_TAG: Final[int] = 1
MYSQL_DIALECT_TAG: Final[int] = 2
SQLITE_DIALECT_TAG: Final[int] = 3
ORACLE_DIALECT_TAG: Final[int] = 4
MSSQL_DIALECT_TAG: Final[int] = 5
DUCKDB_DIALECT_TAG: Final[int] = 6
BIGQUERY_DIALECT_TAG: Final[int] = 7
SPANNER_GOOGLESQL_DIALECT_TAG: Final[int] = 8
SPANGRES_DIALECT_TAG: Final[int] = 9
REDIS_DIALECT_TAG: Final[int] = 10
VALKEY_DIALECT_TAG: Final[int] = 11
DATAFUSION_DIALECT_TAG: Final[int] = 12


class BaseDatabaseConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Base configuration struct for built-in Vertebra database and cache drivers."""

    dialect_tag: ClassVar[int] = 0
    url_schemes: ClassVar[tuple[str, ...]] = ()
    driver_module: ClassVar[str] = ""
    sync_driver_name: ClassVar[str] = ""
    async_driver_name: ClassVar[str] = ""

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Parse a connection URL and keyword overrides into this configuration struct."""
        _ = (url, overrides)
        msg = f"{cls.__name__} does not implement from_url()"
        raise NotImplementedError(msg)

    def pool_key(self) -> tuple[Any, ...] | None:
        """Return a deterministic shared-pool fingerprint tuple, or `None` when unshareable."""
        msg = f"{type(self).__name__} does not implement pool_key()"
        raise NotImplementedError(msg)

    def resolve_driver_classes(self) -> tuple[type[Any], type[Any]]:
        """Lazily import `driver_module` and return `(sync_driver_cls, async_driver_cls)`."""
        if not self.driver_module or not self.sync_driver_name or not self.async_driver_name:
            msg = f"{type(self).__name__} does not declare driver_module, sync_driver_name, and async_driver_name"
            raise NotImplementedError(msg)
        mod = ensure_module(self.driver_module)
        return getattr(mod, self.sync_driver_name), getattr(mod, self.async_driver_name)

    def build_sync_driver(self, registry: Any) -> Any:
        """Instantiate the synchronous driver for this configuration."""
        sync_cls, _ = self.resolve_driver_classes()
        broker = getattr(self, "broker", None)
        if broker is None and registry is not None:
            broker = getattr(registry, "_broker", None)
        return sync_cls(self, broker=broker)

    def build_async_driver(self, registry: Any) -> Any:
        """Instantiate the asynchronous driver for this configuration."""
        _, async_cls = self.resolve_driver_classes()
        broker = getattr(self, "broker", None)
        if broker is None and registry is not None:
            broker = getattr(registry, "_broker", None)
        return async_cls(self, broker=broker)


class CommonDriverMixin[ConfigT: BaseDatabaseConfig]:
    """Shared attributes, SHM lease management, SQL trace annotation, and row hydration for drivers."""

    dialect_tag: ClassVar[int] = 0

    __slots__ = ("_broker", "_closed", "config")

    def __init__(self, config: ConfigT, *, broker: Any = None) -> None:
        self.config: ConfigT = config
        self._broker: Any = broker if broker is not None else getattr(config, "broker", None)
        self._closed: bool = False

    @property
    def broker(self) -> Any:
        """Return the optional shared-memory `LeaseBroker` attached to this driver."""
        return self._broker

    @property
    def closed(self) -> bool:
        """Return `True` when the driver has been closed."""
        return self._closed

    def write_arrow_shm(self, table: Any) -> memoryview:
        """Serialize a PyArrow Table, RecordBatch, or row sequence into a shared-memory Arrow IPC buffer."""
        return write_arrow_shm(table)

    def hydrate_rows(
        self,
        rows: Sequence[Any],
        columns: Sequence[str],
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Hydrate tabular rows into dictionaries or typed `schema_type` instances."""
        return _hydrate_rows_with_schema(rows, columns, schema_type)

    def _acquire_broker_lease(self) -> Any:
        """Acquire a connection lease from the shared-memory `LeaseBroker` when configured."""
        if self._broker is None:
            return None
        acquire_fn = getattr(self._broker, "acquire_lease", None)
        if callable(acquire_fn):
            with contextlib.suppress(RuntimeError):
                return acquire_fn(int(self.dialect_tag), 0)
        return None

    def _release_broker_lease(self, lease: Any) -> None:
        """Release a previously acquired connection lease back to the `LeaseBroker`."""
        if self._broker is None or lease is None:
            return
        release_fn = getattr(self._broker, "release_lease", None)
        if callable(release_fn):
            with contextlib.suppress(RuntimeError):
                release_fn(lease)

    def _acquire_pid_lease(self) -> Any:
        """Acquire a PID-bound lease from the shared-memory `LeaseBroker` when configured."""
        if self._broker is None:
            return None
        acquire_fn = getattr(self._broker, "acquire", None)
        if callable(acquire_fn):
            with contextlib.suppress(Exception):
                return acquire_fn(os.getpid(), timeout_ms=5000)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        """Release a previously acquired PID-bound lease back to the `LeaseBroker`."""
        if lease is None or self._broker is None:
            return
        release_fn = getattr(self._broker, "release", None)
        if callable(release_fn):
            with contextlib.suppress(Exception):
                release_fn(lease.slot_index, lease.generation)

    def _lookup_or_cache_statement(self, sql: str) -> tuple[bool, int]:
        """Lookup or register a SQL statement hash in the shared-memory `LeaseBroker`."""
        stmt_hash = zlib.crc32(sql.encode("utf-8")) & 0xFFFFFFFF
        if self._broker is None:
            return False, stmt_hash
        lookup_fn = getattr(self._broker, "lookup_statement", None)
        insert_fn = getattr(self._broker, "insert_statement", None)
        if callable(lookup_fn):
            with contextlib.suppress(RuntimeError):
                if lookup_fn(int(self.dialect_tag), stmt_hash) is not None:
                    return True, stmt_hash
        if callable(insert_fn):
            with contextlib.suppress(RuntimeError):
                insert_fn(int(self.dialect_tag), stmt_hash, stmt_hash)
        return False, stmt_hash

    @staticmethod
    def _annotate_sql(sql: str, *, propagate_trace: bool = True) -> str:
        """Append an OpenTelemetry `traceparent` SQL comment when trace propagation is enabled."""
        if not propagate_trace:
            return sql
        ctx = TraceContext.current()
        if ctx is not None:
            return f"{sql} /*traceparent='{ctx.format()}'*/"
        return sql

    @staticmethod
    def _extract_column_names(description: Any) -> tuple[str, ...]:
        """Normalize a DB-API cursor `description` or string sequence into `tuple[str, ...]`."""
        if not description:
            return ()
        return tuple(col[0] if isinstance(col, tuple) else str(col) for col in description)
