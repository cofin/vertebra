"""Common base configuration and driver mixin for Vertebra database drivers."""

from __future__ import annotations

import contextlib
import zlib
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, TypeVar

from vertebra.db.arrow import (
    _hydrate_rows_with_schema,
    write_arrow_shm,
)
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import FrozenBaseStruct

__all__ = (
    "BaseDatabaseConfig",
    "CommonDriverMixin",
    "_normalize_driver_params",
)

ConfigT = TypeVar("ConfigT", bound="BaseDatabaseConfig")


def _normalize_driver_params(
    params: tuple[Any, ...],
    parameters: Sequence[Any] | Mapping[str, Any],
) -> Sequence[Any] | Mapping[str, Any]:
    """Normalize variadic positional parameters and explicit `parameters` argument."""
    if parameters:
        return parameters
    if len(params) == 1:
        if params[0] is None:
            return ()
        if isinstance(params[0], (tuple, list, dict)):
            return params[0]
    return params


class BaseDatabaseConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Base configuration struct for built-in Vertebra database and cache drivers."""

    dialect_tag: ClassVar[int] = 0


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
    def _normalize_parameters(
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> Sequence[Any] | Mapping[str, Any] | None:
        """Return normalized query parameters."""
        return parameters
