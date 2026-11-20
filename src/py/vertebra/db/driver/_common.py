"""Common base configuration and driver mixin for Vertebra database drivers."""

from __future__ import annotations

import contextlib
import inspect
import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Final, Self

from vertebra._vertebra import InterfaceError
from vertebra.db.arrow import (
    _hydrate_rows_with_schema,
    write_arrow_shm,
)
from vertebra.telemetry.otel import TraceContext
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable

    from vertebra._vertebra import StatementCache, StatementPlan

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
    requires_statement_autocommit: ClassVar[bool] = False

    @property
    def effective_direct_dsn(self) -> str:
        """Return the effective direct connection DSN or URL for migrations and pooler bypass."""
        for attr in ("direct_dsn", "dsn", "database", "path"):
            value = getattr(self, attr, None)
            if value:
                return str(value)
        return ""

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

    __slots__ = ("_broker", "_broker_lease_plan", "_closed", "config")

    def __init__(self, config: ConfigT, *, broker: Any = None) -> None:
        self.config: ConfigT = config
        self._broker: Any = broker if broker is not None else getattr(config, "broker", None)
        self._broker_lease_plan: _BrokerLeasePlan | None = None
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

    def _lease_plan(self) -> _BrokerLeasePlan:
        """Return the cached lease call plan for the attached broker, rebuilding it when the broker changes."""
        broker = self._broker
        try:
            plan = self._broker_lease_plan
        except AttributeError:
            plan = None
        if plan is None or plan.broker is not broker:
            plan = _BrokerLeasePlan(broker)
            self._broker_lease_plan = plan
        return plan

    def _acquire_broker_lease(self, timeout_ms: int = 5000, bind_key_hash: int = 0) -> Any:
        """Acquire a connection lease from the shared-memory `LeaseBroker` when configured."""
        broker = self._broker
        if broker is None:
            return None
        plan = self._lease_plan()
        if plan.has_acquire:
            if plan.acquire_is_coroutine:
                msg = "Async broker transport cannot acquire leases synchronously; use SyncSHMBrokerTransport"
                raise InterfaceError(msg)
            try:
                return broker.acquire(os.getpid(), **plan.acquire_kwargs(timeout_ms, bind_key_hash))
            except RuntimeError:
                return None
        lease_table = getattr(broker, "lease_table", None)
        if lease_table is not None:
            with contextlib.suppress(RuntimeError):
                return lease_table.acquire(os.getpid())
        return None

    def _release_broker_lease(self, lease: Any) -> None:
        """Release a previously acquired connection lease back to the `LeaseBroker`."""
        broker = self._broker
        if broker is None or lease is None:
            return
        plan = self._lease_plan()
        if plan.lease_releases_itself(lease):
            with contextlib.suppress(RuntimeError):
                lease.release()
            return
        if not (hasattr(lease, "slot_index") and hasattr(lease, "generation")):
            return
        if plan.has_sync_release:
            with contextlib.suppress(RuntimeError):
                broker.release(lease.slot_index, lease.generation)
            return
        lease_table = getattr(broker, "lease_table", None)
        if lease_table is not None:
            with contextlib.suppress(RuntimeError):
                lease_table.release(lease.slot_index, lease.generation)

    def _acquire_pid_lease(self) -> Any:
        """Acquire a PID-bound lease from the shared-memory `LeaseBroker` when configured."""
        broker = self._broker
        if broker is None:
            return None
        plan = self._lease_plan()
        if plan.has_acquire:
            if plan.acquire_is_coroutine:
                msg = "Async broker transport cannot acquire leases synchronously; use SyncSHMBrokerTransport"
                raise InterfaceError(msg)
            kwargs = plan.acquire_kwargs(5000, 0)
            kwargs.pop("bind_key_hash", None)
            with contextlib.suppress(Exception):
                return broker.acquire(os.getpid(), **kwargs)
        return None

    def _release_pid_lease(self, lease: Any) -> None:
        """Release a previously acquired PID-bound lease back to the `LeaseBroker`."""
        broker = self._broker
        if lease is None or broker is None:
            return
        plan = self._lease_plan()
        if plan.lease_releases_itself(lease):
            with contextlib.suppress(Exception):
                lease.release()
            return
        if plan.has_sync_release:
            with contextlib.suppress(Exception):
                broker.release(lease.slot_index, lease.generation)

    def _lookup_or_cache_statement(
        self,
        sql: str,
        *,
        param_count: int = 0,
        compiled_sql: bytes | None = None,
        param_plan: bytes = b"",
    ) -> StatementPlan | None:
        """Lookup or register a UTF-8 SQL statement plan in the broker `StatementCache`."""
        if self._broker is None:
            return None
        sc_attr: StatementCache | Callable[[], StatementCache | None] | None = getattr(
            self._broker, "statement_cache", None
        )
        sc: StatementCache | None = sc_attr() if callable(sc_attr) else sc_attr
        if sc is None:
            return None
        raw_bytes = sql.encode("utf-8")
        tag = int(self.dialect_tag)
        cached: StatementPlan | None = sc.lookup(tag, raw_bytes)
        if cached is not None:
            return cached
        sc.insert(tag, raw_bytes, param_count, compiled_sql or raw_bytes, param_plan)
        return None

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


class _BrokerLeasePlan:
    """Signature facts about one broker's lease API, inspected once instead of on every query."""

    __slots__ = (
        "_self_releasing_lease_types",
        "accepts_bind_key_hash",
        "accepts_timeout_ms",
        "acquire_is_coroutine",
        "broker",
        "has_acquire",
        "has_sync_release",
    )

    def __init__(self, broker: Any) -> None:
        self.broker: Any = broker
        acquire_fn = getattr(broker, "acquire", None)
        self.has_acquire: bool = callable(acquire_fn)
        self.acquire_is_coroutine: bool = self.has_acquire and inspect.iscoroutinefunction(acquire_fn)
        self.accepts_timeout_ms: bool = True
        self.accepts_bind_key_hash: bool = False
        if acquire_fn is not None and self.has_acquire:
            params: Any = None
            with contextlib.suppress(TypeError, ValueError):
                params = inspect.signature(acquire_fn).parameters
            if params is not None:
                has_var_kw = any(param.kind is inspect.Parameter.VAR_KEYWORD for param in params.values())
                self.accepts_timeout_ms = has_var_kw or "timeout_ms" in params
                self.accepts_bind_key_hash = has_var_kw or "bind_key_hash" in params
        release_fn = getattr(broker, "release", None)
        self.has_sync_release: bool = callable(release_fn) and not inspect.iscoroutinefunction(release_fn)
        self._self_releasing_lease_types: dict[type, bool] = {}

    def acquire_kwargs(self, timeout_ms: int, bind_key_hash: int) -> dict[str, Any]:
        """Return the keyword arguments the broker's `acquire` accepts."""
        kwargs: dict[str, Any] = {}
        if self.accepts_timeout_ms:
            kwargs["timeout_ms"] = timeout_ms
        if self.accepts_bind_key_hash:
            kwargs["bind_key_hash"] = bind_key_hash
        return kwargs

    def lease_releases_itself(self, lease: Any) -> bool:
        """Return whether `lease` exposes a synchronous `release()` method, cached per lease type."""
        lease_type = type(lease)
        known = self._self_releasing_lease_types.get(lease_type)
        if known is None:
            release_fn = getattr(lease, "release", None)
            known = callable(release_fn) and not inspect.iscoroutinefunction(release_fn)
            self._self_releasing_lease_types[lease_type] = known
        return known
