"""Shared Vertebra DatabaseWrapper mixin for Django database backends."""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._vertebra import AlloyDBDialer, CloudSQLDialer, LeaseBroker, LeaseGrant
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.django.orm import rows_to_structs
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "BackendPoolerConfig",
    "DatabaseWrapperMixin",
)


class BackendPoolerConfig(msgspec.Struct, frozen=True, gc=False):
    """Connection pooler, Cloud Dialer, and SHM LeaseBroker settings for a Django database backend."""

    vendor: str
    pooler_mode: str = "session"
    pgbouncer: bool = False
    direct_dsn: str | None = None
    direct_host: str | None = None
    direct_port: int | None = None
    cloud_sql_instance: str | None = None
    alloydb_instance_uri: str | None = None
    wallet_zip: str | bytes | None = None
    enable_vector_handlers: bool = True
    max_connections: int = 32

    def needs_direct_for_ddl(self) -> bool:
        """Return True when DDL migrations require bypassing transaction/statement pooling."""
        return bool(
            self.pgbouncer
            or self.pooler_mode in ("transaction", "statement")
            or self.direct_dsn is not None
            or self.direct_host is not None
            or self.direct_port is not None
        )


class DatabaseWrapperMixin:
    """Mixin enhancing Django ``BaseDatabaseWrapper`` with SHM ``LeaseBroker``, Cloud Dialers, and PgBouncer."""

    vendor: str = "generic"
    display_name: str = "Generic (Vertebra)"
    driver_scheme: str = "generic"

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        normalized = dict(settings_dict)
        normalized.setdefault("TIME_ZONE", None)
        normalized.setdefault("AUTOCOMMIT", True)
        normalized.setdefault("ATOMIC_REQUESTS", False)
        normalized.setdefault("CONN_MAX_AGE", 0)
        normalized.setdefault("CONN_HEALTH_CHECKS", False)
        normalized.setdefault("OPTIONS", {})
        normalized.setdefault("HOST", "")
        normalized.setdefault("PORT", "")
        normalized.setdefault("USER", "")
        normalized.setdefault("PASSWORD", "")
        self.settings_dict = normalized
        self.alias = alias
        options = dict(self.settings_dict.get("OPTIONS") or {})
        self.pooler_config = BackendPoolerConfig(
            vendor=self.vendor,
            pooler_mode=str(options.get("pooler_mode", "transaction" if options.get("pgbouncer") else "session")),
            pgbouncer=bool(options.get("pgbouncer", False)),
            direct_dsn=options.get("direct_dsn"),
            direct_host=options.get("direct_host"),
            direct_port=int(options["direct_port"]) if options.get("direct_port") is not None else None,
            cloud_sql_instance=options.get("cloud_sql_instance"),
            alloydb_instance_uri=options.get("alloydb_instance_uri"),
            wallet_zip=options.get("wallet_zip"),
            enable_vector_handlers=bool(options.get("enable_vector_handlers", True)),
            max_connections=int(options.get("max_connections", 32)),
        )
        self._share_pool: bool = bool(options.get("share_pool", True))
        self._broker = (
            DatabaseRegistry.default_broker(pool_size=self.pooler_config.max_connections)
            if self._share_pool
            else LeaseBroker(pool_size=self.pooler_config.max_connections)
        )
        self._active_lease: LeaseGrant | None = None
        self._cloud_dialer: CloudSQLDialer | AlloyDBDialer | None = None
        if self.pooler_config.cloud_sql_instance:
            self._cloud_dialer = CloudSQLDialer(self.pooler_config.cloud_sql_instance)
        elif self.pooler_config.alloydb_instance_uri:
            self._cloud_dialer = AlloyDBDialer(self.pooler_config.alloydb_instance_uri)
        self._native_driver: Any = None
        self._async_native_driver: Any = None

    @property
    def share_pool(self) -> bool:
        """Return True when this wrapper shares its native driver pool and LeaseBroker via DatabaseRegistry.default()."""
        return self._share_pool

    @property
    def native_driver(self) -> Any:
        """Return the synchronous Vertebra native driver instance backing this wrapper."""
        if self._native_driver is None:
            cfg = self._build_native_config()
            registry = DatabaseRegistry.default() if self._share_pool else DatabaseRegistry(broker=self._broker)
            self._native_driver = registry.get_shared_sync_driver(cfg)
        return self._native_driver

    @property
    def async_native_driver(self) -> Any:
        """Return the asynchronous Vertebra native driver instance backing this wrapper."""
        if self._async_native_driver is None:
            cfg = self._build_native_config()
            registry = DatabaseRegistry.default() if self._share_pool else DatabaseRegistry(broker=self._broker)
            self._async_native_driver = registry.get_shared_driver(cfg)
        return self._async_native_driver

    @property
    def engine(self) -> str:
        """Return the canonical Django backend engine identifier."""
        return "sqlite3" if self.vendor == "sqlite" else self.vendor

    @property
    def wallet_zip(self) -> str | bytes | None:
        """Return the configured Oracle Autonomous Database wallet archive."""
        return self.pooler_config.wallet_zip

    @property
    def broker(self) -> LeaseBroker:
        """Return the Rust SHM ``LeaseBroker`` governing this database backend."""
        return self._broker

    @property
    def cloud_dialer(self) -> CloudSQLDialer | AlloyDBDialer | None:
        """Return the configured ``CloudSQLDialer`` or ``AlloyDBDialer`` if enabled."""
        return self._cloud_dialer

    @property
    def disables_server_side_cursors(self) -> bool:
        """Return True when transaction pooling is enabled so Django disables server-side cursors."""
        return self.pooler_config.pgbouncer or self.pooler_config.pooler_mode == "transaction"

    def build_cloud_sql_dialer(self) -> CloudSQLDialer | None:
        """Return the ``CloudSQLDialer`` when ``cloud_sql_instance`` is configured."""
        if isinstance(self._cloud_dialer, CloudSQLDialer):
            return self._cloud_dialer
        if self.pooler_config.cloud_sql_instance:
            return CloudSQLDialer(self.pooler_config.cloud_sql_instance)
        return None

    def build_alloydb_dialer(self) -> AlloyDBDialer | None:
        """Return the ``AlloyDBDialer`` when ``alloydb_instance_uri`` is configured."""
        if isinstance(self._cloud_dialer, AlloyDBDialer):
            return self._cloud_dialer
        if self.pooler_config.alloydb_instance_uri:
            return AlloyDBDialer(self.pooler_config.alloydb_instance_uri)
        return None

    def acquire_lease(self, timeout_ms: int = 5000) -> int:
        """Acquire a connection slot lease from the Rust ``LeaseBroker`` and return the slot index."""
        grant = self._broker.acquire(worker_pid=os.getpid(), timeout_ms=timeout_ms)
        self._active_lease = grant
        return int(grant.slot_index) if grant is not None else -1

    def release_lease(self) -> None:
        """Release the currently held ``LeaseGrant`` if any."""
        if self._active_lease is not None:
            self._broker.release(self._active_lease.slot_index, self._active_lease.generation)
            self._active_lease = None

    def get_connection_params(self) -> dict[str, Any]:
        """Build driver connection parameters including Cloud Dialer port and PgBouncer flags."""
        params: dict[str, Any] = {
            "database": self.settings_dict.get("NAME", ""),
            "user": self.settings_dict.get("USER", ""),
            "password": self.settings_dict.get("PASSWORD", ""),
            "host": self.settings_dict.get("HOST", "localhost"),
            "port": self.settings_dict.get("PORT") or self._default_port(),
        }
        if isinstance(self._cloud_dialer, CloudSQLDialer):
            params["cloud_sql_instance"] = self.pooler_config.cloud_sql_instance
            params["dialer_port"] = 3307
        elif isinstance(self._cloud_dialer, AlloyDBDialer):
            params["alloydb_instance_uri"] = self.pooler_config.alloydb_instance_uri
            params["dialer_port"] = 5433
        if self.disables_server_side_cursors:
            params["statement_cache_size"] = 0
        if self.pooler_config.wallet_zip:
            params["wallet_zip"] = self.pooler_config.wallet_zip
        return params

    def derive_direct_settings(self) -> dict[str, Any]:
        """Return a copy of ``settings_dict`` routed to the direct non-pooled endpoint for migrations."""
        derived = dict(self.settings_dict)
        opts = dict(derived.get("OPTIONS") or {})
        opts["pgbouncer"] = False
        opts["pooler_mode"] = "session"
        derived["OPTIONS"] = opts
        if self.pooler_config.direct_host:
            derived["HOST"] = self.pooler_config.direct_host
        if self.pooler_config.direct_port is not None:
            derived["PORT"] = self.pooler_config.direct_port
        if self.pooler_config.direct_dsn:
            opts["dsn"] = self.pooler_config.direct_dsn
        return derived

    def schema_editor(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate to Django's `BaseDatabaseWrapper.schema_editor`, routing DDL via direct session settings when pooled."""
        super_schema_editor = getattr(super(), "schema_editor", None)
        if not callable(super_schema_editor):
            msg = f"{type(self).__name__} does not inherit a Django schema_editor() implementation"
            raise ImproperConfigurationError(msg)
        if not self.pooler_config.needs_direct_for_ddl():
            return super_schema_editor(*args, **kwargs)
        orig_settings = self.settings_dict
        orig_pooler = self.pooler_config
        self.settings_dict = self.derive_direct_settings()
        self.pooler_config = msgspec.structs.replace(
            self.pooler_config,
            pooler_mode="session",
            pgbouncer=False,
        )
        try:
            editor = super_schema_editor(*args, **kwargs)
        finally:
            self.settings_dict = orig_settings
            self.pooler_config = orig_pooler
        editor.__class__ = _direct_schema_editor_class(type(editor))
        editor._vtb_wrapper = self
        return editor

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        rows: Sequence[dict[str, Any]] | None = None,
    ) -> Any:
        """Execute `sql` via `self.cursor()` when `rows` is `None` and return a `pyarrow.Table`."""
        pa = import_optional("pyarrow")
        if pa is None:
            msg = "pyarrow is required for select_to_arrow()"
            raise RuntimeError(msg)
        if rows is not None:
            return pa.Table.from_pylist(list(rows))
        tuple_rows, columns = self._execute_select_cursor(sql, params)
        if not tuple_rows:
            if columns:
                return pa.table({col: [] for col in columns})
            return pa.Table.from_pylist([])
        dict_rows = [dict(zip(columns, row, strict=False)) for row in tuple_rows]
        return pa.Table.from_pylist(dict_rows)

    def select_to_schema[T](
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        schema_type: type[T],
        rows: Sequence[dict[str, Any] | Sequence[Any]] | None = None,
    ) -> list[T]:
        """Execute `sql` via `self.cursor()` when `rows` is `None` and hydrate rows into `list[schema_type]`."""
        if rows is not None:
            return rows_to_structs(list(rows), schema_type)
        tuple_rows, columns = self._execute_select_cursor(sql, params)
        return rows_to_structs(tuple_rows, schema_type, columns=columns or None)

    def _build_native_config(self) -> BaseDatabaseConfig:
        """Build native configuration from connection parameters."""
        params = self.get_connection_params()
        cfg = params.get("config")
        if isinstance(cfg, BaseDatabaseConfig):
            return cfg
        msg = f"Database configuration not available for {self.vendor}"
        raise ImproperConfigurationError(msg)

    def _execute_select_cursor(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        """Execute `(sql, params)` through `self.cursor()` and return `(tuple_rows, column_names)`."""
        cursor_fn = getattr(self, "cursor", None)
        if not callable(cursor_fn):
            msg = f"{type(self).__name__} does not expose cursor() for SQL execution"
            raise ImproperConfigurationError(msg)
        with cursor_fn() as cur:
            if params is not None:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [str(col[0]) for col in (cur.description or ())]
            fetched = [tuple(row) for row in cur.fetchall()]
        return fetched, cols

    def _default_port(self) -> int:
        ports = {
            "postgresql": 5432,
            "mysql": 3306,
            "oracle": 1521,
            "mssql": 1433,
            "sqlite": 0,
            "duckdb": 0,
        }
        return ports.get(self.vendor, 0)


_DIRECT_SCHEMA_EDITOR_CLASSES: dict[type[Any], type[Any]] = {}


def _direct_schema_editor_class(base_cls: type[Any]) -> type[Any]:
    """Return a subclass of `base_cls` that temporarily activates direct DDL settings on enter/exit."""
    cached = _DIRECT_SCHEMA_EDITOR_CLASSES.get(base_cls)
    if cached is not None:
        return cached

    class _DirectDDLSchemaEditor(base_cls):
        def __enter__(self) -> Any:
            wrapper = getattr(self, "_vtb_wrapper", None)
            if wrapper is not None:
                direct_settings = wrapper.derive_direct_settings()
                self._vtb_saved_state = (
                    wrapper.settings_dict,
                    wrapper.pooler_config,
                    getattr(wrapper, "connection", None),
                )
                wrapper.settings_dict = direct_settings
                wrapper.pooler_config = msgspec.structs.replace(
                    wrapper.pooler_config,
                    pooler_mode="session",
                    pgbouncer=False,
                )
                if hasattr(wrapper, "connection"):
                    wrapper.connection = None
            return super().__enter__()

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
            wrapper = getattr(self, "_vtb_wrapper", None)
            saved = getattr(self, "_vtb_saved_state", None)
            try:
                return super().__exit__(exc_type, exc_val, exc_tb)
            finally:
                if wrapper is not None and saved is not None:
                    orig_settings, orig_pooler, orig_conn = saved
                    temp_conn = getattr(wrapper, "connection", None)
                    if temp_conn is not None and temp_conn is not orig_conn:
                        with contextlib.suppress(Exception):
                            temp_conn.close()
                    wrapper.settings_dict = orig_settings
                    wrapper.pooler_config = orig_pooler
                    if hasattr(wrapper, "connection"):
                        wrapper.connection = orig_conn

    _DirectDDLSchemaEditor.__name__ = f"Direct{base_cls.__name__}"
    _DirectDDLSchemaEditor.__qualname__ = f"Direct{base_cls.__qualname__}"
    _DIRECT_SCHEMA_EDITOR_CLASSES[base_cls] = _DirectDDLSchemaEditor
    return _DirectDDLSchemaEditor
