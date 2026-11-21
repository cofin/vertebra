"""Standalone synchronous PEP 249 DB-API 2.0 connection, module, and exceptions.

The PEP 249 exception names are the ``vertebra._vertebra`` classes, so ``InterfaceError`` is a subclass of
``DatabaseError`` and ``Error`` is ``DatabaseError``.
"""

from __future__ import annotations

import datetime
import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any, Final, Protocol, Self, cast
from urllib.parse import urlparse

import msgspec

import vertebra._vertebra as _native
from vertebra.db.driver._common import BaseDatabaseConfig
from vertebra.db.registry import SCHEME_REGISTRY, AnyDatabaseConfig, DatabaseRegistry
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    UnsupportedBackendError,
    VertebraError,
)
from vertebra.utils.module_loader import import_optional

__all__ = (
    "BFILE",
    "BINARY",
    "BLOB",
    "CLOB",
    "DATETIME",
    "FIXED_CHAR",
    "NCLOB",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "SyncDBAPIConnection",
    "SyncDBAPIModule",
    "SyncSQLDriverProtocol",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
)

apilevel: Final[str] = "2.0"
threadsafety: Final[int] = 2
paramstyle: Final[str] = "qmark"

_POSTGRES_CONNECT_ARGS: Final[frozenset[str]] = frozenset(
    {
        "dsn",
        "direct_dsn",
        "pooler_mode",
        "pgbouncer",
        "pgbouncer_mode",
        "protocol_level_pooler_cache",
        "max_prepared_statements",
        "min_size",
        "max_size",
        "enable_cloud_sql",
        "cloud_sql_instance",
        "cloud_sql_ip_type",
        "enable_alloydb",
        "alloydb_instance_uri",
        "alloydb_ip_type",
        "enable_iam_auth",
        "iam_auth_options",
        "token_provider",
        "ca_pem",
        "enable_alloydb_columnar_engine",
        "application_name",
        "statement_timeout_ms",
        "idle_in_transaction_session_timeout_ms",
        "session_gucs",
    }
)

_SPANNER_CONNECT_ARGS: Final[frozenset[str]] = frozenset(
    {
        "project_id",
        "instance_id",
        "database_id",
        "dsn",
        "database_role",
        "dialect",
        "endpoint",
        "admin_endpoint",
        "pool_size",
        "route_to_leader_enabled",
        "max_commit_delay_ms",
        "data_boost_enabled",
        "max_partitions",
        "credentials_path",
        "service_account_key",
        "quota_project_id",
        "impersonate_service_account",
        "use_gcloud_cli",
        "scopes",
        "access_token",
        "use_shm_cache",
        "statement_cache_capacity",
        "ca_pem",
    }
)

_SUPPORTED_CONNECT_ARGS_BY_DIALECT: Final[dict[str, frozenset[str]]] = {
    "postgres": _POSTGRES_CONNECT_ARGS,
    "postgresql": _POSTGRES_CONNECT_ARGS,
    "mysql": frozenset(
        {
            "dsn",
            "host",
            "port",
            "user",
            "password",
            "database",
            "unix_socket",
            "charset",
            "autocommit",
            "connect_timeout",
            "read_timeout",
            "write_timeout",
            "ssl_mode",
            "ssl_ca",
            "ssl_cert",
            "ssl_key",
            "init_command",
            "min_size",
            "max_size",
            "statement_cache_size",
            "cloud_sql_instance",
            "cloud_sql_ip_type",
            "enable_iam_auth",
            "iam_auth_options",
            "credentials_path",
            "credentials_json",
            "target_service_account",
            "token_provider",
            "pool",
            "statement",
        }
    ),
    "sqlite": frozenset(
        {
            "path",
            "database",
            "wal_mode",
            "synchronous",
            "mmap_size",
            "busy_timeout_ms",
            "max_readers",
            "foreign_keys",
            "enable_jsonb",
        }
    ),
    "duckdb": frozenset(
        {
            "path",
            "database",
            "memory_limit_mb",
            "threads",
            "temp_directory",
            "parquet_metadata_cache",
            "httpfs_connection_caching",
            "enable_http_metadata_cache",
            "read_only",
            "secrets",
            "extensions",
            "init_statements",
            "config",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
        }
    ),
    "oracle": frozenset(
        {
            "dsn",
            "host",
            "port",
            "service_name",
            "sid",
            "connect_descriptor_str",
            "user",
            "password",
            "wallet_path",
            "wallet_zip",
            "tns_alias",
            "edition",
            "topology",
            "drcp",
            "auth_mode",
            "access_token",
            "token_auth_options",
            "driver_features",
            "client_identifier",
            "module",
            "action",
            "ecid",
            "context_namespace",
            "context_package_proc",
            "context_clear_proc",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
            "enable_reset_relay",
        }
    ),
    "mssql": frozenset(
        {
            "dsn",
            "host",
            "port",
            "database",
            "user",
            "password",
            "application_name",
            "encryption",
            "trust_server_certificate",
            "ca_pem",
            "client_cert_pem",
            "client_key_pem",
            "server_name",
            "connect_timeout_ms",
            "query_timeout_ms",
            "read_only_intent",
            "cloud_sql_instance",
            "cloudsql_ip_type",
            "cloudsql_auth_type",
            "cloudsql_proxy_addr",
            "token_provider",
            "pool_min_size",
            "pool_max_size",
            "statement_cache_size",
        }
    ),
    "bigquery": frozenset(
        {
            "project_id",
            "dataset_id",
            "location",
            "dsn",
            "rest_endpoint",
            "storage_endpoint",
            "maximum_bytes_billed",
            "job_timeout_ms",
            "priority",
            "labels",
            "credentials_path",
            "service_account_key",
            "quota_project_id",
            "impersonate_service_account",
            "access_token",
            "use_gcloud_cli",
            "scopes",
            "pool_size",
            "statement_cache_capacity",
            "use_shm_cache",
            "arrow_compression",
            "max_read_streams",
            "ca_pem",
        }
    ),
    "spanner": _SPANNER_CONNECT_ARGS,
    "spangres": _SPANNER_CONNECT_ARGS,
}

_SCHEME_ALIASES: Final[dict[str, str]] = {
    "mariadb": "mysql",
    "mysql+pymysql": "mysql",
    "sqlserver": "mssql",
    "bq": "bigquery",
    "spanner+googlesql": "spanner",
    "spanner+spangres": "spangres",
}


class SyncSQLDriverProtocol(Protocol):
    """Protocol describing synchronous Vertebra SQL drivers consumed by SyncDBAPIConnection."""

    dialect: str
    closed: bool

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> int:
        """Execute a statement and return affected row count."""
        ...

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> int:
        """Execute a statement across parameter sets and return affected row count."""
        ...

    def begin(self) -> None:
        """Begin a native transaction."""
        ...

    def commit(self) -> None:
        """Commit the active transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the active transaction."""
        ...

    def close(self) -> None:
        """Close the underlying driver."""
        ...


Error = _native.DatabaseError
DatabaseError = _native.DatabaseError
InterfaceError = _native.InterfaceError
DataError = _native.DataError
OperationalError = _native.OperationalError
IntegrityError = _native.IntegrityError
InternalError = _native.InternalError
ProgrammingError = _native.ProgrammingError
NotSupportedError = _native.NotSupportedError


class Warning(Exception):
    """PEP 249 DBAPI warning exception."""


class _DBAPITypeObject:
    """PEP 249 type singleton comparing equal to matching driver type codes."""

    __slots__ = ("_name", "_values")

    def __init__(self, name: str, *values: Any) -> None:
        self._name = name
        self._values = frozenset(values)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _DBAPITypeObject):
            return self._name == other._name
        return other in self._values

    def __hash__(self) -> int:
        return hash(self._name)

    def __repr__(self) -> str:
        return f"<DBAPIType {self._name}>"


Binary = bytes
STRING: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "STRING",
    str,
    "str",
    "TEXT",
    "VARCHAR",
    "CHAR",
    "STRING",
    25,
    1043,
)
BINARY: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "BINARY",
    bytes,
    bytearray,
    memoryview,
    "BLOB",
    "BYTEA",
    "BINARY",
    "VARBINARY",
    17,
)
NUMBER: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "NUMBER",
    int,
    float,
    "INTEGER",
    "INT",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "NUMERIC",
    "DECIMAL",
    "NUMBER",
    20,
    21,
    23,
    700,
    701,
    1700,
)
DATETIME: Final[_DBAPITypeObject] = _DBAPITypeObject(
    "DATETIME",
    datetime.datetime,
    datetime.date,
    datetime.time,
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    1082,
    1083,
    1114,
    1184,
)
ROWID: Final[_DBAPITypeObject] = _DBAPITypeObject("ROWID", "ROWID", "OID", 26)
BLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("BLOB", bytes, bytearray, memoryview, "BLOB")
BFILE: Final[_DBAPITypeObject] = _DBAPITypeObject("BFILE", bytes, bytearray, memoryview, "BFILE")
CLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("CLOB", str, "CLOB")
NCLOB: Final[_DBAPITypeObject] = _DBAPITypeObject("NCLOB", str, "NCLOB")
FIXED_CHAR: Final[_DBAPITypeObject] = _DBAPITypeObject("FIXED_CHAR", str, "CHAR", "NCHAR", "FIXED_CHAR")


def Date(year: int, month: int, day: int) -> datetime.date:
    """Construct a PEP 249 date value."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:
    """Construct a PEP 249 time value."""
    return datetime.time(hour, minute, second)


def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Construct a PEP 249 timestamp value."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:
    """Construct a PEP 249 date value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.date(local.tm_year, local.tm_mon, local.tm_mday)


def TimeFromTicks(ticks: float) -> datetime.time:
    """Construct a PEP 249 time value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.time(local.tm_hour, local.tm_min, local.tm_sec)


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    """Construct a PEP 249 timestamp value from epoch seconds."""
    local = time.localtime(ticks)
    return datetime.datetime(
        local.tm_year,
        local.tm_mon,
        local.tm_mday,
        local.tm_hour,
        local.tm_min,
        local.tm_sec,
    )


def _connect_error(exc: Exception) -> Exception | None:
    """Map a configuration or driver-build failure to its PEP 249 class, or return None to re-raise it unchanged."""
    if isinstance(exc, (DatabaseError, Warning)):
        return None
    if isinstance(exc, (ImproperConfigurationError, UnsupportedBackendError, MissingDependencyError)):
        return InterfaceError(str(exc))
    if isinstance(exc, VertebraError):
        return DatabaseError(str(exc))
    if isinstance(exc, (ValueError, TypeError)):
        return InterfaceError(str(exc))
    return None


def _normalize_connect_args_engine(url: str) -> str:
    """Extract normalized engine identifier from a database URL scheme."""
    raw_scheme, sep, _ = url.partition("://")
    scheme = (raw_scheme if sep else urlparse(url).scheme).lower()
    base_scheme = scheme
    if "+" in scheme:
        prefix, _, driver_tag = scheme.rpartition("+")
        if driver_tag in {"vtb", "vertebra", "vtb_async", "vertebra_async"}:
            base_scheme = prefix
    return _SCHEME_ALIASES.get(base_scheme, base_scheme)


def _validate_connect_kwargs(engine: str, connect_kwargs: dict[str, Any]) -> None:
    """Validate connect_args overrides against the engine's supported configuration fields."""
    if not connect_kwargs:
        return
    normalized_engine = _SCHEME_ALIASES.get(engine.lower(), engine.lower())
    allowed = _SUPPORTED_CONNECT_ARGS_BY_DIALECT.get(normalized_engine)
    if allowed is None:
        unsupported = ", ".join(sorted(connect_kwargs))
        msg = f"Unsupported connect_args for {normalized_engine!r}: {unsupported}"
        raise InterfaceError(msg)
    invalid = sorted(set(connect_kwargs) - allowed)
    if invalid:
        unsupported = ", ".join(invalid)
        msg = f"Unsupported connect_args for {normalized_engine!r}: {unsupported}"
        raise InterfaceError(msg)


def _resolve_dbapi_config(
    url: str | BaseDatabaseConfig | None,
    *,
    driver: Any,
    driver_broker: Any,
    connect_kwargs: dict[str, Any],
) -> tuple[str, AnyDatabaseConfig | BaseDatabaseConfig | None]:
    """Resolve URL, validate connect_args, and build typed config inside PEP 249 error translation."""
    if url is None and driver is None:
        exc = ValueError("Unsupported database URL scheme: ''")
        raise InterfaceError(f"Cannot parse Vertebra database URL: {exc}") from exc

    if isinstance(url, BaseDatabaseConfig):
        resolved_url = getattr(url, "dsn", getattr(url, "url", ""))
        if not resolved_url:
            dialect = getattr(url, "dialect", "sqlite")
            path_val = getattr(url, "path", "")
            resolved_url = f"{dialect}:///{path_val}" if path_val else f"{dialect}://"
        if connect_kwargs:
            _validate_connect_kwargs(_config_dialect(url), connect_kwargs)
            try:
                return str(resolved_url), msgspec.structs.replace(url, **connect_kwargs)
            except Exception as exc:
                mapped = _connect_error(exc)
                if mapped is None:
                    raise
                raise mapped from exc
        return str(resolved_url), url

    resolved_url = str(url) if url is not None else str(getattr(driver, "url", getattr(driver, "dsn", "")))
    if not (resolved_url or driver is None):
        if connect_kwargs:
            _validate_connect_kwargs(_config_dialect(driver), connect_kwargs)
        return resolved_url, None

    raw_scheme, sep, rest = resolved_url.partition("://")
    scheme = (raw_scheme if sep else urlparse(resolved_url).scheme).lower()
    base_scheme = scheme
    if "+" in scheme:
        prefix, _, driver_tag = scheme.rpartition("+")
        if driver_tag in {"vtb", "vertebra", "vtb_async", "vertebra_async"}:
            base_scheme = prefix
    known_scheme = (
        SCHEME_REGISTRY.get(scheme)
        or SCHEME_REGISTRY.get(base_scheme)
        or SCHEME_REGISTRY.get(base_scheme.split("+", 1)[0])
    )
    if known_scheme is not None and connect_kwargs:
        engine = _normalize_connect_args_engine(resolved_url)
        _validate_connect_kwargs(engine, connect_kwargs)

    overrides = dict(connect_kwargs)
    if driver_broker is not None:
        overrides["broker"] = driver_broker
    target_url = (
        f"{base_scheme}://{rest}" if sep and known_scheme is not None and base_scheme != scheme else resolved_url
    )
    try:
        parsed_config = DatabaseRegistry.parse_url(target_url, **overrides)
    except ValueError as exc:
        msg = f"Cannot parse Vertebra database URL: {exc}"
        raise InterfaceError(msg) from exc
    except Exception as exc:
        mapped = _connect_error(exc)
        if mapped is None:
            raise
        raise mapped from exc
    return resolved_url, parsed_config


def _build_driver(
    config: AnyDatabaseConfig | BaseDatabaseConfig,
    *,
    broker: Any,
    share_pool: bool,
    lease_managed: bool,
) -> tuple[SyncSQLDriverProtocol, bool]:
    """Construct or acquire a synchronous driver for config inside PEP 249 error translation."""
    try:
        has_shared_key = config.pool_key() is not None
        has_custom_broker = broker is not None and broker is not DatabaseRegistry._DEFAULT_BROKER
        if share_pool and not lease_managed and has_shared_key and not has_custom_broker:
            driver = DatabaseRegistry.default().get_shared_sync_driver(cast("Any", config))
            return cast("SyncSQLDriverProtocol", driver), False
        target_registry = DatabaseRegistry(broker=None if lease_managed else broker)
        build_fn = getattr(config, "build_sync_driver", None)
        if not callable(build_fn):
            msg = f"{type(config).__name__} does not support synchronous DBAPI connections"
            raise InterfaceError(msg)
        driver = build_fn(target_registry)
        return cast("SyncSQLDriverProtocol", driver), True
    except Exception as exc:
        mapped = _connect_error(exc)
        if mapped is None:
            raise
        raise mapped from exc


def _apply_dialect_metadata(module_obj: Any, dialect: str | None) -> None:
    """Attach engine-specific PEP 249 attributes onto a DBAPI module instance."""
    if dialect is None:
        return
    lowered = dialect.lower()
    if lowered == "sqlite":
        module_obj.sqlite_version = sqlite3.sqlite_version
        module_obj.sqlite_version_info = sqlite3.sqlite_version_info
    elif lowered == "oracle":
        oracle_mod = import_optional("vertebra._oracle")
        out_bind_var = getattr(oracle_mod, "OracleOutBindVar", None) if oracle_mod is not None else None
        if out_bind_var is not None:
            module_obj.Var = out_bind_var


_POOL_ROUTED_DIALECTS: Final[frozenset[str]] = frozenset({"postgres"})


class SyncDBAPIConnection:
    """Standalone synchronous PEP 249 connection backed by Vertebra native drivers."""

    __slots__ = (
        "_aggregates",
        "_autocommit",
        "_broker",
        "_collations",
        "_config",
        "_driver",
        "_driver_cursor",
        "_functions",
        "_in_transaction",
        "_owns_driver",
        "_share_pool",
        "_url",
        "closed",
        "cursor",
    )

    def __init__(
        self,
        url: str | BaseDatabaseConfig | None = None,
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        lease_managed: bool = False,
        autocommit: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PEP 249 connection with a non-empty database URL."""
        self._share_pool = bool(share_pool)
        self._broker = (
            broker
            if broker is not None
            else (DatabaseRegistry.default_broker() if self._share_pool and not lease_managed else None)
        )
        self._in_transaction = False
        self.closed: bool = False
        self._functions: dict[str, Any] = {}
        self._aggregates: dict[str, Any] = {}
        self._collations: dict[str, Any] = {}

        driver_broker = None if lease_managed else self._broker
        resolved_url, resolved_config = _resolve_dbapi_config(
            url,
            driver=driver,
            driver_broker=driver_broker,
            connect_kwargs=kwargs,
        )
        self._url: str = resolved_url
        self._config: AnyDatabaseConfig | BaseDatabaseConfig | None = resolved_config
        self._autocommit: bool = bool(getattr(self._config, "requires_statement_autocommit", False))
        self._driver: SyncSQLDriverProtocol | None = driver
        self._owns_driver: bool = driver is not None

        if self._driver is None and self._config is not None:
            self._driver, self._owns_driver = _build_driver(
                self._config,
                broker=broker,
                share_pool=self._share_pool,
                lease_managed=lease_managed,
            )

        self._driver_cursor: Callable[..., Any] | None = None
        if self._driver is not None:
            factory = getattr(self._driver, "cursor", None)
            if not callable(factory):
                msg = f"{type(self._driver).__name__} does not provide cursor()"
                raise InterfaceError(msg)
            self._driver_cursor = factory
        self.cursor: Callable[..., Any] = self._bound_cursor()

        if autocommit is not None:
            self.autocommit = bool(autocommit)

    @property
    def url(self) -> str:
        """Return the connection URL."""
        return self._url

    @property
    def config(self) -> AnyDatabaseConfig | BaseDatabaseConfig | None:
        """Return the parsed database configuration."""
        return self._config

    @property
    def broker(self) -> Any:
        """Return the attached shared-memory database broker, if any."""
        return self._broker

    @property
    def driver(self) -> SyncSQLDriverProtocol | None:
        """Return the active underlying driver instance."""
        return self._driver

    @property
    def autocommit(self) -> bool:
        """Return connection autocommit state."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set autocommit, rejecting transactional DBAPI mode for autocommit-only backends."""
        if not value and getattr(self._config, "requires_statement_autocommit", False):
            config_name = type(self._config).__name__ if self._config is not None else "Backend"
            msg = f"{config_name} DBAPI connections require autocommit; use the native driver transaction API"
            raise NotSupportedError(msg)
        if value and self._transaction_open():
            self.commit()
        self._autocommit = value
        if not self.closed:
            self.cursor = self._bound_cursor()

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | dict[str, Any] = (),
    ) -> Any:
        """Create a cursor and execute a statement."""
        cur = self.cursor()
        cur.execute(operation, parameters)
        return cur

    def create_function(
        self,
        name: str,
        narg: int,
        func: Any,
        *,
        deterministic: bool = False,
    ) -> None:
        """Register a custom scalar function on the connection."""
        self._functions[name] = (narg, func, deterministic)
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_function"):
            raw.create_function(name, narg, func, deterministic=deterministic)

    def create_aggregate(self, name: str, narg: int, aggregate_class: Any) -> None:
        """Register a custom aggregate function on the connection."""
        self._aggregates[name] = (narg, aggregate_class)
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_aggregate"):
            raw.create_aggregate(name, narg, aggregate_class)

    def create_collation(self, name: str, callable: Any) -> None:
        """Register a custom collation on the connection."""
        self._collations[name] = callable
        raw = getattr(self._driver, "_client", None) or getattr(self._driver, "raw_connection", None)
        if raw is not None and hasattr(raw, "create_collation"):
            raw.create_collation(name, callable)

    def begin(self) -> None:
        """Begin a native transaction when implicit commit is disabled."""
        self._require_open()
        if self._autocommit or self._in_transaction:
            return
        if self._driver is not None and hasattr(self._driver, "begin"):
            self._driver.begin()
            self._in_transaction = True

    def commit(self) -> None:
        """Commit the active transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "commit"):
                self._driver.commit()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Roll back the active transaction."""
        self._require_open()
        try:
            if self._driver is not None and hasattr(self._driver, "rollback"):
                self._driver.rollback()
        finally:
            self._in_transaction = False

    def close(self) -> None:
        """Close the underlying driver connection."""
        if not self.closed:
            try:
                if self._transaction_open():
                    if self._autocommit:
                        self.commit()
                    else:
                        self.rollback()
            finally:
                self._in_transaction = False
                if self._owns_driver and self._driver is not None and hasattr(self._driver, "close"):
                    self._driver.close()
                self.closed = True
                self.cursor = self._closed_cursor

    def __enter__(self) -> Self:
        """Enter connection context."""
        self._require_open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit connection context, committing or rolling back transaction."""
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

    def _bound_cursor(self) -> Callable[..., Any]:
        """Return the callable stored in `cursor` for the current autocommit state.

        The driver's `cursor` method is stored unchanged when autocommit is on. PostgreSQL, whose driver
        accepts lazy begin only while a session is pinned, gets `_session_routed_cursor`, which decides on
        each call. Every other engine gets the driver method with `lazy_begin=True` bound.
        """
        factory = self._driver_cursor
        if factory is None:
            return self._no_driver_cursor
        if self._autocommit:
            return factory
        if self._engine_key() in _POOL_ROUTED_DIALECTS:
            return self._session_routed_cursor
        return partial(factory, lazy_begin=True)

    def _session_routed_cursor(self) -> Any:
        """Return a lazy-begin cursor when the driver has a pinned session, else a plain cursor.

        Raises:
            InterfaceError: When the driver cannot provide a cursor at all.
        """
        factory = cast("Callable[..., Any]", self._driver_cursor)
        try:
            return factory(lazy_begin=True)
        except InterfaceError:
            return factory(lazy_begin=False)

    def _engine_key(self) -> str:
        """Return the lowercase engine name from the config, or from the driver when no config resolved."""
        if self._config is not None:
            return _config_dialect(self._config)
        return str(getattr(self._driver, "dialect", "")).lower()

    def _no_driver_cursor(self) -> Any:
        """Raise `InterfaceError` for a connection built without a driver."""
        msg = "SyncDBAPIConnection has no underlying driver cursor factory"
        raise InterfaceError(msg)

    def _closed_cursor(self) -> Any:
        """Raise `InterfaceError` for a cursor request on a closed connection."""
        raise InterfaceError("Cannot operate on a closed connection.")

    def _transaction_open(self) -> bool:
        """Return whether a transaction is open on the underlying driver."""
        if self._driver is not None:
            in_tx = getattr(self._driver, "in_transaction", None)
            if in_tx is not None:
                return bool(in_tx)
        return self._in_transaction

    def _require_open(self) -> None:
        if self.closed:
            raise InterfaceError("Cannot operate on a closed connection.")


class SyncDBAPIModule:
    """Standalone synchronous PEP 249 DBAPI 2.0 module descriptor."""

    apilevel: str = "2.0"
    threadsafety: int = 2
    paramstyle: str = "qmark"

    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    Date = staticmethod(Date)
    Time = staticmethod(Time)
    Timestamp = staticmethod(Timestamp)
    DateFromTicks = staticmethod(DateFromTicks)
    TimeFromTicks = staticmethod(TimeFromTicks)
    TimestampFromTicks = staticmethod(TimestampFromTicks)
    Binary = Binary
    STRING = STRING
    BINARY = BINARY
    BLOB = BLOB
    BFILE = BFILE
    CLOB = CLOB
    NCLOB = NCLOB
    FIXED_CHAR = FIXED_CHAR
    NUMBER = NUMBER
    DATETIME = DATETIME
    ROWID = ROWID

    sqlite_version: str
    sqlite_version_info: tuple[int, ...]

    def __init__(self, *, paramstyle: str = "qmark", dialect: str | None = None) -> None:
        """Initialize PEP 249 DBAPI module with target parameter style and optional dialect."""
        self.paramstyle = paramstyle
        self.dialect = dialect
        _apply_dialect_metadata(self, dialect)

    def connect(self, *args: Any, **kwargs: Any) -> SyncDBAPIConnection:
        """Open a synchronous PEP 249 connection with a required URL."""
        url_arg: str | BaseDatabaseConfig | None = None
        if args and isinstance(args[0], (str, BaseDatabaseConfig)):
            url_arg = args[0]
        elif "url" in kwargs:
            url_arg = kwargs.pop("url")
        if (
            not isinstance(url_arg, BaseDatabaseConfig)
            and (not url_arg or not str(url_arg).strip())
            and "driver" not in kwargs
        ):
            raise InterfaceError("A non-empty database connection URL is required.")
        return SyncDBAPIConnection(url_arg, **kwargs)


def connect(url: str | BaseDatabaseConfig | None = None, **kwargs: Any) -> SyncDBAPIConnection:
    """Open a standalone synchronous PEP 249 DB-API connection."""
    if not isinstance(url, BaseDatabaseConfig) and (not url or not str(url).strip()) and "driver" not in kwargs:
        raise InterfaceError("A non-empty database connection URL is required.")
    return SyncDBAPIConnection(url, **kwargs)


def _config_dialect(config: Any) -> str:
    """Resolve the lowercase dialect identifier exposed by a database configuration object."""
    cls_dialect = getattr(type(config), "dialect", "")
    raw = getattr(config, "dialect", cls_dialect)
    return str(raw).lower()
