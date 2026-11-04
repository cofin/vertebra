"""Shared Vertebra DatabaseWrapper mixin for Django database backends."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

import msgspec

from vertebra._vertebra import AlloyDBDialer, CloudSQLDialer, LeaseBroker, LeaseGrant
from vertebra.db.adapters.bigquery import BigQueryConfig
from vertebra.db.adapters.duckdb import DuckDBConfig
from vertebra.db.adapters.mssql import MSSQLConfig
from vertebra.db.adapters.mysql import MySQLConfig
from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.adapters.postgres import PoolerMode, PostgreSQLConfig
from vertebra.db.adapters.spanner import SpannerConfig
from vertebra.db.adapters.sqlite import SQLiteConfig
from vertebra.db.registry import AnyDatabaseConfig, DatabaseRegistry
from vertebra.extensions.django.orm import rows_to_structs
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

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


class DatabaseWrapperMixin:
    """Mixin enhancing Django ``BaseDatabaseWrapper`` with SHM ``LeaseBroker``, Cloud Dialers, and PgBouncer."""

    vendor: str = "generic"
    display_name: str = "Generic (Vertebra)"
    driver_scheme: str = "generic"

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        self.settings_dict = dict(settings_dict)
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

    def _build_native_config(self) -> AnyDatabaseConfig:
        host = str(self.settings_dict.get("HOST") or "localhost")
        port = int(self.settings_dict.get("PORT") or self._default_port())
        name = str(self.settings_dict.get("NAME") or "")
        user = str(self.settings_dict.get("USER") or "")
        password = str(self.settings_dict.get("PASSWORD") or "")
        auth = f"{user}:{password}@" if (user and password) else (f"{user}@" if user else "")
        if self.vendor == "postgresql":
            db_name = name or "postgres"
            dsn = f"postgresql://{auth}{host}:{port}/{db_name}"
            return PostgreSQLConfig(
                dsn=dsn,
                direct_dsn=self.pooler_config.direct_dsn,
                pooler_mode=cast("PoolerMode", self.pooler_config.pooler_mode),
                pgbouncer=self.pooler_config.pgbouncer,
                cloud_sql_instance=self.pooler_config.cloud_sql_instance,
                alloydb_instance_uri=self.pooler_config.alloydb_instance_uri,
                broker=self._broker,
            )
        if self.vendor == "mysql":
            dsn = f"mysql://{auth}{host}:{port}/{name}"
            return MySQLConfig(
                dsn=dsn,
                cloud_sql_instance=self.pooler_config.cloud_sql_instance,
                broker=self._broker,
            )
        if self.vendor in {"sqlite", "sqlite3"}:
            db_path = name or ":memory:"
            return SQLiteConfig(path=db_path, database=db_path, broker=self._broker)
        if self.vendor == "oracle":
            svc = name or "FREEPDB1"
            dsn = f"oracle://{auth}{host}:{port}/{svc}"
            return OracleConfig(
                dsn=dsn,
                host=host,
                port=port,
                service_name=svc,
                user=user or "system",
                password=password,
                wallet_zip=self.pooler_config.wallet_zip,
                broker=self._broker,
            )
        if self.vendor == "mssql":
            db_name = name or "master"
            dsn = f"mssql://{auth}{host}:{port}/{db_name}"
            return MSSQLConfig(
                dsn=dsn,
                host=host,
                port=port,
                database=db_name,
                user=user or "sa",
                password=password,
                cloudsql_instance=self.pooler_config.cloud_sql_instance,
                broker=self._broker,
            )
        if self.vendor == "duckdb":
            duck_path = name or ":memory:"
            return DuckDBConfig(path=duck_path, database=duck_path, broker=self._broker)
        if self.vendor == "bigquery":
            options = dict(self.settings_dict.get("OPTIONS") or {})
            dsn = str(options.get("dsn") or "")
            if dsn:
                return BigQueryConfig.from_dsn(dsn)
            project_id = str(options.get("project_id") or self.settings_dict.get("PROJECT") or name or "test-project")
            dataset_id = (
                options.get("dataset_id")
                or self.settings_dict.get("DATASET")
                or (name if self.settings_dict.get("PROJECT") and name else None)
            )
            location = str(options.get("location") or self.settings_dict.get("LOCATION") or "US")
            max_billed_raw = options.get("maximum_bytes_billed") or self.settings_dict.get("MAXIMUM_BYTES_BILLED")
            max_billed = int(max_billed_raw) if max_billed_raw is not None else None
            rest_endpoint = str(
                options.get("rest_endpoint")
                or (
                    f"http://{host}:{port}"
                    if host not in {"localhost", ""} and port
                    else "https://bigquery.googleapis.com"
                )
            )
            storage_endpoint = str(options.get("storage_endpoint") or "https://bigquerystorage.googleapis.com")
            return BigQueryConfig(
                project_id=project_id,
                dataset_id=str(dataset_id) if dataset_id is not None else None,
                location=location,
                rest_endpoint=rest_endpoint,
                storage_endpoint=storage_endpoint,
                maximum_bytes_billed=max_billed,
            )
        if self.vendor == "spanner":
            options = dict(self.settings_dict.get("OPTIONS") or {})
            dsn = str(options.get("dsn") or "")
            if dsn:
                return SpannerConfig.from_dsn(dsn)
            if name.startswith("projects/"):
                return SpannerConfig.from_dsn(f"spanner://{name}")
            project_id = str(options.get("project_id") or self.settings_dict.get("PROJECT") or "test-project")
            instance_id = str(options.get("instance_id") or self.settings_dict.get("INSTANCE") or "test-instance")
            db_name = name or str(options.get("database_id") or "test-db")
            dialect_raw = str(options.get("dialect") or self.settings_dict.get("DIALECT") or "googlesql").lower()
            database_role = options.get("database_role")
            endpoint = str(
                options.get("endpoint")
                or (
                    f"http://{host}:{port}"
                    if host not in {"localhost", ""} and port
                    else "https://spanner.googleapis.com"
                )
            )
            return SpannerConfig(
                project_id=project_id,
                instance_id=instance_id,
                database_id=db_name,
                dialect="spangres" if dialect_raw in {"spangres", "postgresql", "pg"} else "googlesql",
                database_role=str(database_role) if database_role is not None else None,
                endpoint=endpoint,
            )
        db_path = name or ":memory:"
        return SQLiteConfig(path=db_path, database=db_path, broker=self._broker)

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

    @property
    def disables_server_side_cursors(self) -> bool:
        """Return True when transaction pooling is enabled so Django disables server-side cursors."""
        return self.pooler_config.pgbouncer or self.pooler_config.pooler_mode == "transaction"

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

    @contextmanager
    def schema_editor(self, *args: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
        """Switch to direct session-mode settings when running DDL / migrations."""
        _ = (args, kwargs)
        direct_settings = self.derive_direct_settings()
        yield {
            "pooler_mode": "session",
            "settings": direct_settings,
            **direct_settings,
        }

    def select_to_arrow(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        rows: Sequence[dict[str, Any]] | None = None,
    ) -> Any:
        """Execute ``sql`` and return a ``pyarrow.Table``."""
        _ = (sql, params)
        pa = import_optional("pyarrow")
        if pa is None:
            msg = "pyarrow is required for select_to_arrow()"
            raise RuntimeError(msg)
        return pa.Table.from_pylist(list(rows or ()))

    def select_to_schema[T](
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        schema_type: type[T],
        rows: Sequence[dict[str, Any] | Sequence[Any]] | None = None,
    ) -> list[T]:
        """Execute ``sql`` and hydrate rows directly into ``list[schema_type]``."""
        _ = (sql, params)
        return rows_to_structs(list(rows or ()), schema_type)

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
