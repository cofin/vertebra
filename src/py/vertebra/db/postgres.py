"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Buffer, Callable, Mapping, Sequence
from typing import Any, Literal

import msgspec

from vertebra._typing import UnsupportedBackendError
from vertebra.db.dialer.alloydb import AlloyDbConfig
from vertebra.db.dialer.cloudsql import CloudSqlConfig

__all__ = (
    "AsyncPostgresDriver",
    "PoolerMode",
    "PostgresConfig",
    "SyncPostgresDriver",
)

type PoolerMode = Literal["direct", "transaction", "session", "statement", "auto"]


class PostgresConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """PostgreSQL configuration supporting Cloud SQL, AlloyDB, and PgBouncer/PgCat/pgdog poolers."""

    dsn: str = "postgresql://localhost:5432/postgres"
    direct_dsn: str | None = None
    pooler_mode: PoolerMode = "direct"
    protocol_level_pooler_cache: bool = False
    max_prepared_statements: int | None = None
    min_size: int = 1
    max_size: int = 10
    cloud_sql_instance: str | None = None
    alloydb_instance_uri: str | None = None
    enable_iam_auth: bool = False
    enable_alloydb_columnar_engine: bool = False
    application_name: str = "vertebra"
    statement_timeout_ms: int | None = None
    idle_in_transaction_session_timeout_ms: int | None = None
    session_gucs: Mapping[str, str] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)
        msgspec.structs.force_setattr(self, "session_gucs", dict(self.session_gucs))

    @property
    def effective_statement_cache_size(self) -> int:
        """Return the prepared statement cache capacity safe for the configured pooler mode.

        In transaction-mode or statement-mode PgBouncer/PgCat/pgdog, prepared statements must be disabled (`0`)
        unless protocol-level statement caching (`PgBouncer 1.21+` / `PgCat` / `pgdog`) is enabled.
        """
        if self.pooler_mode in ("transaction", "statement") and not self.protocol_level_pooler_cache:
            return 0
        return 256 if self.max_prepared_statements is None else max(0, self.max_prepared_statements)

    @property
    def effective_direct_dsn(self) -> str:
        """Return the direct primary DSN for LISTEN/NOTIFY, logical replication, and migrations."""
        return self.direct_dsn or self.dsn

    def derive_direct_config(self) -> PostgresConfig:
        """Return a session-mode PostgresConfig targeting ``effective_direct_dsn`` (`sqlspec>=0.68.0`)."""
        if not self.direct_dsn:
            return self
        base = msgspec.structs.asdict(self)
        base["dsn"] = self.direct_dsn
        base["pooler_mode"] = "session"
        base["min_size"] = min(1, self.max_size)
        base["max_size"] = min(2, self.max_size)
        base["session_gucs"] = dict(self.session_gucs)
        return PostgresConfig(**base)

    @staticmethod
    def compute_worker_pool_budget(
        max_connections: int,
        worker_count: int = 1,
        *,
        replica_count: int = 1,
        workers_per_replica: int | None = None,
        reserved_admin_connections: int = 5,
    ) -> int:
        """Compute per-worker pool size from a cluster-wide database connection budget."""
        usable = max(0, max_connections - reserved_admin_connections)
        per_replica = workers_per_replica if workers_per_replica is not None else worker_count
        total_shards = max(1, replica_count) * max(1, per_replica)
        return max(1, usable // total_shards)

    def build_session_gucs(self) -> dict[str, str]:
        """Build the startup/session GUC parameters for the PostgreSQL connection."""
        gucs: dict[str, str] = {"application_name": self.application_name}
        if self.enable_alloydb_columnar_engine or self.alloydb_instance_uri is not None:
            gucs["google_columnar_engine.enabled"] = "on"
            gucs["alloydb.enable_columnar_engine"] = "on"
        if self.statement_timeout_ms is not None:
            gucs["statement_timeout"] = str(self.statement_timeout_ms)
        if self.idle_in_transaction_session_timeout_ms is not None:
            gucs["idle_in_transaction_session_timeout"] = str(self.idle_in_transaction_session_timeout_ms)
        gucs.update(self.session_gucs)
        return gucs

    def to_cloud_dialer_config(self) -> CloudSqlConfig | AlloyDbConfig | None:
        """Return the zero-sidecar CloudSqlConfig or AlloyDbConfig when configured."""
        if self.alloydb_instance_uri:
            return AlloyDbConfig(
                instance_uri=self.alloydb_instance_uri,
                enable_iam_auth=self.enable_iam_auth,
                enable_columnar_engine=self.enable_alloydb_columnar_engine or True,
            )
        if self.cloud_sql_instance:
            return CloudSqlConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None


class SyncPostgresDriver:
    """Synchronous PostgreSQL driver facade supporting plain-tuple and zero-copy JSONB reads."""

    __slots__ = ("_config", "_executor")

    def __init__(
        self,
        config: PostgresConfig | None = None,
        *,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self._config = config or PostgresConfig()
        self._executor = executor

    @property
    def config(self) -> PostgresConfig:
        """Return the active PostgresConfig."""
        return self._config

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
        view = memoryview(payload).cast("B")
        if len(view) == 0:
            msg = "Empty PostgreSQL binary JSONB payload"
            raise ValueError(msg)
        if view[0] != 0x01:
            msg = f"Unsupported PostgreSQL binary JSONB version byte: 0x{view[0]:02x}"
            raise ValueError(msg)
        return view[1:]

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples (optimized for SQLAlchemy 2.1)."""
        if self._executor is None:
            msg = "SyncPostgresDriver.execute_tuples requires the native vertebra-db PostgreSQL driver"
            raise UnsupportedBackendError(msg)
        return [tuple(row) for row in self._executor(sql, parameters)]

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return self.strip_jsonb_header(raw)


class AsyncPostgresDriver:
    """Asynchronous PostgreSQL driver facade supporting plain-tuple and zero-copy JSONB reads."""

    __slots__ = ("_async_executor", "_config", "_sync")

    def __init__(
        self,
        config: PostgresConfig | None = None,
        *,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
        async_executor: Callable[[str, Sequence[Any]], Awaitable[list[tuple[Any, ...]]]] | None = None,
    ) -> None:
        self._config = config or PostgresConfig()
        self._sync = SyncPostgresDriver(self._config, executor=executor)
        self._async_executor = async_executor

    @property
    def config(self) -> PostgresConfig:
        """Return the active PostgresConfig."""
        return self._config

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        if self._async_executor is not None:
            rows = await self._async_executor(sql, parameters)
            return [tuple(row) for row in rows]
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters)

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return SyncPostgresDriver.strip_jsonb_header(raw)
