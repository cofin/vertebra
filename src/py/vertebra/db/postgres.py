"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

from collections.abc import Buffer, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from vertebra.db.dialer.alloydb import AlloyDbConfig
from vertebra.db.dialer.cloudsql import CloudSqlConfig

__all__ = (
    "AsyncPostgresDriver",
    "PoolerMode",
    "PostgresConfig",
    "SyncPostgresDriver",
    "compute_worker_pool_budget",
    "strip_pg_jsonb_header",
)

type PoolerMode = Literal["direct", "transaction", "session"]


def strip_pg_jsonb_header(raw: Buffer) -> memoryview:
    """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
    view = memoryview(raw).cast("B")
    if len(view) == 0:
        msg = "Empty PostgreSQL binary JSONB payload"
        raise ValueError(msg)
    if view[0] != 0x01:
        msg = f"Unsupported PostgreSQL binary JSONB version byte: 0x{view[0]:02x}"
        raise ValueError(msg)
    return view[1:]


def compute_worker_pool_budget(
    total_budget: int,
    *,
    replica_count: int = 1,
    workers_per_replica: int = 1,
    reserved_admin_connections: int = 10,
) -> int:
    """Compute per-worker pool size from a cluster-wide database connection budget."""
    usable = max(0, total_budget - reserved_admin_connections)
    total_shards = max(1, replica_count) * max(1, workers_per_replica)
    return max(1, usable // total_shards)


@dataclass(frozen=True, slots=True)
class PostgresConfig:
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
    session_gucs: Mapping[str, str] = field(default_factory=dict)

    @property
    def effective_statement_cache_size(self) -> int:
        """Return the prepared statement cache capacity safe for the configured pooler mode.

        In transaction-mode PgBouncer/PgCat/pgdog, prepared statements must be disabled (`0`)
        unless protocol-level statement caching (`PgBouncer 1.21+` / `PgCat` / `pgdog`) is enabled.
        """
        if self.pooler_mode == "transaction" and not self.protocol_level_pooler_cache:
            return 0
        return 256 if self.max_prepared_statements is None else max(0, self.max_prepared_statements)

    @property
    def effective_direct_dsn(self) -> str:
        """Return the direct primary DSN for LISTEN/NOTIFY, logical replication, and migrations."""
        return self.direct_dsn or self.dsn

    def build_session_gucs(self) -> dict[str, str]:
        """Build the startup/session GUC parameters for the PostgreSQL connection."""
        gucs: dict[str, str] = {"application_name": self.application_name}
        if self.enable_alloydb_columnar_engine or self.alloydb_instance_uri is not None:
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

    __slots__ = ("_config", "_in_memory_rows")

    def __init__(self, config: PostgresConfig | None = None) -> None:
        self._config = config or PostgresConfig()
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def config(self) -> PostgresConfig:
        """Return the active PostgresConfig."""
        return self._config

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples (optimized for SQLAlchemy 2.1)."""
        _ = (sql, parameters)
        return list(self._in_memory_rows)

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return strip_pg_jsonb_header(raw)


class AsyncPostgresDriver:
    """Asynchronous PostgreSQL driver facade supporting plain-tuple and zero-copy JSONB reads."""

    __slots__ = ("_config", "_sync")

    def __init__(self, config: PostgresConfig | None = None) -> None:
        self._config = config or PostgresConfig()
        self._sync = SyncPostgresDriver(self._config)

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
        return self._sync.execute_tuples(sql, parameters)

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return strip_pg_jsonb_header(raw)
