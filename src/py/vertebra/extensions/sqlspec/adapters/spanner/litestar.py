"""Cloud Spanner hash-sharded Litestar session store supporting GoogleSQL and Spangres TTL."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Literal

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import CompanionIndex
from sqlspec.migrations.schema import SchemaTarget

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    farm_fingerprint_shard,
    is_spanner_table_missing,
    normalize_spanner_dialect,
    parse_utc_timestamp,
    validate_spanner_identifier,
)

__all__ = (
    "SpannerLitestarConfig",
    "SpannerLitestarStore",
)


class SpannerLitestarConfig(LitestarConfig):
    """Typed extension configuration keys supported by ``SpannerLitestarStore``."""


class SpannerLitestarStore(AdapterLitestarStore):
    """Cloud Spanner Litestar session store with ``FARM_FINGERPRINT`` sharding and ``expires_at`` TTL."""

    __slots__ = ("_shard_count",)

    adapter_name = "vertebra_spanner"
    _commit_after_write: ClassVar[bool] = False
    _create_uses_script: ClassVar[bool] = False
    _check_create_schema_enabled: ClassVar[bool] = False
    _check_expiry_in_python: ClassVar[bool] = True
    _separate_write_session_on_get: ClassVar[bool] = True
    _log_writes: ClassVar[bool] = False

    def __init__(self, config: Any) -> None:
        super().__init__(config)
        self._shard_count = 16

    def _spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        return normalize_spanner_dialect(
            getattr(self._config, "spanner_dialect", None) or getattr(self._config, "dialect", "googlesql")
        )

    def _validated_table(self) -> str:
        return validate_spanner_identifier(self.table_name, "session_table")

    def _split_table_name(self) -> tuple[str | None, str]:
        table = self._validated_table()
        schema_name, dot, bare_table = table.partition(".")
        return (schema_name, bare_table) if dot else (None, table)

    def _companion_index(self) -> CompanionIndex:
        schema_name, bare_table = self._split_table_name()
        table = self._validated_table()
        idx_name = validate_spanner_identifier(f"idx_{table.replace('.', '_')}_expires_at", "session_index")
        if self._spanner_dialect() == "spangres":
            ddl = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}(expires_at) WHERE expires_at IS NOT NULL"
        else:
            ddl = f"CREATE NULL_FILTERED INDEX IF NOT EXISTS {idx_name} ON {table}(expires_at)"
        return CompanionIndex(name=idx_name, table=bare_table, schema=schema_name, ddl=ddl)

    def compute_shard_id(self, key: str) -> int:
        """Compute the deterministic hash shard prefix for session ``key``."""
        return farm_fingerprint_shard(key, self._shard_count)

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered ``SchemaTarget`` definitions for the Cloud Spanner session store."""
        schema_name, bare_table = self._split_table_name()
        parse_dialect = "postgres" if self._spanner_dialect() == "spangres" else "spanner"
        return [SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect=parse_dialect)]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the Spanner session expiration companion index definition."""
        return [self._companion_index()]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion indexes on a synchronous driver without committing."""
        self._run_companion_reconcile_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion indexes on an asynchronous driver without committing."""
        await self._run_companion_reconcile_async(driver)

    def create_statements(self) -> list[str]:
        """Return idempotent ``CREATE TABLE IF NOT EXISTS`` and companion index DDL statements."""
        return [self._table_ddl(), self._companion_index().ddl]

    def drop_statements(self) -> list[str]:
        """Return idempotent ``DROP INDEX IF EXISTS`` and ``DROP TABLE IF EXISTS`` DDL statements."""
        return self._drop_table_sql()

    def _create_table_statements(self) -> list[str]:
        return self.create_statements()

    def _ddl_session_kwargs(self) -> dict[str, Any]:
        return {"transaction": False}

    def _read_session_kwargs(self) -> dict[str, Any]:
        return {"transaction": False, "read_only": True}

    def _write_session_kwargs(self) -> dict[str, Any]:
        return {"transaction": True}

    def _table_ddl(self) -> str:
        table = self._validated_table()
        if self._spanner_dialect() == "spangres":
            return (
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "shard_id BIGINT NOT NULL, "
                "session_id VARCHAR(256) NOT NULL, "
                "data BYTEA NOT NULL, "
                "expires_at TIMESTAMPTZ, "
                "PRIMARY KEY (shard_id, session_id)"
                ") TTL INTERVAL '0 days' ON expires_at"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"shard_id INT64 NOT NULL DEFAULT (MOD(ABS(FARM_FINGERPRINT(session_id)), {self._shard_count})), "
            "session_id STRING(256) NOT NULL, "
            "data BYTES(MAX) NOT NULL, "
            "expires_at TIMESTAMP"
            ") PRIMARY KEY (shard_id, session_id), "
            "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))"
        )

    def _drop_table_sql(self) -> list[str]:
        table = self._validated_table()
        idx = self._companion_index()
        qualified_index = f"{idx.schema}.{idx.name}" if idx.schema else idx.name
        return [f"DROP INDEX IF EXISTS {qualified_index}", f"DROP TABLE IF EXISTS {table}"]

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_spanner_table_missing(exc)

    def _encode_expires_at(self, expires_at: datetime | None) -> str | None:
        return None if expires_at is None else expires_at.isoformat()

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else parse_utc_timestamp(raw)

    def _point_params(self, key: str) -> dict[str, Any]:
        return {"shard_id": self.compute_shard_id(key), "session_id": key}

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        return (
            f"SELECT data, expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1",
            (self._point_params(key),),
        )

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        params = {**self._point_params(key), "expires_at": encoded_expires_at}
        return (
            f"UPDATE {table} SET expires_at = @expires_at WHERE shard_id = @shard_id AND session_id = @session_id",
            (params,),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        if self._spanner_dialect() == "spangres":
            sql = (
                f"INSERT INTO {table} (shard_id, session_id, data, expires_at) "
                "VALUES (@shard_id, @session_id, @data, @expires_at) "
                "ON CONFLICT (shard_id, session_id) DO UPDATE "
                "SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at"
            )
        else:
            sql = (
                f"MERGE INTO {table} AS target "
                "USING (SELECT @shard_id AS shard_id, @session_id AS session_id, "
                "@data AS data, @expires_at AS expires_at) AS source "
                "ON target.shard_id = source.shard_id AND target.session_id = source.session_id "
                "WHEN MATCHED THEN UPDATE SET data = source.data, expires_at = source.expires_at "
                "WHEN NOT MATCHED THEN INSERT (shard_id, session_id, data, expires_at) "
                "VALUES (source.shard_id, source.session_id, source.data, source.expires_at)"
            )
        params = {**self._point_params(key), "data": data, "expires_at": encoded_expires_at}
        return sql, (params,)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        return (
            f"DELETE FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id",
            (self._point_params(key),),
        )

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._validated_table()} WHERE TRUE", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        return (
            f"SELECT expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1",
            (self._point_params(key),),
        )

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        return (
            f"SELECT expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1",
            (self._point_params(key),),
        )

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        table = self._validated_table()
        return (
            f"DELETE FROM {table} WHERE expires_at IS NOT NULL AND expires_at <= @now",
            ({"now": now.isoformat()},),
        )
