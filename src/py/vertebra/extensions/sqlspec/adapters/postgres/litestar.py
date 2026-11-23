"""PostgreSQL Litestar session store backed by Vertebra `PostgresSyncConfig` / `PostgresAsyncConfig`."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.postgres.core import (
    is_pg_table_missing,
    pg_ensure_utc,
    pg_index_name,
    pg_row_value,
    pg_table_ref,
    postgres_storage_params_sql,
    validate_pg_table_name,
)

__all__ = ("PostgresLitestarStore",)


class PostgresLitestarStore(AdapterLitestarStore):
    """Litestar session store persisting BYTEA sessions in PostgreSQL via `provide_session()`."""

    __slots__ = ()

    _use_threadpool_for_sync: ClassVar[bool] = False
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"autovacuum_analyze_scale_factor", "autovacuum_vacuum_scale_factor", "fillfactor"}
    )

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        validate_pg_table_name(table_name)

    def _split_table_name(self) -> tuple[str | None, str]:
        parts = validate_pg_table_name(self._table_name)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])

    def _companion_index(self) -> CompanionIndex:
        schema_name, bare_table = self._split_table_name()
        table_ref = pg_table_ref(self._table_name)
        idx_name = pg_index_name("idx", self._table_name, "expires_at")
        return CompanionIndex(
            name=idx_name,
            table=bare_table,
            schema=schema_name,
            ddl=f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_ref} (expires_at) WHERE expires_at IS NOT NULL;",
        )

    def _table_ddl(self) -> str:
        table_ref = pg_table_ref(self._table_name)
        ext = getattr(self._config, "extension_config", None) or {}
        litestar_cfg = ext.get("litestar", {}) if isinstance(ext, dict) else {}
        tuning = postgres_storage_params_sql(
            litestar_cfg,
            default_fillfactor=80,
            default_vacuum_scale=0.05,
            default_analyze_scale=0.02,
            include_autovacuum=True,
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "session_id TEXT PRIMARY KEY, "
            "data BYTEA NOT NULL, "
            "expires_at TIMESTAMPTZ, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP"
            f"){tuning};\n"
            f"{self._companion_index().ddl}"
        )

    def _drop_table_sql(self) -> list[str]:
        table_ref = pg_table_ref(self._table_name)
        idx_name = pg_index_name("idx", self._table_name, "expires_at")
        return [f"DROP INDEX IF EXISTS {idx_name};", f"DROP TABLE IF EXISTS {table_ref};"]

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the PostgreSQL session store."""
        schema_name, bare_table = self._split_table_name()
        return [SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="postgres")]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the partial PostgreSQL session expiration companion index."""
        return [self._companion_index()]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion partial index on a synchronous driver without committing."""
        self._run_companion_reconcile_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion partial index on an asynchronous driver without committing."""
        await self._run_companion_reconcile_async(driver)

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_pg_table_missing(exc)

    def _row_value(self, row: Any, column: str, index: int) -> Any:
        return pg_row_value(row, column, index)

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else pg_ensure_utc(raw)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        return sql, (key,)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._table_name)
        return (
            f"UPDATE {table_ref} SET expires_at = $1, updated_at = CURRENT_TIMESTAMP WHERE session_id = $2",
            (encoded_expires_at, key),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"INSERT INTO {table_ref} (session_id, data, expires_at) VALUES ($1, $2, $3) "
            "ON CONFLICT (session_id) DO UPDATE SET "
            "data = EXCLUDED.data, expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP"
        )
        return sql, (key, data, encoded_expires_at)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {pg_table_ref(self._table_name)} WHERE session_id = $1", (key,)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {pg_table_ref(self._table_name)}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = pg_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = $1 AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        return sql, (key,)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT expires_at FROM {pg_table_ref(self._table_name)} WHERE session_id = $1", (key,)

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        table_ref = pg_table_ref(self._table_name)
        return f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP", ()
