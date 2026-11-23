"""Oracle Litestar session store backed by `OracleSyncConfig` / `OracleAsyncConfig`."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.oracle.core import (
    escape_oracle_literal,
    is_oracle_table_missing,
    oracle_ensure_utc,
    oracle_index_name,
    oracle_row_value,
    oracle_table_feature_clause,
    oracle_table_ref,
    validate_oracle_table_name,
)

__all__ = ("OracleLitestarStore",)


class OracleLitestarStore(AdapterLitestarStore):
    """Litestar session store persisting BLOB sessions in Oracle via `MERGE INTO` and `provide_session()`."""

    __slots__ = ()

    _use_threadpool_for_sync: ClassVar[bool] = False
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"compression", "in_memory", "table_options"}
    )

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        validate_oracle_table_name(table_name)

    def _split_table_name(self) -> tuple[str | None, str]:
        parts = validate_oracle_table_name(self._table_name)
        return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])

    def _companion_index(self) -> CompanionIndex:
        schema_name, bare_table = self._split_table_name()
        table_ref = oracle_table_ref(self._table_name)
        idx_name = oracle_index_name("idx", self._table_name, "exp")
        create_idx_sql = escape_oracle_literal(f"CREATE INDEX {idx_name} ON {table_ref} (expires_at)")
        ddl = (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE '{create_idx_sql}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN IF SQLCODE NOT IN (-955, -1408) THEN RAISE; END IF;\n"
            "END;"
        )
        return CompanionIndex(name=idx_name, table=bare_table, schema=schema_name, ddl=ddl)

    def _table_ddl(self) -> str:
        table_ref = oracle_table_ref(self._table_name)
        ext = getattr(self._config, "extension_config", None) or {}
        litestar_cfg = ext.get("litestar", {}) if isinstance(ext, dict) else {}
        feature_clause = oracle_table_feature_clause(litestar_cfg)
        create_table_sql = escape_oracle_literal(
            f"CREATE TABLE {table_ref} ("
            "session_id VARCHAR2(128) PRIMARY KEY, "
            "data BLOB NOT NULL, "
            "expires_at TIMESTAMP WITH TIME ZONE, "
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL"
            f"){feature_clause}"
        )
        return (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE '{create_table_sql}';\n"
            "EXCEPTION\n"
            "    WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;\n"
            "END;\n"
            f"{self._companion_index().ddl}"
        )

    def _drop_table_sql(self) -> list[str]:
        table_ref = escape_oracle_literal(oracle_table_ref(self._table_name))
        idx_name = escape_oracle_literal(oracle_index_name("idx", self._table_name, "exp"))
        return [
            (
                "BEGIN\n"
                f"    EXECUTE IMMEDIATE 'DROP INDEX {idx_name}';\n"
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1418 THEN RAISE; END IF;\n"
                "END;"
            ),
            (
                "BEGIN\n"
                f"    EXECUTE IMMEDIATE 'DROP TABLE {table_ref}';\n"
                "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
                "END;"
            ),
        ]

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the Oracle session store."""
        schema_name, bare_table = self._split_table_name()
        return [SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="oracle")]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the Oracle session expiration companion index definition."""
        return [self._companion_index()]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion index on a synchronous driver without committing."""
        self._run_companion_reconcile_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion index on an asynchronous driver without committing."""
        await self._run_companion_reconcile_async(driver)

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_oracle_table_missing(exc)

    def _row_value(self, row: Any, column: str, index: int) -> Any:
        return oracle_row_value(row, column, index)

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else oracle_ensure_utc(raw)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT data, expires_at FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        return sql, ({"session_id": key},)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = oracle_table_ref(self._table_name)
        return (
            f"UPDATE {table_ref} SET expires_at = :expires_at, updated_at = SYSTIMESTAMP WHERE session_id = :session_id",
            ({"expires_at": encoded_expires_at, "session_id": key},),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"MERGE INTO {table_ref} target "
            "USING (SELECT :session_id AS session_id, :data AS data, :expires_at AS expires_at FROM DUAL) src "
            "ON (target.session_id = src.session_id) "
            "WHEN MATCHED THEN UPDATE SET target.data = src.data, target.expires_at = src.expires_at, "
            "target.updated_at = SYSTIMESTAMP "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at, created_at, updated_at) "
            "VALUES (src.session_id, src.data, src.expires_at, SYSTIMESTAMP, SYSTIMESTAMP)"
        )
        return sql, ({"session_id": key, "data": data, "expires_at": encoded_expires_at},)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {oracle_table_ref(self._table_name)} WHERE session_id = :session_id", (
            {"session_id": key},
        )

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {oracle_table_ref(self._table_name)}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        table_ref = oracle_table_ref(self._table_name)
        sql = (
            f"SELECT 1 FROM {table_ref} "
            "WHERE session_id = :session_id AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)"
        )
        return sql, ({"session_id": key},)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT expires_at FROM {oracle_table_ref(self._table_name)} WHERE session_id = :session_id", (
            {"session_id": key},
        )

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        table_ref = oracle_table_ref(self._table_name)
        return f"DELETE FROM {table_ref} WHERE expires_at IS NOT NULL AND expires_at <= SYSTIMESTAMP", ()
