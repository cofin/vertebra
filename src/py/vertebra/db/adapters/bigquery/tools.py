"""Adapter-owned BigQuery operations and execution policies for tool consumers."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from vertebra.db.adapters.bigquery.config import BigQueryConfig, ObjectTableConfig
from vertebra.db.adapters.bigquery.driver import AsyncBigQueryDriver, SyncBigQueryDriver
from vertebra.db.driver import AsyncDatabaseDriver
from vertebra.db.registry import DatabaseRegistry, provide_driver
from vertebra.typing import ToolPolicyError, ToolProvider
from vertebra.utils.text import validate_identifier

__all__ = ("BigQueryToolProvider",)

_MUTATING_VERBS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "EXECUTE",
        "CALL",
        "GRANT",
        "REVOKE",
        "EXPORT",
        "LOAD",
    }
)
_LEADING_COMMENT_RE = re.compile(r"^\s*(?:--[^\n]*(?:\n|$)\s*|/\*.*?\*/\s*)+", re.DOTALL)


class BigQueryToolProvider(ToolProvider):
    """Expose typed native BigQuery operations with lazy driver resolution."""

    def __init__(
        self,
        driver: AsyncBigQueryDriver | SyncBigQueryDriver | None = None,
        *,
        config: BigQueryConfig | str | None = None,
        registry: DatabaseRegistry | None = None,
        maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024,
        enforce_dry_run_limit: bool = True,
    ) -> None:
        self._driver = driver
        self._config = config
        self._registry = registry
        self._maximum_bytes_billed = maximum_bytes_billed
        self._enforce_dry_run_limit = enforce_dry_run_limit

    def operations(self) -> Mapping[str, Callable[..., Awaitable[Any]]]:
        """Return original bound operations without creating a driver or connection."""
        return {
            "bigquery_dry_run": self.bigquery_dry_run,
            "bigquery_query": self.bigquery_query,
            "bigquery_object_table_inspect": self.bigquery_object_table_inspect,
        }

    def confirmation_reason(self, operation_name: str, arguments: Mapping[str, Any]) -> str | None:
        """Require approval for mutations and ambiguous executable SQL without doing I/O."""
        if operation_name == "bigquery_query":
            self._effective_cap(arguments.get("max_bytes_billed"))
        if operation_name in {"bigquery_query", "bigquery_dry_run"}:
            sql = arguments.get("sql")
            if not isinstance(sql, str) or not sql.strip():
                return None
            verb = self._leading_verb(sql)
            if operation_name == "bigquery_query" and verb and verb != "SELECT":
                return f"{verb} statement requires human confirmation."
        return None

    async def bigquery_dry_run(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Estimate bytes processed and validate a BigQuery GoogleSQL query without executing it."""
        drv = self._resolve_driver()
        res = (
            await drv.dry_run(sql, params)
            if isinstance(drv, AsyncDatabaseDriver)
            else await asyncio.to_thread(drv.dry_run, sql, params)
        )
        total_processed = int(getattr(res, "total_bytes_processed", 0))
        total_billed = int(getattr(res, "total_bytes_billed", total_processed))
        return {
            "total_bytes_processed": total_processed,
            "total_bytes_billed": total_billed,
            "cache_hit": bool(getattr(res, "cache_hit", False)),
            "statement_type": str(getattr(res, "statement_type", self._leading_verb(sql) or "SELECT")),
            "referenced_tables": list(getattr(res, "referenced_tables", ())),
            "schema_fields": list(getattr(res, "schema", getattr(res, "schema_fields", ()))),
            "maximum_bytes_billed": self._maximum_bytes_billed,
            "within_budget": total_processed <= self._maximum_bytes_billed,
        }

    async def bigquery_query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        max_bytes_billed: int | None = None,
    ) -> dict[str, Any]:
        """Execute a governed BigQuery query enforcing `maximum_bytes_billed` and DML/DDL confirmation."""
        drv = self._resolve_driver()
        effective_ceiling = self._effective_cap(max_bytes_billed, drv.config.maximum_bytes_billed)

        if self._leading_verb(sql) in _MUTATING_VERBS:
            affected = (
                await drv.execute(sql, params, maximum_bytes_billed=effective_ceiling)
                if isinstance(drv, AsyncDatabaseDriver)
                else await asyncio.to_thread(drv.execute, sql, params, maximum_bytes_billed=effective_ceiling)
            )
            return {
                "mode": "dml",
                "affected_rows": int(affected),
                "maximum_bytes_billed": effective_ceiling,
            }

        estimated_bytes = 0
        if self._enforce_dry_run_limit:
            dry_res = (
                await drv.dry_run(sql, params)
                if isinstance(drv, AsyncDatabaseDriver)
                else await asyncio.to_thread(drv.dry_run, sql, params)
            )
            estimated_bytes = int(getattr(dry_res, "total_bytes_processed", 0))
            if estimated_bytes > effective_ceiling:
                msg = (
                    f"BigQuery dry_run estimated {estimated_bytes} bytes exceeds "
                    f"maximum_bytes_billed ceiling of {effective_ceiling} bytes"
                )
                raise ToolPolicyError(msg)

        rows = (
            await drv.select(sql, params, maximum_bytes_billed=effective_ceiling)
            if isinstance(drv, AsyncDatabaseDriver)
            else await asyncio.to_thread(drv.select, sql, params, maximum_bytes_billed=effective_ceiling)
        )
        return {
            "mode": "query",
            "rows": list(rows),
            "row_count": len(rows),
            "estimated_bytes_processed": estimated_bytes,
            "maximum_bytes_billed": effective_ceiling,
        }

    async def bigquery_object_table_inspect(
        self,
        table_name: str,
        connection_id: str = "us.gcs-conn",
        object_uri_prefix: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Inspect a BigQuery Object Table over GCS and return object metadata rows and DDL.

        Raises:
            ValueError: When table_name is not a dotted identifier.
        """
        validate_identifier(table_name, "table_name", allow_hyphens=True)
        drv = self._resolve_driver()
        bounded_limit = max(1, min(int(limit), 1000))
        uri_pattern = object_uri_prefix or f"gs://{table_name.replace('.', '/')}/*"
        obj_cfg = ObjectTableConfig(
            table_name=table_name,
            connection_id=connection_id,
            uris=(uri_pattern,),
        )
        inspect_sql = f"SELECT uri, content_type, size, md5_hash, updated FROM {table_name} LIMIT {bounded_limit}"
        rows = (
            await drv.select(inspect_sql)
            if isinstance(drv, AsyncDatabaseDriver)
            else await asyncio.to_thread(drv.select, inspect_sql)
        )
        return {
            "table_name": table_name,
            "connection_id": connection_id,
            "uris": list(obj_cfg.uris),
            "ddl": obj_cfg.create_object_table_sql(),
            "rows": list(rows),
            "row_count": len(rows),
        }

    def _resolve_driver(self) -> AsyncBigQueryDriver | SyncBigQueryDriver:
        if self._driver is not None:
            return self._driver
        return cast(
            "AsyncBigQueryDriver | SyncBigQueryDriver",
            provide_driver(self._config or "default", registry=self._registry),
        )

    def _effective_cap(self, caller_cap: int | None, configured_cap: int | None = None) -> int:
        caps = [self._maximum_bytes_billed]
        if caller_cap is not None:
            caps.append(caller_cap)
        if configured_cap is not None:
            caps.append(configured_cap)
        effective = min(caps)
        if effective <= 0:
            raise ToolPolicyError("maximum_bytes_billed must be a positive integer")
        return effective

    @staticmethod
    def _leading_verb(sql: str) -> str:
        body = sql.rstrip().rstrip(";").rstrip()
        if ";" in body:
            raise ToolPolicyError(
                "Multi-statement SQL is not allowed; submit one statement per call and pass values "
                "containing ';' as query parameters"
            )
        cleaned = _LEADING_COMMENT_RE.sub("", body).lstrip(" (;\t\r\n")
        return cleaned.split(None, 1)[0].rstrip(";(,").upper() if cleaned else ""
