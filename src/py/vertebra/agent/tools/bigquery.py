"""Governed BigQuery agent tools backed by the engine adapter."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from vertebra.agent.core import PermissionDeniedError, Tool
from vertebra.agent.runtime import ConfirmationGate
from vertebra.agent.tools import GCPDatabaseToolPolicy, _extract_leading_verb, _require_confirmation, is_mutating_sql
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, ObjectTableConfig, SyncBigQueryDriver
from vertebra.db.driver import AsyncDatabaseDriver
from vertebra.db.registry import provide_driver
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.registry import DatabaseRegistry

__all__ = ("build_bigquery_tools",)


def build_bigquery_tools(
    *,
    driver: AsyncBigQueryDriver | SyncBigQueryDriver | None = None,
    config: BigQueryConfig | str | None = None,
    policy: GCPDatabaseToolPolicy | None = None,
    confirmation_gate: ConfirmationGate | None = None,
    registry: DatabaseRegistry | None = None,
) -> dict[str, Tool]:
    """Build BigQuery tools resolving a configured driver only when a tool is invoked."""
    policy = policy if policy is not None else GCPDatabaseToolPolicy()
    gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()

    def _resolve_driver() -> AsyncBigQueryDriver | SyncBigQueryDriver:
        if driver is not None:
            return driver
        return cast("AsyncBigQueryDriver | SyncBigQueryDriver", provide_driver(config or "default", registry=registry))

    async def bigquery_dry_run(
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Estimate bytes processed and validate a BigQuery GoogleSQL query without executing it."""
        drv = _resolve_driver()
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
            "statement_type": str(getattr(res, "statement_type", _extract_leading_verb(sql) or "SELECT")),
            "referenced_tables": list(getattr(res, "referenced_tables", ())),
            "schema_fields": list(getattr(res, "schema", getattr(res, "schema_fields", ()))),
            "maximum_bytes_billed": policy.maximum_bytes_billed,
            "within_budget": total_processed <= policy.maximum_bytes_billed,
        }

    async def bigquery_query(
        sql: str,
        params: dict[str, Any] | None = None,
        max_bytes_billed: int | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """Execute a governed BigQuery query enforcing `maximum_bytes_billed` and DML/DDL confirmation."""
        drv = _resolve_driver()
        effective_ceiling = (
            min(int(max_bytes_billed), policy.maximum_bytes_billed)
            if max_bytes_billed is not None
            else policy.maximum_bytes_billed
        )
        if effective_ceiling <= 0:
            msg = "maximum_bytes_billed must be a positive integer"
            raise PermissionDeniedError(msg)

        if is_mutating_sql(sql):
            _require_confirmation(gate, "bigquery_query", sql, confirmation_token)
            affected = (
                await drv.execute(sql, params)
                if isinstance(drv, AsyncDatabaseDriver)
                else await asyncio.to_thread(drv.execute, sql, params)
            )
            return {
                "mode": "dml",
                "affected_rows": int(affected),
                "maximum_bytes_billed": effective_ceiling,
            }

        estimated_bytes = 0
        if policy.enforce_dry_run_limit:
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
                raise PermissionDeniedError(msg)

        rows = (
            await drv.select(sql, params)
            if isinstance(drv, AsyncDatabaseDriver)
            else await asyncio.to_thread(drv.select, sql, params)
        )
        return {
            "mode": "query",
            "rows": list(rows),
            "row_count": len(rows),
            "estimated_bytes_processed": estimated_bytes,
            "maximum_bytes_billed": effective_ceiling,
        }

    async def bigquery_object_table_inspect(
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
        drv = _resolve_driver()
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

    return {
        "bigquery_dry_run": Tool(
            bigquery_dry_run,
            name="bigquery_dry_run",
            description="Estimate bytes processed and validate a BigQuery GoogleSQL query via dryRun.",
            requires_confirmation=False,
        ),
        "bigquery_query": Tool(
            bigquery_query,
            name="bigquery_query",
            description="Execute a governed BigQuery query with maximum_bytes_billed enforcement.",
            requires_confirmation=False,
        ),
        "bigquery_object_table_inspect": Tool(
            bigquery_object_table_inspect,
            name="bigquery_object_table_inspect",
            description="Inspect BigQuery Object Table metadata over GCS.",
            requires_confirmation=False,
        ),
    }
