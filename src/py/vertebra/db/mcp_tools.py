"""Governed MCP 2026-07-28 and A2A 1.0 database tools for Google BigQuery and Cloud Spanner."""

from __future__ import annotations

import inspect
import re
from typing import TYPE_CHECKING, Any

from vertebra.agent.core.schema import Tool
from vertebra.agent.runtime import ConfirmationGate
from vertebra.db._registry import provide_bigquery_driver, provide_spanner_driver
from vertebra.db.bigquery import AsyncBigQueryDriver, BigQueryConfig, ObjectTableConfig, SyncBigQueryDriver
from vertebra.db.spanner import AsyncSpannerDriver, SpannerConfig, StalenessBound, SyncSpannerDriver
from vertebra.framework.security import PermissionDeniedError
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "GCPDatabaseToolPolicy",
    "is_mutating_sql",
    "register_gcp_database_tools",
)

_MUTATING_VERBS: frozenset[str] = frozenset(
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
_DDL_VERBS: frozenset[str] = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"})
_MULTI_STATEMENT_MESSAGE: str = (
    "Multi-statement SQL is not allowed; submit one statement per call and pass values containing ';' "
    "as query parameters"
)
_LEADING_COMMENT_RE: re.Pattern[str] = re.compile(r"^\s*(?:--[^\n]*\n|/\*.*?\*/\s*)+", re.DOTALL)


def _extract_leading_verb(sql: str) -> str:
    """Extract the uppercase leading SQL keyword after stripping comments and parentheses."""
    cleaned = _LEADING_COMMENT_RE.sub("", sql or "").lstrip(" (;\t\r\n")
    if not cleaned:
        return ""
    first_token = cleaned.split(None, 1)[0].rstrip(";(,")
    return first_token.upper()


def is_mutating_sql(sql: str) -> bool:
    """Return True when ``sql`` changes data, schema, or privileges, or runs procedural code.

    ``sql`` must hold exactly one statement. Any semicolon before the trailing terminator is
    rejected, including one inside a literal or comment, because GoogleSQL and Spangres quote
    literals differently and a dialect-specific parse could hide a second statement.

    Raises:
        PermissionDeniedError: When ``sql`` contains a semicolon before its trailing terminator.
    """
    body = (sql or "").rstrip().rstrip(";").rstrip()
    if ";" in body:
        raise PermissionDeniedError(_MULTI_STATEMENT_MESSAGE)
    return _extract_leading_verb(body) in _MUTATING_VERBS


class GCPDatabaseToolPolicy(FrozenBaseStruct, frozen=True, gc=False):
    """Governance policy for BigQuery and Cloud Spanner MCP and A2A tools."""

    maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024
    """Hard upper bound on bytes billed per BigQuery query execution (default 10 GiB)."""
    enforce_dry_run_limit: bool = True
    """Whether `bigquery_query` executes a `dry_run` preflight before running SELECT queries."""
    read_only_staleness_seconds: float | None = None
    """Optional exact staleness in seconds for lock-free Spanner `ReadOnly` snapshots."""
    protocol_version: str = "2026-07-28"
    """Target Model Context Protocol specification version."""
    a2a_version: str = "1.0"
    """Target Agent-to-Agent protocol specification version."""


async def _maybe_await(value: Any) -> Any:
    """Await `value` if it is awaitable, otherwise return it directly."""
    if inspect.isawaitable(value):
        return await value
    return value


def register_gcp_database_tools(
    target: Any = None,
    *,
    bigquery_driver: AsyncBigQueryDriver | SyncBigQueryDriver | None = None,
    spanner_driver: AsyncSpannerDriver | SyncSpannerDriver | None = None,
    bigquery_config: BigQueryConfig | str | None = None,
    spanner_config: SpannerConfig | str | None = None,
    maximum_bytes_billed: int = 10 * 1024 * 1024 * 1024,
    enforce_dry_run_limit: bool = True,
    read_only_staleness_seconds: float | None = None,
    confirmation_gate: ConfirmationGate | None = None,
) -> dict[str, Tool]:
    """Build and optionally register governed BigQuery and Cloud Spanner MCP/A2A tools."""
    gate = confirmation_gate if confirmation_gate is not None else ConfirmationGate()
    policy = GCPDatabaseToolPolicy(
        maximum_bytes_billed=maximum_bytes_billed,
        enforce_dry_run_limit=enforce_dry_run_limit,
        read_only_staleness_seconds=read_only_staleness_seconds,
    )

    def _resolve_bq() -> AsyncBigQueryDriver | SyncBigQueryDriver:
        if bigquery_driver is not None:
            return bigquery_driver
        return provide_bigquery_driver(bigquery_config or "default")

    def _resolve_spanner() -> AsyncSpannerDriver | SyncSpannerDriver:
        if spanner_driver is not None:
            return spanner_driver
        return provide_spanner_driver(spanner_config or "default")

    def _require_confirmation(
        tool_name: str,
        sql: str,
        confirmation_token: str | None,
    ) -> None:
        verb = _extract_leading_verb(sql)
        if not gate.is_approved(confirmation_token):
            ticket = gate.request_confirmation(
                tool_name=tool_name,
                call_id=f"call-{tool_name}",
                arguments={"sql": sql},
                reason=f"Mutating {verb} statement requires human confirmation.",
            )
            msg = f"Mutating statement ({verb}) blocked by ConfirmationGate; approval required (ticket={ticket.token})"
            raise PermissionDeniedError(msg)

    async def bigquery_dry_run(
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Estimate bytes processed and validate a BigQuery GoogleSQL query without executing it."""
        drv = _resolve_bq()
        res = await _maybe_await(drv.dry_run(sql, params))
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
        drv = _resolve_bq()
        effective_ceiling = (
            min(int(max_bytes_billed), policy.maximum_bytes_billed)
            if max_bytes_billed is not None
            else policy.maximum_bytes_billed
        )
        if effective_ceiling <= 0:
            msg = "maximum_bytes_billed must be a positive integer"
            raise PermissionDeniedError(msg)

        if is_mutating_sql(sql):
            _require_confirmation("bigquery_query", sql, confirmation_token)
            dml_fn = getattr(drv, "execute_dml", getattr(drv, "execute", None))
            affected = await _maybe_await(dml_fn(sql, params)) if callable(dml_fn) else 0
            return {
                "mode": "dml",
                "affected_rows": int(affected),
                "maximum_bytes_billed": effective_ceiling,
            }

        estimated_bytes = 0
        if policy.enforce_dry_run_limit:
            dry_res = await _maybe_await(drv.dry_run(sql, params))
            estimated_bytes = int(getattr(dry_res, "total_bytes_processed", 0))
            if estimated_bytes > effective_ceiling:
                msg = (
                    f"BigQuery dry_run estimated {estimated_bytes} bytes exceeds "
                    f"maximum_bytes_billed ceiling of {effective_ceiling} bytes"
                )
                raise PermissionDeniedError(msg)

        rows = await _maybe_await(drv.select(sql, params))
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
        drv = _resolve_bq()
        bounded_limit = max(1, min(int(limit), 1000))
        uri_pattern = object_uri_prefix or f"gs://{table_name.replace('.', '/')}/*"
        obj_cfg = ObjectTableConfig(
            table_name=table_name,
            connection_id=connection_id,
            uris=(uri_pattern,),
        )
        inspect_sql = f"SELECT uri, content_type, size, md5_hash, updated FROM {table_name} LIMIT {bounded_limit}"
        rows = await _maybe_await(drv.select(inspect_sql))
        return {
            "table_name": table_name,
            "connection_id": connection_id,
            "uris": list(obj_cfg.uris),
            "ddl": obj_cfg.create_object_table_sql(),
            "rows": list(rows),
            "row_count": len(rows),
        }

    async def spanner_query(
        sql: str,
        params: dict[str, Any] | None = None,
        staleness_seconds: float | None = None,
        confirmation_token: str | None = None,
    ) -> dict[str, Any]:
        """Execute a governed Cloud Spanner query using lock-free ReadOnly snapshots for SELECTs."""
        drv = _resolve_spanner()
        if is_mutating_sql(sql):
            verb = _extract_leading_verb(sql)
            _require_confirmation("spanner_query", sql, confirmation_token)
            if verb in _DDL_VERBS:
                op_Meta = await _maybe_await(drv.update_ddl([sql]))
                return {
                    "mode": "ddl",
                    "operation": op_Meta,
                }
            dml_fn = getattr(drv, "execute_dml", getattr(drv, "execute", None))
            affected = await _maybe_await(dml_fn(sql, params)) if callable(dml_fn) else 0
            return {
                "mode": "dml",
                "affected_rows": int(affected),
            }

        eff_staleness = staleness_seconds if staleness_seconds is not None else policy.read_only_staleness_seconds
        bound = (
            StalenessBound.exact_staleness(int(eff_staleness * 1000))
            if eff_staleness is not None
            else StalenessBound.strong()
        )
        snap = drv.snapshot(bound)
        rows = await _maybe_await(snap.select(sql, params))
        return {
            "mode": "read_only_snapshot",
            "staleness_seconds": eff_staleness,
            "rows": list(rows),
            "row_count": len(rows),
        }

    async def spanner_schema_inspect(
        table_name: str | None = None,
    ) -> dict[str, Any]:
        """Inspect Cloud Spanner `INFORMATION_SCHEMA` tables and columns using a lock-free ReadOnly snapshot."""
        drv = _resolve_spanner()
        bound = (
            StalenessBound.exact_staleness(int(policy.read_only_staleness_seconds * 1000))
            if policy.read_only_staleness_seconds is not None
            else StalenessBound.strong()
        )
        snap = drv.snapshot(bound)
        if table_name:
            tables_sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables WHERE table_name = @table_name"
            )
            cols_sql = (
                "SELECT table_name, column_name, spanner_type, is_nullable "
                "FROM information_schema.columns WHERE table_name = @table_name "
                "ORDER BY ordinal_position"
            )
            query_params: Mapping[str, Any] | None = {"table_name": table_name}
        else:
            tables_sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables WHERE table_schema = ''"
            )
            cols_sql = (
                "SELECT table_name, column_name, spanner_type, is_nullable "
                "FROM information_schema.columns WHERE table_schema = '' "
                "ORDER BY table_name, ordinal_position"
            )
            query_params = None

        tables = await _maybe_await(snap.select(tables_sql, query_params))
        columns = await _maybe_await(snap.select(cols_sql, query_params))
        return {
            "table_name": table_name,
            "tables": list(tables),
            "columns": list(columns),
        }

    tools: dict[str, Tool] = {
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
        "spanner_query": Tool(
            spanner_query,
            name="spanner_query",
            description="Execute a governed Cloud Spanner query using lock-free ReadOnly snapshots.",
            requires_confirmation=False,
        ),
        "spanner_schema_inspect": Tool(
            spanner_schema_inspect,
            name="spanner_schema_inspect",
            description="Inspect Cloud Spanner INFORMATION_SCHEMA tables and columns.",
            requires_confirmation=False,
        ),
    }

    if target is not None:
        if isinstance(target, dict):
            target.update(tools)
        elif hasattr(target, "register_tool") and callable(target.register_tool):
            for t_obj in tools.values():
                target.register_tool(t_obj)
        elif hasattr(target, "add_tool") and callable(target.add_tool):
            for t_obj in tools.values():
                target.add_tool(t_obj)
        elif hasattr(target, "register") and callable(target.register):
            for t_obj in tools.values():
                target.register(t_obj)
        elif hasattr(target, "tools") and isinstance(target.tools, list):
            target.tools.extend(tools.values())

    return tools
