"""Apache DataFusion session builder, source registration, and plan formatting helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import unquote, urlparse

from vertebra.db.adapters.datafusion.config import DataFusionConfig, TableRegistration
from vertebra.db.arrow import ArrowCursor
from vertebra.db.driver import NotSupportedError
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.storage import SyncObjectStore

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DataFusionCore",
)

ARROW_STREAM_MEDIA_TYPE: Final[str] = "application/vnd.apache.arrow.stream"
_TABLE_SCAN_RE: Final[re.Pattern[str]] = re.compile(r"TableScan:\s*([A-Za-z_][A-Za-z0-9_]*)")


class DataFusionCore:
    """Stateless session, Arrow coercion, source registration, and explain helpers for DataFusion."""

    @staticmethod
    def build_session_context(config: DataFusionConfig, *, datafusion_mod: Any = None) -> Any:
        """Build a configured ``datafusion.SessionContext`` from ``DataFusionConfig`` bounds."""
        mod = datafusion_mod if datafusion_mod is not None else ensure_module("datafusion", package="datafusion")
        session_config_cls = getattr(mod, "SessionConfig", None)
        runtime_builder_cls = getattr(mod, "RuntimeEnvBuilder", None)

        session_config: Any = None
        if callable(session_config_cls):
            cfg_obj = session_config_cls()
            with_target_partitions = getattr(cfg_obj, "with_target_partitions", None)
            if callable(with_target_partitions):
                cfg_obj = with_target_partitions(config.target_partitions)
            session_config = cfg_obj

        runtime_env: Any = None
        if callable(runtime_builder_cls):
            rt_obj = runtime_builder_cls()
            memory_bytes = config.memory_limit_mb * 1024 * 1024
            with_fair_spill_pool = getattr(rt_obj, "with_fair_spill_pool", None)
            if callable(with_fair_spill_pool):
                rt_obj = with_fair_spill_pool(memory_bytes)
            build_runtime = getattr(rt_obj, "build", None)
            if callable(build_runtime):
                rt_obj = build_runtime()
            runtime_env = rt_obj

        if session_config is not None and runtime_env is not None:
            return mod.SessionContext(session_config, runtime_env)
        if session_config is not None:
            return mod.SessionContext(session_config)
        return mod.SessionContext()

    @staticmethod
    def coerce_arrow_table(table: Any) -> Any:
        """Normalize an ArrowCursor, PyArrow Table/RecordBatch, or `.to_pylist()` table into a `pyarrow.Table`."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        if isinstance(table, pa.Table):
            return table
        if isinstance(table, pa.RecordBatch):
            return pa.Table.from_batches([table])
        if isinstance(table, ArrowCursor):
            converted = table.to_pyarrow()
            if isinstance(converted, pa.Table):
                return converted
            converted_pylist = getattr(converted, "to_pylist", None)
            if callable(converted_pylist):
                return pa.Table.from_pylist(list(cast("Any", converted_pylist())))
        to_pyarrow_fn = getattr(table, "to_pyarrow", None)
        if callable(to_pyarrow_fn):
            converted = to_pyarrow_fn()
            if isinstance(converted, pa.Table):
                return converted
            converted_pylist_fn = getattr(converted, "to_pylist", None)
            if callable(converted_pylist_fn):
                return pa.Table.from_pylist(list(cast("Any", converted_pylist_fn())))
        to_arrow_fn = getattr(table, "to_arrow", None)
        if callable(to_arrow_fn):
            converted_arrow = to_arrow_fn()
            if isinstance(converted_arrow, pa.Table):
                return converted_arrow
        to_pylist_fn = getattr(table, "to_pylist", None)
        if callable(to_pylist_fn):
            return pa.Table.from_pylist(list(cast("Any", to_pylist_fn())))
        msg = f"Unsupported table input for DataFusion registration: {type(table)!r}"
        raise TypeError(msg)

    @staticmethod
    def register_arrow_source(ctx: Any, name: str, arrow_table: Any) -> None:
        """Register an in-memory ``pyarrow.Table`` on a DataFusion ``SessionContext``."""
        from_arrow = getattr(ctx, "from_arrow", None)
        if callable(from_arrow):
            table_exist = getattr(ctx, "table_exist", None)
            deregister_table = getattr(ctx, "deregister_table", None)
            if callable(table_exist) and callable(deregister_table) and table_exist(name):
                deregister_table(name)
            from_arrow(arrow_table, name=name)
            return
        ctx.register_table(name, arrow_table)

    @staticmethod
    def resolve_local_path(uri: str) -> str:
        """Convert a ``file://`` URI or filesystem path into a normalized local path string."""
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            if parsed.netloc and parsed.netloc != "localhost":
                msg = f"Unsupported non-local file URI host {parsed.netloc!r} in {uri!r}"
                raise ImproperConfigurationError(msg)
            return str(Path(unquote(parsed.path)))
        if parsed.scheme and len(parsed.scheme) > 1:
            msg = f"Cannot resolve remote URI {uri!r} as a local filesystem path"
            raise NotSupportedError(msg)
        return uri

    @staticmethod
    def register_file_source(ctx: Any, registration: TableRegistration) -> None:
        """Register a Parquet, CSV, or NDJSON source with the DataFusion ``SessionContext``."""
        fmt = registration.format
        if fmt == "parquet":
            register_parquet = getattr(ctx, "register_parquet", None)
            if callable(register_parquet):
                target = (
                    DataFusionCore.resolve_local_path(registration.uri)
                    if registration.uri.startswith("file://")
                    else registration.uri
                )
                register_parquet(registration.name, target)
                return
            local_path = DataFusionCore.resolve_local_path(registration.uri)
            pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
            DataFusionCore.register_arrow_source(ctx, registration.name, pq.read_table(local_path))
            return

        if fmt == "csv":
            register_csv = getattr(ctx, "register_csv", None)
            if callable(register_csv):
                target = (
                    DataFusionCore.resolve_local_path(registration.uri)
                    if registration.uri.startswith("file://")
                    else registration.uri
                )
                register_csv(registration.name, target)
                return
            local_path = DataFusionCore.resolve_local_path(registration.uri)
            pa_csv = ensure_module("pyarrow.csv", package="pyarrow", extra="arrow")
            DataFusionCore.register_arrow_source(ctx, registration.name, pa_csv.read_csv(local_path))
            return

        if fmt == "ndjson":
            register_json = getattr(ctx, "register_json", None)
            if callable(register_json):
                target = (
                    DataFusionCore.resolve_local_path(registration.uri)
                    if registration.uri.startswith("file://")
                    else registration.uri
                )
                suffix = Path(target).suffix
                if suffix and suffix.lower() != ".json":
                    try:
                        register_json(registration.name, target, file_extension=suffix)
                    except TypeError:
                        register_json(registration.name, target)
                else:
                    register_json(registration.name, target)
                return
            local_path = DataFusionCore.resolve_local_path(registration.uri)
            pa_json = ensure_module("pyarrow.json", package="pyarrow", extra="arrow")
            DataFusionCore.register_arrow_source(ctx, registration.name, pa_json.read_json(local_path))
            return

        msg = f"Unsupported DataFusion source format {fmt!r} for table {registration.name!r}"
        raise NotSupportedError(msg)

    @staticmethod
    def register_store_parquet(
        ctx: Any,
        name: str,
        store: SyncObjectStore,
        key: str,
    ) -> tuple[TableRegistration, Any | None]:
        """Register a Parquet object from a ``SyncObjectStore`` with the session context."""
        uri = f"{store.url.rstrip('/')}/{key.lstrip('/')}"
        reg = TableRegistration(name=name, uri=uri, format="parquet")
        if store.scheme == "file" or uri.startswith("file://"):
            local_path = DataFusionCore.resolve_local_path(uri)
            if ctx is not None:
                register_parquet = getattr(ctx, "register_parquet", None)
                if callable(register_parquet):
                    register_parquet(name, local_path)
                    return reg, None
                pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
                arrow_table = pq.read_table(local_path)
                DataFusionCore.register_arrow_source(ctx, name, arrow_table)
                return reg, arrow_table
            return reg, None

        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        pq = ensure_module("pyarrow.parquet", package="pyarrow", extra="arrow")
        payload = store.get(key)
        arrow_table = pq.read_table(pa.BufferReader(payload))
        if ctx is not None:
            DataFusionCore.register_arrow_source(ctx, name, arrow_table)
        return reg, arrow_table

    @staticmethod
    def format_explain_plan(
        ctx: Any,
        sql: str,
        *,
        mode: str = "in_process",
        media_type: str = ARROW_STREAM_MEDIA_TYPE,
        registered_tables: Mapping[str, TableRegistration] | None = None,
    ) -> dict[str, Any]:
        """Execute ``ctx.sql(sql)`` and extract logical and physical plan text from DataFusion."""
        df = ctx.sql(sql)
        logical_plan = ""
        physical_plan = ""

        logical_fn = getattr(df, "logical_plan", None)
        if callable(logical_fn):
            lp_obj = logical_fn()
            display_fn = getattr(lp_obj, "display_indent", None)
            logical_plan = str(display_fn()) if callable(display_fn) else str(lp_obj)

        exec_fn = getattr(df, "execution_plan", None)
        if callable(exec_fn):
            pp_obj = exec_fn()
            display_fn = getattr(pp_obj, "display_indent", None)
            physical_plan = str(display_fn()) if callable(display_fn) else str(pp_obj)

        explain_fn = getattr(df, "explain", None)
        if (not logical_plan or not physical_plan) and callable(explain_fn):
            explain_out = explain_fn()
            if isinstance(explain_out, Mapping):
                if not logical_plan:
                    logical_plan = str(explain_out.get("logical_plan") or explain_out.get("plan") or "")
                if not physical_plan:
                    physical_plan = str(explain_out.get("physical_plan") or "")
            elif isinstance(explain_out, str) and not logical_plan:
                logical_plan = explain_out
            elif (
                explain_out is None
                and not logical_plan
                and not physical_plan
                and not sql.lstrip().upper().startswith("EXPLAIN")
            ):
                explain_df = ctx.sql(f"EXPLAIN {sql}")
                to_arrow = getattr(explain_df, "to_arrow_table", None)
                if callable(to_arrow):
                    explain_table = cast("Any", to_arrow())
                    for row in explain_table.to_pylist():
                        plan_type = str(row.get("plan_type", "")).lower()
                        plan_body = str(row.get("plan", ""))
                        if "logical" in plan_type and not logical_plan:
                            logical_plan = plan_body
                        elif "physical" in plan_type and not physical_plan:
                            physical_plan = plan_body

        combined_plan = "\n\n".join(part for part in (logical_plan, physical_plan) if part)
        referenced_tables = DataFusionCore._extract_referenced_tables(
            combined_plan,
            sql,
            registered_tables,
        )
        return {
            "mode": mode,
            "media_type": media_type,
            "logical_plan": logical_plan,
            "physical_plan": physical_plan,
            "plan": combined_plan,
            "referenced_tables": referenced_tables,
            "sql": sql,
        }

    @staticmethod
    def _extract_referenced_tables(
        plan_text: str,
        sql: str,
        registered_tables: Mapping[str, TableRegistration] | None,
    ) -> list[str]:
        """Extract referenced table names from planner ``TableScan`` output."""
        found: list[str] = []
        for match in _TABLE_SCAN_RE.findall(plan_text):
            if match not in found:
                found.append(match)
        if found:
            return found
        if registered_tables:
            search_haystack = plan_text or sql
            for name in registered_tables:
                if re.search(rf"\b{re.escape(name)}\b", search_haystack) and name not in found:
                    found.append(name)
        return found
