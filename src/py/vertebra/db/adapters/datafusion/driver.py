"""Fail-closed Apache DataFusion federated query engine over normalized PyArrow tables."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.adapters.datafusion.config import DataFusionConfig, TableRegistration
from vertebra.db.adapters.datafusion.core import ARROW_STREAM_MEDIA_TYPE, DataFusionCore
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional

if TYPE_CHECKING:
    from vertebra.storage import SyncObjectStore

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DataFusionEngine",
)


class DataFusionEngine:
    """Federated query engine joining ObjectStore datasets with live database Arrow tables."""

    ARROW_STREAM_MEDIA_TYPE: ClassVar[str] = ARROW_STREAM_MEDIA_TYPE
    __slots__ = ("_arrow_tables", "_config", "_ctx", "_tables")

    def __init__(self, config: DataFusionConfig | None = None, *, ctx: Any = None) -> None:
        self._config = config or DataFusionConfig()
        self._tables: dict[str, TableRegistration] = {}
        self._arrow_tables: dict[str, Any] = {}
        if ctx is not None:
            self._ctx: Any = ctx
        else:
            datafusion_mod = import_optional("datafusion")
            self._ctx = (
                DataFusionCore.build_session_context(self._config, datafusion_mod=datafusion_mod)
                if datafusion_mod is not None
                else None
            )

    @property
    def config(self) -> DataFusionConfig:
        """Return the active DataFusionConfig."""
        return self._config

    @property
    def tables(self) -> dict[str, TableRegistration]:
        """Return registered table descriptors keyed by SQL alias."""
        return dict(self._tables)

    @property
    def supports_cloud_run_ingress(self) -> bool:
        """Return True when the engine operates over single-port HTTP/2 Arrow IPC streaming."""
        return self._config.mode == "in_process"

    def register_table(self, registration: TableRegistration) -> TableRegistration:
        """Register an ObjectStore dataset, file source, or registered Arrow table."""
        if registration.format == "arrow":
            if registration.name not in self._arrow_tables:
                msg = f"Table {registration.name!r} uses format='arrow' without registered Arrow data"
                raise ImproperConfigurationError(msg)
            if self._ctx is not None:
                DataFusionCore.register_arrow_source(
                    self._ctx, registration.name, self._arrow_tables[registration.name]
                )
            self._tables[registration.name] = registration
            return registration

        if self._ctx is not None:
            DataFusionCore.register_file_source(self._ctx, registration)
        self._tables[registration.name] = registration
        return registration

    def register_arrow_table(self, name: str, table: Any) -> TableRegistration:
        """Normalize and register an in-memory PyArrow table or ArrowCursor on the session context."""
        arrow_table = DataFusionCore.coerce_arrow_table(table)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            DataFusionCore.register_arrow_source(self._ctx, name, arrow_table)
        reg = TableRegistration(name=name, uri=f"arrow://{name}", format="arrow")
        self._tables[name] = reg
        return reg

    def register_parquet_from_store(
        self,
        name: str,
        store: SyncObjectStore,
        key: str,
    ) -> TableRegistration:
        """Register a Parquet dataset located in an ObjectStore."""
        reg, arrow_table = DataFusionCore.register_store_parquet(self._ctx, name, store, key)
        if arrow_table is not None:
            self._arrow_tables[name] = arrow_table
        self._tables[name] = reg
        return reg

    def explain_plan(self, sql: str) -> dict[str, Any]:
        """Return a federated execution plan descriptor for ``sql``."""
        if self._ctx is not None:
            return DataFusionCore.format_explain_plan(
                self._ctx,
                sql,
                mode=self._config.mode,
                media_type=ARROW_STREAM_MEDIA_TYPE,
                registered_tables=self._tables,
            )
        referenced = [name for name in self._tables if re.search(rf"\b{re.escape(name)}\b", sql)]
        return {
            "mode": self._config.mode,
            "media_type": ARROW_STREAM_MEDIA_TYPE,
            "logical_plan": "",
            "physical_plan": "",
            "plan": "",
            "referenced_tables": referenced,
            "sql": sql,
        }

    def execute_arrow(self, sql: str) -> Any:
        """Execute ``sql`` on the DataFusion SessionContext and return a PyArrow Table."""
        ctx = self._ensure_context()
        df = ctx.sql(sql)
        return df.to_arrow_table()

    def execute_dicts(self, sql: str) -> list[dict[str, Any]]:
        """Execute ``sql`` on the DataFusion SessionContext and return rows as dictionaries."""
        arrow_table = self.execute_arrow(sql)
        return list(arrow_table.to_pylist())

    def stream_arrow_ipc(self, sql: str) -> bytes:
        """Execute ``sql`` and serialize the resulting PyArrow table as Arrow IPC stream bytes."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        ipc = ensure_module("pyarrow.ipc", package="pyarrow", extra="arrow")
        arrow_table = DataFusionCore.coerce_arrow_table(self.execute_arrow(sql))
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        return bytes(sink.getvalue().to_pybytes())

    def execute_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute ``sql`` and return results as a list of dictionaries."""
        return self.execute_dicts(sql)

    def _ensure_context(self) -> Any:
        """Resolve or lazily initialize the underlying DataFusion SessionContext."""
        if self._ctx is None:
            datafusion_mod = ensure_module("datafusion", package="datafusion", extra="datafusion")
            ctx = DataFusionCore.build_session_context(self._config, datafusion_mod=datafusion_mod)
            for tbl_name, tbl_obj in self._arrow_tables.items():
                DataFusionCore.register_arrow_source(ctx, tbl_name, tbl_obj)
            for tbl_name, reg in self._tables.items():
                if tbl_name not in self._arrow_tables:
                    DataFusionCore.register_file_source(ctx, reg)
            self._ctx = ctx
        return self._ctx

    @staticmethod
    def _coerce_arrow_table(table: Any) -> Any:
        """Normalize an ArrowCursor, PyArrow Table/RecordBatch, or `.to_pylist()` table into a `pyarrow.Table`."""
        return DataFusionCore.coerce_arrow_table(table)
