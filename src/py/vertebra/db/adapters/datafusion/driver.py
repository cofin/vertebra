"""Fail-closed Apache DataFusion federated query engine over normalized PyArrow tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Final

from vertebra.db.adapters.datafusion.config import DataFusionConfig, TableRegistration
from vertebra.db.arrow import ArrowCursor
from vertebra.utils.module_loader import ensure_module, import_optional

if TYPE_CHECKING:
    from vertebra.storage import SyncObjectStore

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DataFusionEngine",
)

ARROW_STREAM_MEDIA_TYPE: Final[str] = "application/vnd.apache.arrow.stream"


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
            self._ctx = datafusion_mod.SessionContext() if datafusion_mod is not None else None

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
        """Register an ObjectStore dataset or live relational table descriptor."""
        self._tables[registration.name] = registration
        return registration

    def register_arrow_table(self, name: str, table: Any) -> TableRegistration:
        """Normalize and register an in-memory PyArrow table or ArrowCursor on the session context."""
        arrow_table = self._coerce_arrow_table(table)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            self._ctx.register_table(name, arrow_table)
        reg = TableRegistration(name=name, uri=f"arrow://{name}", format="arrow")
        return self.register_table(reg)

    def register_parquet_from_store(
        self,
        name: str,
        store: SyncObjectStore,
        key: str,
    ) -> TableRegistration:
        """Register a Parquet dataset located in an ObjectStore."""
        uri = f"{store.url.rstrip('/')}/{key.lstrip('/')}"
        reg = TableRegistration(name=name, uri=uri, format="parquet")
        return self.register_table(reg)

    def register_bigquery_table(
        self,
        name: str,
        driver: Any,
        table: str,
        *,
        selected_fields: list[str] | tuple[str, ...] | None = None,
        row_restriction: str | None = None,
    ) -> TableRegistration:
        """Fetch a BigQuery table via zero-copy ``ArrowCursor`` and register it in DataFusion."""
        cursor = driver.fetch_arrow(
            table,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
        )
        arrow_table = self._coerce_arrow_table(cursor)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            self._ctx.register_table(name, arrow_table)
        project = getattr(driver, "project_id", "bigquery")
        reg = TableRegistration(
            name=name,
            uri=f"bigquery://{project}/{table}",
            format="bigquery",
        )
        return self.register_table(reg)

    def register_bigquery_query(
        self,
        name: str,
        driver: Any,
        sql: str,
        parameters: Any = None,
    ) -> TableRegistration:
        """Execute a BigQuery query into a zero-copy ``ArrowCursor`` and register it in DataFusion."""
        cursor = driver.fetch_arrow(sql, parameters)
        arrow_table = self._coerce_arrow_table(cursor)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            self._ctx.register_table(name, arrow_table)
        project = getattr(driver, "project_id", "bigquery")
        reg = TableRegistration(
            name=name,
            uri=f"bigquery://{project}/query/{name}",
            format="bigquery",
        )
        return self.register_table(reg)

    def register_spanner_table(
        self,
        name: str,
        driver: Any,
        table: str,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
    ) -> TableRegistration:
        """Fetch a Cloud Spanner table via zero-copy ``ArrowCursor`` and register it in DataFusion."""
        cursor = driver.fetch_arrow(
            table,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
        )
        arrow_table = self._coerce_arrow_table(cursor)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            self._ctx.register_table(name, arrow_table)
        db_uri = getattr(driver, "database_uri", "projects/p/instances/i/databases/d")
        reg = TableRegistration(
            name=name,
            uri=f"spanner://{db_uri}/tables/{table}",
            format="spanner",
        )
        return self.register_table(reg)

    def register_spanner_query(
        self,
        name: str,
        driver: Any,
        sql: str,
        parameters: Any = None,
        *,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
    ) -> TableRegistration:
        """Execute a partitioned Cloud Spanner query into an ``ArrowCursor`` and register it in DataFusion."""
        cursor = driver.fetch_arrow(
            sql,
            parameters,
            max_partitions=max_partitions,
            data_boost_enabled=data_boost_enabled,
        )
        arrow_table = self._coerce_arrow_table(cursor)
        self._arrow_tables[name] = arrow_table
        if self._ctx is not None:
            self._ctx.register_table(name, arrow_table)
        db_uri = getattr(driver, "database_uri", "projects/p/instances/i/databases/d")
        reg = TableRegistration(
            name=name,
            uri=f"spanner://{db_uri}/query/{name}",
            format="spanner",
        )
        return self.register_table(reg)

    def explain_plan(self, sql: str) -> dict[str, Any]:
        """Return a federated execution plan descriptor for ``sql``."""
        referenced = [name for name in self._tables if name in sql]
        return {
            "mode": self._config.mode,
            "media_type": ARROW_STREAM_MEDIA_TYPE,
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
        arrow_table = self._coerce_arrow_table(self.execute_arrow(sql))
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
            datafusion_mod = ensure_module("datafusion", package="datafusion")
            ctx = datafusion_mod.SessionContext()
            for tbl_name, tbl_obj in self._arrow_tables.items():
                ctx.register_table(tbl_name, tbl_obj)
            self._ctx = ctx
        return self._ctx

    @staticmethod
    def _coerce_arrow_table(table: Any) -> Any:
        """Normalize an ArrowCursor, PyArrow Table/RecordBatch, or `.to_pylist()` table into a `pyarrow.Table`."""
        pa = ensure_module("pyarrow", package="pyarrow", extra="arrow")
        if isinstance(table, pa.Table):
            return table
        if isinstance(table, pa.RecordBatch):
            return pa.Table.from_batches([table])
        if isinstance(table, ArrowCursor) or callable(getattr(table, "to_pyarrow", None)):
            converted = table.to_pyarrow()
            if isinstance(converted, pa.Table):
                return converted
            if callable(getattr(converted, "to_pylist", None)):
                return pa.Table.from_pylist(list(converted.to_pylist()))
        if callable(getattr(table, "to_arrow", None)):
            converted_arrow = table.to_arrow()
            if isinstance(converted_arrow, pa.Table):
                return converted_arrow
        if callable(getattr(table, "to_pylist", None)):
            return pa.Table.from_pylist(list(table.to_pylist()))
        msg = f"Unsupported table input for DataFusion registration: {type(table)!r}"
        raise TypeError(msg)
