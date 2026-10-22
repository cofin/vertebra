"""Embedded Apache DataFusion analytics federation for Cloud Run (HTTP/2 Arrow IPC) and GKE (pgwire FDW)."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

import msgspec

__all__ = (
    "DataFusionConfig",
    "DataFusionEngine",
    "TableRegistration",
)

ARROW_STREAM_MEDIA_TYPE: str = "application/vnd.apache.arrow.stream"

type DataFusionMode = Literal["in_process", "pgwire_fdw"]


class TableRegistration(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Registered Parquet, Vortex, Arrow, or live relational table in DataFusion."""

    name: str
    uri: str
    format: Literal["parquet", "vortex", "arrow", "postgres", "oracle"] = "parquet"


class DataFusionConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for DataFusion analytics execution across Cloud Run and GKE topologies.

    - ``mode="in_process"``: Executes federated joins inside the Vertebra worker process and
      streams results over HTTP/2 or HTTP/3 as ``application/vnd.apache.arrow.stream`` (compatible
      with Cloud Run single-port ingress and GKE).
    - ``mode="pgwire_fdw"``: Additionally exposes an mTLS PostgreSQL wire protocol listener for
      inbound ``postgres_fdw`` queries inside GKE / VPC environments.
    """

    mode: DataFusionMode = "in_process"
    target_partitions: int = 4
    memory_limit_mb: int = 512
    pgwire_host: str = "127.0.0.1"
    pgwire_port: int = 5434
    require_mtls_for_pgwire: bool = True


class DataFusionEngine:
    """Federated query engine joining ObjectStore datasets with live database tables."""

    ARROW_STREAM_MEDIA_TYPE: ClassVar[str] = ARROW_STREAM_MEDIA_TYPE
    __slots__ = ("_config", "_tables")

    def __init__(self, config: DataFusionConfig | None = None) -> None:
        self._config = config or DataFusionConfig()
        self._tables: dict[str, TableRegistration] = {}

    @property
    def config(self) -> DataFusionConfig:
        """Return the active DataFusionConfig."""
        return self._config

    @property
    def tables(self) -> dict[str, TableRegistration]:
        """Return registered tables."""
        return dict(self._tables)

    @property
    def supports_cloud_run_ingress(self) -> bool:
        """Return True when the engine operates over single-port HTTP/2 Arrow IPC streaming."""
        return self._config.mode == "in_process"

    def register_table(self, registration: TableRegistration) -> TableRegistration:
        """Register an ObjectStore dataset or live relational table."""
        self._tables[registration.name] = registration
        return registration

    def explain_plan(self, sql: str) -> dict[str, Any]:
        """Return a federated execution plan descriptor for ``sql``."""
        referenced = [name for name in self._tables if name in sql]
        return {
            "mode": self._config.mode,
            "media_type": ARROW_STREAM_MEDIA_TYPE,
            "referenced_tables": referenced,
            "sql": sql,
        }
