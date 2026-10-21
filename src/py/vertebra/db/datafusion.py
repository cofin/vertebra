"""Embedded Apache DataFusion analytics federation for Cloud Run (HTTP/2 Arrow IPC) and GKE (pgwire FDW)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = (
    "ARROW_STREAM_MEDIA_TYPE",
    "DataFusionConfig",
    "DataFusionMode",
    "DataFusionTableRegistration",
    "EmbeddedDataFusionEngine",
)

ARROW_STREAM_MEDIA_TYPE: str = "application/vnd.apache.arrow.stream"

type DataFusionMode = Literal["in_process", "pgwire_fdw"]


@dataclass(frozen=True, slots=True)
class DataFusionTableRegistration:
    """Registered Parquet, Vortex, Arrow, or live relational table in DataFusion."""

    name: str
    uri: str
    format: Literal["parquet", "vortex", "arrow", "postgres", "oracle"] = "parquet"


@dataclass(frozen=True, slots=True)
class DataFusionConfig:
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


@dataclass(slots=True)
class EmbeddedDataFusionEngine:
    """Federated query engine joining ObjectStore datasets with live database tables."""

    config: DataFusionConfig = field(default_factory=DataFusionConfig)
    _tables: dict[str, DataFusionTableRegistration] = field(default_factory=dict, init=False)

    @property
    def tables(self) -> dict[str, DataFusionTableRegistration]:
        """Return registered tables."""
        return dict(self._tables)

    @property
    def supports_cloud_run_ingress(self) -> bool:
        """Return True when the engine operates over single-port HTTP/2 Arrow IPC streaming."""
        return self.config.mode == "in_process"

    def register_table(self, registration: DataFusionTableRegistration) -> DataFusionTableRegistration:
        """Register an ObjectStore dataset or live relational table."""
        self._tables[registration.name] = registration
        return registration

    def explain_plan(self, sql: str) -> dict[str, Any]:
        """Return a federated execution plan descriptor for ``sql``."""
        referenced = [name for name in self._tables if name in sql]
        return {
            "mode": self.config.mode,
            "media_type": ARROW_STREAM_MEDIA_TYPE,
            "referenced_tables": referenced,
            "sql": sql,
        }
