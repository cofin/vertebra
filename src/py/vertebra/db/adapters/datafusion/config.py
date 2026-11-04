"""Configuration and table registration structs for embedded Apache DataFusion federation."""

from __future__ import annotations

from typing import Literal

from vertebra.typing import FrozenBaseStruct

__all__ = (
    "DataFusionConfig",
    "DataFusionMode",
    "TableRegistration",
)

type DataFusionMode = Literal["in_process", "pgwire_fdw"]


class TableRegistration(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Registered Parquet, Vortex, Arrow, or live relational table in DataFusion."""

    name: str
    """Logical SQL table alias registered in the DataFusion catalog."""

    uri: str
    """Source dataset or upstream database URI backing the registered table."""

    format: Literal[
        "parquet",
        "vortex",
        "arrow",
        "csv",
        "ndjson",
        "duckdb",
        "postgres",
        "oracle",
        "bigquery",
        "spanner",
    ] = "parquet"
    """Storage file format or live relational engine identifier."""


class DataFusionConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """Configuration for DataFusion analytics execution across Cloud Run and GKE topologies.

    - ``mode="in_process"``: Executes federated joins inside the Vertebra worker process and
      streams results over HTTP/2 or HTTP/3 as ``application/vnd.apache.arrow.stream``.
    - ``mode="pgwire_fdw"``: Additionally exposes an mTLS PostgreSQL wire protocol listener for
      inbound ``postgres_fdw`` queries inside GKE and VPC environments.
    """

    mode: DataFusionMode = "in_process"
    """Execution topology (`in_process` HTTP/2 Arrow IPC or `pgwire_fdw` listener)."""

    target_partitions: int = 4
    """Target query execution parallelism across CPU partitions."""

    memory_limit_mb: int = 512
    """Per-context DataFusion execution memory pool cap in megabytes."""

    pgwire_host: str = "127.0.0.1"
    """Bind address for the optional `pgwire_fdw` PostgreSQL wire listener."""

    pgwire_port: int = 5434
    """TCP port for the optional `pgwire_fdw` PostgreSQL wire listener."""

    require_mtls_for_pgwire: bool = True
    """Whether mutual TLS client authentication is required on `pgwire_fdw` connections."""
