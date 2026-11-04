"""Configuration and table registration structs for embedded Apache DataFusion federation."""

from __future__ import annotations

from typing import Any, ClassVar, Literal, Self
from urllib.parse import parse_qsl, urlparse

from vertebra.db.driver._common import BaseDatabaseConfig
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


class DataFusionConfig(BaseDatabaseConfig, frozen=True, gc=False, kw_only=True):
    """Configuration for DataFusion analytics execution across Cloud Run and GKE topologies.

    - ``mode="in_process"``: Executes federated joins inside the Vertebra worker process and
      streams results over HTTP/2 or HTTP/3 as ``application/vnd.apache.arrow.stream``.
    - ``mode="pgwire_fdw"``: Additionally exposes an mTLS PostgreSQL wire protocol listener for
      inbound ``postgres_fdw`` queries inside GKE and VPC environments.
    """

    dialect: ClassVar[str] = "datafusion"
    dialect_tag: ClassVar[int] = 10
    url_schemes: ClassVar[tuple[str, ...]] = ("datafusion",)
    driver_module: ClassVar[str] = "vertebra.db.adapters.datafusion.driver"
    sync_driver_name: ClassVar[str] = "DataFusionEngine"
    async_driver_name: ClassVar[str] = "DataFusionEngine"

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

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `DataFusionConfig` from a `datafusion://` URI."""
        parsed = urlparse(url)
        query_params = dict(parse_qsl(parsed.query)) if parsed.query else {}
        kwargs: dict[str, Any] = {}
        host_token = parsed.netloc.strip().lower()
        if host_token in {"in_process", "pgwire_fdw"}:
            kwargs["mode"] = host_token
        elif host_token and ":" in host_token and parsed.hostname:
            kwargs["mode"] = "pgwire_fdw"
            kwargs["pgwire_host"] = parsed.hostname
            if parsed.port is not None:
                kwargs["pgwire_port"] = int(parsed.port)
        if "mode" in query_params:
            mode_val = query_params["mode"].strip().lower()
            if mode_val in {"in_process", "pgwire_fdw"}:
                kwargs["mode"] = mode_val
        if "target_partitions" in query_params:
            kwargs["target_partitions"] = int(query_params["target_partitions"])
        if "memory_limit_mb" in query_params:
            kwargs["memory_limit_mb"] = int(query_params["memory_limit_mb"])
        if "pgwire_host" in query_params:
            kwargs["pgwire_host"] = query_params["pgwire_host"]
        if "pgwire_port" in query_params:
            kwargs["pgwire_port"] = int(query_params["pgwire_port"])
        if "require_mtls_for_pgwire" in query_params:
            kwargs["require_mtls_for_pgwire"] = query_params["require_mtls_for_pgwire"].lower() in {"1", "true", "yes"}
        filtered_overrides = {k: v for k, v in overrides.items() if k != "broker"}
        kwargs.update(filtered_overrides)
        return cls(**kwargs)

    def pool_key(self) -> tuple[Any, ...]:
        """Return the canonical shared-engine fingerprint tuple."""
        return (
            "datafusion",
            self.mode,
            int(self.target_partitions),
            int(self.memory_limit_mb),
            self.pgwire_host,
            int(self.pgwire_port),
            bool(self.require_mtls_for_pgwire),
        )

    def build_sync_driver(self, registry: Any) -> Any:
        """Instantiate `DataFusionEngine` for this configuration."""
        _ = registry
        engine_cls, _ = self.resolve_driver_classes()
        return engine_cls(self)

    def build_async_driver(self, registry: Any) -> Any:
        """Instantiate `DataFusionEngine` for this configuration."""
        _ = registry
        _, engine_cls = self.resolve_driver_classes()
        return engine_cls(self)
