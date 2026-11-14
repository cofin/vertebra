"""Configuration and table registration structs for embedded Apache DataFusion federation."""

from __future__ import annotations

from typing import Any, ClassVar, Literal, Self
from urllib.parse import parse_qsl, urlparse

from vertebra.db.driver._common import DATAFUSION_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError

__all__ = (
    "DataFusionConfig",
    "DataFusionMode",
    "TableRegistration",
)

type DataFusionMode = Literal["in_process"]


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
    """Configuration for embedded in-process Apache DataFusion analytics execution."""

    dialect: ClassVar[str] = "datafusion"
    dialect_tag: ClassVar[int] = DATAFUSION_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("datafusion",)
    driver_module: ClassVar[str] = "vertebra.db.adapters.datafusion.driver"
    sync_driver_name: ClassVar[str] = "DataFusionEngine"
    async_driver_name: ClassVar[str] = "DataFusionEngine"

    mode: DataFusionMode = "in_process"
    """Execution topology (`in_process` embedded Arrow IPC engine)."""

    target_partitions: int = 4
    """Target query execution parallelism across CPU partitions."""

    memory_limit_mb: int = 512
    """Per-context DataFusion execution memory pool cap in megabytes."""

    def __post_init__(self) -> None:
        """Validate in-process mode bounds and numeric constraints."""
        if self.mode != "in_process":
            msg = f"DataFusionConfig only supports mode='in_process', got {self.mode!r}"
            raise ImproperConfigurationError(msg)
        if (
            type(self.target_partitions) is bool
            or not isinstance(self.target_partitions, int)
            or self.target_partitions < 1
        ):
            msg = "target_partitions must be an integer >= 1"
            raise ImproperConfigurationError(msg)
        if type(self.memory_limit_mb) is bool or not isinstance(self.memory_limit_mb, int) or self.memory_limit_mb < 1:
            msg = "memory_limit_mb must be an integer >= 1"
            raise ImproperConfigurationError(msg)

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `DataFusionConfig` from a `datafusion://` URI."""
        parsed = urlparse(url)
        host_token = parsed.netloc.strip().lower()
        if host_token == "pgwire_fdw" or url.startswith("datafusion://pgwire_fdw"):
            msg = "DataFusionConfig does not support mode='pgwire_fdw'; only mode='in_process' is supported"
            raise ImproperConfigurationError(msg)
        if ":" in host_token or (parsed.hostname and parsed.port is not None):
            msg = f"DataFusionConfig does not support network host:port endpoints ({parsed.netloc!r}); use mode='in_process'"
            raise ImproperConfigurationError(msg)
        if host_token and host_token != "in_process":
            msg = f"Unsupported DataFusion host or mode {host_token!r}; expected 'in_process'"
            raise ImproperConfigurationError(msg)

        query_params = dict(parse_qsl(parsed.query)) if parsed.query else {}
        for key in query_params:
            if key.startswith("pgwire_") or key == "require_mtls_for_pgwire":
                msg = f"Unsupported DataFusion pgwire parameter {key!r}"
                raise ImproperConfigurationError(msg)

        kwargs: dict[str, Any] = {"mode": "in_process"}
        if "mode" in query_params:
            mode_val = query_params["mode"].strip().lower()
            if mode_val != "in_process":
                msg = f"Unsupported DataFusion mode {mode_val!r}; expected 'in_process'"
                raise ImproperConfigurationError(msg)
            kwargs["mode"] = mode_val

        if "target_partitions" in query_params:
            try:
                kwargs["target_partitions"] = int(query_params["target_partitions"])
            except (ValueError, TypeError) as exc:
                msg = "target_partitions must be an integer >= 1"
                raise ImproperConfigurationError(msg) from exc

        if "memory_limit_mb" in query_params:
            try:
                kwargs["memory_limit_mb"] = int(query_params["memory_limit_mb"])
            except (ValueError, TypeError) as exc:
                msg = "memory_limit_mb must be an integer >= 1"
                raise ImproperConfigurationError(msg) from exc

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
