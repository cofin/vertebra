"""Configuration and secret structs for the embedded DuckDB analytical engine."""

from __future__ import annotations

import os
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct
from vertebra.utils.module_loader import import_optional

__all__ = (
    "DuckDBConfig",
    "DuckDBSecretConfig",
    "duckdb_bootstrap_statements",
)


class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=False):
    """Cloud storage secret configuration (`CREATE SECRET`) for DuckDB `httpfs` / S3 / GCS / R2."""

    name: str = "default_s3"
    """Unique DuckDB secret identifier."""
    secret_type: str = "S3"
    """DuckDB secret type (`"S3"`, `"GCS"`, `"R2"`, or `"AZURE"`)."""
    key_id: str = ""
    """Cloud storage access key ID (`KEY_ID`)."""
    secret: str = ""
    """Cloud storage secret access key (`SECRET`)."""
    region: str = "us-east-1"
    """Cloud storage region (`REGION`)."""
    endpoint: str | None = None
    """Optional custom S3-compatible endpoint (`ENDPOINT`, e.g. RustFS or MinIO)."""

    def to_create_secret_sql(self) -> str:
        """Render a `CREATE OR REPLACE SECRET` SQL statement for DuckDB."""
        parts = [f"TYPE {self.secret_type}"]
        if self.key_id:
            parts.append(f"KEY_ID '{self.key_id}'")
        if self.secret:
            parts.append(f"SECRET '{self.secret}'")
        if self.region:
            parts.append(f"REGION '{self.region}'")
        if self.endpoint:
            parts.append(f"ENDPOINT '{self.endpoint}'")
        inner = ", ".join(parts)
        return f"CREATE OR REPLACE SECRET {self.name} ({inner})"


class DuckDBConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Configuration for the embedded DuckDB analytical engine."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG
    url_schemes: ClassVar[tuple[str, ...]] = ("duckdb",)
    driver_module: ClassVar[str] = "vertebra.db.adapters.duckdb.driver"
    sync_driver_name: ClassVar[str] = "SyncDuckDBDriver"
    async_driver_name: ClassVar[str] = "AsyncDuckDBDriver"

    path: str = ":memory:"
    """Filesystem path to the DuckDB database file or `":memory:"`."""
    database: str = ":memory:"
    """Fallback database path alias used when `path` is left at `":memory:"`."""
    memory_limit_mb: int = 512
    """Maximum buffer manager memory allowance in MiB (`SET memory_limit`)."""
    threads: int = 4
    """Number of DuckDB execution threads (`SET threads`)."""
    parquet_metadata_cache: bool = True
    """When True, enable `SET parquet_metadata_cache = true` after connection bootstrap."""
    httpfs_connection_caching: bool = True
    """When True, keep remote HTTP/S3 object storage connections warm (`SET http_keep_alive = true`)."""
    enable_http_metadata_cache: bool = True
    """When True, cache remote HTTP/Parquet `HEAD` metadata (`SET enable_http_metadata_cache = true`)."""
    read_only: bool = False
    """When True, open the DuckDB file in read-only mode allowing multi-process readers."""
    secrets: tuple[DuckDBSecretConfig, ...] = ()
    """Optional cloud storage secrets (`CREATE OR REPLACE SECRET`) executed at connection bootstrap."""
    pool_min_size: int = 1
    """Minimum number of pooled DuckDB cursors per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled DuckDB cursors per worker."""
    statement_cache_size: int = 256
    """Maximum number of prepared analytical statements cached per worker."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""
    gcs_token_cache: Any = None
    """Optional `SyncGcpTokenCache` or `AsyncGcpTokenCache` used to rotate session-local GCS `BEARER_TOKEN` secrets on token refresh."""

    def resolved_path(self) -> str:
        """Resolve the effective database path from `path` or `database`."""
        if self.path != ":memory:":
            return self.path
        return self.database

    def bootstrap_statements(self) -> list[str]:
        """Return post-initialization `SET` statements (`parquet_metadata_cache` is always post-init SQL)."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "duckdb_bootstrap_statements"):
            raw_stmts = native.duckdb_bootstrap_statements(
                self.resolved_path(),
                self.memory_limit_mb,
                self.threads,
                self.parquet_metadata_cache,
                self.httpfs_connection_caching,
                self.enable_http_metadata_cache,
            )
            stmts = [s.replace("SET httpfs_connection_caching = ", "SET http_keep_alive = ") for s in raw_stmts]
        else:
            parquet_flag = "true" if self.parquet_metadata_cache else "false"
            keep_alive_flag = "true" if self.httpfs_connection_caching else "false"
            http_meta_flag = "true" if self.enable_http_metadata_cache else "false"
            stmts = [
                f"SET memory_limit = '{self.memory_limit_mb}MB'",
                f"SET threads = {self.threads}",
                f"SET parquet_metadata_cache = {parquet_flag}",
                f"SET http_keep_alive = {keep_alive_flag}",
                f"SET enable_http_metadata_cache = {http_meta_flag}",
            ]
        for secret in self.secrets:
            stmts.append(secret.to_create_secret_sql())
        return stmts

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `DuckDBConfig` from a `duckdb://` URI."""
        parsed = urlparse(url)
        if parsed.netloc == ":memory:" or (not parsed.netloc and parsed.path in ("", "/", "/:memory:", ":memory:")):
            db_path = ":memory:"
        elif parsed.netloc:
            db_path = f"{parsed.netloc}{parsed.path}"
        elif parsed.path.startswith("//"):
            db_path = parsed.path[1:]
        else:
            db_path = parsed.path.lstrip("/")
        kwargs: dict[str, Any] = {"path": db_path, "database": db_path, **overrides}
        if "path" in overrides and "database" not in overrides:
            kwargs["database"] = overrides["path"]
        elif "database" in overrides and "path" not in overrides and overrides["database"] is not None:
            kwargs["path"] = str(overrides["database"])
        return cls(**kwargs)

    def pool_key(self) -> tuple[Any, ...] | None:
        """Return a deterministic shared-pool fingerprint, or `None` for `:memory:` DBs."""
        resolved = self.resolved_path()
        if resolved == ":memory:":
            return None
        return ("duckdb", os.path.abspath(resolved), bool(self.read_only))


def duckdb_bootstrap_statements(config: DuckDBConfig | None = None) -> list[str]:
    """Generate post-init DuckDB `SET` statements for the given `DuckDBConfig`."""
    cfg = config or DuckDBConfig()
    return cfg.bootstrap_statements()
