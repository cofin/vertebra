"""Configuration and secret structs for the embedded DuckDB analytical engine."""

from __future__ import annotations

import math
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import msgspec

from vertebra._vertebra import InterfaceError
from vertebra.db.driver._common import DUCKDB_DIALECT_TAG, BaseDatabaseConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.text import validate_identifier

__all__ = (
    "DuckDBConfig",
    "DuckDBSecretConfig",
    "duckdb_bootstrap_statements",
)

_SENSITIVE_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "access_token",
        "account_key",
        "api_key",
        "bearer_token",
        "client_secret",
        "connection_string",
        "key",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "session_token",
        "token",
    }
)


class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=True):
    """Cloud-agnostic DuckDB `CREATE SECRET` declaration aligned with `sqlspec`."""

    name: str
    """Secret name matched case-insensitively by DuckDB."""
    secret_type: str = "s3"
    """DuckDB secret type (`"s3"`, `"gcs"`, `"r2"`, `"azure"`, `"http"`, `"bearer"`, `"iceberg"`, etc.)."""
    value: dict[str, Any] = msgspec.field(default_factory=dict)
    """Static secret key/value settings (such as `key_id`, `secret`, `region`, `endpoint`, or `bearer_token`)."""
    value_provider: Callable[[], Mapping[str, Any] | Awaitable[Mapping[str, Any]]] | None = None
    """Optional sync or async callable returning dynamic secret key/value settings on bootstrap and refresh."""
    refresh_interval_seconds: float | None = None
    """Optional interval in seconds after which `value_provider` is re-evaluated (`1800.0` default when `value_provider` is set)."""
    provider: str | None = None
    """Optional DuckDB secret provider (such as `"config"` or `"credential_chain"`)."""
    scope: str | tuple[str, ...] | None = None
    """Optional URI prefix or tuple of URI prefixes restricting where the secret applies."""
    persistent: bool = False
    """When True, create a `PERSISTENT SECRET` stored in DuckDB's configured secret directory."""
    replace: bool = True
    """When True, emit `CREATE OR REPLACE` so rotated credentials overwrite existing secrets."""
    required: bool = True
    """When True, fail loudly when secret creation or `value_provider` resolution fails."""

    def __post_init__(self) -> None:
        if self.persistent and self.value_provider is not None:
            msg = f"DuckDB secret {self.name!r} cannot set persistent=True with a dynamic value_provider"
            raise ImproperConfigurationError(msg)
        if self.refresh_interval_seconds is not None and (
            isinstance(self.refresh_interval_seconds, bool)
            or not math.isfinite(self.refresh_interval_seconds)
            or self.refresh_interval_seconds <= 0
        ):
            msg = f"DuckDB secret {self.name!r} refresh_interval_seconds must be a positive finite number"
            raise ImproperConfigurationError(msg)

    def __repr__(self) -> str:
        """Render a debug representation with sensitive secret values redacted."""
        redacted_value = {
            k: ("<redacted>" if self._is_sensitive_secret_key(k) and v not in ("", None) else v)
            for k, v in self.value.items()
        }
        has_provider = self.value_provider is not None
        return (
            f"DuckDBSecretConfig(name={self.name!r}, secret_type={self.secret_type!r}, "
            f"value={redacted_value!r}, has_value_provider={has_provider!r}, "
            f"refresh_interval_seconds={self.refresh_interval_seconds!r}, provider={self.provider!r}, "
            f"scope={self.scope!r}, persistent={self.persistent!r}, replace={self.replace!r}, "
            f"required={self.required!r})"
        )

    @classmethod
    def from_mapping(cls, raw: DuckDBSecretConfig | Mapping[str, Any]) -> Self:
        """Normalize a `DuckDBSecretConfig` or `sqlspec`-compatible mapping into `DuckDBSecretConfig`."""
        if isinstance(raw, cls):
            return raw
        if not isinstance(raw, Mapping):
            msg = "DuckDB secret configuration must be a DuckDBSecretConfig or mapping"
            raise ImproperConfigurationError(msg)
        data = dict(raw)
        raw_scope = data.get("scope")
        if isinstance(raw_scope, Sequence) and not isinstance(raw_scope, (str, bytes, bytearray)):
            data["scope"] = tuple(str(item) for item in raw_scope)
        raw_value = data.get("value")
        if raw_value is None:
            data["value"] = {}
        elif isinstance(raw_value, Mapping):
            data["value"] = {str(k): v for k, v in raw_value.items()}
        else:
            msg = "DuckDB secret `value` must be a mapping"
            raise ImproperConfigurationError(msg)
        if "name" not in data or "secret_type" not in data:
            msg = "DuckDB secret mapping requires both `name` and `secret_type`"
            raise ImproperConfigurationError(msg)
        return cls(**data)

    def effective_refresh_interval(self) -> float:
        """Return the active refresh interval in seconds when `value_provider` is configured."""
        if self.refresh_interval_seconds is not None:
            return float(self.refresh_interval_seconds)
        return 1800.0

    def to_create_secret_sql(self, dynamic_values: Mapping[str, Any] | None = None) -> str:
        """Render a DuckDB `CREATE [OR REPLACE] [PERSISTENT] SECRET` DDL statement."""
        secret_name = self._validate_identifier(self.name, "secret.name")
        secret_type = self._validate_identifier(self.secret_type, "secret.secret_type")
        parts = [f"TYPE {secret_type}"]
        if self.provider is not None:
            provider_ident = self._validate_identifier(self.provider, "secret.provider")
            parts.append(f"PROVIDER {provider_ident}")

        merged_values: dict[str, Any] = dict(self.value)
        if dynamic_values is not None:
            if not isinstance(dynamic_values, Mapping):
                msg = "DuckDB secret `value_provider` must return a mapping of secret options"
                raise ImproperConfigurationError(msg)
            for key, val in dynamic_values.items():
                merged_values[str(key)] = val

        for raw_key, raw_val in merged_values.items():
            if raw_val is None:
                continue
            option_key = self._validate_identifier(str(raw_key), "secret.value_key").upper()
            if isinstance(raw_val, str) and not raw_val.strip() and self._is_sensitive_secret_key(raw_key):
                msg = f"DuckDB secret {secret_name!r} option {option_key} must not be empty or whitespace"
                raise ImproperConfigurationError(msg)
            parts.append(f"{option_key} {self._format_literal(raw_val, f'secret.{raw_key}')}")

        if self.scope is not None:
            formatted_scope = self._format_scope(self.scope)
            if formatted_scope is not None:
                parts.append(f"SCOPE {formatted_scope}")

        kind = "PERSISTENT SECRET" if self.persistent else "SECRET"
        should_replace = self.replace or self.value_provider is not None
        header = (
            f"CREATE OR REPLACE {kind} {secret_name}"
            if should_replace
            else f"CREATE {kind} IF NOT EXISTS {secret_name}"
        )
        inner = ", ".join(parts)
        return f"{header} ({inner})"

    @staticmethod
    def _is_sensitive_secret_key(key: str) -> bool:
        """Return True when `key` represents credential material that must be redacted and non-blank."""
        lowered = key.strip().lower()
        return lowered in _SENSITIVE_SECRET_KEYS or lowered.endswith(("_secret", "_token", "_password", "_key"))

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> str:
        """Validate a single SQL identifier for DuckDB DDL/SET configuration."""
        DuckDBConfig._reject_nul(value, field_name)
        cleaned = value.strip()
        try:
            return validate_identifier(cleaned, f"DuckDB {field_name}", allow_dots=False)
        except ValueError as exc:
            raise InterfaceError(str(exc)) from exc

    @staticmethod
    def _format_literal(value: Any, field_name: str) -> str:
        """Format a scalar or sequence DuckDB secret option literal safely."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            items = [f"'{DuckDBConfig._escape_literal(str(item), field_name)}'" for item in value]
            return f"[{', '.join(items)}]"
        return f"'{DuckDBConfig._escape_literal(str(value), field_name)}'"

    @staticmethod
    def _format_scope(scope: str | Sequence[str]) -> str | None:
        """Format a single URI prefix or sequence of URI prefixes for `SCOPE`."""
        if isinstance(scope, str):
            escaped = DuckDBConfig._escape_literal(scope, "secret.scope")
            return f"'{escaped}'" if scope else None
        items = [f"'{DuckDBConfig._escape_literal(str(item), 'secret.scope')}'" for item in scope if str(item)]
        if not items:
            return None
        if len(items) == 1:
            return items[0]
        return f"[{', '.join(items)}]"


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
    temp_directory: str | None = None
    """Optional spill directory configured via `SET temp_directory`."""
    parquet_metadata_cache: bool = True
    """When True, enable `SET parquet_metadata_cache = true` after connection bootstrap."""
    httpfs_connection_caching: bool = True
    """When True, keep remote HTTP/object-storage connections warm (`SET http_keep_alive = true`)."""
    enable_http_metadata_cache: bool = True
    """When True, cache remote HTTP/Parquet `HEAD` metadata (`SET enable_http_metadata_cache = true`)."""
    read_only: bool = False
    """When True, open the DuckDB file in read-only mode allowing multi-process readers."""
    secrets: tuple[DuckDBSecretConfig | Mapping[str, Any], ...] = ()
    """Optional DuckDB secrets (`DuckDBSecretConfig` or `sqlspec`-compatible secret mappings) provisioned on sessions."""
    extensions: tuple[str, ...] = ()
    """Optional DuckDB extensions (`"<ext>"` or `"<ext> FROM <repo>"`) loaded on every pooled session."""
    init_statements: tuple[str, ...] = ()
    """Optional SQL statements executed after bootstrap on every pooled session."""
    config: dict[str, str] = msgspec.field(default_factory=dict)
    """Optional DuckDB session `SET` key/value overrides."""
    pool_min_size: int = 1
    """Minimum number of pooled DuckDB cursors per worker."""
    pool_max_size: int = 10
    """Maximum number of pooled DuckDB cursors per worker."""
    statement_cache_size: int = 256
    """Maximum number of prepared analytical statements cached per worker."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` coordinating single-writer/multi-reader leases."""

    def normalized_secrets(self) -> tuple[DuckDBSecretConfig, ...]:
        """Return all configured secrets normalized as `DuckDBSecretConfig` structs."""
        return tuple(DuckDBSecretConfig.from_mapping(item) for item in self.secrets)

    def resolved_path(self) -> str:
        """Resolve the effective database path from `path` or `database`."""
        target = self.path if self.path != ":memory:" else self.database
        return self._reject_nul(target, "path")

    def bootstrap_statements(self) -> list[str]:
        """Return post-initialization `SET` and static `CREATE SECRET` statements."""
        resolved = self.resolved_path()
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "duckdb_bootstrap_statements"):
            raw_stmts = native.duckdb_bootstrap_statements(
                resolved,
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
        if self.temp_directory is not None:
            escaped_dir = self._escape_literal(self.temp_directory, "temp_directory")
            stmts.append(f"SET temp_directory = '{escaped_dir}'")
        for key, val in self.config.items():
            clean_key = DuckDBSecretConfig._validate_identifier(str(key), "config.key")
            escaped_val = self._escape_literal(str(val), "config.value")
            stmts.append(f"SET {clean_key} = '{escaped_val}'")
        for secret in self.normalized_secrets():
            if secret.value_provider is None:
                stmts.append(secret.to_create_secret_sql())
        for stmt in self.init_statements:
            self._reject_nul(stmt, "init_statements")
            stmts.append(stmt)
        return stmts

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> Self:
        """Construct a `DuckDBConfig` from a `duckdb://` URI."""
        cls._reject_nul(url, "url")
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

    @staticmethod
    def _reject_nul(value: str, field_name: str) -> str:
        """Raise `InterfaceError` when `value` contains an embedded NUL byte."""
        if "\0" in value:
            msg = f"DuckDB {field_name} must not contain NUL bytes"
            raise InterfaceError(msg)
        return value

    @staticmethod
    def _escape_literal(value: str, field_name: str) -> str:
        """Reject NUL, CR, and LF characters and double single quotes for SQL literals."""
        DuckDBConfig._reject_nul(value, field_name)
        if "\r" in value or "\n" in value:
            msg = f"DuckDB {field_name} must not contain CR or LF characters"
            raise InterfaceError(msg)
        return value.replace("'", "''")


def duckdb_bootstrap_statements(config: DuckDBConfig | None = None) -> list[str]:
    """Generate post-init DuckDB `SET` statements for the given `DuckDBConfig`."""
    cfg = config or DuckDBConfig()
    return cfg.bootstrap_statements()
