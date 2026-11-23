"""DuckDB SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.db.adapters.duckdb.config import DuckDBSecretConfig
from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)
from vertebra.utils.text import quote_identifier

if TYPE_CHECKING:
    from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, SyncDuckDBDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.config import DuckDBAsyncConfig, DuckDBSyncConfig
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver, DuckDBSyncDriver

__all__ = (
    "DuckDBAsyncConnectionContext",
    "DuckDBAsyncSessionContext",
    "DuckDBConnectionParams",
    "DuckDBDriverFeatures",
    "DuckDBPoolParams",
    "DuckDBSyncConnectionContext",
    "DuckDBSyncSessionContext",
)

_MISSING = object()
_SYSTEM_DATABASES: frozenset[str] = frozenset({"memory", "system", "temp"})


class DuckDBConnectionParams(TypedDict, total=False):
    """Typed connection parameters accepted by `DuckDBSyncConfig` and `DuckDBAsyncConfig`."""

    database: NotRequired[str]
    """Target database name, catalog, or file path."""
    path: NotRequired[str]
    """Filesystem path to the local database file."""
    db: NotRequired[str]
    """Alternative database name alias."""
    file: NotRequired[str]
    """Database file path alias."""
    url: NotRequired[str]
    """DuckDB connection URL string."""
    dsn: NotRequired[str]
    """DuckDB connection DSN string."""
    memory_id: NotRequired[str]
    """Shared named in-memory DuckDB identifier."""
    read_only: NotRequired[bool]
    """Open the database connection in read-only mode."""
    config: NotRequired[dict[str, Any]]
    """Raw DuckDB configuration settings dictionary."""
    memory_limit: NotRequired[str | int]
    """DuckDB per-instance memory limit string or megabytes."""
    memory_limit_mb: NotRequired[int]
    """DuckDB per-instance memory limit in megabytes."""
    threads: NotRequired[int]
    """Worker thread count allocated to DuckDB execution."""
    temp_directory: NotRequired[str | None]
    """Spill directory path for DuckDB out-of-core execution."""
    parquet_metadata_cache: NotRequired[bool]
    """Enable in-memory Parquet metadata caching in DuckDB."""
    httpfs_connection_caching: NotRequired[bool]
    """Reuse HTTP/S3 keep-alive connections across DuckDB reads."""
    enable_http_metadata_cache: NotRequired[bool]
    """Enable HTTP metadata caching in DuckDB httpfs."""
    statement_cache_size: NotRequired[int]
    """Maximum cached prepared statements per connection."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class DuckDBPoolParams(DuckDBConnectionParams, total=False):
    """Typed pool and registry-broker parameters extending `DuckDBConnectionParams`."""

    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    pool_min_size: NotRequired[int]
    """Alias for minimum warm pool connections."""
    pool_max_size: NotRequired[int]
    """Alias for maximum pool connections."""
    pool_recycle: NotRequired[int]
    """Maximum connection lifetime in seconds before recycling."""
    pool_recycle_seconds: NotRequired[int]
    """Alias for connection recycle lifetime in seconds."""
    health_check_interval: NotRequired[float]
    """Interval in seconds between idle connection health checks."""
    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""


class DuckDBDriverFeatures(TypedDict, total=False):
    """Typed feature flags and lifecycle callbacks for DuckDB SQLSpec adapters."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    extensions: NotRequired[Sequence[str | Mapping[str, Any]]]
    """DuckDB extensions to install or load on startup."""
    secrets: NotRequired[Sequence[DuckDBSecretConfig | Mapping[str, Any]]]
    """DuckDB CREATE SECRET definitions configured on startup."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    enable_numpy_arrays: NotRequired[bool]
    """Return numeric DuckDB vector columns as NumPy arrays."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel storage and dispatch."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend implementation identifier."""
    enable_uuid_conversion: NotRequired[bool]
    """Convert UUID strings/bytes into Python ``uuid.UUID`` objects."""
    enable_statement_profiling: NotRequired[bool]
    """Enable DuckDB per-query JSON profiling output."""
    extension_flags: NotRequired[dict[str, Any]]
    """Boolean feature toggles controlling extension behaviors."""


def _is_unopened_native_driver(conn: Any) -> bool:
    """Return whether `conn` is a closed or uninitialized native DuckDB driver."""
    return getattr(conn, "_client", _MISSING) is None or bool(getattr(conn, "closed", False))


def _render_extension_statements(entry: str | Mapping[str, Any]) -> tuple[str, str, bool]:
    """Return `(load_sql, install_sql, required)` for a DuckDB extension specification."""
    if isinstance(entry, str):
        cleaned = entry.strip()
        parts = cleaned.split()
        if len(parts) >= 3 and parts[1].upper() == "FROM":
            name = quote_identifier(parts[0])
            repo = parts[2].replace("'", "''")
            return f"LOAD {name}", f"INSTALL {name} FROM '{repo}'; LOAD {name}", True
        name = quote_identifier(cleaned)
        return f"LOAD {name}", f"INSTALL {name}; LOAD {name}", True

    name_raw = str(entry.get("name", "")).strip()
    if not name_raw:
        return "", "", False
    name = quote_identifier(name_raw)
    required = bool(entry.get("required", True))
    install_parts = ["INSTALL", name]
    repo_raw = entry.get("repository_url") or entry.get("repository")
    if isinstance(repo_raw, str) and repo_raw.strip():
        escaped_repo = repo_raw.strip().replace("'", "''")
        install_parts.append(f"FROM '{escaped_repo}'")
    version = entry.get("version")
    if isinstance(version, str) and version.strip():
        escaped_version = version.strip().replace("'", "''")
        install_parts.append(f"VERSION '{escaped_version}'")
    force_install = bool(entry.get("force_install"))
    prefix = "FORCE " if force_install else ""
    install_sql = f"{prefix}{' '.join(install_parts)}; LOAD {name}"
    explicit_install = bool(entry.get("install") or repo_raw or version or force_install)
    load_sql = install_sql if explicit_install else f"LOAD {name}"
    return load_sql, install_sql, required


def _extension_already_configured(conn: Any, entry: str | Mapping[str, Any]) -> bool:
    """Return whether a native `DuckDBConfig` already bootstrapped `entry` at driver startup."""
    cfg = getattr(conn, "config", None)
    configured = getattr(cfg, "extensions", None)
    if not configured:
        return False
    if isinstance(entry, str):
        return entry.strip() in configured
    name = str(entry.get("name", "")).strip()
    return bool(name and name in configured)


def _secret_already_configured(conn: Any, secret: DuckDBSecretConfig) -> bool:
    """Return whether a static secret was already created at native driver startup."""
    if secret.value_provider is not None:
        return False
    cfg = getattr(conn, "config", None)
    if cfg is None or not hasattr(cfg, "normalized_secrets"):
        return False
    existing_names = {s.name.lower() for s in cfg.normalized_secrets() if s.value_provider is None}
    return secret.name.lower() in existing_names


def _apply_duckdb_extensions_sync(conn: Any, config: Any) -> None:
    """Load configured `driver_features['extensions']` synchronously when not already loaded."""
    extensions = config.driver_features.get("extensions")
    if not extensions or not callable(getattr(conn, "execute", None)):
        return
    for entry in extensions:
        if _extension_already_configured(conn, entry):
            continue
        load_sql, install_sql, required = _render_extension_statements(entry)
        if not load_sql:
            continue
        try:
            conn.execute(load_sql)
        except Exception:
            if install_sql == load_sql:
                if required:
                    raise
                continue
            try:
                conn.execute(install_sql)
            except Exception:
                if required:
                    raise


async def _apply_duckdb_extensions_async(conn: Any, config: Any) -> None:
    """Load configured `driver_features['extensions']` asynchronously when not already loaded."""
    extensions = config.driver_features.get("extensions")
    if not extensions or not callable(getattr(conn, "execute", None)):
        return
    for entry in extensions:
        if _extension_already_configured(conn, entry):
            continue
        load_sql, install_sql, required = _render_extension_statements(entry)
        if not load_sql:
            continue
        try:
            res = conn.execute(load_sql)
            if inspect.isawaitable(res):
                await res
        except Exception:
            if install_sql == load_sql:
                if required:
                    raise
                continue
            try:
                res = conn.execute(install_sql)
                if inspect.isawaitable(res):
                    await res
            except Exception:
                if required:
                    raise


def _apply_duckdb_secrets_sync(conn: Any, config: Any) -> None:
    """Provision `driver_features['secrets']` and refresh native dynamic secrets synchronously."""
    secrets = config.driver_features.get("secrets")
    if secrets and callable(getattr(conn, "execute", None)):
        for raw_secret in secrets:
            secret = DuckDBSecretConfig.from_mapping(raw_secret)
            if _secret_already_configured(conn, secret):
                continue
            dynamic_values: Mapping[str, Any] | None = None
            try:
                if secret.value_provider is not None:
                    provided = secret.value_provider()
                    if inspect.isawaitable(provided):
                        msg = "Async secret value_provider cannot be invoked on a synchronous DuckDB connection"
                        raise TypeError(msg)
                    dynamic_values = provided
                conn.execute(secret.to_create_secret_sql(dynamic_values))
            except Exception:
                if secret.required:
                    raise
    refresh_fn = getattr(conn, "refresh_secrets", None)
    if callable(refresh_fn) and getattr(conn, "_dynamic_secrets", None):
        refresh_fn(force=False)


async def _apply_duckdb_secrets_async(conn: Any, config: Any) -> None:
    """Provision `driver_features['secrets']` and refresh native dynamic secrets asynchronously."""
    secrets = config.driver_features.get("secrets")
    if secrets and callable(getattr(conn, "execute", None)):
        for raw_secret in secrets:
            secret = DuckDBSecretConfig.from_mapping(raw_secret)
            if _secret_already_configured(conn, secret):
                continue
            dynamic_values: Mapping[str, Any] | None = None
            try:
                if secret.value_provider is not None:
                    provided = secret.value_provider()
                    if inspect.isawaitable(provided):
                        provided = await provided
                    dynamic_values = provided
                res = conn.execute(secret.to_create_secret_sql(dynamic_values))
                if inspect.isawaitable(res):
                    await res
            except Exception:
                if secret.required:
                    raise
    refresh_fn = getattr(conn, "refresh_secrets", None)
    if callable(refresh_fn) and getattr(conn, "_dynamic_secrets", None):
        refresh_res = refresh_fn(force=False)
        if inspect.isawaitable(refresh_res):
            await refresh_res


def _capture_baseline_databases_sync(conn: Any, config: Any) -> None:
    """Capture baseline attached DuckDB database names and default catalog on first connection init."""
    baselines: dict[int, tuple[frozenset[str], str]] | None = getattr(config, "_baseline_databases", None)
    if baselines is None or id(conn) in baselines or not callable(getattr(conn, "select", None)):
        return
    with contextlib.suppress(Exception):
        db_rows = conn.select("SELECT database_name FROM duckdb_databases() WHERE NOT internal")
        names = frozenset(
            str(row["database_name"])
            for row in db_rows
            if isinstance(row, Mapping) and isinstance(row.get("database_name"), str)
        )
        cur_rows = conn.select("SELECT current_database() AS db")
        default_db = ""
        if cur_rows and isinstance(cur_rows[0], Mapping) and isinstance(cur_rows[0].get("db"), str):
            default_db = cur_rows[0]["db"]
        if names:
            baselines[id(conn)] = (names, default_db)


async def _capture_baseline_databases_async(conn: Any, config: Any) -> None:
    """Asynchronously capture baseline attached DuckDB database names and default catalog."""
    baselines: dict[int, tuple[frozenset[str], str]] | None = getattr(config, "_baseline_databases", None)
    if baselines is None or id(conn) in baselines or not callable(getattr(conn, "select", None)):
        return
    with contextlib.suppress(Exception):
        db_rows = conn.select("SELECT database_name FROM duckdb_databases() WHERE NOT internal")
        if inspect.isawaitable(db_rows):
            db_rows = await db_rows
        names = frozenset(
            str(row["database_name"])
            for row in db_rows
            if isinstance(row, Mapping) and isinstance(row.get("database_name"), str)
        )
        cur_rows = conn.select("SELECT current_database() AS db")
        if inspect.isawaitable(cur_rows):
            cur_rows = await cur_rows
        default_db = ""
        if cur_rows and isinstance(cur_rows[0], Mapping) and isinstance(cur_rows[0].get("db"), str):
            default_db = cur_rows[0]["db"]
        if names:
            baselines[id(conn)] = (names, default_db)


def _baseline_reset_statements(config: Any) -> list[str]:
    """Return baseline `RESET search_path` and configured `SET` statements for DuckDB session reset."""
    statements = ["RESET search_path"]
    driver_cfg = getattr(config, "driver_config", None)
    if driver_cfg is None:
        return statements
    threads = getattr(driver_cfg, "threads", None)
    if isinstance(threads, int):
        statements.append(f"SET threads = {threads}")
    memory_limit_mb = getattr(driver_cfg, "memory_limit_mb", None)
    if isinstance(memory_limit_mb, int):
        statements.append(f"SET memory_limit = '{memory_limit_mb}MB'")
    raw_settings = getattr(driver_cfg, "config", None)
    if isinstance(raw_settings, Mapping):
        for key, val in raw_settings.items():
            clean_key = DuckDBSecretConfig._validate_identifier(str(key), "config.key")
            escaped_val = str(val).replace("'", "''")
            statements.append(f"SET {clean_key} = '{escaped_val}'")
    return statements


def _rollback_duckdb_connection_sync(conn: Any) -> None:
    """Roll back any active synchronous transaction on `conn`."""
    in_tx = getattr(conn, "in_transaction", False)
    if callable(in_tx):
        in_tx = in_tx()
    if not in_tx:
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        rollback_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        execute_fn("ROLLBACK")


def _reset_duckdb_connection_sync(conn: Any, config: Any) -> None:
    """Drop temp objects, detach extra databases, and reset session settings synchronously."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        reset_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if not callable(execute_fn) or _is_unopened_native_driver(conn):
        return
    if callable(getattr(conn, "select", None)):
        with contextlib.suppress(Exception):
            for row in conn.select(
                "SELECT function_name, function_type FROM duckdb_functions() WHERE database_name = 'temp' AND NOT internal"
            ):
                if isinstance(row, Mapping) and isinstance(row.get("function_name"), str):
                    keyword = "MACRO TABLE" if row.get("function_type") == "table_macro" else "MACRO"
                    execute_fn(f"DROP {keyword} IF EXISTS temp.main.{quote_identifier(row['function_name'])}")
            for query, key, keyword in (
                ("SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal", "view_name", "VIEW"),
                ("SELECT table_name FROM duckdb_tables() WHERE temporary", "table_name", "TABLE"),
                ("SELECT sequence_name FROM duckdb_sequences() WHERE temporary", "sequence_name", "SEQUENCE"),
            ):
                for row in conn.select(query):
                    if isinstance(row, Mapping) and isinstance(row.get(key), str) and row[key]:
                        execute_fn(f"DROP {keyword} IF EXISTS temp.main.{quote_identifier(row[key])}")
            baselines: dict[int, tuple[frozenset[str], str]] | None = getattr(config, "_baseline_databases", None)
            baseline = baselines.get(id(conn)) if baselines is not None else None
            if baseline is not None:
                names, default_db = baseline
                if default_db:
                    execute_fn(f"USE {quote_identifier(default_db)}")
                for row in conn.select("SELECT database_name FROM duckdb_databases() WHERE NOT internal"):
                    db_name = row.get("database_name") if isinstance(row, Mapping) else None
                    if isinstance(db_name, str) and db_name not in names and db_name not in _SYSTEM_DATABASES:
                        execute_fn(f"DETACH DATABASE {quote_identifier(db_name)}")
    for stmt in _baseline_reset_statements(config):
        execute_fn(stmt)


async def _rollback_duckdb_connection_async(conn: Any) -> None:
    """Roll back any active asynchronous transaction on `conn`."""
    in_tx = getattr(conn, "in_transaction", False)
    if callable(in_tx):
        in_tx = in_tx()
    if not in_tx:
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        with CancelScope(shield=True):
            res = execute_fn("ROLLBACK")
            if inspect.isawaitable(res):
                await res


async def _reset_duckdb_connection_async(conn: Any, config: Any) -> None:
    """Drop temp objects, detach extra databases, and reset session settings asynchronously."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if not callable(execute_fn) or _is_unopened_native_driver(conn):
        return
    if callable(getattr(conn, "select", None)):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            macro_rows = conn.select(
                "SELECT function_name, function_type FROM duckdb_functions() WHERE database_name = 'temp' AND NOT internal"
            )
            if inspect.isawaitable(macro_rows):
                macro_rows = await macro_rows
            for row in macro_rows:
                if isinstance(row, Mapping) and isinstance(row.get("function_name"), str):
                    keyword = "MACRO TABLE" if row.get("function_type") == "table_macro" else "MACRO"
                    drop_res = execute_fn(
                        f"DROP {keyword} IF EXISTS temp.main.{quote_identifier(row['function_name'])}"
                    )
                    if inspect.isawaitable(drop_res):
                        await drop_res
            for query, key, keyword in (
                ("SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal", "view_name", "VIEW"),
                ("SELECT table_name FROM duckdb_tables() WHERE temporary", "table_name", "TABLE"),
                ("SELECT sequence_name FROM duckdb_sequences() WHERE temporary", "sequence_name", "SEQUENCE"),
            ):
                rows = conn.select(query)
                if inspect.isawaitable(rows):
                    rows = await rows
                for row in rows:
                    if isinstance(row, Mapping) and isinstance(row.get(key), str) and row[key]:
                        drop_res = execute_fn(f"DROP {keyword} IF EXISTS temp.main.{quote_identifier(row[key])}")
                        if inspect.isawaitable(drop_res):
                            await drop_res
            baselines: dict[int, tuple[frozenset[str], str]] | None = getattr(config, "_baseline_databases", None)
            baseline = baselines.get(id(conn)) if baselines is not None else None
            if baseline is not None:
                names, default_db = baseline
                if default_db:
                    use_res = execute_fn(f"USE {quote_identifier(default_db)}")
                    if inspect.isawaitable(use_res):
                        await use_res
                db_rows = conn.select("SELECT database_name FROM duckdb_databases() WHERE NOT internal")
                if inspect.isawaitable(db_rows):
                    db_rows = await db_rows
                for row in db_rows:
                    db_name = row.get("database_name") if isinstance(row, Mapping) else None
                    if isinstance(db_name, str) and db_name not in names and db_name not in _SYSTEM_DATABASES:
                        detach_res = execute_fn(f"DETACH DATABASE {quote_identifier(db_name)}")
                        if inspect.isawaitable(detach_res):
                            await detach_res
    with CancelScope(shield=True):
        for stmt in _baseline_reset_statements(config):
            res = execute_fn(stmt)
            if inspect.isawaitable(res):
                await res


class DuckDBSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring DuckDB drivers/leases and running lifecycle hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "DuckDB"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: DuckDBSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _on_first_acquire(self, conn: Any) -> None:
        _capture_baseline_databases_sync(conn, self._config)
        _apply_duckdb_extensions_sync(conn, self._config)
        _apply_duckdb_secrets_sync(conn, self._config)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_duckdb_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_duckdb_connection_sync(conn, self._config)

    def __enter__(self) -> SyncDuckDBDriver:
        return cast("SyncDuckDBDriver", super().__enter__())


class DuckDBAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring DuckDB drivers/leases and running lifecycle hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "DuckDB"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: DuckDBAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _on_first_acquire(self, conn: Any) -> None:
        await _capture_baseline_databases_async(conn, self._config)
        await _apply_duckdb_extensions_async(conn, self._config)
        await _apply_duckdb_secrets_async(conn, self._config)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_duckdb_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_duckdb_connection_async(conn, self._config)

    async def __aenter__(self) -> AsyncDuckDBDriver:
        return cast("AsyncDuckDBDriver", await super().__aenter__())


class DuckDBSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager building a fresh `DuckDBSyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = DuckDBSyncConnectionContext
    _engine_label: ClassVar[str] = "DuckDB"
    _strict_cleanup: ClassVar[bool] = True

    def __enter__(self) -> DuckDBSyncDriver:
        return cast("DuckDBSyncDriver", super().__enter__())


class DuckDBAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager building a fresh `DuckDBAsyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = DuckDBAsyncConnectionContext
    _engine_label: ClassVar[str] = "DuckDB"
    _strict_cleanup: ClassVar[bool] = True

    async def __aenter__(self) -> DuckDBAsyncDriver:
        return cast("DuckDBAsyncDriver", await super().__aenter__())
