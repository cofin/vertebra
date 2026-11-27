"""Standalone SQL, SQLAlchemy `+vtb`, and SQLSpec adapter benchmark comparators."""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sqlite3
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import unquote, urlsplit

import msgspec

from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    DEFAULT_MAX_LATENCY_RATIO,
    DEFAULT_MIN_THROUGHPUT_RATIO,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    DriverMode,
    assert_free_threaded_gil_disabled,
    compare_adapter_reports,
    compute_adapter_case_stats,
    format_adapter_report_bytes,
    from_postgres_report,
    parse_adapter_report_bytes,
    redact_target_uri,
    sample_async,
    sample_sync,
)
from tools.benchmarks.postgres import capture_live_report as capture_postgres_live_report
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, SyncSpannerDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.registry import DatabaseRegistry
from vertebra.utils.module_loader import import_optional

type MySQLEngine = Literal["mysql", "mariadb"]
type SqlSpecEngine = Literal["sqlite", "duckdb", "postgres", "mysql", "oracle", "mssql"]
type AdvancedAlchemyEngine = Literal["sqlite", "postgres"]
type ComparatorStatus = Literal["pass", "fail", "allowed", "missing", "skipped"]

SUPPORTED_SQL_ENGINES: Final[tuple[str, ...]] = (
    "postgres",
    "mysql",
    "mariadb",
    "sqlite",
    "duckdb",
    "oracle",
    "mssql",
    "spanner",
    "bigquery",
    "sqlalchemy",
    "sqlspec",
    "advanced_alchemy",
)

COMPARATOR_PAIRS: Final[dict[str, tuple[str, ...]]] = {
    "vertebra-sqlite-sync": ("sqlite3",),
    "vertebra-sqlite-async": ("aiosqlite",),
    "vertebra-duckdb-sync": ("duckdb",),
    "vertebra-mysql-sync": ("pymysql", "MySQLdb"),
    "vertebra-mariadb-sync": ("pymysql", "MySQLdb"),
    "vertebra-mysql-async": ("aiomysql",),
    "vertebra-mariadb-async": ("aiomysql",),
    "vertebra-oracle-sync": ("oracledb-sync",),
    "vertebra-oracle-async": ("oracledb-async",),
    "vertebra-mssql-sync": ("pymssql", "pyodbc"),
    "vertebra-spanner-sync": ("google-cloud-spanner-sync",),
    "vertebra-bigquery-sync": ("google-cloud-bigquery-sync",),
    "sqlalchemy-vtb-sync": ("sqlalchemy-vanilla-sync",),
    "vertebra-sqlspec-sqlite-sync": ("sqlspec-sqlite-sync",),
    "vertebra-sqlspec-duckdb-sync": ("sqlspec-duckdb-sync",),
    "vertebra-sqlspec-postgres-async": ("sqlspec-asyncpg",),
    "vertebra-aa-sqlite-sync": ("vanilla-aa-sqlite-sync",),
    "vertebra-aa-sqlite-async": ("vanilla-aa-sqlite-async",),
    "vertebra-aa-postgres-sync": ("vanilla-aa-postgres-sync",),
    "vertebra-aa-postgres-async": ("vanilla-aa-postgres-async",),
    "vertebra-sync": ("psycopg-sync",),
    "vertebra-async": ("asyncpg",),
}

COMPARATOR_ALLOW_LIST: Final[dict[str, str]] = {
    "vertebra-bigquery-sync": "Emulator round trip dominates and drifts with job history.",
    "vertebra-spanner-sync": "Emulator round trip dominates the measurement.",
    "vertebra-mssql-sync": "Nearly all time is inside the engine round trip; results sit within run-to-run variation.",
    "vertebra-duckdb-sync": "Bundled libduckdb-sys cc build omits DuckDB jemalloc until upstream ships it on crates.io.",
    "vertebra-sqlspec-duckdb-sync": (
        "Bundled libduckdb-sys cc build omits DuckDB jemalloc until upstream ships it on crates.io."
    ),
    "vertebra-oracle-sync": (
        "Upstream oracle/rust-oracledb#101 clears the statement cache when AUTH_MAX_OPEN_CURSORS is absent."
    ),
    "vertebra-oracle-async": (
        "Upstream oracle/rust-oracledb#101 clears the statement cache when AUTH_MAX_OPEN_CURSORS is absent."
    ),
}

COMPARATORLESS_CASES: Final[dict[tuple[str, str], str]] = {
    ("vertebra-async", "pipeline_select_16"): "No comparator driver runs a pipelined statement case.",
}


class ComparatorCheckOutcome(msgspec.Struct, frozen=True, gc=False):
    """Comparison outcome between one Vertebra case and its comparator driver."""

    engine: str
    case: str
    vertebra_driver: str
    comparator_driver: str | None
    vertebra_median_ms: float
    comparator_median_ms: float | None
    median_ratio: float | None
    status: ComparatorStatus
    reason: str | None = None


_SQLSPEC_ENGINES: Final[tuple[SqlSpecEngine, ...]] = (
    "sqlite",
    "duckdb",
    "postgres",
    "mysql",
    "oracle",
    "mssql",
)
_ADVANCED_ALCHEMY_ENGINES: Final[tuple[AdvancedAlchemyEngine, ...]] = (
    "sqlite",
    "postgres",
)

_SQLITE_MULTI_ROW_QUERY: Final[str] = (
    "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10) SELECT x FROM cnt"
)
_DUCKDB_SCAN_QUERY: Final[str] = "SELECT sum(i) FROM range(1000) t(i)"

__all__ = (
    "COMPARATORLESS_CASES",
    "COMPARATOR_ALLOW_LIST",
    "COMPARATOR_PAIRS",
    "SUPPORTED_SQL_ENGINES",
    "AdvancedAlchemyEngine",
    "ComparatorCheckOutcome",
    "ComparatorStatus",
    "MySQLEngine",
    "SqlSpecEngine",
    "capture_advanced_alchemy_report",
    "capture_bigquery_report",
    "capture_duckdb_report",
    "capture_mssql_report",
    "capture_mysql_report",
    "capture_oracle_report",
    "capture_postgres_adapter_report",
    "capture_spanner_report",
    "capture_sqlalchemy_report",
    "capture_sqlite_report",
    "capture_sqlspec_report",
    "check_report_against_comparators",
    "main",
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_sampling(iterations: int, warmup: int) -> None:
    if iterations < 1 or warmup < 0:
        msg = f"iterations must be >= 1 and warmup must be >= 0 (got iterations={iterations}, warmup={warmup})"
        raise ValueError(msg)


def _strip_vtb_dialect_suffix(url: str) -> str:
    if "://" not in url:
        return url
    scheme, remainder = url.split("://", 1)
    if "+" not in scheme:
        return url
    base_scheme, driver_part = scheme.split("+", 1)
    if driver_part not in {"vtb", "vtb_async"}:
        return url
    return f"{base_scheme}://{remainder}"


def _resolve_sqlite_config(database: str) -> SQLiteConfig:
    cfg = SQLiteConfig.from_url(database) if "://" in database else SQLiteConfig(database=database)
    if database == ":memory:" and cfg.pool_key() is not None:
        msg = "SQLiteConfig(database=':memory:').pool_key() must remain None for isolated in-memory databases"
        raise RuntimeError(msg)
    return cfg


def _resolve_duckdb_config(database: str) -> DuckDBConfig:
    cfg = DuckDBConfig.from_url(database) if "://" in database else DuckDBConfig(database=database)
    if database == ":memory:" and cfg.pool_key() is not None:
        msg = "DuckDBConfig(database=':memory:').pool_key() must remain None for isolated in-memory databases"
        raise RuntimeError(msg)
    return cfg


def _mysql_connect_kwargs(dsn: str) -> dict[str, Any]:
    parsed = urlsplit(dsn)
    kwargs: dict[str, Any] = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
    }
    if parsed.username:
        kwargs["user"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)
    database = unquote(parsed.path.lstrip("/"))
    if database:
        kwargs["database"] = database
    return kwargs


def _oracle_connect_kwargs(dsn: str) -> dict[str, Any]:
    parsed = urlsplit(dsn)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1521
    service = unquote(parsed.path.lstrip("/")) or "FREEPDB1"
    kwargs: dict[str, Any] = {"dsn": f"{host}:{port}/{service}"}
    if parsed.username:
        kwargs["user"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)
    return kwargs


def _mssql_connect_kwargs(dsn: str) -> dict[str, Any]:
    parsed = urlsplit(dsn)
    kwargs: dict[str, Any] = {
        "server": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 1433,
    }
    if parsed.username:
        kwargs["user"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)
    database = unquote(parsed.path.lstrip("/"))
    if database:
        kwargs["database"] = database
    return kwargs


async def capture_postgres_adapter_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Delegate live PostgreSQL benchmarks to `tools.benchmarks.postgres` and convert to adapter schema."""
    _validate_sampling(iterations, warmup)
    assert_free_threaded_gil_disabled(strict=strict_free_threading)
    pg_report = await capture_postgres_live_report(
        dsn,
        iterations=iterations,
        warmup=warmup,
        include_external=include_external,
    )
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    return from_postgres_report(pg_report, gil_enabled=gil_enabled)


async def capture_sqlite_report(
    database: str = ":memory:",
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra SQLite drivers and a reused native cursor against stdlib sqlite3 and optional aiosqlite."""
    _validate_sampling(iterations, warmup)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cfg = _resolve_sqlite_config(database)
    db_path = str(cfg.path)
    cases: list[AdapterBenchmarkCase] = []

    with SyncSQLiteDriver(cfg) as sync_drv:
        sync_point = sample_sync(lambda: sync_drv.select_one("SELECT ?", (1,)), iterations=iterations, warmup=warmup)
        sync_multi = sample_sync(lambda: sync_drv.select(_SQLITE_MULTI_ROW_QUERY), iterations=iterations, warmup=warmup)
        native_cursor = sync_drv.cursor()
        try:
            sync_cursor_point = sample_sync(
                lambda: native_cursor.execute("SELECT ?", (1,)).fetchone(),
                iterations=iterations,
                warmup=warmup,
            )
        finally:
            native_cursor.close()
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-sqlite-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_point,
            gil_enabled=gil_enabled,
        )
    )
    cases.append(
        compute_adapter_case_stats(
            case="multi_row_select_10",
            driver="vertebra-sqlite-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_multi,
            gil_enabled=gil_enabled,
        )
    )
    cases.append(
        compute_adapter_case_stats(
            case="cursor_point_select_1",
            driver="vertebra-sqlite-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_cursor_point,
            gil_enabled=gil_enabled,
        )
    )

    async with AsyncSQLiteDriver(cfg) as async_drv:
        async_point = await sample_async(
            lambda: async_drv.select_one("SELECT ?", (1,)),
            iterations=iterations,
            warmup=warmup,
        )
        async_multi = await sample_async(
            lambda: async_drv.select(_SQLITE_MULTI_ROW_QUERY),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-sqlite-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_point,
            gil_enabled=gil_enabled,
        )
    )
    cases.append(
        compute_adapter_case_stats(
            case="multi_row_select_10",
            driver="vertebra-sqlite-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_multi,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        conn = sqlite3.connect(db_path)
        stdlib_cursor = conn.cursor()
        try:
            stdlib_point = sample_sync(
                lambda: conn.execute("SELECT ?", (1,)).fetchone(),
                iterations=iterations,
                warmup=warmup,
            )
            stdlib_multi = sample_sync(
                lambda: conn.execute(_SQLITE_MULTI_ROW_QUERY).fetchall(),
                iterations=iterations,
                warmup=warmup,
            )
            stdlib_cursor_point = sample_sync(
                lambda: stdlib_cursor.execute("SELECT ?", (1,)).fetchone(),
                iterations=iterations,
                warmup=warmup,
            )
        finally:
            stdlib_cursor.close()
            conn.close()
        cases.append(
            compute_adapter_case_stats(
                case="point_select_1",
                driver="sqlite3",
                category="sql",
                driver_mode="sync_blocking",
                latencies_ns=stdlib_point,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case="multi_row_select_10",
                driver="sqlite3",
                category="sql",
                driver_mode="sync_blocking",
                latencies_ns=stdlib_multi,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case="cursor_point_select_1",
                driver="sqlite3",
                category="sql",
                driver_mode="sync_blocking",
                latencies_ns=stdlib_cursor_point,
                gil_enabled=gil_enabled,
            )
        )

        aiosqlite_mod = import_optional("aiosqlite")
        if aiosqlite_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            async with aiosqlite_mod.connect(db_path) as aio_conn:

                async def _aio_point() -> Any:
                    async with aio_conn.execute("SELECT ?", (1,)) as cur:
                        return await cur.fetchone()

                async def _aio_multi() -> Any:
                    async with aio_conn.execute(_SQLITE_MULTI_ROW_QUERY) as cur:
                        return await cur.fetchall()

                aio_point_samples = await sample_async(_aio_point, iterations=iterations, warmup=warmup)
                aio_multi_samples = await sample_async(_aio_multi, iterations=iterations, warmup=warmup)

            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="aiosqlite",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=aio_point_samples,
                    gil_enabled=gil_enabled,
                )
            )
            cases.append(
                compute_adapter_case_stats(
                    case="multi_row_select_10",
                    driver="aiosqlite",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=aio_multi_samples,
                    gil_enabled=gil_enabled,
                )
            )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="sqlite",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(database),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def capture_duckdb_report(
    database: str = ":memory:",
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra DuckDB drivers against optional third-party `duckdb`."""
    _validate_sampling(iterations, warmup)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cfg = _resolve_duckdb_config(database)
    db_path = cfg.resolved_path()
    cases: list[AdapterBenchmarkCase] = []

    with SyncDuckDBDriver(cfg) as sync_drv:
        sync_point = sample_sync(lambda: sync_drv.select_one("SELECT ?", (1,)), iterations=iterations, warmup=warmup)
        sync_scan = sample_sync(lambda: sync_drv.select_one(_DUCKDB_SCAN_QUERY), iterations=iterations, warmup=warmup)
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-duckdb-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_point,
            gil_enabled=gil_enabled,
        )
    )
    cases.append(
        compute_adapter_case_stats(
            case="analytical_scan_1000",
            driver="vertebra-duckdb-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_scan,
            gil_enabled=gil_enabled,
        )
    )

    async with AsyncDuckDBDriver(cfg) as async_drv:
        async_point = await sample_async(
            lambda: async_drv.select_one("SELECT ?", (1,)),
            iterations=iterations,
            warmup=warmup,
        )
        async_scan = await sample_async(
            lambda: async_drv.select_one(_DUCKDB_SCAN_QUERY),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-duckdb-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_point,
            gil_enabled=gil_enabled,
        )
    )
    cases.append(
        compute_adapter_case_stats(
            case="analytical_scan_1000",
            driver="vertebra-duckdb-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_scan,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        duckdb_mod = import_optional("duckdb")
        if duckdb_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            ext_conn = duckdb_mod.connect(db_path)
            try:
                ext_point = sample_sync(
                    lambda: ext_conn.execute("SELECT ?", [1]).fetchone(),
                    iterations=iterations,
                    warmup=warmup,
                )
                ext_scan = sample_sync(
                    lambda: ext_conn.execute(_DUCKDB_SCAN_QUERY).fetchone(),
                    iterations=iterations,
                    warmup=warmup,
                )
            finally:
                ext_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="duckdb",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=ext_point,
                    gil_enabled=gil_enabled,
                )
            )
            cases.append(
                compute_adapter_case_stats(
                    case="analytical_scan_1000",
                    driver="duckdb",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=ext_scan,
                    gil_enabled=gil_enabled,
                )
            )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="duckdb",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(database),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def capture_mysql_report(
    dsn: str,
    *,
    engine: MySQLEngine = "mysql",
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra MySQL/MariaDB drivers against optional `aiomysql`, `pymysql`, and `MySQLdb`."""
    _validate_sampling(iterations, warmup)
    if engine not in ("mysql", "mariadb"):
        msg = f"Unsupported MySQL engine variant: {engine!r}"
        raise ValueError(msg)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cfg = MySQLConfig.from_url(dsn)
    cases: list[AdapterBenchmarkCase] = []

    with SyncMySQLDriver(cfg) as sync_drv:
        sync_point = sample_sync(lambda: sync_drv.select_one("SELECT 1"), iterations=iterations, warmup=warmup)
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver=f"vertebra-{engine}-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_point,
            gil_enabled=gil_enabled,
        )
    )

    async with AsyncMySQLDriver(cfg) as async_drv:
        async_point = await sample_async(
            lambda: async_drv.select_one("SELECT 1"),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver=f"vertebra-{engine}-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_point,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        conn_kwargs = _mysql_connect_kwargs(dsn)

        aiomysql_mod = import_optional("aiomysql")
        if aiomysql_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            aio_kwargs = {k: v for k, v in conn_kwargs.items() if k != "database"}
            if "database" in conn_kwargs:
                aio_kwargs["db"] = conn_kwargs["database"]
            aio_conn = await aiomysql_mod.connect(**aio_kwargs)
            try:

                async def _aiomysql_select() -> Any:
                    async with aio_conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        return await cur.fetchone()

                aio_samples = await sample_async(_aiomysql_select, iterations=iterations, warmup=warmup)
            finally:
                aio_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="aiomysql",
                    category="sql",
                    driver_mode="async_native",
                    latencies_ns=aio_samples,
                    gil_enabled=gil_enabled,
                )
            )

        pymysql_mod = import_optional("pymysql")
        if pymysql_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            py_conn = pymysql_mod.connect(**conn_kwargs)
            try:

                def _pymysql_select() -> Any:
                    with py_conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        return cur.fetchone()

                py_samples = sample_sync(_pymysql_select, iterations=iterations, warmup=warmup)
            finally:
                py_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="pymysql",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=py_samples,
                    gil_enabled=gil_enabled,
                )
            )

        mysqldb_mod = import_optional("MySQLdb")
        if mysqldb_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            c_kwargs = {k: v for k, v in conn_kwargs.items() if k != "database"}
            if "database" in conn_kwargs:
                c_kwargs["db"] = conn_kwargs["database"]
            c_conn = mysqldb_mod.connect(**c_kwargs)
            try:

                def _mysqldb_select() -> Any:
                    cur = c_conn.cursor()
                    try:
                        cur.execute("SELECT 1")
                        return cur.fetchone()
                    finally:
                        cur.close()

                c_samples = sample_sync(_mysqldb_select, iterations=iterations, warmup=warmup)
            finally:
                c_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="MySQLdb",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=c_samples,
                    gil_enabled=gil_enabled,
                )
            )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine=engine,
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def capture_oracle_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
    enable_reset_relay: bool = True,
) -> AdapterBenchmarkReport:
    """Measure Vertebra Oracle drivers on `SELECT 1 FROM DUAL` against optional `oracledb`.

    Set `enable_reset_relay=False` to disable the in-process TNS relay on both Vertebra Oracle drivers.
    """
    _validate_sampling(iterations, warmup)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cfg = msgspec.structs.replace(OracleConfig.from_url(dsn), enable_reset_relay=enable_reset_relay)
    cases: list[AdapterBenchmarkCase] = []

    with SyncOracleDriver(cfg) as sync_drv:
        sync_point = sample_sync(
            lambda: sync_drv.select_one("SELECT 1 FROM DUAL"),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-oracle-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_point,
            gil_enabled=gil_enabled,
        )
    )

    async with AsyncOracleDriver(cfg) as async_drv:
        async_point = await sample_async(
            lambda: async_drv.select_one("SELECT 1 FROM DUAL"),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-oracle-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_point,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        oracledb_mod = import_optional("oracledb")
        if oracledb_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            ora_kwargs = _oracle_connect_kwargs(dsn)

            with oracledb_mod.connect(**ora_kwargs) as sync_conn:

                def _oracledb_sync() -> Any:
                    with sync_conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM DUAL")
                        return cur.fetchone()

                sync_samples = sample_sync(_oracledb_sync, iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="oracledb-sync",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=sync_samples,
                    gil_enabled=gil_enabled,
                )
            )

            connect_async: Any = getattr(oracledb_mod, "connect_async", None)
            if connect_async is not None:
                async_conn = await connect_async(**ora_kwargs)
                try:

                    async def _oracledb_async() -> Any:
                        with async_conn.cursor() as cur:
                            await cur.execute("SELECT 1 FROM DUAL")
                            return await cur.fetchone()

                    async_samples = await sample_async(_oracledb_async, iterations=iterations, warmup=warmup)
                finally:
                    await async_conn.close()
                cases.append(
                    compute_adapter_case_stats(
                        case="point_select_1",
                        driver="oracledb-async",
                        category="sql",
                        driver_mode="async_native",
                        latencies_ns=async_samples,
                        gil_enabled=gil_enabled,
                    )
                )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="oracle",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def capture_mssql_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra MSSQL drivers on `SELECT 1` against optional `pymssql` and `pyodbc`."""
    _validate_sampling(iterations, warmup)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cfg = MSSQLConfig.from_url(dsn)
    cases: list[AdapterBenchmarkCase] = []

    with SyncMSSQLDriver(cfg) as sync_drv:
        sync_point = sample_sync(lambda: sync_drv.select_one("SELECT 1"), iterations=iterations, warmup=warmup)
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-mssql-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=sync_point,
            gil_enabled=gil_enabled,
        )
    )

    async with AsyncMSSQLDriver(cfg) as async_drv:
        async_point = await sample_async(
            lambda: async_drv.select_one("SELECT 1"),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-mssql-async",
            category="sql",
            driver_mode="async_native",
            latencies_ns=async_point,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        ms_kwargs = _mssql_connect_kwargs(dsn)

        pymssql_mod = import_optional("pymssql")
        if pymssql_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            pyms_conn = pymssql_mod.connect(**ms_kwargs)
            try:

                def _pymssql_select() -> Any:
                    cur = pyms_conn.cursor()
                    try:
                        cur.execute("SELECT 1")
                        return cur.fetchone()
                    finally:
                        cur.close()

                pyms_samples = sample_sync(_pymssql_select, iterations=iterations, warmup=warmup)
            finally:
                pyms_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="pymssql",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=pyms_samples,
                    gil_enabled=gil_enabled,
                )
            )

        pyodbc_mod = import_optional("pyodbc")
        if pyodbc_mod is not None:
            gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
            conn_str_parts = [
                "DRIVER={ODBC Driver 18 for SQL Server}",
                f"SERVER={ms_kwargs['server']},{ms_kwargs['port']}",
                "TrustServerCertificate=yes",
            ]
            if "database" in ms_kwargs:
                conn_str_parts.append(f"DATABASE={ms_kwargs['database']}")
            if "user" in ms_kwargs:
                conn_str_parts.append(f"UID={ms_kwargs['user']}")
            if "password" in ms_kwargs:
                conn_str_parts.append(f"PWD={ms_kwargs['password']}")
            odbc_conn = pyodbc_mod.connect(";".join(conn_str_parts))
            try:

                def _pyodbc_select() -> Any:
                    cur = odbc_conn.cursor()
                    try:
                        cur.execute("SELECT 1")
                        return cur.fetchone()
                    finally:
                        cur.close()

                odbc_samples = sample_sync(_pyodbc_select, iterations=iterations, warmup=warmup)
            finally:
                odbc_conn.close()
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="pyodbc",
                    category="sql",
                    driver_mode="sync_blocking",
                    latencies_ns=odbc_samples,
                    gil_enabled=gil_enabled,
                )
            )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="mssql",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def _capture_emulated_cloud_report(
    engine: str,
    dsn: str,
    sync_driver: Any,
    async_driver: Any,
    external_driver_label: str,
    external_query: Callable[[], Any] | None,
    *,
    iterations: int,
    warmup: int,
    strict_free_threading: bool,
) -> AdapterBenchmarkReport:
    """Measure `SELECT 1` on Vertebra sync and async drivers plus an optional official client.

    Emulator latency drifts as its job history grows, so the drivers take turns one query at a
    time instead of running back-to-back blocks; every driver then sees the same emulator state.
    """
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    sync_samples: list[int] = []
    async_samples: list[int] = []
    external_samples: list[int] = []

    with sync_driver as sync_drv:
        async with async_driver as async_drv:
            for _ in range(warmup):
                sync_drv.select_one("SELECT 1")
                await async_drv.select_one("SELECT 1")
                if external_query is not None:
                    external_query()
            for _ in range(iterations):
                started = time.perf_counter_ns()
                sync_drv.select_one("SELECT 1")
                sync_samples.append(max(1, time.perf_counter_ns() - started))
                started = time.perf_counter_ns()
                await async_drv.select_one("SELECT 1")
                async_samples.append(max(1, time.perf_counter_ns() - started))
                if external_query is not None:
                    started = time.perf_counter_ns()
                    external_query()
                    external_samples.append(max(1, time.perf_counter_ns() - started))

    measured: list[tuple[str, DriverMode, list[int]]] = [
        (f"vertebra-{engine}-sync", "sync_blocking", sync_samples),
        (f"vertebra-{engine}-async", "async_native", async_samples),
    ]
    if external_samples:
        measured.append((external_driver_label, "sync_blocking", external_samples))
    cases = [
        compute_adapter_case_stats(
            case="point_select_1",
            driver=driver,
            category="sql",
            driver_mode=driver_mode,
            latencies_ns=samples,
            gil_enabled=gil_enabled,
        )
        for driver, driver_mode, samples in measured
    ]

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine=engine,
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def capture_spanner_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra Spanner drivers on `SELECT 1` against optional `google-cloud-spanner`.

    The official client only speaks plaintext gRPC when `SPANNER_EMULATOR_HOST` is set, so an
    `http://` endpoint in `dsn` is exported under that variable for the duration of the comparison.
    """
    _validate_sampling(iterations, warmup)
    cfg = SpannerConfig.from_url(dsn)
    external_query: Callable[[], Any] | None = None
    previous_emulator_host = os.environ.get("SPANNER_EMULATOR_HOST")
    spanner_mod = import_optional("google.cloud.spanner") if include_external else None
    credentials_mod = import_optional("google.auth.credentials") if include_external else None
    try:
        if spanner_mod is not None and credentials_mod is not None:
            if cfg.endpoint.startswith("http://"):
                os.environ["SPANNER_EMULATOR_HOST"] = urlsplit(cfg.endpoint).netloc
            client = spanner_mod.Client(project=cfg.project_id, credentials=credentials_mod.AnonymousCredentials())
            database = client.instance(cfg.instance_id).database(cfg.database_id)

            def _official_select() -> Any:
                with database.snapshot() as snapshot:
                    return list(snapshot.execute_sql("SELECT 1"))

            external_query = _official_select
        return await _capture_emulated_cloud_report(
            "spanner",
            dsn,
            SyncSpannerDriver(cfg),
            AsyncSpannerDriver(cfg),
            "google-cloud-spanner-sync",
            external_query,
            iterations=iterations,
            warmup=warmup,
            strict_free_threading=strict_free_threading,
        )
    finally:
        if previous_emulator_host is None:
            os.environ.pop("SPANNER_EMULATOR_HOST", None)
        else:
            os.environ["SPANNER_EMULATOR_HOST"] = previous_emulator_host


async def capture_bigquery_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra BigQuery drivers on `SELECT 1` against optional `google-cloud-bigquery`."""
    _validate_sampling(iterations, warmup)
    cfg = BigQueryConfig.from_url(dsn)
    external_query: Callable[[], Any] | None = None
    bigquery_mod = import_optional("google.cloud.bigquery") if include_external else None
    credentials_mod = import_optional("google.auth.credentials") if include_external else None
    options_mod = import_optional("google.api_core.client_options") if include_external else None
    if bigquery_mod is not None and credentials_mod is not None and options_mod is not None:
        client = bigquery_mod.Client(
            project=cfg.effective_project_id(),
            credentials=credentials_mod.AnonymousCredentials(),
            client_options=options_mod.ClientOptions(api_endpoint=cfg.rest_endpoint),
        )

        def _official_select() -> Any:
            return list(client.query_and_wait("SELECT 1"))

        external_query = _official_select
    return await _capture_emulated_cloud_report(
        "bigquery",
        dsn,
        SyncBigQueryDriver(cfg),
        AsyncBigQueryDriver(cfg),
        "google-cloud-bigquery-sync",
        external_query,
        iterations=iterations,
        warmup=warmup,
        strict_free_threading=strict_free_threading,
    )


async def capture_sqlalchemy_report(
    url: str,
    *,
    use_broker_pool: bool = False,
    broker: Any = None,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure SQLAlchemy `+vtb` dialect (with optional `BrokerPool`) against vanilla SQLAlchemy."""
    _validate_sampling(iterations, warmup)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)

    sqlalchemy_mod = import_optional("sqlalchemy")
    sa_pool_mod = import_optional("sqlalchemy.pool")
    if sqlalchemy_mod is None or sa_pool_mod is None:
        msg = "sqlalchemy is required to run capture_sqlalchemy_report"
        raise RuntimeError(msg)

    vtb_sa_mod = import_optional("vertebra.extensions.sqlalchemy")
    vtb_pool_mod = import_optional("vertebra.extensions.sqlalchemy.pool")
    if vtb_sa_mod is None or vtb_pool_mod is None:
        msg = "vertebra.extensions.sqlalchemy is required to run capture_sqlalchemy_report"
        raise RuntimeError(msg)

    if use_broker_pool:
        resolved_broker = broker if broker is not None else DatabaseRegistry.default_broker()
        if resolved_broker is None:
            msg = "use_broker_pool=True requires an explicit broker or DatabaseRegistry.default_broker()"
            raise ValueError(msg)
        poolclass = vtb_pool_mod.BrokerPool.bind_class(broker=resolved_broker, url=url)
    else:
        poolclass = sa_pool_mod.NullPool

    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    cases: list[AdapterBenchmarkCase] = []

    vtb_engine = sqlalchemy_mod.create_engine(url, poolclass=poolclass)
    try:
        with vtb_engine.connect() as conn:
            vtb_samples = sample_sync(
                lambda: conn.execute(sqlalchemy_mod.text("SELECT 1")).scalar_one(),
                iterations=iterations,
                warmup=warmup,
            )
    finally:
        vtb_engine.dispose()

    cases.append(
        compute_adapter_case_stats(
            case="sa_point_select_1",
            driver="sqlalchemy-vtb-sync",
            category="orm",
            driver_mode="sync_blocking",
            latencies_ns=vtb_samples,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        vanilla_url = _strip_vtb_dialect_suffix(url)
        vanilla_engine = sqlalchemy_mod.create_engine(vanilla_url, poolclass=sa_pool_mod.NullPool)
        try:
            with vanilla_engine.connect() as conn:
                vanilla_samples = sample_sync(
                    lambda: conn.execute(sqlalchemy_mod.text("SELECT 1")).scalar_one(),
                    iterations=iterations,
                    warmup=warmup,
                )
        finally:
            vanilla_engine.dispose()

        cases.append(
            compute_adapter_case_stats(
                case="sa_point_select_1",
                driver="sqlalchemy-vanilla-sync",
                category="orm",
                driver_mode="sync_blocking",
                latencies_ns=vanilla_samples,
                gil_enabled=gil_enabled,
            )
        )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="orm",
        engine="sqlalchemy",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(url),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


def _sqlspec_connection_config(engine: SqlSpecEngine, dsn: str) -> dict[str, Any]:
    if engine in ("sqlite", "duckdb"):
        return {"url": dsn} if "://" in dsn else {"database": dsn}
    return {"dsn": dsn}


def _sqlspec_adapter_classes(engine: SqlSpecEngine) -> tuple[type[Any], type[Any]]:
    class_map: dict[SqlSpecEngine, tuple[str, str, str]] = {
        "sqlite": ("vertebra.extensions.sqlspec.adapters.sqlite", "SQLiteSyncConfig", "SQLiteAsyncConfig"),
        "duckdb": ("vertebra.extensions.sqlspec.adapters.duckdb", "DuckDBSyncConfig", "DuckDBAsyncConfig"),
        "postgres": ("vertebra.extensions.sqlspec.adapters.postgres", "PostgresSyncConfig", "PostgresAsyncConfig"),
        "mysql": ("vertebra.extensions.sqlspec.adapters.mysql", "MySQLSyncConfig", "MySQLAsyncConfig"),
        "oracle": ("vertebra.extensions.sqlspec.adapters.oracle", "OracleSyncConfig", "OracleAsyncConfig"),
        "mssql": ("vertebra.extensions.sqlspec.adapters.mssql", "MSSQLSyncConfig", "MSSQLAsyncConfig"),
    }
    module_name, sync_name, async_name = class_map[engine]
    mod = import_optional(module_name)
    if mod is None:
        msg = f"{module_name} is unavailable for capture_sqlspec_report"
        raise RuntimeError(msg)
    return getattr(mod, sync_name), getattr(mod, async_name)


async def _measure_upstream_sqlspec_cases(
    engine: SqlSpecEngine,
    dsn: str,
    query: str,
    *,
    iterations: int,
    warmup: int,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    cases: list[AdapterBenchmarkCase] = []
    if engine == "sqlite":
        sqlite_mod = import_optional("sqlspec.adapters.sqlite")
        if sqlite_mod is not None and hasattr(sqlite_mod, "SqliteConfig"):
            upstream_cfg = sqlite_mod.SqliteConfig(connection_config={"database": dsn})
            with upstream_cfg.provide_session() as session:
                samples = sample_sync(lambda: session.select_one(query), iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="sqlspec-sqlite-sync",
                    category="orm",
                    driver_mode="sync_blocking",
                    latencies_ns=samples,
                    gil_enabled=gil_enabled,
                )
            )
    elif engine == "duckdb":
        duckdb_mod = import_optional("sqlspec.adapters.duckdb")
        if duckdb_mod is not None and hasattr(duckdb_mod, "DuckDBConfig"):
            upstream_cfg = duckdb_mod.DuckDBConfig(connection_config={"database": dsn})
            with upstream_cfg.provide_session() as session:
                samples = sample_sync(lambda: session.select_one(query), iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="sqlspec-duckdb-sync",
                    category="orm",
                    driver_mode="sync_blocking",
                    latencies_ns=samples,
                    gil_enabled=gil_enabled,
                )
            )
    elif engine == "postgres":
        asyncpg_mod = import_optional("sqlspec.adapters.asyncpg")
        if asyncpg_mod is not None and hasattr(asyncpg_mod, "AsyncpgConfig"):
            upstream_cfg = asyncpg_mod.AsyncpgConfig(connection_config={"dsn": dsn})
            async with upstream_cfg.provide_session() as session:
                samples = await sample_async(lambda: session.select_one(query), iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case="point_select_1",
                    driver="sqlspec-asyncpg",
                    category="orm",
                    driver_mode="async_native",
                    latencies_ns=samples,
                    gil_enabled=gil_enabled,
                )
            )
    return cases


async def capture_sqlspec_report(
    engine: SqlSpecEngine,
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra SQLSpec sync and async adapters against optional upstream `sqlspec` comparators."""
    _validate_sampling(iterations, warmup)
    if engine not in _SQLSPEC_ENGINES:
        msg = f"Unsupported SQLSpec engine: {engine!r}"
        raise ValueError(msg)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)

    sqlspec_mod = import_optional("sqlspec")
    if sqlspec_mod is None:
        msg = "sqlspec is required to run capture_sqlspec_report"
        raise RuntimeError(msg)

    sync_cls, async_cls = _sqlspec_adapter_classes(engine)
    conn_cfg = _sqlspec_connection_config(engine, dsn)
    query = "SELECT 1 FROM DUAL" if engine == "oracle" else "SELECT 1"
    cases: list[AdapterBenchmarkCase] = []

    sync_cfg = sync_cls(connection_config=conn_cfg)
    with sync_cfg.provide_session() as sync_session:
        sync_samples = sample_sync(lambda: sync_session.select_one(query), iterations=iterations, warmup=warmup)
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver=f"vertebra-sqlspec-{engine}-sync",
            category="orm",
            driver_mode="sync_blocking",
            latencies_ns=sync_samples,
            gil_enabled=gil_enabled,
        )
    )

    async_cfg = async_cls(connection_config=conn_cfg)
    async with async_cfg.provide_session() as async_session:
        async_samples = await sample_async(
            lambda: async_session.select_one(query),
            iterations=iterations,
            warmup=warmup,
        )
    cases.append(
        compute_adapter_case_stats(
            case="point_select_1",
            driver=f"vertebra-sqlspec-{engine}-async",
            category="orm",
            driver_mode="async_native",
            latencies_ns=async_samples,
            gil_enabled=gil_enabled,
        )
    )

    if include_external:
        gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
        upstream_cases = await _measure_upstream_sqlspec_cases(
            engine,
            dsn,
            query,
            iterations=iterations,
            warmup=warmup,
            gil_enabled=gil_enabled,
        )
        cases.extend(upstream_cases)

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="orm",
        engine=f"sqlspec:{engine}",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


def _rewrite_aa_url(engine: AdvancedAlchemyEngine, dsn: str, *, dialect: str) -> str:
    """Rewrite a SQLite path/URI or PostgreSQL DSN into an explicit SQLAlchemy URL."""
    cleaned = dsn.strip()
    if engine == "sqlite":
        from tools.benchmarks.apps._db_drivers import normalize_sqlite_target

        target = normalize_sqlite_target(cleaned)
        if target == ":memory:":
            return f"{dialect}:///:memory:"
        return f"{dialect}:///{target}"
    _, remainder = cleaned.split("://", 1) if "://" in cleaned else ("postgresql", cleaned)
    return f"{dialect}://{remainder}"


def _build_aa_bench_types() -> tuple[Any, Any, Any, Any, Any]:
    """Build declarative ``aa_bench_world`` model and Advanced Alchemy sync/async services."""
    sa_mod = import_optional("sqlalchemy")
    sa_orm_mod = import_optional("sqlalchemy.orm")
    aa_repo_mod = import_optional("advanced_alchemy.repository")
    aa_svc_mod = import_optional("advanced_alchemy.service")
    aa_filters_mod = import_optional("advanced_alchemy.filters")
    if sa_mod is None or sa_orm_mod is None or aa_repo_mod is None or aa_svc_mod is None or aa_filters_mod is None:
        msg = "sqlalchemy and advanced_alchemy are required to run capture_advanced_alchemy_report"
        raise RuntimeError(msg)

    base_cls = type("_BenchBase", (sa_orm_mod.DeclarativeBase,), {})
    model_cls = type(
        "_BenchWorld",
        (base_cls,),
        {
            "__tablename__": "aa_bench_world",
            "id": sa_orm_mod.mapped_column(sa_mod.Integer, primary_key=True, autoincrement=False),
            "randomnumber": sa_orm_mod.mapped_column(sa_mod.Integer, nullable=False),
        },
    )
    sync_repo_cls = type("_SyncRepo", (aa_repo_mod.SQLAlchemySyncRepository,), {"model_type": model_cls})
    async_repo_cls = type("_AsyncRepo", (aa_repo_mod.SQLAlchemyAsyncRepository,), {"model_type": model_cls})
    sync_svc_cls = type(
        "_SyncService",
        (aa_svc_mod.SQLAlchemySyncRepositoryService,),
        {"repository_type": sync_repo_cls},
    )
    async_svc_cls = type(
        "_AsyncService",
        (aa_svc_mod.SQLAlchemyAsyncRepositoryService,),
        {"repository_type": async_repo_cls},
    )

    limit_filter_cls = getattr(aa_filters_mod, "LimitOffsetFilter", None) or aa_filters_mod.LimitOffset
    return base_cls, sync_svc_cls, async_svc_cls, limit_filter_cls, sa_mod


def _seed_aa_bench_sync(engine: Any, base_cls: Any, sa_mod: Any, engine_kind: AdvancedAlchemyEngine) -> None:
    """Idempotently create and seed 10 rows in ``aa_bench_world`` using a sync engine."""
    base_cls.metadata.create_all(engine)
    insert_sql = (
        "INSERT OR IGNORE INTO aa_bench_world (id, randomnumber) VALUES (:id, :rn)"
        if engine_kind == "sqlite"
        else "INSERT INTO aa_bench_world (id, randomnumber) VALUES (:id, :rn) ON CONFLICT (id) DO NOTHING"
    )
    with engine.begin() as conn:
        conn.execute(sa_mod.text(insert_sql), [{"id": idx, "rn": idx * 10} for idx in range(1, 11)])


async def _seed_aa_bench_async(engine: Any, base_cls: Any, sa_mod: Any, engine_kind: AdvancedAlchemyEngine) -> None:
    """Idempotently create and seed 10 rows in ``aa_bench_world`` using an async engine."""
    insert_sql = (
        "INSERT OR IGNORE INTO aa_bench_world (id, randomnumber) VALUES (:id, :rn)"
        if engine_kind == "sqlite"
        else "INSERT INTO aa_bench_world (id, randomnumber) VALUES (:id, :rn) ON CONFLICT (id) DO NOTHING"
    )
    async with engine.begin() as conn:
        await conn.run_sync(base_cls.metadata.create_all)
        await conn.execute(sa_mod.text(insert_sql), [{"id": idx, "rn": idx * 10} for idx in range(1, 11)])


async def capture_advanced_alchemy_report(
    engine: AdvancedAlchemyEngine,
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Measure Vertebra ``+vtb``/``+vtb_async`` Advanced Alchemy services against vanilla SQLAlchemy baselines."""
    _validate_sampling(iterations, warmup)
    if engine not in _ADVANCED_ALCHEMY_ENGINES:
        msg = f"Unsupported Advanced Alchemy engine: {engine!r}"
        raise ValueError(msg)
    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)

    base_cls, sync_service_cls, async_service_cls, limit_offset_cls, sa_mod = _build_aa_bench_types()
    sa_orm_mod = import_optional("sqlalchemy.orm")
    sa_async_mod = import_optional("sqlalchemy.ext.asyncio")
    vtb_sa_mod = import_optional("vertebra.extensions.sqlalchemy")
    if sa_orm_mod is None or sa_async_mod is None or vtb_sa_mod is None:
        msg = "sqlalchemy, sqlalchemy.ext.asyncio, and vertebra.extensions.sqlalchemy are required"
        raise RuntimeError(msg)

    limit_filter = limit_offset_cls(limit=10, offset=0)
    cases: list[AdapterBenchmarkCase] = []

    def _run_sync_suite(url: str, driver_label: str) -> None:
        sync_engine = sa_mod.create_engine(url)
        try:
            _seed_aa_bench_sync(sync_engine, base_cls, sa_mod, engine)
            session_factory = sa_orm_mod.sessionmaker(bind=sync_engine, expire_on_commit=False)
            with session_factory() as session:
                service = sync_service_cls(session=session)
                point_samples = sample_sync(lambda: service.get(1), iterations=iterations, warmup=warmup)
                multi_samples = sample_sync(
                    lambda: service.get_many(limit_filter), iterations=iterations, warmup=warmup
                )
        finally:
            sync_engine.dispose()
        cases.append(
            compute_adapter_case_stats(
                case="aa_point_select_1",
                driver=driver_label,
                category="orm",
                driver_mode="sync_blocking",
                latencies_ns=point_samples,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case="aa_multi_row_select_10",
                driver=driver_label,
                category="orm",
                driver_mode="sync_blocking",
                latencies_ns=multi_samples,
                gil_enabled=gil_enabled,
            )
        )

    async def _run_async_suite(url: str, driver_label: str) -> None:
        async_engine = sa_async_mod.create_async_engine(url)
        try:
            await _seed_aa_bench_async(async_engine, base_cls, sa_mod, engine)
            session_factory = sa_async_mod.async_sessionmaker(bind=async_engine, expire_on_commit=False)
            async with session_factory() as session:
                service = async_service_cls(session=session)
                point_samples = await sample_async(lambda: service.get(1), iterations=iterations, warmup=warmup)
                multi_samples = await sample_async(
                    lambda: service.get_many(limit_filter),
                    iterations=iterations,
                    warmup=warmup,
                )
        finally:
            await async_engine.dispose()
        cases.append(
            compute_adapter_case_stats(
                case="aa_point_select_1",
                driver=driver_label,
                category="orm",
                driver_mode="async_native",
                latencies_ns=point_samples,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case="aa_multi_row_select_10",
                driver=driver_label,
                category="orm",
                driver_mode="async_native",
                latencies_ns=multi_samples,
                gil_enabled=gil_enabled,
            )
        )

    vtb_sync_dialect = "sqlite+vtb" if engine == "sqlite" else "postgresql+vtb"
    vtb_async_dialect = "sqlite+vtb_async" if engine == "sqlite" else "postgresql+vtb_async"
    _run_sync_suite(_rewrite_aa_url(engine, dsn, dialect=vtb_sync_dialect), f"vertebra-aa-{engine}-sync")
    await _run_async_suite(_rewrite_aa_url(engine, dsn, dialect=vtb_async_dialect), f"vertebra-aa-{engine}-async")

    if include_external:
        if engine == "sqlite":
            _run_sync_suite(_rewrite_aa_url(engine, dsn, dialect="sqlite"), "vanilla-aa-sqlite-sync")
            if import_optional("aiosqlite") is not None:
                gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
                await _run_async_suite(
                    _rewrite_aa_url(engine, dsn, dialect="sqlite+aiosqlite"),
                    "vanilla-aa-sqlite-async",
                )
        else:
            if import_optional("psycopg") is not None:
                gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
                _run_sync_suite(
                    _rewrite_aa_url(engine, dsn, dialect="postgresql+psycopg"),
                    "vanilla-aa-postgres-sync",
                )
            if import_optional("asyncpg") is not None:
                gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
                await _run_async_suite(
                    _rewrite_aa_url(engine, dsn, dialect="postgresql+asyncpg"),
                    "vanilla-aa-postgres-async",
                )

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="orm",
        engine=f"advanced_alchemy:{engine}",
        generated_at=_utc_now_iso(),
        target_redacted=redact_target_uri(dsn),
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


async def _dispatch_capture(args: argparse.Namespace) -> AdapterBenchmarkReport:
    engine: str = args.engine
    include_external = not args.no_external
    common_kwargs: dict[str, Any] = {
        "iterations": args.iterations,
        "warmup": args.warmup,
        "include_external": include_external,
        "strict_free_threading": args.strict_free_threading,
    }
    if engine == "postgres":
        return await capture_postgres_adapter_report(args.dsn, **common_kwargs)
    if engine == "sqlite":
        return await capture_sqlite_report(args.dsn, **common_kwargs)
    if engine == "duckdb":
        return await capture_duckdb_report(args.dsn, **common_kwargs)
    if engine in ("mysql", "mariadb"):
        mysql_variant: MySQLEngine = "mariadb" if engine == "mariadb" else "mysql"
        return await capture_mysql_report(args.dsn, engine=mysql_variant, **common_kwargs)
    if engine == "oracle":
        return await capture_oracle_report(
            args.dsn,
            enable_reset_relay=args.oracle_reset_relay == "on",
            **common_kwargs,
        )
    if engine == "mssql":
        return await capture_mssql_report(args.dsn, **common_kwargs)
    if engine == "spanner":
        return await capture_spanner_report(args.dsn, **common_kwargs)
    if engine == "bigquery":
        return await capture_bigquery_report(args.dsn, **common_kwargs)
    if engine == "sqlalchemy":
        return await capture_sqlalchemy_report(
            args.dsn,
            use_broker_pool=bool(args.use_broker_pool),
            **common_kwargs,
        )
    if engine == "sqlspec":
        sqlspec_engine: SqlSpecEngine = args.sqlspec_engine
        return await capture_sqlspec_report(sqlspec_engine, args.dsn, **common_kwargs)
    if engine == "advanced_alchemy":
        aa_engine: AdvancedAlchemyEngine = args.aa_engine
        return await capture_advanced_alchemy_report(aa_engine, args.dsn, **common_kwargs)
    msg = f"Unsupported SQL benchmark engine: {engine!r}"
    raise ValueError(msg)


def check_report_against_comparators(
    report: AdapterBenchmarkReport,
    *,
    max_median_ratio: float = 1.0,
) -> tuple[ComparatorCheckOutcome, ...]:
    """Compare each paired Vertebra case in a captured report against its comparator driver(s)."""
    if not math.isfinite(max_median_ratio) or max_median_ratio <= 0.0:
        msg = f"max_median_ratio must be finite and > 0 (got {max_median_ratio!r})"
        raise ValueError(msg)

    cases_by_key: dict[tuple[str, str], AdapterBenchmarkCase] = {
        (item.case, item.driver): item for item in report.cases
    }
    outcomes: list[ComparatorCheckOutcome] = []

    for item in report.cases:
        comparator_labels = COMPARATOR_PAIRS.get(item.driver)
        if comparator_labels is None:
            continue

        matched = [
            cases_by_key[(item.case, label)] for label in comparator_labels if (item.case, label) in cases_by_key
        ]
        if not matched:
            skip_reason = COMPARATORLESS_CASES.get((item.driver, item.case))
            if skip_reason is not None:
                missing_status: ComparatorStatus = "skipped"
                missing_reason = skip_reason
            else:
                missing_status = "missing"
                missing_reason = "no comparator case in report"
            outcomes.append(
                ComparatorCheckOutcome(
                    engine=report.engine,
                    case=item.case,
                    vertebra_driver=item.driver,
                    comparator_driver=None,
                    vertebra_median_ms=item.median_ms,
                    comparator_median_ms=None,
                    median_ratio=None,
                    status=missing_status,
                    reason=missing_reason,
                )
            )
            continue

        for comparator in matched:
            ratio = item.median_ms / comparator.median_ms
            if ratio <= max_median_ratio:
                status: ComparatorStatus = "pass"
                reason: str | None = None
            elif item.driver in COMPARATOR_ALLOW_LIST:
                status = "allowed"
                reason = COMPARATOR_ALLOW_LIST[item.driver]
            else:
                status = "fail"
                reason = f"median_ratio {ratio:.3f} > {max_median_ratio:.3f}"
            outcomes.append(
                ComparatorCheckOutcome(
                    engine=report.engine,
                    case=item.case,
                    vertebra_driver=item.driver,
                    comparator_driver=comparator.driver,
                    vertebra_median_ms=item.median_ms,
                    comparator_median_ms=comparator.median_ms,
                    median_ratio=ratio,
                    status=status,
                    reason=reason,
                )
            )

    return tuple(outcomes)


def _format_comparator_outcome(outcome: ComparatorCheckOutcome) -> str:
    if outcome.status == "missing":
        return f"[MISSING] {outcome.engine} {outcome.case} ({outcome.vertebra_driver}): {outcome.reason}"
    if outcome.status == "skipped":
        return f"[SKIP] {outcome.engine} {outcome.case} ({outcome.vertebra_driver}): {outcome.reason}"

    tag = {"pass": "PASS", "allowed": "ALLOW", "fail": "FAIL"}[outcome.status]
    ratio = outcome.median_ratio if outcome.median_ratio is not None else 0.0
    line = (
        f"[{tag}] {outcome.engine} {outcome.case} "
        f"({outcome.vertebra_driver} vs {outcome.comparator_driver}): median_ratio={ratio:.3f}"
    )
    if outcome.reason:
        line += f" ({outcome.reason})"
    return line


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Standalone SQL and ORM adapter benchmark harness.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_cmd = subparsers.add_parser("capture", help="Run SQL/ORM adapter benchmarks and emit JSON.")
    capture_cmd.add_argument("--engine", required=True, help="Target engine name.")
    capture_cmd.add_argument("--dsn", required=True, help="Target DSN, URL, or database path.")
    capture_cmd.add_argument(
        "--sqlspec-engine",
        choices=_SQLSPEC_ENGINES,
        default="sqlite",
        help="Underlying database engine when --engine=sqlspec.",
    )
    capture_cmd.add_argument(
        "--aa-engine",
        choices=_ADVANCED_ALCHEMY_ENGINES,
        default="sqlite",
        help="Underlying database engine when --engine=advanced_alchemy.",
    )
    capture_cmd.add_argument("--iterations", type=int, default=100, help="Measured iterations per workload.")
    capture_cmd.add_argument("--warmup", type=int, default=10, help="Warmup iterations per workload.")
    capture_cmd.add_argument(
        "--use-broker-pool",
        action="store_true",
        help="Bind SQLAlchemy BrokerPool when --engine=sqlalchemy.",
    )
    capture_cmd.add_argument(
        "--oracle-reset-relay",
        choices=("on", "off"),
        default="on",
        help="In-process TNS relay for Vertebra Oracle drivers when --engine=oracle.",
    )
    capture_cmd.add_argument(
        "--no-external",
        action="store_true",
        help="Benchmark Vertebra drivers only without optional third-party comparators.",
    )
    capture_cmd.add_argument(
        "--strict-free-threading",
        action="store_true",
        help="Fail if the Python GIL is enabled or re-enabled during capture.",
    )
    capture_cmd.add_argument("--output", type=Path, default=None, help="Optional output file path.")

    check_cmd = subparsers.add_parser(
        "check",
        help="Check Vertebra cases in captured reports against their comparator drivers.",
    )
    check_cmd.add_argument(
        "--report",
        action="append",
        type=Path,
        required=True,
        help="Captured adapter benchmark JSON report path (repeatable).",
    )
    check_cmd.add_argument(
        "--max-median-ratio",
        type=float,
        default=1.0,
        help="Maximum allowed Vertebra/comparator median latency ratio.",
    )
    check_cmd.add_argument(
        "--allow-missing",
        action="store_true",
        help="Do not fail when a Vertebra case has no comparator case in the report.",
    )

    compare_cmd = subparsers.add_parser("compare", help="Compare candidate and baseline adapter benchmark reports.")
    compare_cmd.add_argument("--candidate", type=Path, required=True, help="Candidate JSON report path.")
    compare_cmd.add_argument("--baseline", type=Path, required=True, help="Baseline JSON report path.")
    compare_cmd.add_argument("--candidate-driver", default=None, help="Candidate driver label within report.")
    compare_cmd.add_argument("--baseline-driver", default=None, help="Baseline driver label within report.")
    compare_cmd.add_argument(
        "--driver-mode",
        choices=("async_native", "sync_blocking"),
        default=None,
        help="Optional driver mode filter for comparison.",
    )
    compare_cmd.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=DEFAULT_MIN_THROUGHPUT_RATIO,
        help="Minimum allowed candidate/baseline ops/sec ratio.",
    )
    compare_cmd.add_argument(
        "--max-latency-ratio",
        type=float,
        default=DEFAULT_MAX_LATENCY_RATIO,
        help="Maximum allowed candidate/baseline p99 latency ratio.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run CLI `capture`, `check`, or `compare` commands returning exit status `0` (pass) or non-zero (failure)."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "capture":
        if args.engine not in SUPPORTED_SQL_ENGINES:
            sys.stderr.write(
                f"Unsupported engine {args.engine!r}; expected one of: {', '.join(SUPPORTED_SQL_ENGINES)}\n"
            )
            return 2
        report = asyncio.run(_dispatch_capture(args))
        encoded = format_adapter_report_bytes(report)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(encoded + b"\n")
        else:
            sys.stdout.write(encoded.decode("utf-8") + "\n")
        return 0

    if args.command == "check":
        check_outcomes: list[ComparatorCheckOutcome] = []
        for report_path in args.report:
            report = parse_adapter_report_bytes(report_path.read_bytes())
            check_outcomes.extend(
                check_report_against_comparators(
                    report,
                    max_median_ratio=args.max_median_ratio,
                )
            )
        if not check_outcomes:
            sys.stderr.write("No Vertebra case with a known comparator found\n")
            return 2
        has_failure = False
        for check_outcome in check_outcomes:
            sys.stdout.write(_format_comparator_outcome(check_outcome) + "\n")
            if check_outcome.status == "fail" or (check_outcome.status == "missing" and not args.allow_missing):
                has_failure = True
        return 1 if has_failure else 0

    candidate_report = parse_adapter_report_bytes(args.candidate.read_bytes())
    baseline_report = parse_adapter_report_bytes(args.baseline.read_bytes())
    driver_mode: DriverMode | None = args.driver_mode
    outcomes = compare_adapter_reports(
        candidate_report,
        baseline_report,
        candidate_driver=args.candidate_driver,
        baseline_driver=args.baseline_driver,
        driver_mode=driver_mode,
        min_throughput_ratio=args.min_throughput_ratio,
        max_latency_ratio=args.max_latency_ratio,
    )
    failures = [outcome for outcome in outcomes if not outcome.passed]
    for outcome in outcomes:
        status = "PASS" if outcome.passed else "FAIL"
        line = (
            f"[{status}] {outcome.case} ({outcome.candidate_driver} vs {outcome.baseline_driver}): "
            f"throughput_ratio={outcome.throughput_ratio:.3f}, p99_ratio={outcome.p99_ratio:.3f}"
        )
        if outcome.reason:
            line += f" ({outcome.reason})"
        sys.stdout.write(line + "\n")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
