"""Offline unit tests for standalone SQL, SQLAlchemy `+vtb`, and SQLSpec benchmark comparators."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.adapters import (
    ADAPTER_SCHEMA_VERSION,
    format_adapter_report_bytes,
    parse_adapter_report_bytes,
)
from tools.benchmarks.adapters import sql as sql_bench
from tools.benchmarks.adapters.sql import (
    SUPPORTED_SQL_ENGINES,
    capture_duckdb_report,
    capture_postgres_adapter_report,
    capture_sqlalchemy_report,
    capture_sqlite_report,
    capture_sqlspec_report,
    main,
)
from tools.benchmarks.postgres import SCHEMA_VERSION, PostgresBenchmarkCase, PostgresBenchmarkReport

from vertebra.db.adapters.duckdb import DuckDBConfig
from vertebra.db.adapters.sqlite import SQLiteConfig
from vertebra.db.registry import DatabaseRegistry
from vertebra.utils.module_loader import import_optional


class _FakeLease:
    def __init__(self, lease_id: str, released: list[str]) -> None:
        self.lease_id = lease_id
        self._released = released

    def release_sync(self) -> None:
        self._released.append(self.lease_id)


class _FakeBroker:
    def __init__(self) -> None:
        self.registered: list[str] = []
        self.acquired: list[tuple[str, int]] = []
        self.released: list[str] = []
        self._counter = 0

    def register_partition(self, partition_key: str) -> None:
        self.registered.append(partition_key)

    def acquire(self, *, bind_key: str, worker_id: int) -> _FakeLease:
        self._counter += 1
        self.acquired.append((bind_key, worker_id))
        return _FakeLease(f"lease-{self._counter}", self.released)

    def release(self, lease_id: str) -> None:
        self.released.append(lease_id)


def test_capture_sqlite_report_runs_offline_and_preserves_memory_isolation() -> None:
    """Run real SQLite capture offline and verify driver modes, positive metrics, and `:memory:` isolation."""
    assert SQLiteConfig(database=":memory:").pool_key() is None
    report = asyncio.run(capture_sqlite_report(":memory:", iterations=5, warmup=1, include_external=True))

    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped.schema_version == ADAPTER_SCHEMA_VERSION
    assert roundtripped.category == "sql"
    assert roundtripped.engine == "sqlite"
    assert roundtripped.target_redacted == ":memory:"

    by_key = {(item.case, item.driver): item for item in roundtripped.cases}
    expected_modes = {
        ("point_select_1", "vertebra-sqlite-sync"): "sync_blocking",
        ("multi_row_select_10", "vertebra-sqlite-sync"): "sync_blocking",
        ("point_select_1", "vertebra-sqlite-async"): "async_native",
        ("multi_row_select_10", "vertebra-sqlite-async"): "async_native",
        ("point_select_1", "sqlite3"): "sync_blocking",
        ("multi_row_select_10", "sqlite3"): "sync_blocking",
    }
    for key, mode in expected_modes.items():
        assert key in by_key
        case = by_key[key]
        assert case.driver_mode == mode
        assert case.iterations == 5
        assert case.median_ms > 0.0
        assert case.p99_ms > 0.0
        assert case.ops_per_sec > 0.0

    if import_optional("aiosqlite") is not None:
        assert ("point_select_1", "aiosqlite") in by_key
        assert by_key[("point_select_1", "aiosqlite")].driver_mode == "sync_blocking"


def test_capture_duckdb_report_runs_offline() -> None:
    """Run real DuckDB capture offline and verify point_select_1, analytical_scan_1000, and `:memory:` isolation."""
    assert DuckDBConfig(database=":memory:").pool_key() is None
    report = asyncio.run(capture_duckdb_report(":memory:", iterations=5, warmup=1, include_external=False))

    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped.schema_version == ADAPTER_SCHEMA_VERSION
    assert roundtripped.category == "sql"
    assert roundtripped.engine == "duckdb"
    assert roundtripped.target_redacted == ":memory:"

    by_key = {(item.case, item.driver): item for item in roundtripped.cases}
    assert ("point_select_1", "vertebra-duckdb-sync") in by_key
    assert ("analytical_scan_1000", "vertebra-duckdb-sync") in by_key
    assert ("point_select_1", "vertebra-duckdb-async") in by_key
    assert ("analytical_scan_1000", "vertebra-duckdb-async") in by_key
    assert by_key[("point_select_1", "vertebra-duckdb-sync")].driver_mode == "sync_blocking"
    assert by_key[("analytical_scan_1000", "vertebra-duckdb-async")].driver_mode == "async_native"


def test_capture_sqlalchemy_report_vtb_vs_vanilla_sqlite_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run SQLAlchemy `+vtb` vs vanilla SQLite offline and verify ORM cases and engine disposal."""
    sa_mod = import_optional("sqlalchemy")
    if sa_mod is None:
        pytest.skip("sqlalchemy is not installed")

    disposed_urls: list[str] = []
    real_create_engine = sa_mod.create_engine

    def _tracking_create_engine(url: str, *args: Any, **kwargs: Any) -> Any:
        engine = real_create_engine(url, *args, **kwargs)
        real_dispose = engine.dispose

        def _tracked_dispose(*d_args: Any, **d_kwargs: Any) -> Any:
            disposed_urls.append(str(url))
            return real_dispose(*d_args, **d_kwargs)

        monkeypatch.setattr(engine, "dispose", _tracked_dispose)
        return engine

    monkeypatch.setattr(sa_mod, "create_engine", _tracking_create_engine)

    report = asyncio.run(capture_sqlalchemy_report("sqlite+vtb:///:memory:", iterations=5, warmup=1))
    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped.category == "orm"
    assert roundtripped.engine == "sqlalchemy"

    drivers = {item.driver: item for item in roundtripped.cases}
    assert "sqlalchemy-vtb-sync" in drivers
    assert "sqlalchemy-vanilla-sync" in drivers
    assert drivers["sqlalchemy-vtb-sync"].case == "sa_point_select_1"
    assert drivers["sqlalchemy-vtb-sync"].category == "orm"
    assert drivers["sqlalchemy-vanilla-sync"].category == "orm"
    assert "sqlite+vtb:///:memory:" in disposed_urls
    assert "sqlite:///:memory:" in disposed_urls


def test_capture_sqlalchemy_report_broker_pool_binding_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `BrokerPool.bind_class` wiring with a fake broker and `ValueError` when no broker is available."""
    if import_optional("sqlalchemy") is None:
        pytest.skip("sqlalchemy is not installed")

    pool_mod = import_optional("vertebra.extensions.sqlalchemy.pool")
    assert pool_mod is not None

    bind_calls: list[dict[str, Any]] = []
    real_bind_class = pool_mod.BrokerPool.bind_class

    def _spy_bind_class(**kwargs: Any) -> Any:
        bind_calls.append(kwargs)
        return real_bind_class(**kwargs)

    monkeypatch.setattr(pool_mod.BrokerPool, "bind_class", _spy_bind_class)

    fake_broker = _FakeBroker()
    report = asyncio.run(
        capture_sqlalchemy_report(
            "sqlite+vtb:///:memory:",
            use_broker_pool=True,
            broker=fake_broker,
            iterations=4,
            warmup=1,
            include_external=False,
        )
    )
    assert report.category == "orm"
    assert len(bind_calls) == 1
    assert bind_calls[0]["broker"] is fake_broker
    assert bind_calls[0]["url"] == "sqlite+vtb:///:memory:"
    assert len(fake_broker.acquired) >= 1
    assert len(fake_broker.released) >= 1

    monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: None))
    with pytest.raises(ValueError, match="broker"):
        asyncio.run(
            capture_sqlalchemy_report(
                "sqlite+vtb:///:memory:",
                use_broker_pool=True,
                broker=None,
                iterations=4,
                warmup=1,
            )
        )


def test_capture_sqlspec_report_sqlite_offline() -> None:
    """Run real SQLSpec SQLite capture offline and verify `category="orm"` cases."""
    if import_optional("sqlspec") is None:
        pytest.skip("sqlspec is not installed")

    report = asyncio.run(capture_sqlspec_report("sqlite", ":memory:", iterations=5, warmup=1))
    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped.category == "orm"
    assert roundtripped.engine == "sqlspec:sqlite"

    by_driver = {item.driver: item for item in roundtripped.cases}
    assert "vertebra-sqlspec-sqlite-sync" in by_driver
    assert "vertebra-sqlspec-sqlite-async" in by_driver
    assert by_driver["vertebra-sqlspec-sqlite-sync"].driver_mode == "sync_blocking"
    assert by_driver["vertebra-sqlspec-sqlite-async"].driver_mode == "async_native"
    assert by_driver["vertebra-sqlspec-sqlite-sync"].category == "orm"
    assert by_driver["vertebra-sqlspec-sqlite-async"].category == "orm"


def test_sql_cli_capture_and_compare_offline(tmp_path: Path) -> None:
    """Verify CLI `capture` and `compare` subcommands execute offline and return exit code 0."""
    out_file = tmp_path / "sqlite-report.json"
    rc_capture = main(
        [
            "capture",
            "--engine",
            "sqlite",
            "--dsn",
            ":memory:",
            "--iterations",
            "4",
            "--warmup",
            "1",
            "--no-external",
            "--output",
            str(out_file),
        ]
    )
    assert rc_capture == 0
    assert out_file.exists()

    parsed = parse_adapter_report_bytes(out_file.read_bytes())
    assert parsed.engine == "sqlite"
    assert parsed.category == "sql"

    rc_compare = main(
        [
            "compare",
            "--candidate",
            str(out_file),
            "--baseline",
            str(out_file),
            "--driver-mode",
            "sync_blocking",
        ]
    )
    assert rc_compare == 0


@pytest.mark.parametrize(
    ("iterations", "warmup"),
    [(0, 0), (-1, 0), (5, -1)],
)
def test_sql_comparator_rejects_invalid_iterations_and_unknown_engine(
    iterations: int,
    warmup: int,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify `ValueError` on invalid sampling parameters and non-zero exit on unsupported engines."""
    assert {"mariadb", "sqlalchemy", "sqlspec", "oracle", "mssql", "spanner", "bigquery"}.issubset(
        SUPPORTED_SQL_ENGINES
    )

    with pytest.raises(ValueError, match="iterations"):
        asyncio.run(capture_sqlite_report(":memory:", iterations=iterations, warmup=warmup))

    with pytest.raises(ValueError, match="iterations"):
        asyncio.run(capture_duckdb_report(":memory:", iterations=iterations, warmup=warmup))

    rc = main(["capture", "--engine", "unsupported_db", "--dsn", ":memory:"])
    assert rc != 0
    captured = capsys.readouterr()
    assert "Unsupported engine" in captured.err


def test_capture_postgres_adapter_report_delegates_and_converts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify PostgreSQL adapter capture delegates to `tools.benchmarks.postgres` and converts cleanly."""
    fake_pg = PostgresBenchmarkReport(
        schema_version=SCHEMA_VERSION,
        generated_at="2026-11-19T22:00:00Z",
        dsn_redacted="postgresql://postgres@127.0.0.1:5432/postgres",
        cases=(
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-sync",
                iterations=10,
                median_ms=0.5,
                p99_ms=0.8,
                ops_per_sec=2000.0,
            ),
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-async",
                iterations=10,
                median_ms=0.3,
                p99_ms=0.6,
                ops_per_sec=3200.0,
            ),
        ),
    )

    async def _fake_capture_live(
        dsn: str,
        *,
        iterations: int = 100,
        warmup: int = 10,
        include_external: bool = True,
    ) -> PostgresBenchmarkReport:
        assert dsn == "postgresql://user:test-token-placeholder@127.0.0.1:5432/postgres"
        assert iterations == 10
        assert warmup == 2
        assert include_external is False
        return fake_pg

    monkeypatch.setattr(sql_bench, "capture_postgres_live_report", _fake_capture_live)

    report = asyncio.run(
        capture_postgres_adapter_report(
            "postgresql://user:test-token-placeholder@127.0.0.1:5432/postgres",
            iterations=10,
            warmup=2,
            include_external=False,
        )
    )
    assert report.category == "sql"
    assert report.engine == "postgres"
    assert "test-token-placeholder" not in report.target_redacted
    assert len(report.cases) == 2
