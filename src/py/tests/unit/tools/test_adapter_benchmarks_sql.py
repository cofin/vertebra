"""Offline unit tests for standalone SQL, SQLAlchemy `+vtb`, and SQLSpec benchmark comparators."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

import pytest
from tools.benchmarks.adapters import (
    ADAPTER_SCHEMA_VERSION,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    format_adapter_report_bytes,
    parse_adapter_report_bytes,
)
from tools.benchmarks.adapters import sql as sql_bench
from tools.benchmarks.adapters.sql import (
    COMPARATOR_ALLOW_LIST,
    COMPARATOR_PAIRS,
    COMPARATORLESS_CASES,
    SUPPORTED_SQL_ENGINES,
    capture_advanced_alchemy_report,
    capture_duckdb_report,
    capture_oracle_report,
    capture_postgres_adapter_report,
    capture_sqlalchemy_report,
    capture_sqlite_report,
    capture_sqlspec_report,
    check_report_against_comparators,
    main,
)
from tools.benchmarks.postgres import SCHEMA_VERSION, PostgresBenchmarkCase, PostgresBenchmarkReport

from vertebra.db.adapters.duckdb import DuckDBConfig
from vertebra.db.adapters.oracle import OracleConfig
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
        ("cursor_point_select_1", "vertebra-sqlite-sync"): "sync_blocking",
        ("point_select_1", "vertebra-sqlite-async"): "async_native",
        ("multi_row_select_10", "vertebra-sqlite-async"): "async_native",
        ("point_select_1", "sqlite3"): "sync_blocking",
        ("multi_row_select_10", "sqlite3"): "sync_blocking",
        ("cursor_point_select_1", "sqlite3"): "sync_blocking",
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
    """Verify `SyncBrokerPool.bind_class` wiring with a fake broker and `ValueError` when no broker is available."""
    if import_optional("sqlalchemy") is None:
        pytest.skip("sqlalchemy is not installed")

    pool_mod = import_optional("vertebra.extensions.sqlalchemy.pool")
    assert pool_mod is not None

    bind_calls: list[dict[str, Any]] = []
    real_bind_class = pool_mod.SyncBrokerPool.bind_class

    def _spy_bind_class(**kwargs: Any) -> Any:
        bind_calls.append(kwargs)
        return real_bind_class(**kwargs)

    monkeypatch.setattr(pool_mod.SyncBrokerPool, "bind_class", _spy_bind_class)

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


def _report(engine: str, cases: list[tuple[str, str, float]]) -> AdapterBenchmarkReport:
    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine=engine,
        generated_at="2026-11-21T00:00:00Z",
        target_redacted=f"{engine}://localhost",
        gil_enabled=None,
        cases=tuple(
            AdapterBenchmarkCase(
                case=case_name,
                driver=driver,
                category="sql",
                driver_mode="async_native" if "async" in driver else "sync_blocking",
                iterations=10,
                median_ms=median_ms,
                p99_ms=median_ms,
                ops_per_sec=1000.0 / median_ms,
            )
            for case_name, driver, median_ms in cases
        ),
    )


def test_comparator_check_passes_when_vertebra_is_not_slower() -> None:
    """Verify comparator check passes when Vertebra median is lower than or equal to comparator."""
    report = _report(
        "sqlite",
        [
            ("point_select_1", "vertebra-sqlite-sync", 0.0010),
            ("point_select_1", "sqlite3", 0.0013),
            ("equal_case", "vertebra-sqlite-sync", 0.0020),
            ("equal_case", "sqlite3", 0.0020),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 2
    assert outcomes[0].status == "pass"
    assert outcomes[0].reason is None
    assert outcomes[0].median_ratio == pytest.approx(0.769, abs=1e-3)
    assert outcomes[1].status == "pass"
    assert outcomes[1].reason is None
    assert outcomes[1].median_ratio == 1.0


def test_comparator_check_fails_slower_case_outside_allow_list() -> None:
    """Verify comparator check fails when an unallow-listed Vertebra driver is slower."""
    report = _report(
        "mysql",
        [
            ("point_select_1", "vertebra-mysql-sync", 0.089),
            ("point_select_1", "pymysql", 0.074),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].engine == "mysql"
    assert outcomes[0].status == "fail"
    assert outcomes[0].reason is not None
    assert "> 1.000" in outcomes[0].reason


def test_comparator_check_allows_listed_driver_with_reason() -> None:
    """Verify comparator check marks slower allow-listed drivers as allowed with their reason."""
    report = _report(
        "mssql",
        [
            ("point_select_1", "vertebra-mssql-sync", 0.500),
            ("point_select_1", "pymssql", 0.480),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].status == "allowed"
    assert outcomes[0].reason == COMPARATOR_ALLOW_LIST["vertebra-mssql-sync"]


def test_comparator_check_allows_slower_duckdb_case() -> None:
    """Verify comparator check marks slower DuckDB cases as allowed with the allow-list reason."""
    report = _report(
        "duckdb",
        [
            ("point_select_1", "vertebra-duckdb-sync", 0.426),
            ("point_select_1", "duckdb", 0.245),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].status == "allowed"
    assert outcomes[0].reason == COMPARATOR_ALLOW_LIST["vertebra-duckdb-sync"]


@pytest.mark.parametrize(
    ("vertebra_driver", "comparator_driver"),
    [
        ("vertebra-oracle-sync", "oracledb-sync"),
        ("vertebra-oracle-async", "oracledb-async"),
    ],
)
def test_comparator_check_allows_slower_oracle_case(
    vertebra_driver: str,
    comparator_driver: str,
) -> None:
    """Verify comparator check marks slower Oracle cases as allowed with the upstream issue reason."""
    report = _report(
        "oracle",
        [
            ("point_select_1", vertebra_driver, 0.089),
            ("point_select_1", comparator_driver, 0.074),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].status == "allowed"
    assert outcomes[0].reason == COMPARATOR_ALLOW_LIST[vertebra_driver]


def test_comparator_check_reports_each_present_comparator() -> None:
    """Verify comparator check emits outcomes only for comparators present in the report."""
    report = _report(
        "mysql",
        [
            ("point_select_1", "vertebra-mysql-sync", 0.120),
            ("point_select_1", "pymysql", 0.150),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].comparator_driver == "pymysql"
    assert outcomes[0].status == "pass"


def test_comparator_check_marks_missing_comparator() -> None:
    """Verify missing comparators emit `missing` and unpaired drivers emit no outcome."""
    report = _report(
        "sqlite",
        [
            ("point_select_1", "vertebra-sqlite-sync", 0.0010),
            ("point_select_1", "vertebra-duckdb-async", 0.0500),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].vertebra_driver == "vertebra-sqlite-sync"
    assert outcomes[0].comparator_driver is None
    assert outcomes[0].comparator_median_ms is None
    assert outcomes[0].median_ratio is None
    assert outcomes[0].status == "missing"
    assert outcomes[0].reason == "no comparator case in report"


def test_comparator_check_skips_listed_comparatorless_case() -> None:
    """Verify listed comparator-less cases emit `skipped` with their listed reason while paired cases still pass."""
    report = _report(
        "postgres",
        [
            ("pipeline_select_16", "vertebra-async", 0.50),
            ("point_select_1", "vertebra-async", 0.07),
            ("point_select_1", "asyncpg", 0.08),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 2
    assert outcomes[0].case == "pipeline_select_16"
    assert outcomes[0].vertebra_driver == "vertebra-async"
    assert outcomes[0].status == "skipped"
    assert outcomes[0].reason == COMPARATORLESS_CASES[("vertebra-async", "pipeline_select_16")]
    assert outcomes[0].comparator_driver is None
    assert outcomes[0].comparator_median_ms is None
    assert outcomes[0].median_ratio is None
    assert outcomes[1].case == "point_select_1"
    assert outcomes[1].status == "pass"


@pytest.mark.parametrize(("vertebra_ms", "expected_status"), [(1.0, "pass"), (3.0, "fail")])
def test_comparator_check_compares_listed_case_when_comparator_present(
    vertebra_ms: float,
    expected_status: str,
) -> None:
    """Verify listed comparator-less cases are compared normally when a comparator case is present."""
    report = _report(
        "postgres",
        [
            ("pipeline_select_16", "vertebra-async", vertebra_ms),
            ("pipeline_select_16", "asyncpg", 2.0),
        ],
    )
    outcomes = check_report_against_comparators(report)
    assert len(outcomes) == 1
    assert outcomes[0].status == expected_status
    assert outcomes[0].comparator_driver == "asyncpg"


@pytest.mark.parametrize(("driver_case", "reason"), sorted(COMPARATORLESS_CASES.items()))
def test_comparatorless_entries_are_paired_and_explained(
    driver_case: tuple[str, str],
    reason: str,
) -> None:
    """Verify every comparator-less entry targets a paired driver, non-empty case, and single-line reason."""
    driver, case = driver_case
    assert driver in COMPARATOR_PAIRS
    assert case.strip() != ""
    assert reason.strip() != ""
    assert "\n" not in reason


@pytest.mark.parametrize("bad_ratio", [0.0, -1.0, float("nan"), float("inf")])
def test_comparator_check_rejects_invalid_ratio(bad_ratio: float) -> None:
    """Verify non-positive or non-finite max_median_ratio raises ValueError."""
    report = _report("sqlite", [("point_select_1", "vertebra-sqlite-sync", 0.0010)])
    with pytest.raises(ValueError, match="max_median_ratio"):
        check_report_against_comparators(report, max_median_ratio=bad_ratio)


def test_allow_list_entries_are_paired_and_explained() -> None:
    """Verify every allow-list entry belongs to COMPARATOR_PAIRS with a single-line reason."""
    assert COMPARATOR_ALLOW_LIST
    for driver, reason in COMPARATOR_ALLOW_LIST.items():
        assert driver in COMPARATOR_PAIRS
        assert reason.strip() != ""
        assert "\n" not in reason


def test_sql_cli_check_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify CLI `check` exit codes across pass, fail, missing, allow-missing, and empty reports."""
    pass_path = tmp_path / "pass.json"
    fail_path = tmp_path / "fail.json"
    missing_path = tmp_path / "missing.json"
    empty_path = tmp_path / "empty.json"

    pass_path.write_bytes(
        format_adapter_report_bytes(
            _report(
                "sqlite",
                [
                    ("point_select_1", "vertebra-sqlite-sync", 0.0010),
                    ("point_select_1", "sqlite3", 0.0013),
                ],
            )
        )
    )
    fail_path.write_bytes(
        format_adapter_report_bytes(
            _report(
                "mysql",
                [
                    ("point_select_1", "vertebra-mysql-sync", 0.089),
                    ("point_select_1", "pymysql", 0.074),
                ],
            )
        )
    )
    missing_path.write_bytes(
        format_adapter_report_bytes(
            _report(
                "sqlite",
                [
                    ("point_select_1", "vertebra-sqlite-sync", 0.0010),
                ],
            )
        )
    )
    empty_path.write_bytes(
        format_adapter_report_bytes(
            _report(
                "duckdb",
                [
                    ("point_select_1", "vertebra-duckdb-async", 0.0500),
                ],
            )
        )
    )

    assert main(["check", "--report", str(pass_path)]) == 0
    capsys.readouterr()

    assert main(["check", "--report", str(pass_path), "--report", str(fail_path)]) == 1
    captured_fail = capsys.readouterr()
    assert "[FAIL] mysql point_select_1 (vertebra-mysql-sync vs pymysql):" in captured_fail.out

    assert main(["check", "--report", str(missing_path)]) == 1
    capsys.readouterr()

    assert main(["check", "--report", str(missing_path), "--allow-missing"]) == 0
    capsys.readouterr()

    assert main(["check", "--report", str(empty_path)]) == 2
    captured_empty = capsys.readouterr()
    assert "No Vertebra case with a known comparator found" in captured_empty.err


@pytest.mark.parametrize(
    ("cases", "extra_args", "expected_exit", "expected_line_prefix", "expected_extra_prefix"),
    [
        (
            [("pipeline_select_16", "vertebra-async", 0.50)],
            [],
            0,
            f"[SKIP] postgres pipeline_select_16 (vertebra-async): {COMPARATORLESS_CASES[('vertebra-async', 'pipeline_select_16')]}",
            None,
        ),
        (
            [("other_case", "vertebra-async", 0.50)],
            [],
            1,
            "[MISSING] postgres other_case (vertebra-async): no comparator case in report",
            None,
        ),
        (
            [("other_case", "vertebra-async", 0.50)],
            ["--allow-missing"],
            0,
            "[MISSING] postgres other_case (vertebra-async): no comparator case in report",
            None,
        ),
        (
            [
                ("pipeline_select_16", "vertebra-async", 0.50),
                ("point_select_1", "vertebra-async", 0.09),
                ("point_select_1", "asyncpg", 0.07),
            ],
            [],
            1,
            f"[SKIP] postgres pipeline_select_16 (vertebra-async): {COMPARATORLESS_CASES[('vertebra-async', 'pipeline_select_16')]}",
            "[FAIL] postgres point_select_1 (vertebra-async vs asyncpg):",
        ),
    ],
)
def test_sql_cli_check_skip_exit_codes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    cases: list[tuple[str, str, float]],
    extra_args: list[str],
    expected_exit: int,
    expected_line_prefix: str,
    expected_extra_prefix: str | None,
) -> None:
    """Verify CLI `check` exit codes and formatted lines for skipped, missing, allow-missing, and failing reports."""
    report_path = tmp_path / "pg.json"
    report_path.write_bytes(format_adapter_report_bytes(_report("postgres", cases)))

    rc = main(["check", "--report", str(report_path), *extra_args])
    captured = capsys.readouterr()
    assert rc == expected_exit
    assert expected_line_prefix in captured.out
    if expected_extra_prefix is not None:
        assert expected_extra_prefix in captured.out


def _install_oracle_recording_drivers(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[OracleConfig], list[OracleConfig]]:
    sync_configs: list[OracleConfig] = []
    async_configs: list[OracleConfig] = []

    class _RecordingSyncOracleDriver:
        def __init__(self, config: OracleConfig) -> None:
            sync_configs.append(config)

        def __enter__(self) -> _RecordingSyncOracleDriver:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def select_one(self, query: str) -> dict[str, int]:
            return {"1": 1}

    class _RecordingAsyncOracleDriver:
        def __init__(self, config: OracleConfig) -> None:
            async_configs.append(config)

        async def __aenter__(self) -> _RecordingAsyncOracleDriver:
            return self

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        async def select_one(self, query: str) -> dict[str, int]:
            return {"1": 1}

    monkeypatch.setattr(sql_bench, "SyncOracleDriver", _RecordingSyncOracleDriver)
    monkeypatch.setattr(sql_bench, "AsyncOracleDriver", _RecordingAsyncOracleDriver)
    return sync_configs, async_configs


def test_capture_oracle_report_passes_relay_switch_to_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `capture_oracle_report` forwards `enable_reset_relay=False` to both Oracle drivers."""
    sync_configs, async_configs = _install_oracle_recording_drivers(monkeypatch)

    report = asyncio.run(
        capture_oracle_report(
            "oracle://u:test-token-placeholder@127.0.0.1:1521/FREEPDB1",
            iterations=2,
            warmup=0,
            include_external=False,
            enable_reset_relay=False,
        )
    )

    assert len(sync_configs) == 1
    assert len(async_configs) == 1
    assert sync_configs[0].enable_reset_relay is False
    assert async_configs[0].enable_reset_relay is False
    assert report.engine == "oracle"
    assert {item.driver for item in report.cases} == {"vertebra-oracle-sync", "vertebra-oracle-async"}


def test_capture_oracle_report_keeps_relay_on_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `capture_oracle_report` keeps `enable_reset_relay=True` when omitted."""
    sync_configs, async_configs = _install_oracle_recording_drivers(monkeypatch)

    report = asyncio.run(
        capture_oracle_report(
            "oracle://u:test-token-placeholder@127.0.0.1:1521/FREEPDB1",
            iterations=2,
            warmup=0,
            include_external=False,
        )
    )

    assert len(sync_configs) == 1
    assert len(async_configs) == 1
    assert sync_configs[0].enable_reset_relay is True
    assert async_configs[0].enable_reset_relay is True
    assert report.engine == "oracle"
    assert {item.driver for item in report.cases} == {"vertebra-oracle-sync", "vertebra-oracle-async"}


def test_sql_cli_oracle_reset_relay_flag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify CLI `capture --engine oracle --oracle-reset-relay off` disables the relay and writes JSON."""
    sync_configs, async_configs = _install_oracle_recording_drivers(monkeypatch)
    out_file = tmp_path / "oracle-relay-off.json"

    rc = main(
        [
            "capture",
            "--engine",
            "oracle",
            "--dsn",
            "oracle://u:test-token-placeholder@127.0.0.1:1521/FREEPDB1",
            "--iterations",
            "2",
            "--warmup",
            "0",
            "--no-external",
            "--oracle-reset-relay",
            "off",
            "--output",
            str(out_file),
        ]
    )

    assert rc == 0
    assert len(sync_configs) == 1
    assert len(async_configs) == 1
    assert sync_configs[0].enable_reset_relay is False
    assert async_configs[0].enable_reset_relay is False
    parsed = parse_adapter_report_bytes(out_file.read_bytes())
    assert parsed.engine == "oracle"
    assert {item.driver for item in parsed.cases} == {"vertebra-oracle-sync", "vertebra-oracle-async"}


def test_capture_advanced_alchemy_report_sqlite_and_cli(tmp_path: Path) -> None:
    """Run offline Advanced Alchemy SQLite comparator, check comparator pairs, and verify CLI capture."""
    has_aiosqlite = import_optional("aiosqlite") is not None

    assert "advanced_alchemy" in SUPPORTED_SQL_ENGINES
    assert COMPARATOR_PAIRS["vertebra-aa-sqlite-sync"] == ("vanilla-aa-sqlite-sync",)
    assert COMPARATOR_PAIRS["vertebra-aa-sqlite-async"] == ("vanilla-aa-sqlite-async",)
    assert COMPARATOR_PAIRS["vertebra-aa-postgres-sync"] == ("vanilla-aa-postgres-sync",)
    assert COMPARATOR_PAIRS["vertebra-aa-postgres-async"] == ("vanilla-aa-postgres-async",)

    with pytest.raises(ValueError, match="Unsupported Advanced Alchemy engine"):
        asyncio.run(capture_advanced_alchemy_report(cast("Any", "duckdb"), ":memory:"))

    report = asyncio.run(capture_advanced_alchemy_report("sqlite", ":memory:", iterations=3, warmup=1))
    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped.schema_version == ADAPTER_SCHEMA_VERSION
    assert roundtripped.category == "orm"
    assert roundtripped.engine == "advanced_alchemy:sqlite"

    by_key = {(item.case, item.driver): item for item in roundtripped.cases}
    for case_name in ("aa_point_select_1", "aa_multi_row_select_10"):
        assert (case_name, "vertebra-aa-sqlite-sync") in by_key
        assert (case_name, "vertebra-aa-sqlite-async") in by_key
        assert (case_name, "vanilla-aa-sqlite-sync") in by_key
        if has_aiosqlite:
            assert (case_name, "vanilla-aa-sqlite-async") in by_key

    outcomes = check_report_against_comparators(roundtripped, max_median_ratio=100.0)
    assert outcomes
    sync_outcomes = [outcome for outcome in outcomes if outcome.vertebra_driver == "vertebra-aa-sqlite-sync"]
    async_outcomes = [outcome for outcome in outcomes if outcome.vertebra_driver == "vertebra-aa-sqlite-async"]
    assert len(sync_outcomes) == 2
    assert all(outcome.status != "missing" for outcome in sync_outcomes)
    assert len(async_outcomes) == 2
    if has_aiosqlite:
        assert all(outcome.status != "missing" for outcome in async_outcomes)
    else:
        assert all(outcome.status == "missing" for outcome in async_outcomes)

    out_file = tmp_path / "aa-sqlite.json"
    rc = main(
        [
            "capture",
            "--engine",
            "advanced_alchemy",
            "--aa-engine",
            "sqlite",
            "--dsn",
            ":memory:",
            "--iterations",
            "2",
            "--warmup",
            "1",
            "--output",
            str(out_file),
        ]
    )
    assert rc == 0
    cli_report = parse_adapter_report_bytes(out_file.read_bytes())
    assert cli_report.engine == "advanced_alchemy:sqlite"
