"""Consolidated offline end-to-end tests for the integrated benchmark suite."""

from __future__ import annotations

import asyncio
import json
import socket
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from tools.benchmarks import cli as bench_cli
from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    compare_adapter_reports,
    format_adapter_report_bytes,
    from_postgres_report,
    parse_adapter_report_bytes,
)
from tools.benchmarks.adapters.sql import capture_sqlite_report
from tools.benchmarks.adapters.storage import build_storage_backend_spec, capture_storage_report
from tools.benchmarks.apps._data_backend import load_backend_config
from tools.benchmarks.apps.data_grpc import grpc_asgi_app, grpc_native_app
from tools.benchmarks.apps.data_native import native_data_app
from tools.benchmarks.drivers import (
    DriverBinaryNotFoundError,
    GhzDriver,
    H2LoadDriver,
    K6Driver,
    LoadConfig,
    OhaDriver,
    create_driver,
)
from tools.benchmarks.fixtures import (
    FIXTURE_SPECS,
    STORAGE_LOCAL_BACKENDS,
    build_docker_run_command,
    build_storage_config,
    get_fixture_spec,
)
from tools.benchmarks.postgres import PostgresBenchmarkCase, PostgresBenchmarkReport
from tools.benchmarks.runner import BrokerHookConfig, BrokerHostHook, ServerRunner
from tools.benchmarks.store import BenchmarkStore
from tools.benchmarks.targets import ALL_TARGETS, DATA_TARGETS, GRPC_TARGETS

_LEGACY_SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS bench_result_seq START 1;
CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id VARCHAR PRIMARY KEY,
    git_sha VARCHAR,
    git_branch VARCHAR,
    machine_os VARCHAR,
    machine_arch VARCHAR,
    machine_cpu VARCHAR,
    python_version VARCHAR,
    started_at TIMESTAMP,
    note VARCHAR DEFAULT ''
);
CREATE TABLE IF NOT EXISTS benchmark_results (
    id INTEGER PRIMARY KEY DEFAULT nextval('bench_result_seq'),
    run_id VARCHAR NOT NULL REFERENCES benchmark_runs(run_id),
    target VARCHAR,
    scenario VARCHAR,
    concurrency INTEGER,
    duration_s INTEGER,
    rps DOUBLE,
    latency_mean_ms DOUBLE,
    latency_p50_ms DOUBLE,
    latency_p99_ms DOUBLE,
    latency_max_ms DOUBLE,
    total_requests INTEGER,
    errors INTEGER DEFAULT 0,
    url VARCHAR,
    timestamp TIMESTAMP
);
"""

_OHA_SAMPLE_JSON = json.dumps(
    {
        "summary": {
            "successRate": 1.0,
            "total": 2.0,
            "slowest": 0.012,
            "average": 0.002,
        },
        "rps": {"mean": 5000.0},
        "latencyPercentiles": {"p50": 0.0015, "p95": 0.006, "p99": 0.009, "p99.9": 0.011},
        "statusCodeDistribution": {"200": 10000},
        "errorDistribution": {},
    }
)

_H2LOAD_SAMPLE_TEXT = """\
finished in 2.00s, 4500.50 req/s, 1.25MB/s
requests: 9001 total, 9001 started, 9001 done, 9001 succeeded, 0 failed, 0 errored, 0 timeout
status codes: 9001 2xx, 0 3xx, 0 4xx, 0 5xx
time for request:       120us       15.00ms      2.50ms      1.10ms    95.00%
"""

_GHZ_SAMPLE_JSON = json.dumps(
    {
        "count": 4000,
        "rps": 2000.0,
        "average": 1_500_000,
        "slowest": 8_000_000,
        "latencyDistribution": [
            {"percentage": 50, "latency": 1_200_000},
            {"percentage": 95, "latency": 4_000_000},
            {"percentage": 99, "latency": 6_500_000},
            {"percentage": 99.9, "latency": 7_800_000},
        ],
        "statusCodeDistribution": {"OK": 4000},
        "errorDistribution": {},
    }
)

_K6_SAMPLE_JSON = json.dumps(
    {
        "metrics": {
            "http_reqs": {"values": {"count": 3000, "rate": 1500.0}},
            "http_req_duration": {
                "values": {
                    "avg": 3.2,
                    "med": 2.4,
                    "p(95)": 7.5,
                    "p(99)": 10.2,
                    "p(99.9)": 14.0,
                    "max": 18.0,
                }
            },
            "http_req_failed": {"values": {"passes": 0, "fails": 3000}},
        }
    }
)


def test_duckdb_matrix_migration_and_roundtrip(tmp_path: Path) -> None:
    """Verify legacy 2-table DuckDB schema upgrades idempotently and stores/queries all 8 matrix columns."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "legacy_benchmarks.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_LEGACY_SCHEMA_SQL)
    conn.close()

    store = BenchmarkStore(db_path)
    try:
        store.ensure_schema()

        baseline_id = store.create_run(note="baseline")
        current_id = store.create_run(note="current")

        base_row: dict[str, Any] = {
            "target": "vtb-shm-data-pg-4w",
            "topology": "shm",
            "scenario": "db",
            "concurrency": 64,
            "duration": 5,
            "rps": 10000.0,
            "latency_mean_s": 0.004,
            "latency_p50_s": 0.003,
            "latency_p99_s": 0.009,
            "latency_max_s": 0.015,
            "total_requests": 50000,
            "errors": 0,
            "url": "http://127.0.0.1:8100/db",
            "timestamp": "2026-11-19T00:00:00+00:00",
            "adapter": "postgres",
            "protocol": "http2",
            "transport": "rsgi",
            "broker_mode": "shm_broker",
            "load_mode": "open_loop",
            "target_rps": 12000.0,
            "pool_size": 40,
            "gil_enabled": False,
        }
        curr_row = {**base_row, "rps": 11500.0, "latency_p99_s": 0.008}

        store.insert_result(baseline_id, base_row)
        store.insert_result(current_id, curr_row)

        filtered = store.get_run_results(
            current_id,
            adapter="postgres",
            protocol="http2",
            transport="rsgi",
            broker_mode="shm_broker",
            load_mode="open_loop",
            gil_enabled=False,
        )
        assert len(filtered) == 1
        row = filtered[0]
        assert row["adapter"] == "postgres"
        assert row["protocol"] == "http2"
        assert row["transport"] == "rsgi"
        assert row["broker_mode"] == "shm_broker"
        assert row["load_mode"] == "open_loop"
        assert row["target_rps"] == pytest.approx(12000.0)
        assert row["pool_size"] == 40
        assert row["gil_enabled"] is False

        deltas = store.compare(current_id, baseline_id)
        assert len(deltas) == 1
        assert deltas[0]["target"] == "vtb-shm-data-pg-4w"
        assert deltas[0]["rps_delta_pct"] == pytest.approx(15.0)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("driver_name", "driver_cls", "config", "sample_output", "expected_flag", "expected_rps"),
    [
        (
            "oha",
            OhaDriver,
            LoadConfig(url="http://127.0.0.1:8000/hello", duration=2, concurrency=32, rate=2500.0),
            _OHA_SAMPLE_JSON,
            "-q",
            5000.0,
        ),
        (
            "h2load",
            H2LoadDriver,
            LoadConfig(url="https://127.0.0.1:8443/db", duration=2, concurrency=16, rate=1600.0),
            _H2LOAD_SAMPLE_TEXT,
            "--rps",
            4500.5,
        ),
        (
            "ghz",
            GhzDriver,
            LoadConfig(
                url="http://127.0.0.1:50051",
                duration=2,
                concurrency=16,
                rate=1000.0,
                grpc_call="vertebra.bench.BenchmarkService.GetDb",
                grpc_data="{}",
            ),
            _GHZ_SAMPLE_JSON,
            "--rps",
            2000.0,
        ),
        (
            "k6",
            K6Driver,
            LoadConfig(
                url="http://127.0.0.1:8000/db",
                duration=2,
                concurrency=16,
                rate=500.0,
                k6_script="bench_script.js",
            ),
            _K6_SAMPLE_JSON,
            "run",
            1500.0,
        ),
    ],
)
def test_load_drivers_parse_and_fail_loud_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    driver_name: str,
    driver_cls: type[OhaDriver | H2LoadDriver | GhzDriver | K6Driver],
    config: LoadConfig,
    sample_output: str,
    expected_flag: str,
    expected_rps: float,
) -> None:
    """Verify driver command assembly, sample output parsing, and fail-loud missing-binary errors."""
    driver = create_driver(driver_name)
    assert isinstance(driver, driver_cls)

    cmd = driver.build_command(config)
    assert cmd[0] == driver.binary
    assert expected_flag in cmd

    parsed = driver.parse_output(sample_output, config)
    assert parsed["rps"] == pytest.approx(expected_rps)
    assert parsed["total_requests"] > 0
    assert parsed["errors"] == 0
    assert parsed["load_mode"] == "open_loop"
    assert parsed["target_rps"] == pytest.approx(config.rate)
    for key in ("latency_mean_s", "latency_p50_s", "latency_p99_s", "latency_max_s"):
        assert parsed[key] >= 0.0

    monkeypatch.setattr("tools.benchmarks.drivers.shutil.which", lambda _binary: None)
    with pytest.raises(DriverBinaryNotFoundError) as exc_info:
        driver.run(config)
    assert isinstance(exc_info.value, FileNotFoundError)
    assert driver.binary in str(exc_info.value)
    assert driver.install_hint in str(exc_info.value)


def test_server_runner_broker_hooks_run_and_teardown(tmp_path: Path) -> None:
    """Verify BrokerHostHook pre_start runs before spawn and post_stop runs even on startup failure."""
    events: list[str] = []
    marker_file = tmp_path / "env_snapshot.json"

    class _FakeBrokerHost:
        def start(self) -> None:
            events.append("broker_start")

        def stop(self) -> None:
            events.append("broker_stop")

    config = BrokerHookConfig(
        shm_name="vtb_e2e_test_broker",
        pool_size=24,
        socket_path=str(tmp_path / "broker.sock"),
        attach_secret="test-token-placeholder",
        shm_dir=str(tmp_path),
    )
    hook = BrokerHostHook(config, host_factory=lambda _cfg: _FakeBrokerHost())

    child_script = (
        "import json, os, pathlib, sys; "
        f"pathlib.Path({str(marker_file)!r}).write_text("
        "json.dumps({'shm': os.environ.get('VERTEBRA_BROKER_SHM'), 'pool': os.environ.get('VERTEBRA_BROKER_POOL_SIZE')}), "
        "encoding='utf-8'); "
        "sys.exit(7)"
    )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        ephemeral_port = int(sock.getsockname()[1])

    runner = ServerRunner(
        cmd=[sys.executable, "-c", child_script],
        host="127.0.0.1",
        port=ephemeral_port,
        hooks=[hook],
    )
    with pytest.raises(RuntimeError, match="Server process exited with code 7"):
        runner.start()

    assert events == ["broker_start", "broker_stop"]
    snapshot = json.loads(marker_file.read_text(encoding="utf-8"))
    assert snapshot == {"shm": "vtb_e2e_test_broker", "pool": "24"}


def test_fixtures_use_rustfs_floci_azurite_and_forbid_minio() -> None:
    """Verify fixture specs use RustFS/Floci/Azurite with tmpfs mounts and forbid MinIO everywhere."""
    required_storage = {"rustfs", "floci_aws", "floci_gcp", "floci_azure", "floci_oci", "azurite"}
    assert required_storage.issubset(FIXTURE_SPECS)
    assert STORAGE_LOCAL_BACKENDS == ("local", "memory")

    for name in ("postgres", "mysql", "mariadb", "rustfs"):
        spec = get_fixture_spec(name)
        assert spec.tmpfs_mounts
        docker_cmd = build_docker_run_command(spec, host_port=19000, container_name=f"bench-{name}")
        assert "--tmpfs" in docker_cmd

    for key, spec in FIXTURE_SPECS.items():
        assert "minio" not in key.lower()
        assert "minio" not in spec.image.lower()
        assert "minio" not in spec.plugin_module.lower()

    with pytest.raises(ValueError, match="MinIO"):
        get_fixture_spec("minio")
    with pytest.raises(ValueError, match="MinIO"):
        build_storage_config("minio")
    with pytest.raises(ValueError, match="MinIO"):
        load_backend_config({"VTB_BENCH_STORAGE_BACKEND": "minio"})


def test_standalone_adapters_offline_sqlite_and_memory_storage() -> None:
    """Verify offline SQLite and Memory ObjectStore comparators plus postgres schema_version=1 stratification."""
    pg_report = PostgresBenchmarkReport(
        schema_version=1,
        generated_at="2026-11-19T00:00:00+00:00",
        dsn_redacted="postgresql://bench:***@127.0.0.1:5432/bench",
        cases=(
            PostgresBenchmarkCase(
                case="select_1",
                driver="vertebra-async",
                iterations=10,
                median_ms=0.25,
                p99_ms=0.60,
                ops_per_sec=4000.0,
            ),
            PostgresBenchmarkCase(
                case="select_1",
                driver="vertebra-sync",
                iterations=10,
                median_ms=0.35,
                p99_ms=0.80,
                ops_per_sec=2857.0,
            ),
        ),
    )
    converted = from_postgres_report(pg_report, gil_enabled=True)
    assert converted.schema_version == ADAPTER_SCHEMA_VERSION == 1
    modes = {entry.driver: entry.driver_mode for entry in converted.cases}
    assert modes == {"vertebra-async": "async_native", "vertebra-sync": "sync_blocking"}

    with pytest.raises(ValueError, match="driver_mode"):
        compare_adapter_reports(
            converted,
            converted,
            candidate_driver="vertebra-async",
            baseline_driver="vertebra-sync",
            stratify_by_driver_mode=True,
        )

    sqlite_report = asyncio.run(capture_sqlite_report(":memory:", iterations=3, warmup=1, include_external=False))
    assert sqlite_report.schema_version == 1
    assert sqlite_report.engine == "sqlite"
    assert sqlite_report.cases
    assert {c.driver_mode for c in sqlite_report.cases}.issubset({"sync_blocking", "async_native"})
    sqlite_roundtrip = parse_adapter_report_bytes(format_adapter_report_bytes(sqlite_report))
    assert sqlite_roundtrip == sqlite_report

    mem_spec = build_storage_backend_spec("memory")
    storage_report = asyncio.run(
        capture_storage_report(
            mem_spec,
            iterations=3,
            warmup=1,
            payload_sizes=("4kib",),
            include_range_read=True,
            include_external=False,
        )
    )
    assert storage_report.schema_version == 1
    assert storage_report.category == "storage"
    assert storage_report.cases
    assert {c.driver_mode for c in storage_report.cases} == {"sync_blocking", "async_native"}
    storage_roundtrip = parse_adapter_report_bytes(format_adapter_report_bytes(storage_report))
    assert storage_roundtrip == storage_report


def test_fullstack_cli_matrix_e2e_offline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify full-stack data/gRPC apps and CLI matrix filters interoperate end-to-end offline."""
    assert native_data_app is not None
    assert grpc_asgi_app is not None
    assert grpc_native_app is not None
    assert DATA_TARGETS
    assert GRPC_TARGETS

    data_target_name = DATA_TARGETS[0]
    grpc_target_name = GRPC_TARGETS[0]
    selected_targets = f"{data_target_name},{grpc_target_name}"
    dispatched: list[tuple[str, LoadConfig]] = []

    class _FakeServerRunner:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> _FakeServerRunner:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

    def _fake_run_load(driver_name: str, config: LoadConfig) -> dict[str, Any]:
        dispatched.append((driver_name, config))
        return {
            "rps": 3210.0,
            "latency_mean_s": 0.002,
            "latency_p50_s": 0.0015,
            "latency_p95_s": 0.004,
            "latency_p99_s": 0.006,
            "latency_p999_s": 0.008,
            "latency_max_s": 0.010,
            "total_requests": 3210,
            "errors": 0,
            "load_mode": "open_loop" if config.rate is not None else "closed_loop",
            "target_rps": config.rate,
        }

    def _fake_store_unavailable(*_args: object, **_kwargs: object) -> Any:
        raise ImportError("offline test skips default DuckDB persistence")

    monkeypatch.setattr(bench_cli, "ServerRunner", _FakeServerRunner)
    monkeypatch.setattr(bench_cli, "_run_load", _fake_run_load)
    monkeypatch.setattr(bench_cli, "BenchmarkStore", _fake_store_unavailable)
    monkeypatch.setattr("tools.benchmarks.cli.time.sleep", lambda _secs: None)

    out_jsonl = tmp_path / "e2e_matrix.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        bench_cli.cli,
        [
            "run",
            "-t",
            selected_targets,
            "-s",
            "db,grpc_db",
            "-d",
            "1",
            "-c",
            "8",
            "-w",
            "0",
            "--load-mode",
            "open_loop",
            "--rate",
            "500",
            "-o",
            str(out_jsonl),
        ],
    )
    assert result.exit_code == 0, result.output

    lines = [json.loads(line) for line in out_jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 4
    expected_matrix_keys = {
        "topology",
        "adapter",
        "protocol",
        "transport",
        "broker_mode",
        "load_mode",
        "target_rps",
        "pool_size",
        "gil_enabled",
    }
    for row in lines:
        assert expected_matrix_keys.issubset(row)
        assert row["load_mode"] == "open_loop"
        assert row["target_rps"] == pytest.approx(500.0)
        target = ALL_TARGETS[row["target"]]
        assert row["adapter"] == target.adapter
        assert row["protocol"] == target.protocol
        assert row["transport"] == target.transport
        assert row["broker_mode"] == target.broker_mode

    drivers_used = {driver_name for driver_name, _ in dispatched}
    assert "ghz" in drivers_used
