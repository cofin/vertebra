"""Offline unit tests for benchmark load drivers and Docker command builders."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.drivers import (
    DOCKER_DRIVER_IMAGES,
    DriverBinaryNotFoundError,
    GhzDriver,
    H2LoadDriver,
    K6Driver,
    LoadConfig,
    OhaDriver,
    build_docker_driver_command,
    create_driver,
)

EXPECTED_RESULT_KEYS = {
    "rps",
    "latency_mean_s",
    "latency_p50_s",
    "latency_p95_s",
    "latency_p99_s",
    "latency_p999_s",
    "latency_max_s",
    "total_requests",
    "errors",
    "load_mode",
    "target_rps",
}


def test_oha_driver_closed_loop_and_open_loop_commands_and_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify OhaDriver closed-loop vs open-loop commands, NO_COLOR stripping, and JSON parsing."""
    driver = OhaDriver()

    closed_cfg = LoadConfig(
        url="http://127.0.0.1:8000/plaintext",
        duration=15,
        concurrency=32,
        method="POST",
        body="payload",
        headers=(("X-Test", "1"),),
    )
    closed_cmd = driver.build_command(closed_cfg)
    assert "--latency-correction" in closed_cmd
    assert "-q" not in closed_cmd
    assert closed_cmd[closed_cmd.index("-H") : closed_cmd.index("-H") + 2] == ["-H", "X-Test: 1"]
    assert closed_cmd[closed_cmd.index("-d") : closed_cmd.index("-d") + 4] == [
        "-d",
        "payload",
        "-T",
        "application/octet-stream",
    ]
    assert closed_cmd[-1] == "http://127.0.0.1:8000/plaintext"

    open_cfg = LoadConfig(
        url="http://127.0.0.1:8000/plaintext",
        duration=10,
        concurrency=64,
        rate=1500.0,
    )
    open_cmd = driver.build_command(open_cfg)
    assert "--latency-correction" not in open_cmd
    q_idx = open_cmd.index("-q")
    assert open_cmd[q_idx + 1] == "1500.0"

    monkeypatch.setenv("NO_COLOR", "1")
    child_env = driver._build_env()
    assert "NO_COLOR" not in child_env

    sample_json = json.dumps(
        {
            "summary": {
                "successRate": 0.99,
                "total": 10.0,
                "average": 0.0012,
                "slowest": 0.025,
            },
            "latencyPercentiles": {
                "p50": 0.001,
                "p95": 0.004,
                "p99": 0.009,
                "p99.9": 0.018,
            },
            "rps": {"mean": 1400.0},
            "statusCodeDistribution": {"200": 14950, "500": 30},
            "errorDistribution": {"connection reset": 20},
        }
    )

    parsed = driver.parse_output(sample_json, open_cfg)
    assert set(parsed.keys()) == EXPECTED_RESULT_KEYS
    assert parsed["rps"] == pytest.approx(1495.0)
    assert parsed["latency_mean_s"] == pytest.approx(0.0012)
    assert parsed["latency_p50_s"] == pytest.approx(0.001)
    assert parsed["latency_p95_s"] == pytest.approx(0.004)
    assert parsed["latency_p99_s"] == pytest.approx(0.009)
    assert parsed["latency_p999_s"] == pytest.approx(0.018)
    assert parsed["latency_max_s"] == pytest.approx(0.025)
    assert parsed["total_requests"] == 15000
    assert parsed["errors"] == 50
    assert parsed["load_mode"] == "open_loop"
    assert parsed["target_rps"] == pytest.approx(1500.0)

    closed_parsed = driver.parse_output(sample_json, closed_cfg)
    assert closed_parsed["load_mode"] == "closed_loop"
    assert closed_parsed["target_rps"] is None


@pytest.mark.parametrize(
    ("max_token", "mean_token", "expected_max_s", "expected_mean_s"),
    [
        ("850us", "125us", 8.5e-4, 1.25e-4),
        ("15.50ms", "1.25ms", 0.0155, 0.00125),
        ("1.50s", "0.25s", 1.5, 0.25),
    ],
)
def test_h2load_driver_command_and_unit_conversion(
    max_token: str,
    mean_token: str,
    expected_max_s: float,
    expected_mean_s: float,
) -> None:
    """Verify h2load command assembly, per-client RPS calculation, and unit conversion."""
    driver = H2LoadDriver()
    cfg = LoadConfig(
        url="https://127.0.0.1:8443/json",
        duration=12,
        concurrency=16,
        max_concurrent_streams=64,
        rate=1600.0,
        headers=(("Authorization", "Bearer test-token-placeholder"),),
    )

    cmd = driver.build_command(cfg)
    assert cmd[:7] == ["h2load", "-D", "12", "-c", "16", "-m", "64"]
    rps_idx = cmd.index("--rps")
    assert float(cmd[rps_idx + 1]) == pytest.approx(100.0)
    assert "-H" in cmd
    assert cmd[-1] == "https://127.0.0.1:8443/json"

    stdout = (
        "starting benchmark...\n"
        "finished in 12.00s, 1598.50 req/s, 2.40MB/s\n"
        "requests: 19200 total, 19200 started, 19200 done, 19190 succeeded, 10 failed, 3 errored, 0 timeout\n"
        "status codes: 19190 2xx, 0 3xx, 0 4xx, 7 5xx\n"
        "                     min         max         mean         sd        +/- sd\n"
        f"time for request:      50us     {max_token}      {mean_token}      100us    88.00%\n"
    )

    parsed = driver.parse_output(stdout, cfg)
    assert set(parsed.keys()) == EXPECTED_RESULT_KEYS
    assert parsed["rps"] == pytest.approx(1598.50)
    assert parsed["latency_mean_s"] == pytest.approx(expected_mean_s)
    assert parsed["latency_p50_s"] == pytest.approx(expected_mean_s)
    assert parsed["latency_p95_s"] == pytest.approx(expected_max_s)
    assert parsed["latency_p99_s"] == pytest.approx(expected_max_s)
    assert parsed["latency_p999_s"] == pytest.approx(expected_max_s)
    assert parsed["latency_max_s"] == pytest.approx(expected_max_s)
    assert parsed["total_requests"] == 19200
    assert parsed["errors"] == 10
    assert parsed["load_mode"] == "open_loop"
    assert parsed["target_rps"] == pytest.approx(1600.0)


def test_ghz_driver_command_nanosecond_conversion_and_validation() -> None:
    """Verify ghz command assembly, nanosecond conversion, error counting, and validation."""
    driver = GhzDriver()

    with pytest.raises(ValueError, match="grpc_call"):
        driver.build_command(LoadConfig(url="grpc://127.0.0.1:50051"))

    cfg = LoadConfig(
        url="grpc://127.0.0.1:50051/",
        duration=8,
        concurrency=16,
        rate=2000.0,
        grpc_call="bench.EchoService.Echo",
        grpc_proto="bench.proto",
        grpc_data='{"msg":"hi"}',
        insecure=True,
    )
    cmd = driver.build_command(cfg)
    assert cmd == [
        "ghz",
        "--format",
        "json",
        "-z",
        "8s",
        "-c",
        "16",
        "--call",
        "bench.EchoService.Echo",
        "--insecure",
        "--proto",
        "bench.proto",
        "-d",
        '{"msg":"hi"}',
        "--rps",
        "2000",
        "127.0.0.1:50051",
    ]

    sample_json = json.dumps(
        {
            "count": 16000,
            "rps": 1995.25,
            "average": 1_500_000,
            "slowest": 25_000_000,
            "latencyDistribution": [
                {"percentage": 50, "latency": 1_000_000},
                {"percentage": 95, "latency": 4_500_000},
                {"percentage": 99, "latency": 9_000_000},
                {"percentage": 99.9, "latency": 18_000_000},
            ],
            "statusCodeDistribution": {"OK": 15990, "Unavailable": 7, "DeadlineExceeded": 3},
            "errorDistribution": {"rpc error: code = Unavailable": 2},
        }
    )

    parsed = driver.parse_output(sample_json, cfg)
    assert set(parsed.keys()) == EXPECTED_RESULT_KEYS
    assert parsed["rps"] == pytest.approx(1995.25)
    assert parsed["latency_mean_s"] == pytest.approx(0.0015)
    assert parsed["latency_p50_s"] == pytest.approx(0.001)
    assert parsed["latency_p95_s"] == pytest.approx(0.0045)
    assert parsed["latency_p99_s"] == pytest.approx(0.009)
    assert parsed["latency_p999_s"] == pytest.approx(0.018)
    assert parsed["latency_max_s"] == pytest.approx(0.025)
    assert parsed["total_requests"] == 16000
    assert parsed["errors"] == 12
    assert parsed["load_mode"] == "open_loop"
    assert parsed["target_rps"] == pytest.approx(2000.0)


def test_k6_driver_command_millisecond_conversion_and_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify k6 command assembly, summary export file handling, millisecond conversion, and validation."""
    driver = K6Driver()

    with pytest.raises(ValueError, match="k6_script"):
        driver.build_command(LoadConfig(url="http://127.0.0.1:8000/json"))

    cfg = LoadConfig(
        url="http://127.0.0.1:8000/json",
        duration=10,
        concurrency=32,
        rate=500.0,
        k6_script="tools/benchmarks/scripts/open_loop.js",
    )
    cmd = driver.build_command(cfg, summary_path="/tmp/k6-summary.json")
    assert cmd == [
        "k6",
        "run",
        "--summary-export",
        "/tmp/k6-summary.json",
        "-e",
        "TARGET_URL=http://127.0.0.1:8000/json",
        "-e",
        "DURATION=10s",
        "-e",
        "CONCURRENCY=32",
        "-e",
        "TARGET_RPS=500.0",
        "tools/benchmarks/scripts/open_loop.js",
    ]

    sample_summary = json.dumps(
        {
            "metrics": {
                "http_reqs": {"count": 5000, "rate": 499.8},
                "http_req_duration": {
                    "avg": 2.5,
                    "med": 1.8,
                    "p(95)": 6.2,
                    "p(99)": 12.4,
                    "p(99.9)": 20.0,
                    "max": 35.0,
                },
                "http_req_failed": {"passes": 4, "fails": 4996},
            }
        }
    )

    parsed = driver.parse_output(sample_summary, cfg)
    assert set(parsed.keys()) == EXPECTED_RESULT_KEYS
    assert parsed["rps"] == pytest.approx(499.8)
    assert parsed["latency_mean_s"] == pytest.approx(0.0025)
    assert parsed["latency_p50_s"] == pytest.approx(0.0018)
    assert parsed["latency_p95_s"] == pytest.approx(0.0062)
    assert parsed["latency_p99_s"] == pytest.approx(0.0124)
    assert parsed["latency_p999_s"] == pytest.approx(0.02)
    assert parsed["latency_max_s"] == pytest.approx(0.035)
    assert parsed["total_requests"] == 5000
    assert parsed["errors"] == 4
    assert parsed["load_mode"] == "open_loop"
    assert parsed["target_rps"] == pytest.approx(500.0)

    captured_timeout: list[int | float | None] = []

    def _fake_run(
        args: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int | float | None = None,
        **_kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert text is True
        assert check is True
        captured_timeout.append(timeout)
        export_idx = args.index("--summary-export")
        Path(args[export_idx + 1]).write_text(sample_summary, encoding="utf-8")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("tools.benchmarks.drivers.shutil.which", lambda _binary: "/usr/bin/k6")
    monkeypatch.setattr("tools.benchmarks.drivers.subprocess.run", _fake_run)

    run_result = driver.run(cfg)
    assert captured_timeout == [40]
    assert run_result["total_requests"] == 5000


@pytest.mark.parametrize(
    ("driver_name", "expected_type", "config"),
    [
        ("oha", OhaDriver, LoadConfig(url="http://127.0.0.1:8000/")),
        ("h2load", H2LoadDriver, LoadConfig(url="https://127.0.0.1:8443/")),
        ("ghz", GhzDriver, LoadConfig(url="127.0.0.1:50051", grpc_call="bench.Svc.Call")),
        ("k6", K6Driver, LoadConfig(url="http://127.0.0.1:8000/", k6_script="script.js")),
    ],
)
def test_missing_binary_raises_actionable_error_and_docker_builder(
    monkeypatch: pytest.MonkeyPatch,
    driver_name: str,
    expected_type: type[Any],
    config: LoadConfig,
) -> None:
    """Verify missing binary fails loudly before subprocess.run and Docker builder maps all drivers."""
    driver = create_driver(driver_name)
    assert isinstance(driver, expected_type)

    monkeypatch.setattr("tools.benchmarks.drivers.shutil.which", lambda _binary: None)

    def _fail_subprocess(*_args: Any, **_kwargs: Any) -> Any:
        msg = "subprocess.run must not be invoked when binary is missing"
        raise AssertionError(msg)

    monkeypatch.setattr("tools.benchmarks.drivers.subprocess.run", _fail_subprocess)

    with pytest.raises(DriverBinaryNotFoundError) as exc_info:
        driver.run(config)

    message = str(exc_info.value)
    assert driver_name in message
    assert "make bench-tools" in message

    docker_cmd = build_docker_driver_command(driver_name, ["--version"])
    assert docker_cmd == [
        "docker",
        "run",
        "--rm",
        "--network",
        "host",
        DOCKER_DRIVER_IMAGES[driver_name],
        "--version",
    ]


@pytest.mark.parametrize("bad_rate", [0.0, -1.0, -500.5])
@pytest.mark.parametrize("driver_name", ["oha", "h2load", "ghz", "k6"])
def test_invalid_rate_raises_value_error(driver_name: str, bad_rate: float) -> None:
    """Verify non-positive rate values raise ValueError across every driver."""
    driver = create_driver(driver_name)
    cfg = LoadConfig(
        url="http://127.0.0.1:8000/",
        rate=bad_rate,
        grpc_call="bench.Svc.Call",
        k6_script="script.js",
    )
    with pytest.raises(ValueError, match="rate must be positive"):
        driver.build_command(cfg)


@pytest.mark.parametrize(
    ("driver_name", "bad_output"),
    [
        ("oha", ""),
        ("oha", "{}"),
        ("oha", '{"summary": {"total": 1.0}, "statusCodeDistribution": {}}'),
        ("h2load", ""),
        ("h2load", "unparseable output"),
        ("ghz", ""),
        ("ghz", '{"count": 0, "rps": 0}'),
        ("k6", ""),
        ("k6", '{"metrics": {"http_reqs": {"count": 0}}}'),
    ],
)
def test_malformed_or_zero_request_output_raises(driver_name: str, bad_output: str) -> None:
    """Verify empty, malformed, or zero-request outputs raise ValueError or JSONDecodeError."""
    driver = create_driver(driver_name)
    cfg = LoadConfig(
        url="http://127.0.0.1:8000/",
        grpc_call="bench.Svc.Call",
        k6_script="script.js",
    )
    with pytest.raises(ValueError):
        driver.parse_output(bad_output, cfg)


def test_unknown_driver_names_raise_value_error() -> None:
    """Verify unknown driver names raise ValueError in create_driver and build_docker_driver_command."""
    with pytest.raises(ValueError, match="Unknown load driver"):
        create_driver("wrk")

    with pytest.raises(ValueError, match="Unknown load driver"):
        build_docker_driver_command("wrk", [])
