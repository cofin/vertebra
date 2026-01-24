"""Unit tests for benchmarks.servers.results module."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from benchmarks.servers.config import BenchmarkCategory
from benchmarks.servers.parsers import OhaResult
from benchmarks.servers.results import BenchmarkResult, BenchmarkSuite, EnvironmentInfo


@pytest.mark.unit
def test_environment_info_dataclass() -> None:
    """Test EnvironmentInfo dataclass creation."""
    env = EnvironmentInfo(
        cpu="AMD Ryzen 7 5700X",
        cpu_cores=16,
        os="Linux",
        os_version="6.14.0",
        python_version="3.12.0",
        oha_version="1.4.0",
        timestamp="2026-01-24T10:00:00Z",
    )

    assert env.cpu == "AMD Ryzen 7 5700X"
    assert env.cpu_cores == 16
    assert env.os == "Linux"
    assert env.python_version == "3.12.0"
    assert env.oha_version == "1.4.0"


@pytest.mark.unit
def test_environment_info_detect() -> None:
    """Test EnvironmentInfo.detect() creates valid environment info."""
    env = EnvironmentInfo.detect(oha_version="1.4.0")

    assert env.cpu is not None
    assert env.cpu_cores > 0
    assert env.os in ["Linux", "Darwin", "Windows"]
    assert env.python_version.count(".") == 2  # major.minor.micro
    assert env.oha_version == "1.4.0"
    # Timestamp should be valid ISO format
    datetime.fromisoformat(env.timestamp.replace("Z", "+00:00"))


@pytest.mark.unit
def test_benchmark_result_dataclass() -> None:
    """Test BenchmarkResult dataclass creation."""
    result = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
        total_data_bytes=1000000,
        bytes_per_second=500000.0,
    )

    assert result.server == "uvicorn_h11"
    assert result.scenario == "hello"
    assert result.rps == 10000.0
    assert result.category == BenchmarkCategory.SERVER


@pytest.mark.unit
def test_benchmark_result_from_oha() -> None:
    """Test BenchmarkResult.from_oha() factory method."""
    oha_result = OhaResult(
        total_requests=100000,
        successful_requests=99950,
        failed_requests=50,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
        total_data_bytes=1000000,
        bytes_per_second=500000.0,
        raw_data={},
    )

    result = BenchmarkResult.from_oha(
        oha_result=oha_result,
        server="vertebra",
        server_display="Vertebra",
        category=BenchmarkCategory.RUNTIME,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        extra_key="extra_value",
    )

    assert result.server == "vertebra"
    assert result.server_display == "Vertebra"
    assert result.category == BenchmarkCategory.RUNTIME
    assert result.scenario == "hello"
    assert result.rps == 10000.0
    assert result.total_requests == 100000
    assert result.metadata == {"extra_key": "extra_value"}


@pytest.mark.unit
def test_benchmark_result_sanity_check_pass() -> None:
    """Test BenchmarkResult sanity check with valid data."""
    result = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=500000,
        rps=50000.0,
        success_rate=100.0,
        latency_avg_ms=1.2,
        latency_p50_ms=1.0,
        latency_p75_ms=1.5,
        latency_p90_ms=2.0,
        latency_p95_ms=2.5,
        latency_p99_ms=3.0,
        latency_max_ms=5.0,
        latency_min_ms=0.5,
    )

    assert result.passes_sanity_check is True


@pytest.mark.unit
def test_benchmark_result_sanity_check_fail_low_rps() -> None:
    """Test BenchmarkResult sanity check fails with low RPS."""
    result = BenchmarkResult(
        server="slow_server",
        server_display="Slow Server",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=500,
        rps=50.0,  # Below 100 RPS threshold
        success_rate=100.0,
        latency_avg_ms=1.2,
        latency_p50_ms=1.0,
        latency_p75_ms=1.5,
        latency_p90_ms=2.0,
        latency_p95_ms=2.5,
        latency_p99_ms=3.0,
        latency_max_ms=5.0,
        latency_min_ms=0.5,
    )

    assert result.passes_sanity_check is False


@pytest.mark.unit
def test_benchmark_result_to_dict() -> None:
    """Test BenchmarkResult.to_dict() serialization."""
    result = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    d = result.to_dict()

    assert d["server"] == "uvicorn_h11"
    assert d["category"] == "server"  # Enum value
    assert d["rps"] == 10000.0


@pytest.mark.unit
def test_benchmark_suite_creation() -> None:
    """Test BenchmarkSuite creation and basic operations."""
    env = EnvironmentInfo(
        cpu="Test CPU",
        cpu_cores=8,
        os="Linux",
        os_version="6.0.0",
        python_version="3.12.0",
        oha_version="1.4.0",
        timestamp=datetime.now(UTC).isoformat(),
    )

    suite = BenchmarkSuite(environment=env)

    assert suite.environment == env
    assert len(suite.results) == 0
    assert len(suite.errors) == 0


@pytest.mark.unit
def test_benchmark_suite_add_result() -> None:
    """Test adding results to BenchmarkSuite."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=150000,
        rps=15000.0,
        success_rate=100.0,
        latency_avg_ms=1.8,
        latency_p50_ms=1.5,
        latency_p75_ms=2.0,
        latency_p90_ms=2.5,
        latency_p95_ms=3.0,
        latency_p99_ms=4.0,
        latency_max_ms=8.0,
        latency_min_ms=0.8,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    assert len(suite.results) == 2


@pytest.mark.unit
def test_benchmark_suite_add_error() -> None:
    """Test adding errors to BenchmarkSuite."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    suite.add_error(
        server="broken_server",
        scenario="hello",
        error="Failed to start server",
        extra_info="Port 8000 already in use",
    )

    assert len(suite.errors) == 1
    assert suite.errors[0]["server"] == "broken_server"
    assert suite.errors[0]["error"] == "Failed to start server"
    assert suite.errors[0]["extra_info"] == "Port 8000 already in use"


@pytest.mark.unit
def test_benchmark_suite_get_results_by_scenario() -> None:
    """Test filtering results by scenario."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="body_1k",
        concurrency=64,
        duration_seconds=10,
        total_requests=80000,
        rps=8000.0,
        success_rate=99.95,
        latency_avg_ms=3.0,
        latency_p50_ms=2.5,
        latency_p75_ms=3.2,
        latency_p90_ms=4.0,
        latency_p95_ms=4.5,
        latency_p99_ms=6.0,
        latency_max_ms=12.0,
        latency_min_ms=1.2,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    hello_results = suite.get_results_by_scenario("hello")
    assert len(hello_results) == 1
    assert hello_results[0].scenario == "hello"

    body_results = suite.get_results_by_scenario("body_1k")
    assert len(body_results) == 1
    assert body_results[0].scenario == "body_1k"


@pytest.mark.unit
def test_benchmark_suite_get_results_by_server() -> None:
    """Test filtering results by server."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=150000,
        rps=15000.0,
        success_rate=100.0,
        latency_avg_ms=1.8,
        latency_p50_ms=1.5,
        latency_p75_ms=2.0,
        latency_p90_ms=2.5,
        latency_p95_ms=3.0,
        latency_p99_ms=4.0,
        latency_max_ms=8.0,
        latency_min_ms=0.8,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    uvicorn_results = suite.get_results_by_server("uvicorn_h11")
    assert len(uvicorn_results) == 1
    assert uvicorn_results[0].server == "uvicorn_h11"

    granian_results = suite.get_results_by_server("granian")
    assert len(granian_results) == 1
    assert granian_results[0].server == "granian"


@pytest.mark.unit
def test_benchmark_suite_get_results_by_category() -> None:
    """Test filtering results by category."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="vertebra",
        server_display="Vertebra",
        category=BenchmarkCategory.RUNTIME,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=120000,
        rps=12000.0,
        success_rate=100.0,
        latency_avg_ms=2.0,
        latency_p50_ms=1.8,
        latency_p75_ms=2.2,
        latency_p90_ms=2.8,
        latency_p95_ms=3.2,
        latency_p99_ms=4.5,
        latency_max_ms=9.0,
        latency_min_ms=0.9,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    server_results = suite.get_results_by_category(BenchmarkCategory.SERVER)
    assert len(server_results) == 1
    assert server_results[0].category == BenchmarkCategory.SERVER

    runtime_results = suite.get_results_by_category(BenchmarkCategory.RUNTIME)
    assert len(runtime_results) == 1
    assert runtime_results[0].category == BenchmarkCategory.RUNTIME


@pytest.mark.unit
def test_benchmark_suite_get_scenarios() -> None:
    """Test getting unique scenario names."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="body_1k",
        concurrency=64,
        duration_seconds=10,
        total_requests=80000,
        rps=8000.0,
        success_rate=99.95,
        latency_avg_ms=3.0,
        latency_p50_ms=2.5,
        latency_p75_ms=3.2,
        latency_p90_ms=4.0,
        latency_p95_ms=4.5,
        latency_p99_ms=6.0,
        latency_max_ms=12.0,
        latency_min_ms=1.2,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    scenarios = suite.get_scenarios()
    assert scenarios == ["body_1k", "hello"]  # Sorted


@pytest.mark.unit
def test_benchmark_suite_get_servers() -> None:
    """Test getting unique server names."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    result1 = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    result2 = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=150000,
        rps=15000.0,
        success_rate=100.0,
        latency_avg_ms=1.8,
        latency_p50_ms=1.5,
        latency_p75_ms=2.0,
        latency_p90_ms=2.5,
        latency_p95_ms=3.0,
        latency_p99_ms=4.0,
        latency_max_ms=8.0,
        latency_min_ms=0.8,
    )

    suite.add_result(result1)
    suite.add_result(result2)

    servers = suite.get_servers()
    assert servers == ["granian", "uvicorn_h11"]  # Sorted


@pytest.mark.unit
def test_benchmark_suite_json_serialization(tmp_path: Path) -> None:
    """Test JSON serialization and deserialization."""
    env = EnvironmentInfo(
        cpu="Test CPU",
        cpu_cores=8,
        os="Linux",
        os_version="6.0.0",
        python_version="3.12.0",
        oha_version="1.4.0",
        timestamp="2026-01-24T10:00:00Z",
    )

    suite = BenchmarkSuite(environment=env)

    result = BenchmarkResult(
        server="uvicorn_h11",
        server_display="Uvicorn (h11)",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=100000,
        rps=10000.0,
        success_rate=99.95,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
    )

    suite.add_result(result)
    suite.add_error(server="broken_server", scenario="hello", error="Failed to start")

    # Serialize to JSON
    json_str = suite.to_json()
    data = json.loads(json_str)

    assert data["environment"]["cpu"] == "Test CPU"
    assert len(data["results"]) == 1
    assert len(data["errors"]) == 1

    # Deserialize from JSON
    loaded_suite = BenchmarkSuite.from_json(json_str)

    assert loaded_suite.environment.cpu == "Test CPU"
    assert len(loaded_suite.results) == 1
    assert loaded_suite.results[0].server == "uvicorn_h11"
    assert len(loaded_suite.errors) == 1


@pytest.mark.unit
def test_benchmark_suite_save_and_load_json(tmp_path: Path) -> None:
    """Test saving to and loading from JSON file."""
    env = EnvironmentInfo(
        cpu="Test CPU",
        cpu_cores=8,
        os="Linux",
        os_version="6.0.0",
        python_version="3.12.0",
        oha_version="1.4.0",
        timestamp="2026-01-24T10:00:00Z",
    )

    suite = BenchmarkSuite(environment=env)

    result = BenchmarkResult(
        server="granian",
        server_display="Granian",
        category=BenchmarkCategory.SERVER,
        scenario="hello",
        concurrency=64,
        duration_seconds=10,
        total_requests=150000,
        rps=15000.0,
        success_rate=100.0,
        latency_avg_ms=1.8,
        latency_p50_ms=1.5,
        latency_p75_ms=2.0,
        latency_p90_ms=2.5,
        latency_p95_ms=3.0,
        latency_p99_ms=4.0,
        latency_max_ms=8.0,
        latency_min_ms=0.8,
    )

    suite.add_result(result)

    # Save to file
    json_file = tmp_path / "results.json"
    suite.save_json(json_file)

    assert json_file.exists()

    # Load from file
    loaded_suite = BenchmarkSuite.load_json(json_file)

    assert loaded_suite.environment.cpu == "Test CPU"
    assert len(loaded_suite.results) == 1
    assert loaded_suite.results[0].server == "granian"
    assert loaded_suite.results[0].rps == 15000.0
