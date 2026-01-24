"""Unit tests for benchmarks.servers.parsers module."""

import json

import pytest
from benchmarks.servers.parsers import OhaError, OhaResult, parse_oha_output


@pytest.mark.unit
def test_oha_result_dataclass() -> None:
    """Test OhaResult dataclass creation and properties."""
    result = OhaResult(
        total_requests=10000,
        successful_requests=9950,
        failed_requests=50,
        rps=5000.0,
        success_rate=99.5,
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

    assert result.total_requests == 10000
    assert result.rps == 5000.0
    assert result.success_rate == 99.5
    assert result.latency_p99_ms == 5.5
    assert result.passes_sanity_check is True
    assert result.error_rate == 0.5


@pytest.mark.unit
def test_oha_result_sanity_check_pass() -> None:
    """Test OhaResult sanity check with valid data."""
    result = OhaResult(
        total_requests=10000,
        successful_requests=9950,
        failed_requests=50,
        rps=5000.0,
        success_rate=99.5,
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

    assert result.passes_sanity_check is True


@pytest.mark.unit
def test_oha_result_sanity_check_fail_low_rps() -> None:
    """Test OhaResult sanity check fails with zero RPS."""
    result = OhaResult(
        total_requests=0,
        successful_requests=0,
        failed_requests=0,
        rps=0.0,
        success_rate=100.0,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=5.5,
        latency_max_ms=10.0,
        latency_min_ms=1.0,
        total_data_bytes=0,
        bytes_per_second=0.0,
        raw_data={},
    )

    assert result.passes_sanity_check is False


@pytest.mark.unit
def test_oha_result_sanity_check_fail_low_success_rate() -> None:
    """Test OhaResult sanity check fails with low success rate."""
    result = OhaResult(
        total_requests=10000,
        successful_requests=9000,
        failed_requests=1000,
        rps=5000.0,
        success_rate=90.0,
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

    assert result.passes_sanity_check is False


@pytest.mark.unit
def test_oha_result_sanity_check_fail_high_p99() -> None:
    """Test OhaResult sanity check fails with very high p99 latency."""
    result = OhaResult(
        total_requests=10000,
        successful_requests=9950,
        failed_requests=50,
        rps=5000.0,
        success_rate=99.5,
        latency_avg_ms=2.5,
        latency_p50_ms=2.0,
        latency_p75_ms=2.8,
        latency_p90_ms=3.5,
        latency_p95_ms=4.0,
        latency_p99_ms=65000.0,  # Over 60 seconds
        latency_max_ms=70000.0,
        latency_min_ms=1.0,
        total_data_bytes=1000000,
        bytes_per_second=500000.0,
        raw_data={},
    )

    assert result.passes_sanity_check is False


@pytest.mark.unit
def test_parse_oha_output_v1_format() -> None:
    """Test parsing oha v1.x JSON output format."""
    sample_output = {
        "summary": {
            "total": 100000,
            "succeeded": 99950,
            "requestsPerSec": 10000.0,
            "average": 0.0025,  # 2.5ms in seconds
            "slowest": 0.010,  # 10ms in seconds
            "fastest": 0.001,  # 1ms in seconds
            "totalData": 1000000,
            "sizePerSec": 500000.0,
            "latencyDistribution": {
                "50": 0.002,  # 2ms in seconds
                "75": 0.0028,
                "90": 0.0035,
                "95": 0.004,
                "99": 0.0055,
            },
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    assert result.total_requests == 100000
    assert result.successful_requests == 99950
    assert result.failed_requests == 50
    assert result.rps == 10000.0
    assert result.success_rate == 99.95
    assert result.latency_avg_ms == pytest.approx(2.5, rel=0.01)
    assert result.latency_p50_ms == pytest.approx(2.0, rel=0.01)
    assert result.latency_p99_ms == pytest.approx(5.5, rel=0.01)
    assert result.latency_max_ms == pytest.approx(10.0, rel=0.01)
    assert result.latency_min_ms == pytest.approx(1.0, rel=0.01)


@pytest.mark.unit
def test_parse_oha_output_v0_format() -> None:
    """Test parsing oha v0.x JSON output format (legacy)."""
    sample_output = {
        "totalRequests": 100000,
        "successfulRequests": 99950,
        "rps": 10000.0,
        "latencyAvg": 0.0025,
        "latencyMax": 0.010,
        "latencyMin": 0.001,
        "totalBytes": 1000000,
        "bytesPerSec": 500000.0,
        "latencyDistribution": {"p50": 0.002, "p75": 0.0028, "p90": 0.0035, "p95": 0.004, "p99": 0.0055},
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    assert result.total_requests == 100000
    assert result.rps == 10000.0
    assert result.latency_avg_ms == pytest.approx(2.5, rel=0.01)


@pytest.mark.unit
def test_parse_oha_output_minimal() -> None:
    """Test parsing oha output with minimal fields."""
    sample_output = {
        "summary": {
            "total": 1000,
            "succeeded": 1000,
            "requestsPerSec": 100.0,
            "average": 0.01,
            "slowest": 0.05,
            "fastest": 0.005,
            "latencyDistribution": {},
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    assert result.total_requests == 1000
    assert result.successful_requests == 1000
    assert result.failed_requests == 0
    assert result.success_rate == 100.0


@pytest.mark.unit
def test_parse_oha_output_with_failures() -> None:
    """Test parsing oha output with request failures."""
    sample_output = {
        "summary": {
            "total": 10000,
            "succeeded": 9000,
            "requestsPerSec": 1000.0,
            "average": 0.01,
            "slowest": 0.1,
            "fastest": 0.001,
            "latencyDistribution": {"50": 0.008, "99": 0.05},
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    assert result.total_requests == 10000
    assert result.successful_requests == 9000
    assert result.failed_requests == 1000
    assert result.success_rate == 90.0
    assert result.error_rate == 10.0


@pytest.mark.unit
def test_parse_oha_output_latency_conversion() -> None:
    """Test that latencies are correctly converted from seconds to milliseconds."""
    sample_output = {
        "summary": {
            "total": 1000,
            "succeeded": 1000,
            "requestsPerSec": 100.0,
            "average": 0.001234,  # 1.234ms
            "slowest": 0.05,  # 50ms
            "fastest": 0.0001,  # 0.1ms
            "latencyDistribution": {
                "50": 0.001,  # 1ms
                "99": 0.010,  # 10ms
            },
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    # All latencies should be in milliseconds
    assert result.latency_avg_ms == pytest.approx(1.234, rel=0.01)
    assert result.latency_max_ms == pytest.approx(50.0, rel=0.01)
    assert result.latency_min_ms == pytest.approx(0.1, rel=0.01)
    assert result.latency_p50_ms == pytest.approx(1.0, rel=0.01)
    assert result.latency_p99_ms == pytest.approx(10.0, rel=0.01)


@pytest.mark.unit
def test_parse_oha_output_invalid_json() -> None:
    """Test parsing invalid JSON raises OhaError."""
    invalid_json = "not valid json {["

    with pytest.raises(OhaError, match="Failed to parse oha output"):
        parse_oha_output(invalid_json)


@pytest.mark.unit
def test_parse_oha_output_zero_total_requests() -> None:
    """Test parsing oha output with zero total requests."""
    sample_output = {
        "summary": {
            "total": 0,
            "succeeded": 0,
            "requestsPerSec": 0.0,
            "average": 0.0,
            "slowest": 0.0,
            "fastest": 0.0,
            "latencyDistribution": {},
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    assert result.total_requests == 0
    assert result.success_rate == 0.0


@pytest.mark.unit
def test_parse_oha_output_already_in_ms() -> None:
    """Test parsing oha output where values are already in milliseconds."""
    sample_output = {
        "summary": {
            "total": 1000,
            "succeeded": 1000,
            "requestsPerSec": 100.0,
            "average": 1500.0,  # Already in ms (large value)
            "slowest": 5000.0,  # Already in ms
            "fastest": 100.0,  # Already in ms
            "latencyDistribution": {"50": 1200.0, "99": 4000.0},
        }
    }

    json_str = json.dumps(sample_output)
    result = parse_oha_output(json_str)

    # Values should not be converted again
    assert result.latency_avg_ms == 1500.0
    assert result.latency_max_ms == 5000.0
    assert result.latency_min_ms == 100.0
