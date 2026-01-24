"""Unit tests for benchmarks.servers.scenarios module."""

import pytest
from benchmarks.servers.scenarios import (
    DEFAULT_CONCURRENCY,
    DEFAULT_SCENARIOS,
    FULL_CONCURRENCY,
    FULL_SCENARIOS,
    IO_SCENARIOS,
    MEASURE_DURATION,
    PRIMER_DURATION,
    QUICK_CONCURRENCY,
    QUICK_MEASURE_DURATION,
    QUICK_PRIMER_DURATION,
    QUICK_SCENARIOS,
    QUICK_WARMUP_DURATION,
    SCENARIOS,
    SIZE_SCENARIOS,
    WARMUP_DURATION,
    Scenario,
    get_concurrency,
    get_durations,
    get_scenarios,
)


@pytest.mark.unit
def test_scenario_dataclass() -> None:
    """Test Scenario dataclass creation."""
    scenario = Scenario(
        name="hello",
        endpoint="/hello",
        description="12-byte JSON response",
        method="GET",
        body=None,
        headers={},
        expected_response_bytes=17,
    )

    assert scenario.name == "hello"
    assert scenario.endpoint == "/hello"
    assert scenario.description == "12-byte JSON response"
    assert scenario.method == "GET"
    assert scenario.body is None
    assert scenario.headers == {}
    assert scenario.expected_response_bytes == 17


@pytest.mark.unit
def test_scenario_get_url() -> None:
    """Test Scenario.get_url() constructs correct URL."""
    scenario = Scenario(
        name="hello",
        endpoint="/hello",
        description="Test",
    )

    url = scenario.get_url(port=8000)
    assert url == "http://127.0.0.1:8000/hello"

    url = scenario.get_url(port=9999)
    assert url == "http://127.0.0.1:9999/hello"


@pytest.mark.unit
def test_scenario_with_post_body() -> None:
    """Test Scenario with POST method and body."""
    scenario = Scenario(
        name="echo",
        endpoint="/echo",
        description="Echo POST body",
        method="POST",
        body=b"test data",
        headers={"Content-Type": "text/plain"},
    )

    assert scenario.method == "POST"
    assert scenario.body == b"test data"
    assert scenario.headers["Content-Type"] == "text/plain"


@pytest.mark.unit
def test_scenario_frozen() -> None:
    """Test that Scenario is frozen (immutable)."""
    scenario = Scenario(
        name="hello",
        endpoint="/hello",
        description="Test",
    )

    with pytest.raises(AttributeError):
        scenario.name = "changed"  # type: ignore[misc]


@pytest.mark.unit
def test_scenarios_dict_contains_all_scenarios() -> None:
    """Test that SCENARIOS dict contains expected scenarios."""
    assert "hello" in SCENARIOS
    assert "body_10b" in SCENARIOS
    assert "body_1k" in SCENARIOS
    assert "body_10k" in SCENARIOS
    assert "body_100k" in SCENARIOS
    assert "echo" in SCENARIOS
    assert "io_10ms" in SCENARIOS
    assert "io_100ms" in SCENARIOS
    assert "file" in SCENARIOS
    assert "json" in SCENARIOS


@pytest.mark.unit
def test_hello_scenario() -> None:
    """Test hello scenario configuration."""
    scenario = SCENARIOS["hello"]

    assert scenario.name == "hello"
    assert scenario.endpoint == "/hello"
    assert scenario.method == "GET"
    assert scenario.body is None
    assert scenario.expected_response_bytes == 17


@pytest.mark.unit
def test_body_scenarios() -> None:
    """Test body size scenarios."""
    body_10b = SCENARIOS["body_10b"]
    assert body_10b.endpoint == "/b10"
    assert body_10b.expected_response_bytes == 10

    body_1k = SCENARIOS["body_1k"]
    assert body_1k.endpoint == "/b1k"
    assert body_1k.expected_response_bytes == 1024

    body_10k = SCENARIOS["body_10k"]
    assert body_10k.endpoint == "/b10k"
    assert body_10k.expected_response_bytes == 10240

    body_100k = SCENARIOS["body_100k"]
    assert body_100k.endpoint == "/b100k"
    assert body_100k.expected_response_bytes == 102400


@pytest.mark.unit
def test_echo_scenario() -> None:
    """Test echo scenario with POST body."""
    scenario = SCENARIOS["echo"]

    assert scenario.name == "echo"
    assert scenario.endpoint == "/echo"
    assert scenario.method == "POST"
    assert scenario.body == b"x" * 64
    assert scenario.headers["Content-Type"] == "text/plain"


@pytest.mark.unit
def test_io_scenarios() -> None:
    """Test I/O simulation scenarios."""
    io_10ms = SCENARIOS["io_10ms"]
    assert io_10ms.endpoint == "/io10"
    assert "10ms" in io_10ms.description

    io_100ms = SCENARIOS["io_100ms"]
    assert io_100ms.endpoint == "/io100"
    assert "100ms" in io_100ms.description


@pytest.mark.unit
def test_file_scenario() -> None:
    """Test file serving scenario."""
    scenario = SCENARIOS["file"]

    assert scenario.endpoint == "/file"
    assert scenario.expected_response_bytes == 51200  # 50KB


@pytest.mark.unit
def test_json_scenario() -> None:
    """Test JSON serialization scenario."""
    scenario = SCENARIOS["json"]

    assert scenario.endpoint == "/json"
    assert scenario.method == "GET"


@pytest.mark.unit
def test_scenario_groups() -> None:
    """Test scenario group constants."""
    assert len(DEFAULT_SCENARIOS) > 0
    assert len(QUICK_SCENARIOS) > 0
    assert len(FULL_SCENARIOS) > 0
    assert len(SIZE_SCENARIOS) > 0
    assert len(IO_SCENARIOS) > 0

    # Quick scenarios should be a subset of default scenarios
    assert set(QUICK_SCENARIOS).issubset(set(DEFAULT_SCENARIOS))

    # Default scenarios should be a subset of full scenarios
    assert set(DEFAULT_SCENARIOS).issubset(set(FULL_SCENARIOS))


@pytest.mark.unit
def test_size_scenarios_group() -> None:
    """Test SIZE_SCENARIOS group contains body size scenarios."""
    assert "body_10b" in SIZE_SCENARIOS
    assert "body_1k" in SIZE_SCENARIOS
    assert "body_10k" in SIZE_SCENARIOS
    assert "body_100k" in SIZE_SCENARIOS


@pytest.mark.unit
def test_io_scenarios_group() -> None:
    """Test IO_SCENARIOS group contains I/O scenarios."""
    assert "io_10ms" in IO_SCENARIOS
    assert "io_100ms" in IO_SCENARIOS


@pytest.mark.unit
def test_concurrency_levels() -> None:
    """Test concurrency level constants."""
    assert len(DEFAULT_CONCURRENCY) > 0
    assert len(QUICK_CONCURRENCY) > 0
    assert len(FULL_CONCURRENCY) > 0

    # Quick concurrency should be a subset of default
    assert set(QUICK_CONCURRENCY).issubset(set(DEFAULT_CONCURRENCY))

    # Default should be a subset of full
    assert set(DEFAULT_CONCURRENCY).issubset(set(FULL_CONCURRENCY))


@pytest.mark.unit
def test_duration_constants() -> None:
    """Test duration constants are positive integers."""
    assert PRIMER_DURATION > 0
    assert WARMUP_DURATION > 0
    assert MEASURE_DURATION > 0

    assert QUICK_PRIMER_DURATION > 0
    assert QUICK_WARMUP_DURATION > 0
    assert QUICK_MEASURE_DURATION > 0

    # Quick durations should be shorter
    assert QUICK_PRIMER_DURATION <= PRIMER_DURATION
    assert QUICK_WARMUP_DURATION <= WARMUP_DURATION
    assert QUICK_MEASURE_DURATION <= MEASURE_DURATION


@pytest.mark.unit
def test_get_scenarios_default() -> None:
    """Test get_scenarios() with default parameters."""
    scenarios = get_scenarios()

    assert len(scenarios) > 0
    assert all(name in SCENARIOS for name in scenarios)
    # Should return DEFAULT_SCENARIOS
    assert set(scenarios.keys()) == set(DEFAULT_SCENARIOS)


@pytest.mark.unit
def test_get_scenarios_quick() -> None:
    """Test get_scenarios() with quick=True."""
    scenarios = get_scenarios(quick=True)

    assert len(scenarios) > 0
    assert all(name in SCENARIOS for name in scenarios)
    # Should return QUICK_SCENARIOS
    assert set(scenarios.keys()) == set(QUICK_SCENARIOS)


@pytest.mark.unit
def test_get_scenarios_with_names() -> None:
    """Test get_scenarios() with specific scenario names."""
    scenarios = get_scenarios(names=["hello", "body_1k"])

    assert len(scenarios) == 2
    assert "hello" in scenarios
    assert "body_1k" in scenarios
    assert scenarios["hello"] == SCENARIOS["hello"]
    assert scenarios["body_1k"] == SCENARIOS["body_1k"]


@pytest.mark.unit
def test_get_scenarios_with_invalid_names() -> None:
    """Test get_scenarios() filters out invalid scenario names."""
    scenarios = get_scenarios(names=["hello", "invalid_scenario", "body_1k"])

    # Should only return valid scenarios
    assert len(scenarios) == 2
    assert "hello" in scenarios
    assert "body_1k" in scenarios
    assert "invalid_scenario" not in scenarios


@pytest.mark.unit
def test_get_scenarios_empty_names() -> None:
    """Test get_scenarios() with empty names list."""
    scenarios = get_scenarios(names=[])

    assert len(scenarios) == 0


@pytest.mark.unit
def test_get_concurrency_default() -> None:
    """Test get_concurrency() with default parameters."""
    concurrency = get_concurrency()

    assert len(concurrency) > 0
    assert all(c > 0 for c in concurrency)
    assert concurrency == DEFAULT_CONCURRENCY


@pytest.mark.unit
def test_get_concurrency_quick() -> None:
    """Test get_concurrency() with quick=True."""
    concurrency = get_concurrency(quick=True)

    assert len(concurrency) > 0
    assert all(c > 0 for c in concurrency)
    assert concurrency == QUICK_CONCURRENCY


@pytest.mark.unit
def test_get_durations_default() -> None:
    """Test get_durations() with default parameters."""
    primer, warmup, measure = get_durations()

    assert primer == PRIMER_DURATION
    assert warmup == WARMUP_DURATION
    assert measure == MEASURE_DURATION

    # All should be positive
    assert primer > 0
    assert warmup > 0
    assert measure > 0


@pytest.mark.unit
def test_get_durations_quick() -> None:
    """Test get_durations() with quick=True."""
    primer, warmup, measure = get_durations(quick=True)

    assert primer == QUICK_PRIMER_DURATION
    assert warmup == QUICK_WARMUP_DURATION
    assert measure == QUICK_MEASURE_DURATION

    # All should be positive
    assert primer > 0
    assert warmup > 0
    assert measure > 0


@pytest.mark.unit
def test_durations_tuple_unpacking() -> None:
    """Test that get_durations() returns a tuple that can be unpacked."""
    durations = get_durations()

    assert isinstance(durations, tuple)
    assert len(durations) == 3

    primer, warmup, measure = durations
    assert isinstance(primer, int)
    assert isinstance(warmup, int)
    assert isinstance(measure, int)
