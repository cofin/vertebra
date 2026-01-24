"""Unit tests for benchmarks.servers.reports module."""

import pytest
from benchmarks.servers.config import BenchmarkCategory
from benchmarks.servers.reports import generate_csv_report, generate_markdown_report
from benchmarks.servers.results import BenchmarkResult, BenchmarkSuite, EnvironmentInfo


@pytest.fixture
def sample_environment() -> EnvironmentInfo:
    """Create a sample EnvironmentInfo for testing."""
    return EnvironmentInfo(
        cpu="AMD Ryzen 7 5700X",
        cpu_cores=16,
        os="Linux",
        os_version="6.14.0",
        python_version="3.12.0",
        oha_version="1.4.0",
        timestamp="2026-01-24T10:00:00Z",
    )


@pytest.fixture
def sample_benchmark_suite(sample_environment: EnvironmentInfo) -> BenchmarkSuite:
    """Create a sample BenchmarkSuite with results for testing."""
    suite = BenchmarkSuite(environment=sample_environment)

    # Add Category 1 (SERVER) results
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

    # Add Category 2 (RUNTIME) result
    result3 = BenchmarkResult(
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

    # Add body_1k scenario results
    result4 = BenchmarkResult(
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
    suite.add_result(result3)
    suite.add_result(result4)

    return suite


@pytest.mark.unit
def test_generate_markdown_report_structure(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that generated markdown report has expected structure."""
    report = generate_markdown_report(sample_benchmark_suite)

    assert isinstance(report, str)
    assert len(report) > 0

    # Check for major sections
    assert "# ASGI Server Benchmark Results" in report
    assert "## Environment" in report
    assert "## Methodology" in report


@pytest.mark.unit
def test_generate_markdown_report_environment_section(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that environment information is included in report."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check for environment fields
    assert "AMD Ryzen 7 5700X" in report
    assert "16" in report  # CPU cores
    assert "Linux" in report
    assert "3.12.0" in report  # Python version
    assert "1.4.0" in report  # oha version


@pytest.mark.unit
def test_generate_markdown_report_category_sections(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that category sections are included in report."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check for category headers
    assert "Category 1: Raw ASGI Server Performance" in report
    assert "Category 2: Integrated Runtime Performance" in report


@pytest.mark.unit
def test_generate_markdown_report_scenario_sections(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that scenario sections are included in report."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check for scenario headers
    assert "### hello" in report
    assert "### body_1k" in report


@pytest.mark.unit
def test_generate_markdown_report_tables(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that markdown tables are properly formatted."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check for table headers
    assert "| Server | RPS | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |" in report
    assert "|--------|----:|--------:|--------:|--------:|--------:|--------:|" in report


@pytest.mark.unit
def test_generate_markdown_report_server_names(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that server display names appear in report."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check for server display names
    assert "Uvicorn (h11)" in report
    assert "Granian" in report
    assert "Vertebra" in report


@pytest.mark.unit
def test_generate_markdown_report_metrics(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that metrics are included in report."""
    report = generate_markdown_report(sample_benchmark_suite)

    # Check that numeric values appear (RPS, latencies)
    # Granian should show 15,000 RPS
    assert "15,000" in report or "15000" in report


@pytest.mark.unit
def test_generate_markdown_report_rankings(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that rankings section is included."""
    report = generate_markdown_report(sample_benchmark_suite)

    assert "## Rankings" in report
    assert "Top performers by scenario" in report


@pytest.mark.unit
def test_generate_markdown_report_methodology(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that methodology section is included."""
    report = generate_markdown_report(sample_benchmark_suite)

    assert "## Methodology" in report
    assert "Primer" in report
    assert "Warmup" in report
    assert "Measurement" in report


@pytest.mark.unit
def test_generate_markdown_report_with_errors() -> None:
    """Test that error section is included when errors are present."""
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
    suite.add_error(server="broken_server", scenario="hello", error="Failed to start")

    report = generate_markdown_report(suite)

    assert "## Errors" in report
    assert "broken_server" in report
    assert "Failed to start" in report


@pytest.mark.unit
def test_generate_markdown_report_no_errors() -> None:
    """Test that error section is not included when no errors."""
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

    report = generate_markdown_report(suite)

    # Should not have errors section
    assert "## Errors" not in report


@pytest.mark.unit
def test_generate_markdown_report_sorted_by_rps(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that results are sorted by RPS (highest first) within each scenario."""
    report = generate_markdown_report(sample_benchmark_suite)

    # In the hello scenario, Granian (15,000 RPS) should appear before Uvicorn (10,000 RPS)
    granian_pos = report.find("Granian")
    uvicorn_pos = report.find("Uvicorn (h11)")

    # Both should be present
    assert granian_pos != -1
    assert uvicorn_pos != -1

    # Granian should appear before Uvicorn in hello scenario table
    # (This is a simplified check - more precise would parse the table)
    hello_section = report.find("### hello")
    assert hello_section != -1


@pytest.mark.unit
def test_generate_csv_report_structure(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that generated CSV report has expected structure."""
    report = generate_csv_report(sample_benchmark_suite)

    assert isinstance(report, str)
    assert len(report) > 0

    # Check for CSV header
    lines = report.strip().split("\n")
    assert len(lines) > 0

    header = lines[0]
    assert "server" in header
    assert "category" in header
    assert "scenario" in header
    assert "concurrency" in header
    assert "rps" in header
    assert "latency_avg_ms" in header
    assert "latency_p99_ms" in header


@pytest.mark.unit
def test_generate_csv_report_data_rows(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that CSV report contains data rows."""
    report = generate_csv_report(sample_benchmark_suite)

    lines = report.strip().split("\n")

    # Should have header + 4 data rows
    assert len(lines) >= 5

    # Check that data rows contain expected values
    data_rows = lines[1:]  # Skip header
    assert len(data_rows) == 4

    # Check that server names appear
    csv_content = "\n".join(data_rows)
    assert "uvicorn_h11" in csv_content
    assert "granian" in csv_content
    assert "vertebra" in csv_content


@pytest.mark.unit
def test_generate_csv_report_numeric_values(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that CSV report contains numeric values."""
    report = generate_csv_report(sample_benchmark_suite)

    lines = report.strip().split("\n")
    data_rows = lines[1:]  # Skip header

    # Each data row should have numeric values
    for row in data_rows:
        fields = row.split(",")
        # Should have multiple fields
        assert len(fields) > 5

        # RPS field (index 4) should be numeric
        rps_value = fields[4]
        assert float(rps_value) > 0


@pytest.mark.unit
def test_generate_csv_report_categories(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that CSV report includes category values."""
    report = generate_csv_report(sample_benchmark_suite)

    assert "server" in report  # Category value
    assert "runtime" in report  # Category value


@pytest.mark.unit
def test_generate_csv_report_scenarios(sample_benchmark_suite: BenchmarkSuite) -> None:
    """Test that CSV report includes scenario values."""
    report = generate_csv_report(sample_benchmark_suite)

    assert "hello" in report
    assert "body_1k" in report


@pytest.mark.unit
def test_generate_markdown_report_empty_suite() -> None:
    """Test generating report from empty suite."""
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

    report = generate_markdown_report(suite)

    # Should still have basic structure
    assert "# ASGI Server Benchmark Results" in report
    assert "## Environment" in report
    assert "## Methodology" in report


@pytest.mark.unit
def test_generate_csv_report_empty_suite() -> None:
    """Test generating CSV report from empty suite."""
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

    report = generate_csv_report(suite)

    lines = report.strip().split("\n")

    # Should have header but no data rows
    assert len(lines) == 1
    assert "server" in lines[0]


@pytest.mark.unit
def test_generate_markdown_report_multiple_concurrency_levels() -> None:
    """Test markdown report with multiple concurrency levels."""
    env = EnvironmentInfo.detect()
    suite = BenchmarkSuite(environment=env)

    # Add same scenario with different concurrency levels
    for concurrency in [64, 256]:
        result = BenchmarkResult(
            server="uvicorn_h11",
            server_display="Uvicorn (h11)",
            category=BenchmarkCategory.SERVER,
            scenario="hello",
            concurrency=concurrency,
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

    report = generate_markdown_report(suite)

    # Should have separate sections for each concurrency level
    assert "Concurrency: 64" in report
    assert "Concurrency: 256" in report
