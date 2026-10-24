"""Unit tests for benchmark tooling, free-threaded guards, and regression detection."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from tools.benchmarks.baseline import BaselineRecord, compare_baselines
from tools.benchmarks.micro import run_all_micro_benchmarks
from tools.benchmarks.targets import _ft_run


def test_ft_run_missing_binary_raises_file_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _ft_run raises FileNotFoundError when the free-threaded binary does not exist."""
    monkeypatch.setattr(
        "tools.benchmarks.targets._ft_server_binary",
        lambda: "/tmp/nonexistent-path-to-vertebra-server-ft",
    )
    with pytest.raises(FileNotFoundError, match="Free-threaded server binary not found"):
        _ft_run("test-arg")


def test_micro_benchmarks_emit_expected_keys() -> None:
    """Verify run_all_micro_benchmarks executes and populates all required metrics."""
    with patch("tools.benchmarks.micro.measure_import_time_ms", return_value=12.5):
        suite = run_all_micro_benchmarks(iterations=5)

    expected_samples = {
        "request_headers_materialization",
        "asgi_scope_build_and_copy",
        "wsgi_environ_items",
        "rsgi_scope_build",
        "response_construct",
        "serializers_msgpack_json",
    }
    assert set(suite.samples.keys()) == expected_samples

    for name in expected_samples:
        sample = suite.samples[name]
        assert sample.iterations == 5
        assert sample.median_ns > 0
        assert sample.p99_ns >= sample.median_ns
        assert sample.ops_per_sec > 0

    assert "import_vertebra_ms" in suite.import_ms
    assert "import_vertebra_framework_ms" in suite.import_ms


def test_baseline_check_fails_on_3pct_throughput_drop_or_5pct_p99_increase() -> None:
    """Verify compare_baselines detects throughput drops > 3% and latency increases > 5%."""
    baseline = BaselineRecord(
        name="pre-refactor",
        commit="edceea6",
        timestamp="2026-10-22T00:00:00Z",
        criterion_ns_per_iter={"spsc::throughput": 100.0},
        micro_ops_per_sec={"request_headers_materialization": 1000.0},
        micro_p99_ns={"request_headers_materialization": 200.0},
        import_ms={"import_vertebra_ms": 25.0},
        e2e_rps={"vtb-embedded-native::plaintext": 50000.0},
    )

    clean_current = BaselineRecord(
        name="current",
        commit="edceea6",
        timestamp="2026-10-22T01:00:00Z",
        criterion_ns_per_iter={"spsc::throughput": 102.0},
        micro_ops_per_sec={"request_headers_materialization": 990.0},
        micro_p99_ns={"request_headers_materialization": 205.0},
        import_ms={"import_vertebra_ms": 26.0},
        e2e_rps={"vtb-embedded-native::plaintext": 49000.0},
    )
    assert compare_baselines(baseline, clean_current) == []

    regressed_throughput = BaselineRecord(
        name="current",
        commit="edceea6",
        timestamp="2026-10-22T01:00:00Z",
        criterion_ns_per_iter={"spsc::throughput": 100.0},
        micro_ops_per_sec={"request_headers_materialization": 900.0},
        micro_p99_ns={"request_headers_materialization": 200.0},
        import_ms={"import_vertebra_ms": 25.0},
        e2e_rps={"vtb-embedded-native::plaintext": 50000.0},
    )
    regs_tp = compare_baselines(baseline, regressed_throughput)
    assert len(regs_tp) == 1
    assert (
        "REGRESSION [micro_ops_per_sec] request_headers_materialization: -10.00% exceeds -3.00% threshold" in regs_tp[0]
    )

    regressed_latency = BaselineRecord(
        name="current",
        commit="edceea6",
        timestamp="2026-10-22T01:00:00Z",
        criterion_ns_per_iter={"spsc::throughput": 110.0},
        micro_ops_per_sec={"request_headers_materialization": 1000.0},
        micro_p99_ns={"request_headers_materialization": 220.0},
        import_ms={"import_vertebra_ms": 27.5},
        e2e_rps={"vtb-embedded-native::plaintext": 50000.0},
    )
    regs_lat = compare_baselines(baseline, regressed_latency)
    assert len(regs_lat) == 3
    assert any("criterion_ns_per_iter" in r and "+10.00% exceeds +5.00% threshold" in r for r in regs_lat)
    assert any("micro_p99_ns" in r and "+10.00% exceeds +5.00% threshold" in r for r in regs_lat)
    assert any("import_ms" in r and "+10.00% exceeds +5.00% threshold" in r for r in regs_lat)
