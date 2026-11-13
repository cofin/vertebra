"""Offline unit tests for PostgreSQL benchmark report schemas, statistics, and threshold comparator."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from tools.benchmarks.postgres import (
    SCHEMA_VERSION,
    PostgresBenchmarkCase,
    PostgresBenchmarkReport,
    compare_reports,
    compute_case_stats,
    format_report_bytes,
    main,
    parse_report_bytes,
    redact_config_dsn,
)

from vertebra.db.adapters.postgres import PostgreSQLConfig


def _sample_report(
    *,
    ops_sync: float = 1000.0,
    p99_sync: float = 1.0,
    ops_async: float = 2000.0,
    p99_async: float = 0.5,
) -> PostgresBenchmarkReport:
    return PostgresBenchmarkReport(
        schema_version=SCHEMA_VERSION,
        generated_at="2026-11-13T22:00:00Z",
        dsn_redacted="postgresql://postgres@127.0.0.1:5432/postgres",
        cases=(
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-sync",
                iterations=100,
                median_ms=0.8,
                p99_ms=p99_sync,
                ops_per_sec=ops_sync,
            ),
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-async",
                iterations=100,
                median_ms=0.4,
                p99_ms=p99_async,
                ops_per_sec=ops_async,
            ),
        ),
    )


def test_compute_case_stats_calculates_exact_quantiles_and_ops() -> None:
    """Verify compute_case_stats produces accurate median, p99, and ops/sec without mocking."""
    samples = [1_000_000 for _ in range(99)] + [10_000_000]
    case = compute_case_stats(case="point_select_1", driver="vertebra-sync", latencies_ns=samples)
    assert case.iterations == 100
    assert case.median_ms == pytest.approx(1.0)
    assert case.p99_ms == pytest.approx(1.0)
    assert case.ops_per_sec == pytest.approx(100.0 / 0.109)


@pytest.mark.parametrize("bad_samples", [[0.0], [-1.0], [float("nan")], [float("inf")]])
def test_compute_case_stats_rejects_non_positive_and_non_finite_samples(bad_samples: list[float]) -> None:
    """Verify empty, non-positive, or non-finite latency series raise ValueError."""
    with pytest.raises(ValueError, match="empty"):
        compute_case_stats(case="point_select_1", driver="vertebra-sync", latencies_ns=[])
    with pytest.raises(ValueError, match="positive finite"):
        compute_case_stats(case="point_select_1", driver="vertebra-sync", latencies_ns=bad_samples)


def test_report_serialization_roundtrip_and_schema_validation() -> None:
    """Verify schema_version=1 round-trips cleanly and invalid schemas fail closed."""
    report = _sample_report()
    encoded = format_report_bytes(report)
    decoded = parse_report_bytes(encoded)
    assert decoded == report

    bad_version = PostgresBenchmarkReport(
        schema_version=2,
        generated_at="2026-11-13T22:00:00Z",
        dsn_redacted="postgresql://localhost/postgres",
        cases=report.cases,
    )
    with pytest.raises(ValueError, match="schema_version"):
        format_report_bytes(bad_version)

    with pytest.raises(ValueError, match="schema_version"):
        parse_report_bytes(encoded.replace(b'"schema_version":1', b'"schema_version":99'))


@pytest.mark.parametrize(
    ("ops_sync", "p99_sync", "expect_pass", "expected_fragment"),
    [
        (950.0, 1.15, True, None),
        (850.0, 1.0, False, "throughput_ratio"),
        (1000.0, 1.40, False, "p99_ratio"),
    ],
    ids=["within-threshold", "throughput-regression", "latency-regression"],
)
def test_compare_reports_enforces_throughput_and_latency_thresholds(
    ops_sync: float,
    p99_sync: float,
    expect_pass: bool,
    expected_fragment: str | None,
) -> None:
    """Verify compare_reports calculates ratios and flags regressions outside bounds."""
    baseline = _sample_report()
    candidate = _sample_report(ops_sync=ops_sync, p99_sync=p99_sync)
    outcomes = compare_reports(candidate, baseline, min_throughput_ratio=0.90, max_latency_ratio=1.25)
    sync_outcome = next(o for o in outcomes if o.candidate_driver == "vertebra-sync")
    assert sync_outcome.passed is expect_pass
    assert sync_outcome.throughput_ratio == pytest.approx(ops_sync / 1000.0)
    assert sync_outcome.p99_ratio == pytest.approx(p99_sync / 1.0)
    if expected_fragment is not None:
        assert sync_outcome.reason is not None and expected_fragment in sync_outcome.reason
    else:
        assert sync_outcome.reason is None


def test_compare_reports_supports_cross_driver_comparison_within_single_report() -> None:
    """Verify explicit candidate_driver vs baseline_driver comparison evaluates driver ratios."""
    report = _sample_report(ops_sync=900.0, p99_sync=1.0, ops_async=1800.0, p99_async=0.5)
    outcomes = compare_reports(
        report,
        report,
        candidate_driver="vertebra-async",
        baseline_driver="vertebra-sync",
        min_throughput_ratio=1.5,
        max_latency_ratio=1.0,
    )
    assert len(outcomes) == 1
    assert outcomes[0].throughput_ratio == pytest.approx(2.0)
    assert outcomes[0].p99_ratio == pytest.approx(0.5)
    assert outcomes[0].passed is True


def test_postgres_benchmark_cli_compare_exit_codes(tmp_path: Path) -> None:
    """Verify CLI `compare` returns exit code 0 when passing and 1 when a regression is detected."""
    baseline_path = tmp_path / "baseline.json"
    ok_path = tmp_path / "ok.json"
    regressed_path = tmp_path / "regressed.json"

    baseline_path.write_bytes(format_report_bytes(_sample_report()))
    ok_path.write_bytes(format_report_bytes(_sample_report(ops_sync=980.0)))
    regressed_path.write_bytes(format_report_bytes(_sample_report(ops_sync=500.0, p99_sync=2.5)))

    assert (
        main(
            [
                "compare",
                "--candidate",
                str(ok_path),
                "--baseline",
                str(baseline_path),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "compare",
                "--candidate",
                str(regressed_path),
                "--baseline",
                str(baseline_path),
            ]
        )
        == 1
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.benchmarks.postgres",
            "compare",
            "--candidate",
            str(regressed_path),
            "--baseline",
            str(baseline_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "[FAIL]" in proc.stdout


def test_redact_config_dsn_strips_password_and_query_tokens() -> None:
    """Verify redact_config_dsn never retains credentials or secret query parameters."""
    cfg = PostgreSQLConfig.from_url("postgresql://alice:s3cr3t_pw@db.internal:5433/prod_db?password=leak")
    redacted = redact_config_dsn(cfg)
    assert redacted == "postgresql://alice@db.internal:5433/prod_db"
    assert "s3cr3t_pw" not in redacted
    assert "leak" not in redacted


@pytest.mark.parametrize(
    ("kwargs", "match_pattern"),
    [
        ({"min_throughput_ratio": 0.0}, "min_throughput_ratio"),
        ({"min_throughput_ratio": float("nan")}, "min_throughput_ratio"),
        ({"max_latency_ratio": -1.0}, "max_latency_ratio"),
        ({"max_latency_ratio": float("inf")}, "max_latency_ratio"),
        ({"candidate_driver": "vertebra-sync"}, "Both candidate_driver and baseline_driver"),
        ({"candidate_driver": "vertebra-sync", "baseline_driver": "missing-driver"}, "Missing comparison pair"),
    ],
)
def test_compare_reports_rejects_invalid_thresholds_and_missing_pairs(
    kwargs: dict[str, object],
    match_pattern: str,
) -> None:
    """Verify compare_reports fails deterministically on invalid ratios and unmatched driver filters."""
    report = _sample_report()
    with pytest.raises(ValueError, match=match_pattern):
        compare_reports(report, report, **kwargs)  # type: ignore[arg-type]


def test_compare_reports_rejects_empty_overlap_when_required() -> None:
    """Verify compare_reports rejects disjoint driver/case sets when require_nonempty=True."""
    cand = _sample_report()
    disjoint_base = PostgresBenchmarkReport(
        schema_version=SCHEMA_VERSION,
        generated_at="2026-11-13T22:00:00Z",
        dsn_redacted="postgresql://postgres@127.0.0.1:5432/postgres",
        cases=(
            PostgresBenchmarkCase(
                case="other_case",
                driver="other-driver",
                iterations=10,
                median_ms=1.0,
                p99_ms=1.5,
                ops_per_sec=500.0,
            ),
        ),
    )
    with pytest.raises(ValueError, match="No matching benchmark cases"):
        compare_reports(cand, disjoint_base, require_nonempty=True)
    assert compare_reports(cand, disjoint_base, require_nonempty=False) == ()
