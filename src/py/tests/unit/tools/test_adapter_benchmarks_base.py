"""Offline unit tests for shared adapter benchmark schemas, samplers, redaction, and comparator."""

from __future__ import annotations

import asyncio
import sys
import threading

import pytest
from tools.benchmarks.adapters import (
    ADAPTER_SCHEMA_VERSION,
    DEFAULT_MAX_LATENCY_RATIO,
    DEFAULT_MIN_THROUGHPUT_RATIO,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    DriverMode,
    assert_free_threaded_gil_disabled,
    compare_adapter_reports,
    compute_adapter_case_stats,
    detect_gil_enabled,
    format_adapter_report_bytes,
    from_postgres_report,
    parse_adapter_report_bytes,
    redact_target_uri,
    sample_async,
    sample_sync,
    sample_threaded,
)
from tools.benchmarks.postgres import SCHEMA_VERSION, PostgresBenchmarkCase, PostgresBenchmarkReport


def _sample_adapter_report(
    *,
    ops_sync: float = 1000.0,
    p99_sync: float = 1.0,
    ops_async: float = 2000.0,
    p99_async: float = 0.5,
    gil_enabled: bool | None = True,
) -> AdapterBenchmarkReport:
    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="postgres",
        generated_at="2026-11-19T22:00:00Z",
        target_redacted="postgresql://postgres@127.0.0.1:5432/postgres",
        gil_enabled=gil_enabled,
        cases=(
            AdapterBenchmarkCase(
                case="point_select_1",
                driver="vertebra-sync",
                category="sql",
                driver_mode="sync_blocking",
                iterations=100,
                median_ms=0.8,
                p99_ms=p99_sync,
                ops_per_sec=ops_sync,
                gil_enabled=gil_enabled,
                threads=1,
            ),
            AdapterBenchmarkCase(
                case="point_select_1",
                driver="vertebra-async",
                category="sql",
                driver_mode="async_native",
                iterations=100,
                median_ms=0.4,
                p99_ms=p99_async,
                ops_per_sec=ops_async,
                gil_enabled=gil_enabled,
                threads=1,
            ),
        ),
    )


@pytest.mark.parametrize("bad_samples", [[0.0], [-1.0], [float("nan")], [float("inf")]])
def test_compute_adapter_case_stats_quantiles_and_validation(bad_samples: list[float]) -> None:
    """Verify exact quantiles, ops/sec, and rejection of invalid latency series or thread counts."""
    samples = [1_000_000 for _ in range(99)] + [10_000_000]
    case = compute_adapter_case_stats(
        case="point_select_1",
        driver="vertebra-sync",
        category="sql",
        driver_mode="sync_blocking",
        latencies_ns=samples,
        threads=4,
        gil_enabled=False,
    )
    assert case.iterations == 100
    assert case.median_ms == pytest.approx(1.0)
    assert case.p99_ms == pytest.approx(1.0)
    assert case.ops_per_sec == pytest.approx(100.0 / 0.109)
    assert case.threads == 4
    assert case.gil_enabled is False
    assert case.category == "sql"
    assert case.driver_mode == "sync_blocking"

    with pytest.raises(ValueError, match="empty"):
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=[],
        )
    with pytest.raises(ValueError, match="positive finite"):
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=bad_samples,
        )
    with pytest.raises(ValueError, match="threads"):
        compute_adapter_case_stats(
            case="point_select_1",
            driver="vertebra-sync",
            category="sql",
            driver_mode="sync_blocking",
            latencies_ns=samples,
            threads=0,
        )


@pytest.mark.parametrize(
    ("bad_field", "bad_value"),
    [
        ("median_ms", 0.0),
        ("p99_ms", -0.5),
        ("ops_per_sec", 0.0),
        ("iterations", 0),
        ("threads", 0),
    ],
)
def test_report_roundtrip_and_schema_version_rejection(bad_field: str, bad_value: int | float) -> None:
    """Verify schema_version=1 JSON round-trip and rejection of unsupported schema versions and bad metrics."""
    report = _sample_adapter_report()
    encoded = format_adapter_report_bytes(report)
    decoded = parse_adapter_report_bytes(encoded)
    assert decoded == report
    assert parse_adapter_report_bytes(encoded.decode("utf-8")) == report

    bad_version = AdapterBenchmarkReport(
        schema_version=2,
        category=report.category,
        engine=report.engine,
        generated_at=report.generated_at,
        target_redacted=report.target_redacted,
        gil_enabled=report.gil_enabled,
        cases=report.cases,
    )
    with pytest.raises(ValueError, match="schema_version"):
        format_adapter_report_bytes(bad_version)
    with pytest.raises(ValueError, match="schema_version"):
        parse_adapter_report_bytes(encoded.replace(b'"schema_version":1', b'"schema_version":2'))

    valid_case = report.cases[0]
    invalid_case = AdapterBenchmarkCase(
        case=valid_case.case,
        driver=valid_case.driver,
        category=valid_case.category,
        driver_mode=valid_case.driver_mode,
        iterations=int(bad_value) if bad_field == "iterations" else valid_case.iterations,
        median_ms=float(bad_value) if bad_field == "median_ms" else valid_case.median_ms,
        p99_ms=float(bad_value) if bad_field == "p99_ms" else valid_case.p99_ms,
        ops_per_sec=float(bad_value) if bad_field == "ops_per_sec" else valid_case.ops_per_sec,
        gil_enabled=valid_case.gil_enabled,
        threads=int(bad_value) if bad_field == "threads" else valid_case.threads,
    )
    bad_metric_report = AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category=report.category,
        engine=report.engine,
        generated_at=report.generated_at,
        target_redacted=report.target_redacted,
        gil_enabled=report.gil_enabled,
        cases=(invalid_case,),
    )
    with pytest.raises(ValueError, match=bad_field):
        format_adapter_report_bytes(bad_metric_report)


@pytest.mark.parametrize(
    ("ops_sync", "p99_sync", "expect_pass", "expected_fragment"),
    [
        (950.0, 1.15, True, None),
        (850.0, 1.0, False, "throughput_ratio"),
        (1000.0, 1.40, False, "p99_ratio"),
    ],
    ids=["within-threshold", "throughput-regression", "latency-regression"],
)
def test_compare_adapter_reports_stratifies_driver_modes(
    ops_sync: float,
    p99_sync: float,
    expect_pass: bool,
    expected_fragment: str | None,
) -> None:
    """Verify threshold comparison, driver_mode filtering, and driver_mode stratification enforcement."""
    baseline = _sample_adapter_report()
    candidate = _sample_adapter_report(ops_sync=ops_sync, p99_sync=p99_sync)

    outcomes = compare_adapter_reports(
        candidate,
        baseline,
        min_throughput_ratio=DEFAULT_MIN_THROUGHPUT_RATIO,
        max_latency_ratio=DEFAULT_MAX_LATENCY_RATIO,
    )
    assert len(outcomes) == 2
    sync_outcome = next(o for o in outcomes if o.candidate_driver == "vertebra-sync")
    assert sync_outcome.driver_mode == "sync_blocking"
    assert sync_outcome.passed is expect_pass
    assert sync_outcome.throughput_ratio == pytest.approx(ops_sync / 1000.0)
    assert sync_outcome.p99_ratio == pytest.approx(p99_sync / 1.0)
    if expected_fragment is not None:
        assert sync_outcome.reason is not None and expected_fragment in sync_outcome.reason
    else:
        assert sync_outcome.reason is None

    async_only = compare_adapter_reports(candidate, baseline, driver_mode="async_native")
    assert len(async_only) == 1
    assert async_only[0].driver_mode == "async_native"
    assert async_only[0].candidate_driver == "vertebra-async"

    with pytest.raises(ValueError, match="stratify_by_driver_mode=False"):
        compare_adapter_reports(
            candidate,
            baseline,
            candidate_driver="vertebra-async",
            baseline_driver="vertebra-sync",
            stratify_by_driver_mode=True,
        )

    mixed = compare_adapter_reports(
        candidate,
        baseline,
        candidate_driver="vertebra-async",
        baseline_driver="vertebra-sync",
        stratify_by_driver_mode=False,
    )
    assert len(mixed) == 1
    assert mixed[0].driver_mode == "async_native"
    assert mixed[0].throughput_ratio == pytest.approx(2.0)

    mismatched_implicit_candidate = AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="sql",
        engine="postgres",
        generated_at="2026-11-19T22:00:00Z",
        target_redacted="postgresql://postgres@127.0.0.1:5432/postgres",
        gil_enabled=True,
        cases=(
            AdapterBenchmarkCase(
                case="point_select_1",
                driver="vertebra-sync",
                category="sql",
                driver_mode="async_native",
                iterations=100,
                median_ms=0.8,
                p99_ms=1.0,
                ops_per_sec=1000.0,
            ),
        ),
    )
    with pytest.raises(ValueError, match="stratify_by_driver_mode=False"):
        compare_adapter_reports(mismatched_implicit_candidate, baseline, stratify_by_driver_mode=True)


def test_gil_detection_and_strict_free_threaded_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify detect_gil_enabled and assert_free_threaded_gil_disabled across GIL states."""
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert detect_gil_enabled() is False
    assert assert_free_threaded_gil_disabled(strict=True) is False
    assert assert_free_threaded_gil_disabled(strict=False) is False

    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    assert detect_gil_enabled() is True
    assert assert_free_threaded_gil_disabled(strict=False) is True
    with pytest.raises(RuntimeError, match="GIL"):
        assert_free_threaded_gil_disabled(strict=True)

    monkeypatch.delattr(sys, "_is_gil_enabled", raising=False)
    assert detect_gil_enabled() is True


def test_sample_sync_async_and_threaded_barriers() -> None:
    """Verify sync, async, and barrier-synchronized threaded samplers count warmups and propagate errors."""
    sync_calls = 0

    def _sync_op() -> None:
        nonlocal sync_calls
        sync_calls += 1

    sync_samples = sample_sync(_sync_op, iterations=5, warmup=2)
    assert len(sync_samples) == 5
    assert sync_calls == 7
    assert all(sample >= 1 for sample in sync_samples)

    async_calls = 0

    async def _async_op() -> None:
        nonlocal async_calls
        async_calls += 1

    async_samples = asyncio.run(sample_async(_async_op, iterations=4, warmup=3))
    assert len(async_samples) == 4
    assert async_calls == 7
    assert all(sample >= 1 for sample in async_samples)

    lock = threading.Lock()
    threaded_calls = 0

    def _threaded_op() -> None:
        nonlocal threaded_calls
        with lock:
            threaded_calls += 1

    threaded_samples = sample_threaded(_threaded_op, iterations=5, warmup=2, threads=4)
    assert len(threaded_samples) == 20
    assert threaded_calls == 28
    assert all(sample >= 1 for sample in threaded_samples)

    def _failing_op() -> None:
        msg = "simulated worker failure"
        raise KeyError(msg)

    with pytest.raises(KeyError, match="simulated worker failure"):
        sample_threaded(_failing_op, iterations=2, warmup=1, threads=4)

    with pytest.raises(ValueError, match="iterations"):
        sample_sync(_sync_op, iterations=0, warmup=0)
    with pytest.raises(ValueError, match="warmup"):
        asyncio.run(sample_async(_async_op, iterations=1, warmup=-1))
    with pytest.raises(ValueError, match="threads"):
        sample_threaded(_threaded_op, iterations=1, warmup=0, threads=0)


@pytest.mark.parametrize(
    ("raw_uri", "expected_uri"),
    [
        (":memory:", ":memory:"),
        (
            "postgresql://benchuser:test-token-placeholder@127.0.0.1:5432/benchdb?sslmode=require&password=test-token-placeholder#section",
            "postgresql://benchuser@127.0.0.1:5432/benchdb",
        ),
        (
            "redis://:test-token-placeholder@127.0.0.1:6379/0?token=test-token-placeholder",
            "redis://127.0.0.1:6379/0",
        ),
        (
            "https://account.blob.core.windows.net/container/blob?sv=2022-11-02&sig=test-token-placeholder#part",
            "https://account.blob.core.windows.net/container/blob",
        ),
        (
            "s3://accesskey:test-token-placeholder@bucket/prefix/key?X-Amz-Signature=test-token-placeholder",
            "s3://accesskey@bucket/prefix/key",
        ),
        (
            "sqlite:///tmp/bench.db?mode=rwc",
            "sqlite:///tmp/bench.db",
        ),
    ],
)
def test_redact_target_uri_and_postgres_bridge(raw_uri: str, expected_uri: str) -> None:
    """Verify URI credential/query redaction and PostgresBenchmarkReport bridge driver_mode mapping."""
    assert redact_target_uri(raw_uri) == expected_uri

    pg_report = PostgresBenchmarkReport(
        schema_version=SCHEMA_VERSION,
        generated_at="2026-11-19T22:00:00Z",
        dsn_redacted="postgresql://postgres:test-token-placeholder@127.0.0.1:5432/postgres?sslmode=disable",
        cases=(
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-async",
                iterations=100,
                median_ms=0.4,
                p99_ms=0.6,
                ops_per_sec=2500.0,
            ),
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="asyncpg",
                iterations=100,
                median_ms=0.5,
                p99_ms=0.7,
                ops_per_sec=2000.0,
            ),
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="vertebra-sync",
                iterations=100,
                median_ms=0.8,
                p99_ms=1.1,
                ops_per_sec=1200.0,
            ),
            PostgresBenchmarkCase(
                case="point_select_1",
                driver="psycopg-sync",
                iterations=100,
                median_ms=0.9,
                p99_ms=1.2,
                ops_per_sec=1000.0,
            ),
        ),
    )
    bridged = from_postgres_report(pg_report, gil_enabled=False)
    assert bridged.schema_version == ADAPTER_SCHEMA_VERSION
    assert bridged.category == "sql"
    assert bridged.engine == "postgres"
    assert bridged.target_redacted == "postgresql://postgres@127.0.0.1:5432/postgres"
    assert bridged.gil_enabled is False

    modes_by_driver: dict[str, DriverMode] = {entry.driver: entry.driver_mode for entry in bridged.cases}
    assert modes_by_driver == {
        "vertebra-async": "async_native",
        "asyncpg": "async_native",
        "vertebra-sync": "sync_blocking",
        "psycopg-sync": "sync_blocking",
    }
    assert all(entry.category == "sql" and entry.threads == 1 and entry.gil_enabled is False for entry in bridged.cases)
