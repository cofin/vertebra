"""Shared adapter benchmark schemas, timing samplers, URI redaction, and comparator."""

from __future__ import annotations

import math
import statistics
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal
from urllib.parse import urlsplit, urlunsplit

import msgspec

from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    from tools.benchmarks.postgres import PostgresBenchmarkReport

ADAPTER_SCHEMA_VERSION: Final[int] = 1
DEFAULT_MIN_THROUGHPUT_RATIO: Final[float] = 0.90
DEFAULT_MAX_LATENCY_RATIO: Final[float] = 1.25

type AdapterCategory = Literal["sql", "cache", "storage", "orm"]
type DriverMode = Literal["async_native", "sync_blocking"]

__all__ = (
    "ADAPTER_SCHEMA_VERSION",
    "DEFAULT_MAX_LATENCY_RATIO",
    "DEFAULT_MIN_THROUGHPUT_RATIO",
    "AdapterBenchmarkCase",
    "AdapterBenchmarkReport",
    "AdapterCategory",
    "AdapterComparisonOutcome",
    "DriverMode",
    "assert_free_threaded_gil_disabled",
    "compare_adapter_reports",
    "compute_adapter_case_stats",
    "detect_gil_enabled",
    "format_adapter_report_bytes",
    "from_postgres_report",
    "parse_adapter_report_bytes",
    "redact_target_uri",
    "sample_async",
    "sample_sync",
    "sample_threaded",
)


class AdapterBenchmarkCase(msgspec.Struct, frozen=True, gc=False):
    """Measured throughput and latency summary for one adapter driver/workload pair."""

    case: str
    driver: str
    category: AdapterCategory
    driver_mode: DriverMode
    iterations: int
    median_ms: float
    p99_ms: float
    ops_per_sec: float
    gil_enabled: bool | None = None
    threads: int = 1


class AdapterBenchmarkReport(msgspec.Struct, frozen=True, gc=False):
    """Versioned (`schema_version=1`) adapter benchmark report payload."""

    schema_version: int
    category: AdapterCategory
    engine: str
    generated_at: str
    target_redacted: str
    gil_enabled: bool | None
    cases: tuple[AdapterBenchmarkCase, ...]


class AdapterComparisonOutcome(msgspec.Struct, frozen=True, gc=False):
    """Comparison outcome evaluating a candidate adapter driver against a baseline."""

    case: str
    candidate_driver: str
    baseline_driver: str
    driver_mode: DriverMode
    candidate_ops_per_sec: float
    baseline_ops_per_sec: float
    throughput_ratio: float
    candidate_p99_ms: float
    baseline_p99_ms: float
    p99_ratio: float
    passed: bool
    reason: str | None = None


def detect_gil_enabled() -> bool:
    """Return whether the Python GIL is currently enabled in this interpreter."""
    fn = getattr(sys, "_is_gil_enabled", None)
    if callable(fn):
        return bool(fn())
    return True


def assert_free_threaded_gil_disabled(*, strict: bool = True) -> bool:
    """Verify GIL status for free-threaded runs and optionally raise when the GIL is active."""
    gil_enabled = detect_gil_enabled()
    if strict and gil_enabled:
        msg = (
            "Free-threaded benchmark requires GIL disabled, but the GIL is active "
            "or was re-enabled by a comparator extension"
        )
        raise RuntimeError(msg)
    return gil_enabled


def compute_adapter_case_stats(
    *,
    case: str,
    driver: str,
    category: AdapterCategory,
    driver_mode: DriverMode,
    latencies_ns: Sequence[int | float],
    threads: int = 1,
    gil_enabled: bool | None = None,
) -> AdapterBenchmarkCase:
    """Compute finite median/p99 latencies (ms) and ops/sec from nanosecond timings."""
    if threads < 1:
        msg = f"threads must be >= 1 for {case}/{driver} (got {threads})"
        raise ValueError(msg)
    if not latencies_ns:
        msg = f"Cannot compute benchmark statistics from empty sample sequence for {case}/{driver}"
        raise ValueError(msg)
    cleaned = [float(sample) for sample in latencies_ns]
    if any(not math.isfinite(sample) or sample <= 0.0 for sample in cleaned):
        msg = f"All latency samples must be positive finite numbers for {case}/{driver}"
        raise ValueError(msg)

    ordered = sorted(cleaned)
    median_ns = statistics.median(ordered)
    p99_idx = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * 0.99) - 1))
    p99_ns = ordered[p99_idx]
    total_s = sum(ordered) / 1_000_000_000.0
    ops_per_sec = len(ordered) / total_s
    return AdapterBenchmarkCase(
        case=case,
        driver=driver,
        category=category,
        driver_mode=driver_mode,
        iterations=len(ordered),
        median_ms=median_ns / 1_000_000.0,
        p99_ms=p99_ns / 1_000_000.0,
        ops_per_sec=ops_per_sec,
        gil_enabled=gil_enabled,
        threads=threads,
    )


def sample_sync(fn: Callable[[], Any], *, iterations: int, warmup: int) -> list[int]:
    """Run synchronous warmup and timed iterations, returning nanosecond samples clamped to >= 1."""
    if iterations < 1 or warmup < 0:
        msg = f"iterations must be >= 1 and warmup must be >= 0 (got iterations={iterations}, warmup={warmup})"
        raise ValueError(msg)
    for _ in range(warmup):
        fn()
    samples: list[int] = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        fn()
        samples.append(max(1, time.perf_counter_ns() - start))
    return samples


async def sample_async(fn: Callable[[], Awaitable[Any]], *, iterations: int, warmup: int) -> list[int]:
    """Run asynchronous warmup and timed iterations, returning nanosecond samples clamped to >= 1."""
    if iterations < 1 or warmup < 0:
        msg = f"iterations must be >= 1 and warmup must be >= 0 (got iterations={iterations}, warmup={warmup})"
        raise ValueError(msg)
    for _ in range(warmup):
        await fn()
    samples: list[int] = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await fn()
        samples.append(max(1, time.perf_counter_ns() - start))
    return samples


def sample_threaded(fn: Callable[[], Any], *, iterations: int, warmup: int, threads: int) -> list[int]:
    """Run barrier-synchronized multi-threaded iterations and aggregate nanosecond timings."""
    if threads < 1:
        msg = f"threads must be >= 1 (got {threads})"
        raise ValueError(msg)
    if iterations < 1 or warmup < 0:
        msg = f"iterations must be >= 1 and warmup must be >= 0 (got iterations={iterations}, warmup={warmup})"
        raise ValueError(msg)

    barrier = threading.Barrier(threads)
    per_thread_samples: list[list[int]] = [[] for _ in range(threads)]
    errors: list[BaseException | None] = [None] * threads

    def _worker(worker_index: int) -> None:
        try:
            for _ in range(warmup):
                fn()
            barrier.wait()
            local_samples: list[int] = []
            for _ in range(iterations):
                start = time.perf_counter_ns()
                fn()
                local_samples.append(max(1, time.perf_counter_ns() - start))
            per_thread_samples[worker_index] = local_samples
        except BaseException as exc:
            barrier.abort()
            errors[worker_index] = exc

    workers = [threading.Thread(target=_worker, args=(idx,), daemon=True) for idx in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    primary_error = next(
        (err for err in errors if err is not None and not isinstance(err, threading.BrokenBarrierError)),
        None,
    )
    if primary_error is not None:
        raise primary_error
    fallback_error = next((err for err in errors if err is not None), None)
    if fallback_error is not None:
        raise fallback_error

    aggregated: list[int] = []
    for chunk in per_thread_samples:
        aggregated.extend(chunk)
    return aggregated


def redact_target_uri(uri: str) -> str:
    """Strip passwords, query parameters, and fragments from a target URI while preserving `:memory:`."""
    if uri == ":memory:":
        return uri
    parsed = urlsplit(uri)
    redacted_netloc = ""
    if parsed.hostname is not None:
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        port_part = f":{parsed.port}" if parsed.port is not None else ""
        user_part = f"{parsed.username}@" if parsed.username else ""
        redacted_netloc = f"{user_part}{host}{port_part}"
    if parsed.scheme and "://" in uri:
        return f"{parsed.scheme}://{redacted_netloc}{parsed.path}"
    return urlunsplit((parsed.scheme, redacted_netloc, parsed.path, "", ""))


def format_adapter_report_bytes(report: AdapterBenchmarkReport) -> bytes:
    """Serialize `AdapterBenchmarkReport` as UTF-8 JSON bytes after strict schema validation."""
    _validate_adapter_report(report)
    return encode_json_bytes(report)


def parse_adapter_report_bytes(payload: bytes | str) -> AdapterBenchmarkReport:
    """Parse and strictly validate a `schema_version=1` adapter benchmark JSON report."""
    report: AdapterBenchmarkReport = decode_json(payload, type=AdapterBenchmarkReport)
    _validate_adapter_report(report)
    return report


def compare_adapter_reports(
    candidate: AdapterBenchmarkReport,
    baseline: AdapterBenchmarkReport,
    *,
    candidate_driver: str | None = None,
    baseline_driver: str | None = None,
    driver_mode: DriverMode | None = None,
    stratify_by_driver_mode: bool = True,
    min_throughput_ratio: float = DEFAULT_MIN_THROUGHPUT_RATIO,
    max_latency_ratio: float = DEFAULT_MAX_LATENCY_RATIO,
    require_nonempty: bool = True,
) -> tuple[AdapterComparisonOutcome, ...]:
    """Compare candidate and baseline adapter benchmark reports with optional driver_mode stratification."""
    _validate_adapter_report(candidate)
    _validate_adapter_report(baseline)
    if not math.isfinite(min_throughput_ratio) or min_throughput_ratio <= 0.0:
        msg = f"min_throughput_ratio must be > 0 (got {min_throughput_ratio})"
        raise ValueError(msg)
    if not math.isfinite(max_latency_ratio) or max_latency_ratio <= 0.0:
        msg = f"max_latency_ratio must be > 0 (got {max_latency_ratio})"
        raise ValueError(msg)

    cand_cases = tuple(c for c in candidate.cases if driver_mode is None or c.driver_mode == driver_mode)
    base_cases = tuple(c for c in baseline.cases if driver_mode is None or c.driver_mode == driver_mode)

    cand_map: dict[tuple[str, str], AdapterBenchmarkCase] = {(entry.case, entry.driver): entry for entry in cand_cases}
    base_map: dict[tuple[str, str], AdapterBenchmarkCase] = {(entry.case, entry.driver): entry for entry in base_cases}

    outcomes: list[AdapterComparisonOutcome] = []
    if candidate_driver is not None or baseline_driver is not None:
        if candidate_driver is None or baseline_driver is None:
            msg = "Both candidate_driver and baseline_driver must be set when comparing drivers explicitly"
            raise ValueError(msg)
        case_names = sorted({entry.case for entry in cand_cases if entry.driver == candidate_driver})
        for case_name in case_names:
            cand_entry = cand_map.get((case_name, candidate_driver))
            base_entry = base_map.get((case_name, baseline_driver))
            if cand_entry is None or base_entry is None:
                msg = f"Missing comparison pair for case {case_name!r} ({candidate_driver} vs {baseline_driver})"
                raise ValueError(msg)
            _check_driver_mode_stratification(cand_entry, base_entry, stratify_by_driver_mode=stratify_by_driver_mode)
            outcomes.append(
                _evaluate_adapter_pair(
                    cand_entry,
                    base_entry,
                    min_throughput_ratio=min_throughput_ratio,
                    max_latency_ratio=max_latency_ratio,
                )
            )
    else:
        for (case_name, drv_name), cand_entry in sorted(cand_map.items()):
            base_entry = base_map.get((case_name, drv_name))
            if base_entry is None:
                continue
            _check_driver_mode_stratification(cand_entry, base_entry, stratify_by_driver_mode=stratify_by_driver_mode)
            outcomes.append(
                _evaluate_adapter_pair(
                    cand_entry,
                    base_entry,
                    min_throughput_ratio=min_throughput_ratio,
                    max_latency_ratio=max_latency_ratio,
                )
            )

    if require_nonempty and not outcomes:
        msg = "No matching benchmark cases found between candidate and baseline reports"
        raise ValueError(msg)
    return tuple(outcomes)


def from_postgres_report(
    report: PostgresBenchmarkReport,
    *,
    gil_enabled: bool | None = None,
) -> AdapterBenchmarkReport:
    """Convert a legacy `PostgresBenchmarkReport` into an `AdapterBenchmarkReport`."""
    cases = tuple(
        AdapterBenchmarkCase(
            case=entry.case,
            driver=entry.driver,
            category="sql",
            driver_mode=_infer_postgres_driver_mode(entry.driver),
            iterations=entry.iterations,
            median_ms=entry.median_ms,
            p99_ms=entry.p99_ms,
            ops_per_sec=entry.ops_per_sec,
            gil_enabled=gil_enabled,
            threads=1,
        )
        for entry in report.cases
    )
    converted = AdapterBenchmarkReport(
        schema_version=report.schema_version,
        category="sql",
        engine="postgres",
        generated_at=report.generated_at,
        target_redacted=redact_target_uri(report.dsn_redacted),
        gil_enabled=gil_enabled,
        cases=cases,
    )
    _validate_adapter_report(converted)
    return converted


def _infer_postgres_driver_mode(driver: str) -> DriverMode:
    normalized = driver.lower()
    if "async" in normalized:
        return "async_native"
    if "sync" in normalized:
        return "sync_blocking"
    msg = f"Cannot infer driver_mode for PostgreSQL driver {driver!r}"
    raise ValueError(msg)


def _check_driver_mode_stratification(
    candidate: AdapterBenchmarkCase,
    baseline: AdapterBenchmarkCase,
    *,
    stratify_by_driver_mode: bool,
) -> None:
    if stratify_by_driver_mode and candidate.driver_mode != baseline.driver_mode:
        msg = (
            f"Cannot compare {candidate.driver!r} ({candidate.driver_mode}) against "
            f"{baseline.driver!r} ({baseline.driver_mode}) for case {candidate.case!r}: "
            "async_native and sync_blocking drivers cannot be mixed without stratify_by_driver_mode=False"
        )
        raise ValueError(msg)


def _validate_adapter_report(report: AdapterBenchmarkReport) -> None:
    if report.schema_version != ADAPTER_SCHEMA_VERSION:
        msg = (
            f"Unsupported adapter benchmark report schema_version={report.schema_version} "
            f"(expected {ADAPTER_SCHEMA_VERSION})"
        )
        raise ValueError(msg)
    for item in report.cases:
        if item.iterations < 1:
            msg = f"Benchmark case {item.case}/{item.driver} has invalid iterations={item.iterations}"
            raise ValueError(msg)
        if item.threads < 1:
            msg = f"Benchmark case {item.case}/{item.driver} has invalid threads={item.threads}"
            raise ValueError(msg)
        for metric_name, metric in (
            ("median_ms", item.median_ms),
            ("p99_ms", item.p99_ms),
            ("ops_per_sec", item.ops_per_sec),
        ):
            if not math.isfinite(metric) or metric <= 0.0:
                msg = f"Benchmark case {item.case}/{item.driver} has invalid {metric_name}={metric}"
                raise ValueError(msg)


def _evaluate_adapter_pair(
    candidate: AdapterBenchmarkCase,
    baseline: AdapterBenchmarkCase,
    *,
    min_throughput_ratio: float,
    max_latency_ratio: float,
) -> AdapterComparisonOutcome:
    throughput_ratio = candidate.ops_per_sec / baseline.ops_per_sec
    p99_ratio = candidate.p99_ms / baseline.p99_ms
    reasons: list[str] = []
    if throughput_ratio < min_throughput_ratio:
        reasons.append(f"throughput_ratio {throughput_ratio:.3f} < {min_throughput_ratio:.3f}")
    if p99_ratio > max_latency_ratio:
        reasons.append(f"p99_ratio {p99_ratio:.3f} > {max_latency_ratio:.3f}")
    return AdapterComparisonOutcome(
        case=candidate.case,
        candidate_driver=candidate.driver,
        baseline_driver=baseline.driver,
        driver_mode=candidate.driver_mode,
        candidate_ops_per_sec=candidate.ops_per_sec,
        baseline_ops_per_sec=baseline.ops_per_sec,
        throughput_ratio=throughput_ratio,
        candidate_p99_ms=candidate.p99_ms,
        baseline_p99_ms=baseline.p99_ms,
        p99_ratio=p99_ratio,
        passed=not reasons,
        reason="; ".join(reasons) if reasons else None,
    )
