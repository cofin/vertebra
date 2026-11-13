"""PostgreSQL comparative benchmark capture and deterministic threshold comparator."""

from __future__ import annotations

import argparse
import asyncio
import math
import statistics
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import msgspec

from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes

SCHEMA_VERSION: Final[int] = 1
DEFAULT_MIN_THROUGHPUT_RATIO: Final[float] = 0.90
DEFAULT_MAX_LATENCY_RATIO: Final[float] = 1.25

__all__ = (
    "DEFAULT_MAX_LATENCY_RATIO",
    "DEFAULT_MIN_THROUGHPUT_RATIO",
    "SCHEMA_VERSION",
    "PostgresBenchmarkCase",
    "PostgresBenchmarkReport",
    "PostgresComparisonOutcome",
    "capture_live_report",
    "compare_reports",
    "compute_case_stats",
    "format_report_bytes",
    "main",
    "parse_report_bytes",
)


class PostgresBenchmarkCase(msgspec.Struct, frozen=True, gc=False):
    """Measured throughput and latency summary for one driver/workload pair."""

    case: str
    driver: str
    iterations: int
    median_ms: float
    p99_ms: float
    ops_per_sec: float


class PostgresBenchmarkReport(msgspec.Struct, frozen=True, gc=False):
    """Versioned (`schema_version=1`) PostgreSQL benchmark report payload."""

    schema_version: int
    generated_at: str
    dsn_redacted: str
    cases: tuple[PostgresBenchmarkCase, ...]


class PostgresComparisonOutcome(msgspec.Struct, frozen=True, gc=False):
    """Comparison outcome evaluating a candidate driver/report against a baseline."""

    case: str
    candidate_driver: str
    baseline_driver: str
    candidate_ops_per_sec: float
    baseline_ops_per_sec: float
    throughput_ratio: float
    candidate_p99_ms: float
    baseline_p99_ms: float
    p99_ratio: float
    passed: bool
    reason: str | None = None


def compute_case_stats(
    *,
    case: str,
    driver: str,
    latencies_ns: Sequence[int | float],
) -> PostgresBenchmarkCase:
    """Compute finite median/p99 latencies (ms) and ops/sec from per-iteration nanosecond timings."""
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
    return PostgresBenchmarkCase(
        case=case,
        driver=driver,
        iterations=len(ordered),
        median_ms=median_ns / 1_000_000.0,
        p99_ms=p99_ns / 1_000_000.0,
        ops_per_sec=ops_per_sec,
    )


def format_report_bytes(report: PostgresBenchmarkReport) -> bytes:
    """Serialize `PostgresBenchmarkReport` as UTF-8 JSON bytes using repository serializer conventions."""
    _validate_report(report)
    return encode_json_bytes(report)


def parse_report_bytes(payload: bytes | str) -> PostgresBenchmarkReport:
    """Parse and strictly validate a `schema_version=1` PostgreSQL benchmark JSON report."""
    report: PostgresBenchmarkReport = decode_json(payload, type=PostgresBenchmarkReport)
    _validate_report(report)
    return report


def compare_reports(
    candidate: PostgresBenchmarkReport,
    baseline: PostgresBenchmarkReport,
    *,
    candidate_driver: str | None = None,
    baseline_driver: str | None = None,
    min_throughput_ratio: float = DEFAULT_MIN_THROUGHPUT_RATIO,
    max_latency_ratio: float = DEFAULT_MAX_LATENCY_RATIO,
    require_nonempty: bool = True,
) -> tuple[PostgresComparisonOutcome, ...]:
    """Compare candidate measurements against baseline measurements across shared cases."""
    _validate_report(candidate)
    _validate_report(baseline)
    if not math.isfinite(min_throughput_ratio) or min_throughput_ratio <= 0.0:
        msg = f"min_throughput_ratio must be > 0 (got {min_throughput_ratio})"
        raise ValueError(msg)
    if not math.isfinite(max_latency_ratio) or max_latency_ratio <= 0.0:
        msg = f"max_latency_ratio must be > 0 (got {max_latency_ratio})"
        raise ValueError(msg)

    cand_map: dict[tuple[str, str], PostgresBenchmarkCase] = {
        (entry.case, entry.driver): entry for entry in candidate.cases
    }
    base_map: dict[tuple[str, str], PostgresBenchmarkCase] = {
        (entry.case, entry.driver): entry for entry in baseline.cases
    }

    outcomes: list[PostgresComparisonOutcome] = []
    if candidate_driver is not None or baseline_driver is not None:
        if candidate_driver is None or baseline_driver is None:
            msg = "Both candidate_driver and baseline_driver must be set when comparing drivers explicitly"
            raise ValueError(msg)
        case_names = sorted({entry.case for entry in candidate.cases if entry.driver == candidate_driver})
        for case_name in case_names:
            cand_entry = cand_map.get((case_name, candidate_driver))
            base_entry = base_map.get((case_name, baseline_driver))
            if cand_entry is None or base_entry is None:
                msg = f"Missing comparison pair for case {case_name!r} ({candidate_driver} vs {baseline_driver})"
                raise ValueError(msg)
            outcomes.append(
                _evaluate_pair(
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
            outcomes.append(
                _evaluate_pair(
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


async def capture_live_report(
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
) -> PostgresBenchmarkReport:
    """Execute live PostgreSQL benchmarks for `vertebra` and optional `psycopg`/`asyncpg` baselines."""
    if iterations < 1 or warmup < 0:
        msg = "iterations must be >= 1 and warmup must be >= 0"
        raise ValueError(msg)

    cfg = PostgreSQLConfig.from_url(dsn)
    cases: list[PostgresBenchmarkCase] = []

    cases.append(_measure_sync_vertebra_point_select(cfg, iterations=iterations, warmup=warmup))
    cases.append(await _measure_async_vertebra_point_select(cfg, iterations=iterations, warmup=warmup))
    cases.append(await _measure_async_vertebra_pipeline_batch(cfg, iterations=max(10, iterations // 5), warmup=warmup))

    if include_external:
        psycopg_mod = import_optional("psycopg")
        if psycopg_mod is not None:
            cases.append(_measure_psycopg_sync_point_select(psycopg_mod, dsn, iterations=iterations, warmup=warmup))
            cases.append(
                await _measure_psycopg_async_point_select(psycopg_mod, dsn, iterations=iterations, warmup=warmup)
            )
        asyncpg_mod = import_optional("asyncpg")
        if asyncpg_mod is not None:
            cases.append(await _measure_asyncpg_point_select(asyncpg_mod, dsn, iterations=iterations, warmup=warmup))

    return PostgresBenchmarkReport(
        schema_version=SCHEMA_VERSION,
        generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        dsn_redacted=_redact_config_dsn(cfg),
        cases=tuple(cases),
    )


def _redact_config_dsn(config: PostgreSQLConfig) -> str:
    _, host, port, database, user, *_ = config.pool_key()
    prefix = f"{user}@" if user else ""
    return f"postgresql://{prefix}{host}:{port}/{database}"


def main(argv: list[str] | None = None) -> int:
    """Run CLI `capture` or `compare` commands returning exit status `0` (pass) or `1` (regression)."""
    parser = argparse.ArgumentParser(description="PostgreSQL driver benchmark harness and threshold comparator.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_cmd = subparsers.add_parser("capture", help="Run live PostgreSQL benchmark suite and emit JSON.")
    capture_cmd.add_argument("--dsn", required=True, help="PostgreSQL connection DSN.")
    capture_cmd.add_argument("--iterations", type=int, default=100, help="Measured iterations per workload.")
    capture_cmd.add_argument("--warmup", type=int, default=10, help="Warmup iterations per workload.")
    capture_cmd.add_argument("--output", type=Path, default=None, help="Optional output file path.")
    capture_cmd.add_argument(
        "--no-external",
        action="store_true",
        help="Benchmark Vertebra drivers only without optional psycopg/asyncpg comparators.",
    )

    compare_cmd = subparsers.add_parser("compare", help="Compare candidate and baseline schema_version=1 reports.")
    compare_cmd.add_argument("--candidate", type=Path, required=True, help="Candidate JSON report path.")
    compare_cmd.add_argument("--baseline", type=Path, required=True, help="Baseline JSON report path.")
    compare_cmd.add_argument("--candidate-driver", default=None, help="Candidate driver label within report.")
    compare_cmd.add_argument("--baseline-driver", default=None, help="Baseline driver label within report.")
    compare_cmd.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=DEFAULT_MIN_THROUGHPUT_RATIO,
        help="Minimum allowed candidate/baseline ops/sec ratio.",
    )
    compare_cmd.add_argument(
        "--max-latency-ratio",
        type=float,
        default=DEFAULT_MAX_LATENCY_RATIO,
        help="Maximum allowed candidate/baseline p99 latency ratio.",
    )

    args = parser.parse_args(argv)
    if args.command == "capture":
        report = asyncio.run(
            capture_live_report(
                args.dsn,
                iterations=args.iterations,
                warmup=args.warmup,
                include_external=not args.no_external,
            )
        )
        encoded = format_report_bytes(report)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(encoded + b"\n")
        else:
            sys.stdout.write(encoded.decode("utf-8") + "\n")
        return 0

    candidate_report = parse_report_bytes(args.candidate.read_bytes())
    baseline_report = parse_report_bytes(args.baseline.read_bytes())
    outcomes = compare_reports(
        candidate_report,
        baseline_report,
        candidate_driver=args.candidate_driver,
        baseline_driver=args.baseline_driver,
        min_throughput_ratio=args.min_throughput_ratio,
        max_latency_ratio=args.max_latency_ratio,
    )
    failures = [outcome for outcome in outcomes if not outcome.passed]
    for outcome in outcomes:
        status = "PASS" if outcome.passed else "FAIL"
        line = (
            f"[{status}] {outcome.case} ({outcome.candidate_driver} vs {outcome.baseline_driver}): "
            f"throughput_ratio={outcome.throughput_ratio:.3f}, p99_ratio={outcome.p99_ratio:.3f}"
        )
        if outcome.reason:
            line += f" ({outcome.reason})"
        sys.stdout.write(line + "\n")
    return 1 if failures else 0


def _validate_report(report: PostgresBenchmarkReport) -> None:
    if report.schema_version != SCHEMA_VERSION:
        msg = f"Unsupported PostgreSQL benchmark report schema_version={report.schema_version} (expected {SCHEMA_VERSION})"
        raise ValueError(msg)
    for item in report.cases:
        if item.iterations < 1:
            msg = f"Benchmark case {item.case}/{item.driver} has invalid iterations={item.iterations}"
            raise ValueError(msg)
        for metric_name, metric in (
            ("median_ms", item.median_ms),
            ("p99_ms", item.p99_ms),
            ("ops_per_sec", item.ops_per_sec),
        ):
            if not math.isfinite(metric) or metric <= 0.0:
                msg = f"Benchmark case {item.case}/{item.driver} has invalid {metric_name}={metric}"
                raise ValueError(msg)


def _evaluate_pair(
    candidate: PostgresBenchmarkCase,
    baseline: PostgresBenchmarkCase,
    *,
    min_throughput_ratio: float,
    max_latency_ratio: float,
) -> PostgresComparisonOutcome:
    throughput_ratio = candidate.ops_per_sec / baseline.ops_per_sec
    p99_ratio = candidate.p99_ms / baseline.p99_ms
    reasons: list[str] = []
    if throughput_ratio < min_throughput_ratio:
        reasons.append(f"throughput_ratio {throughput_ratio:.3f} < {min_throughput_ratio:.3f}")
    if p99_ratio > max_latency_ratio:
        reasons.append(f"p99_ratio {p99_ratio:.3f} > {max_latency_ratio:.3f}")
    return PostgresComparisonOutcome(
        case=candidate.case,
        candidate_driver=candidate.driver,
        baseline_driver=baseline.driver,
        candidate_ops_per_sec=candidate.ops_per_sec,
        baseline_ops_per_sec=baseline.ops_per_sec,
        throughput_ratio=throughput_ratio,
        candidate_p99_ms=candidate.p99_ms,
        baseline_p99_ms=baseline.p99_ms,
        p99_ratio=p99_ratio,
        passed=not reasons,
        reason="; ".join(reasons) if reasons else None,
    )


def _sample_sync(fn: Callable[[], Any], *, iterations: int, warmup: int) -> list[int]:
    for _ in range(warmup):
        fn()
    samples: list[int] = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        fn()
        samples.append(max(1, time.perf_counter_ns() - start))
    return samples


async def _sample_async(fn: Callable[[], Awaitable[Any]], *, iterations: int, warmup: int) -> list[int]:
    for _ in range(warmup):
        await fn()
    samples: list[int] = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await fn()
        samples.append(max(1, time.perf_counter_ns() - start))
    return samples


def _measure_sync_vertebra_point_select(
    config: PostgreSQLConfig,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    with SyncPostgreSQLDriver(config) as driver:
        samples = _sample_sync(
            lambda: driver.select_value("SELECT $1::int4", (42,), value_type=int),
            iterations=iterations,
            warmup=warmup,
        )
    return compute_case_stats(case="point_select_1", driver="vertebra-sync", latencies_ns=samples)


async def _measure_async_vertebra_point_select(
    config: PostgreSQLConfig,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    async with AsyncPostgreSQLDriver(config) as driver:
        samples = await _sample_async(
            lambda: driver.select_value("SELECT $1::int4", (42,), value_type=int),
            iterations=iterations,
            warmup=warmup,
        )
    return compute_case_stats(case="point_select_1", driver="vertebra-async", latencies_ns=samples)


async def _measure_async_vertebra_pipeline_batch(
    config: PostgreSQLConfig,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    async with AsyncPostgreSQLDriver(config) as driver:

        async def _run_batch() -> None:
            async with driver.pipeline() as pipe:
                for val in range(16):
                    pipe.select("SELECT $1::int4 AS val", (val,))
                await pipe.results()

        samples = await _sample_async(_run_batch, iterations=iterations, warmup=warmup)
    return compute_case_stats(case="pipeline_select_16", driver="vertebra-async", latencies_ns=samples)


def _measure_psycopg_sync_point_select(
    psycopg_mod: Any,
    dsn: str,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    with psycopg_mod.connect(dsn) as conn:
        samples = _sample_sync(
            lambda: conn.execute("SELECT %s::int4", (42,)).fetchone(),
            iterations=iterations,
            warmup=warmup,
        )
    return compute_case_stats(case="point_select_1", driver="psycopg-sync", latencies_ns=samples)


async def _measure_psycopg_async_point_select(
    psycopg_mod: Any,
    dsn: str,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    async with await psycopg_mod.AsyncConnection.connect(dsn) as conn:

        async def _step() -> Any:
            cur = await conn.execute("SELECT %s::int4", (42,))
            return await cur.fetchone()

        samples = await _sample_async(_step, iterations=iterations, warmup=warmup)
    return compute_case_stats(case="point_select_1", driver="psycopg-async", latencies_ns=samples)


async def _measure_asyncpg_point_select(
    asyncpg_mod: Any,
    dsn: str,
    *,
    iterations: int,
    warmup: int,
) -> PostgresBenchmarkCase:
    conn = await asyncpg_mod.connect(dsn)
    try:
        samples = await _sample_async(
            lambda: conn.fetchval("SELECT $1::int4", 42),
            iterations=iterations,
            warmup=warmup,
        )
    finally:
        await conn.close()
    return compute_case_stats(case="point_select_1", driver="asyncpg", latencies_ns=samples)


if __name__ == "__main__":
    raise SystemExit(main())
