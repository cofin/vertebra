"""Live comparative PostgreSQL driver benchmark suite gated behind `postgres_benchmark`."""

from __future__ import annotations

import math
import os

import pytest
from tools.benchmarks.postgres import SCHEMA_VERSION, capture_live_report, format_report_bytes, parse_report_bytes

from vertebra.utils.module_loader import import_optional

_BENCH_ENV = "VERTEBRA_BENCHMARK_POSTGRES"
_DSN_ENV = "VERTEBRA_CONFORMANCE_POSTGRES_DSN"


def _resolve_benchmark_dsn() -> str:
    if os.environ.get(_BENCH_ENV) != "1":
        pytest.skip(f"PostgreSQL comparative benchmarks require {_BENCH_ENV}=1")
    dsn = os.environ.get(_DSN_ENV, "").strip()
    if not dsn:
        pytest.skip(f"PostgreSQL comparative benchmarks require {_DSN_ENV}")
    return dsn


@pytest.mark.anyio
@pytest.mark.postgres_benchmark
async def test_postgres_benchmark_live_suite() -> None:
    """Run live `tools.benchmarks.postgres` capture against PostgreSQL when explicitly enabled."""
    dsn = _resolve_benchmark_dsn()
    report = await capture_live_report(dsn, iterations=25, warmup=5, include_external=True)
    assert report.schema_version == SCHEMA_VERSION
    assert parse_report_bytes(format_report_bytes(report)) == report

    recorded_pairs = {(item.case, item.driver) for item in report.cases}
    assert ("point_select_1", "vertebra-sync") in recorded_pairs
    assert ("point_select_1", "vertebra-async") in recorded_pairs
    assert ("pipeline_select_16", "vertebra-async") in recorded_pairs
    assert all(
        item.iterations >= 1
        and math.isfinite(item.median_ms)
        and item.median_ms > 0.0
        and math.isfinite(item.p99_ms)
        and item.p99_ms > 0.0
        and math.isfinite(item.ops_per_sec)
        and item.ops_per_sec > 0.0
        for item in report.cases
    )

    if import_optional("psycopg") is not None:
        assert ("point_select_1", "psycopg-sync") in recorded_pairs
        assert ("point_select_1", "psycopg-async") in recorded_pairs
    if import_optional("asyncpg") is not None:
        assert ("point_select_1", "asyncpg") in recorded_pairs
