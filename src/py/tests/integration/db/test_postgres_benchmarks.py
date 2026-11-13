"""Live comparative PostgreSQL driver benchmark suite gated behind `postgres_benchmark`."""

from __future__ import annotations

import os

import pytest
from tools.benchmarks.postgres import SCHEMA_VERSION, capture_live_report

_BENCH_ENV = "VERTEBRA_BENCHMARK_POSTGRES"
_DSN_ENV = "VERTEBRA_CONFORMANCE_POSTGRES_DSN"


def _resolve_benchmark_dsn() -> str:
    if os.environ.get(_BENCH_ENV) != "1":
        pytest.skip(f"PostgreSQL comparative benchmarks require {_BENCH_ENV}=1")
    dsn = os.environ.get(_DSN_ENV, "").strip()
    if not dsn:
        pytest.skip(f"PostgreSQL comparative benchmarks require {_DSN_ENV}")
    return dsn


@pytest.mark.postgres_benchmark
def test_postgres_benchmark_live_suite() -> None:
    """Run live `tools.benchmarks.postgres` capture against PostgreSQL when explicitly enabled."""
    import asyncio

    dsn = _resolve_benchmark_dsn()
    report = asyncio.run(capture_live_report(dsn, iterations=25, warmup=5, include_external=True))
    assert report.schema_version == SCHEMA_VERSION
    assert any(item.driver == "vertebra-sync" for item in report.cases)
    assert any(item.driver == "vertebra-async" for item in report.cases)
