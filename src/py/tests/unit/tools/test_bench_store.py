"""Unit tests for tools.benchmarks.store DuckDB matrix schema and queries."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest
from tools.benchmarks.store import (
    _RESULT_COLUMN_MIGRATIONS,
    BenchmarkStore,
    _parse_named_queries,
    _read_sql,
)

if TYPE_CHECKING:
    from pathlib import Path

_MATRIX_COLUMNS: tuple[str, ...] = (
    "adapter",
    "protocol",
    "transport",
    "broker_mode",
    "load_mode",
    "target_rps",
    "pool_size",
    "gil_enabled",
)

_COMPARE_KEY_COLUMNS: tuple[str, ...] = (
    "adapter",
    "protocol",
    "transport",
    "broker_mode",
    "load_mode",
)


def _sample_result(**overrides: Any) -> dict[str, Any]:
    """Build a baseline benchmark result payload with optional field overrides."""
    payload: dict[str, Any] = {
        "target": "vertebra",
        "topology": "multi-worker",
        "scenario": "json",
        "concurrency": 64,
        "duration": 10,
        "rps": 50000.0,
        "latency_mean_s": 0.0012,
        "latency_p50_s": 0.001,
        "latency_p99_s": 0.0025,
        "latency_max_s": 0.01,
        "total_requests": 500000,
        "errors": 0,
        "url": "http://127.0.0.1:8000/json",
        "timestamp": "2026-11-19T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def test_schema_and_query_files_define_all_matrix_columns() -> None:
    """Verify schema, insert, select, compare SQL and migration table include all 8 matrix columns."""
    schema_queries = _parse_named_queries(_read_sql("schema.sql"))
    insert_queries = _parse_named_queries(_read_sql("insert.sql"))
    read_queries = _parse_named_queries(_read_sql("queries.sql"))

    create_results_sql = schema_queries["create_benchmark_results"]
    insert_result_sql = insert_queries["insert_result"]
    select_results_sql = read_queries["select_run_results"]
    compare_runs_sql = read_queries["compare_runs"]

    for col in _MATRIX_COLUMNS:
        assert col in create_results_sql
        assert col in insert_result_sql
        assert col in select_results_sql

    for idx in range(1, 24):
        assert f"${idx}" in insert_result_sql

    for col in _COMPARE_KEY_COLUMNS:
        assert f"COALESCE(c.{col}, b.{col}) AS {col}" in compare_runs_sql
        assert f"c.{col} = b.{col}" in compare_runs_sql

    migration_names = [name for name, _ in _RESULT_COLUMN_MIGRATIONS]
    assert migration_names == ["topology", *_MATRIX_COLUMNS]


@pytest.mark.parametrize("include_topology", [False, True])
def test_store_migrates_legacy_duckdb_idempotently(tmp_path: Path, include_topology: bool) -> None:
    """Verify BenchmarkStore upgrades legacy 14-column and 15-column DuckDB tables idempotently."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / f"legacy_{'15' if include_topology else '14'}.duckdb"

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SEQUENCE bench_result_seq START 1")
        conn.execute(
            """
            CREATE TABLE benchmark_runs (
                run_id VARCHAR PRIMARY KEY,
                git_sha VARCHAR,
                git_branch VARCHAR,
                machine_os VARCHAR,
                machine_arch VARCHAR,
                machine_cpu VARCHAR,
                python_version VARCHAR,
                started_at TIMESTAMP,
                note VARCHAR DEFAULT ''
            )
            """
        )
        topology_ddl = "topology VARCHAR," if include_topology else ""
        conn.execute(
            f"""
            CREATE TABLE benchmark_results (
                id INTEGER PRIMARY KEY DEFAULT nextval('bench_result_seq'),
                run_id VARCHAR NOT NULL REFERENCES benchmark_runs(run_id),
                target VARCHAR,
                {topology_ddl}
                scenario VARCHAR,
                concurrency INTEGER,
                duration_s INTEGER,
                rps DOUBLE,
                latency_mean_ms DOUBLE,
                latency_p50_ms DOUBLE,
                latency_p99_ms DOUBLE,
                latency_max_ms DOUBLE,
                total_requests INTEGER,
                errors INTEGER DEFAULT 0,
                url VARCHAR,
                timestamp TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO benchmark_runs (
                run_id, git_sha, git_branch, machine_os, machine_arch,
                machine_cpu, python_version, started_at, note
            ) VALUES ('legacy-run', 'abc1234', 'main', 'Linux', 'x86_64', 'cpu', '3.12.0', '2026-01-01T00:00:00', 'old')
            """
        )
        if include_topology:
            conn.execute(
                """
                INSERT INTO benchmark_results (
                    run_id, target, topology, scenario, concurrency, duration_s,
                    rps, latency_mean_ms, latency_p50_ms, latency_p99_ms, latency_max_ms,
                    total_requests, errors, url, timestamp
                ) VALUES (
                    'legacy-run', 'vertebra', 'single-worker', 'json', 32, 10,
                    42000.0, 1.0, 0.8, 2.0, 5.0,
                    420000, 0, 'http://127.0.0.1:8000/json', '2026-01-01T00:00:00'
                )
                """
            )
        else:
            conn.execute(
                """
                INSERT INTO benchmark_results (
                    run_id, target, scenario, concurrency, duration_s,
                    rps, latency_mean_ms, latency_p50_ms, latency_p99_ms, latency_max_ms,
                    total_requests, errors, url, timestamp
                ) VALUES (
                    'legacy-run', 'vertebra', 'json', 32, 10,
                    42000.0, 1.0, 0.8, 2.0, 5.0,
                    420000, 0, 'http://127.0.0.1:8000/json', '2026-01-01T00:00:00'
                )
                """
            )
    finally:
        conn.close()

    store_first = BenchmarkStore(db_path)
    store_first.close()

    store_second = BenchmarkStore(db_path)
    try:
        rows = store_second.get_run_results("legacy-run")
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "vertebra"
        assert row["topology"] == ("single-worker" if include_topology else "unknown")
        assert row["rps"] == 42000.0
        assert row["adapter"] == "none"
        assert row["protocol"] == "http1"
        assert row["transport"] == "native"
        assert row["broker_mode"] == "none"
        assert row["load_mode"] == "closed_loop"
        assert row["target_rps"] is None
        assert row["pool_size"] is None
        assert row["gil_enabled"] is True
    finally:
        store_second.close()


@pytest.mark.parametrize(
    ("filter_kwargs", "expected_rps"),
    [
        ({"adapter": "postgres"}, [40000.0, 48000.0]),
        ({"protocol": "http2"}, [48000.0]),
        ({"transport": "asgi"}, [30000.0]),
        ({"broker_mode": "shm_broker"}, [48000.0]),
        ({"load_mode": "open_loop"}, [30000.0]),
        ({"gil_enabled": False}, [48000.0]),
        ({"adapter": "postgres", "broker_mode": "per_worker"}, [40000.0]),
    ],
)
def test_store_round_trip_matrix_insert_filter_and_compare(
    tmp_path: Path,
    filter_kwargs: dict[str, Any],
    expected_rps: list[float],
) -> None:
    """Verify matrix round-trip insert, parameterized filtering, and full-key run comparison."""
    pytest.importorskip("duckdb")
    store = BenchmarkStore(tmp_path / "matrix.duckdb")
    try:
        baseline_id = store.create_run(note="baseline")
        store.insert_result(
            baseline_id,
            _sample_result(
                rps=36000.0,
                adapter="postgres",
                protocol="http1",
                transport="native",
                broker_mode="per_worker",
                load_mode="closed_loop",
                target_rps=None,
                pool_size=16,
                gil_enabled=True,
            ),
        )
        store.insert_result(
            baseline_id,
            _sample_result(
                rps=40000.0,
                adapter="postgres",
                protocol="http2",
                transport="native",
                broker_mode="shm_broker",
                load_mode="closed_loop",
                target_rps=50000.0,
                pool_size=32,
                gil_enabled=False,
            ),
        )

        current_id = store.create_run(note="current")
        store.insert_result(
            current_id,
            _sample_result(
                rps=40000.0,
                adapter="postgres",
                protocol="http1",
                transport="native",
                broker_mode="per_worker",
                load_mode="closed_loop",
                target_rps=None,
                pool_size=16,
                gil_enabled=True,
            ),
        )
        store.insert_result(
            current_id,
            _sample_result(
                rps=48000.0,
                adapter="postgres",
                protocol="http2",
                transport="native",
                broker_mode="shm_broker",
                load_mode="closed_loop",
                target_rps=50000.0,
                pool_size=32,
                gil_enabled=False,
            ),
        )
        store.insert_result(
            current_id,
            _sample_result(
                rps=30000.0,
                adapter="redis",
                protocol="http1",
                transport="asgi",
                broker_mode="per_worker",
                load_mode="open_loop",
                target_rps=35000.0,
                pool_size=8,
                gil_enabled=True,
            ),
        )

        filtered = store.get_run_results(current_id, **filter_kwargs)
        assert sorted(row["rps"] for row in filtered) == sorted(expected_rps)

        comparison = store.compare(current_id, baseline_id)
        assert len(comparison) == 3

        by_key = {
            (row["adapter"], row["protocol"], row["transport"], row["broker_mode"], row["load_mode"]): row
            for row in comparison
        }
        per_worker_row = by_key[("postgres", "http1", "native", "per_worker", "closed_loop")]
        assert per_worker_row["current_rps"] == 40000.0
        assert per_worker_row["baseline_rps"] == 36000.0
        assert per_worker_row["rps_delta_pct"] == pytest.approx(((40000.0 - 36000.0) / 36000.0) * 100.0)

        shm_row = by_key[("postgres", "http2", "native", "shm_broker", "closed_loop")]
        assert shm_row["current_rps"] == 48000.0
        assert shm_row["baseline_rps"] == 40000.0
        assert shm_row["rps_delta_pct"] == pytest.approx(20.0)

        unmatched_row = by_key[("redis", "http1", "asgi", "per_worker", "open_loop")]
        assert unmatched_row["current_rps"] == 30000.0
        assert unmatched_row["baseline_rps"] is None
        assert unmatched_row["rps_delta_pct"] is None
    finally:
        store.close()


def test_store_ingest_legacy_jsonl_applies_defaults(tmp_path: Path) -> None:
    """Verify ingest_jsonl inserts legacy JSONL payloads with default matrix column values."""
    pytest.importorskip("duckdb")
    jsonl_path = tmp_path / "legacy.jsonl"
    legacy_record = _sample_result()
    jsonl_path.write_text(f"{json.dumps(legacy_record)}\n")

    store = BenchmarkStore(tmp_path / "ingest.duckdb")
    try:
        run_id = store.ingest_jsonl(jsonl_path)
        rows = store.get_run_results(run_id)
        assert len(rows) == 1
        row = rows[0]
        assert row["target"] == "vertebra"
        assert row["adapter"] == "none"
        assert row["protocol"] == "http1"
        assert row["transport"] == "native"
        assert row["broker_mode"] == "none"
        assert row["load_mode"] == "closed_loop"
        assert row["target_rps"] is None
        assert row["pool_size"] is None
        assert row["gil_enabled"] is True
    finally:
        store.close()
