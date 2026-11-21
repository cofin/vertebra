"""DuckDB-backed benchmark result storage."""

from __future__ import annotations

import json
import platform
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vertebra.utils.module_loader import ensure_module

_SQL_DIR = Path(__file__).parent / "sql"
_DEFAULT_DB = Path(__file__).parent / "results" / "benchmarks.duckdb"
_RESULT_COLUMN_MIGRATIONS: tuple[tuple[str, str], ...] = (
    ("topology", "VARCHAR DEFAULT 'unknown'"),
    ("adapter", "VARCHAR DEFAULT 'none'"),
    ("protocol", "VARCHAR DEFAULT 'http1'"),
    ("transport", "VARCHAR DEFAULT 'native'"),
    ("broker_mode", "VARCHAR DEFAULT 'none'"),
    ("load_mode", "VARCHAR DEFAULT 'closed_loop'"),
    ("target_rps", "DOUBLE DEFAULT NULL"),
    ("pool_size", "INTEGER DEFAULT NULL"),
    ("gil_enabled", "BOOLEAN DEFAULT TRUE"),
)
_RESULT_FILTER_COLUMNS: frozenset[str] = frozenset(
    {"adapter", "protocol", "transport", "broker_mode", "load_mode", "gil_enabled"}
)


class BenchmarkStore:
    """Manages benchmark data in a DuckDB database."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        duckdb_mod = ensure_module("duckdb", package="duckdb")
        self.db_path = Path(db_path) if db_path else _DEFAULT_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb_mod.connect(str(self.db_path))

        self._schema = _parse_named_queries(_read_sql("schema.sql"))
        self._inserts = _parse_named_queries(_read_sql("insert.sql"))
        self._queries = _parse_named_queries(_read_sql("queries.sql"))

        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Create tables and sequences if they don't exist."""
        self._conn.execute(self._schema["create_bench_result_seq"])
        self._conn.execute(self._schema["create_benchmark_runs"])
        self._conn.execute(self._schema["create_benchmark_results"])
        self._migrate_result_columns()

    def _migrate_result_columns(self) -> None:
        """Add missing matrix columns to benchmark_results for pre-existing DBs."""
        cols = {
            row[0]
            for row in self._conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'benchmark_results'"
            ).fetchall()
        }
        for col_name, col_def in _RESULT_COLUMN_MIGRATIONS:
            if col_name not in cols:
                self._conn.execute(f"ALTER TABLE benchmark_results ADD COLUMN {col_name} {col_def}")

    def create_run(self, note: str = "") -> str:
        """Create a new benchmark run and return its run_id."""
        now = datetime.now(tz=UTC)
        run_id = f"{now.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"

        self._conn.execute(
            self._inserts["insert_run"],
            [
                run_id,
                _git("rev-parse", "--short", "HEAD"),
                _git("rev-parse", "--abbrev-ref", "HEAD"),
                platform.system(),
                platform.machine(),
                platform.processor() or "unknown",
                f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                now.isoformat(),
                note,
            ],
        )
        return run_id

    def insert_result(self, run_id: str, result: dict[str, Any]) -> None:
        """Insert a single benchmark result, converting latency s to ms."""
        adapter_val = result.get("adapter")
        protocol_val = result.get("protocol")
        transport_val = result.get("transport")
        broker_mode_val = result.get("broker_mode")
        load_mode_val = result.get("load_mode")
        target_rps_val = result.get("target_rps")
        pool_size_val = result.get("pool_size")
        gil_enabled_val = result.get("gil_enabled")

        self._conn.execute(
            self._inserts["insert_result"],
            [
                run_id,
                result["target"],
                result.get("topology", "unknown"),
                result["scenario"],
                result["concurrency"],
                result["duration"],
                result["rps"],
                result["latency_mean_s"] * 1000,
                result["latency_p50_s"] * 1000,
                result["latency_p99_s"] * 1000,
                result["latency_max_s"] * 1000,
                result["total_requests"],
                result["errors"],
                result["url"],
                result["timestamp"],
                str(adapter_val) if adapter_val is not None else "none",
                str(protocol_val) if protocol_val is not None else "http1",
                str(transport_val) if transport_val is not None else "native",
                str(broker_mode_val) if broker_mode_val is not None else "none",
                str(load_mode_val) if load_mode_val is not None else "closed_loop",
                float(target_rps_val) if target_rps_val is not None else None,
                int(pool_size_val) if pool_size_val is not None else None,
                bool(gil_enabled_val) if gil_enabled_val is not None else True,
            ],
        )

    def ingest_jsonl(self, jsonl_path: str | Path, note: str = "") -> str:
        """Ingest a JSONL file into the database.

        Returns:
            The run_id created for the ingested data.
        """
        jsonl_path = Path(jsonl_path)
        if not jsonl_path.exists():
            msg = f"JSONL file not found: {jsonl_path}"
            raise FileNotFoundError(msg)

        run_id = self.create_run(note=note or f"Ingested from {jsonl_path.name}")

        with jsonl_path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                result = json.loads(line)
                result.setdefault("errors", 0)
                self.insert_result(run_id, result)

        return run_id

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """List recent benchmark runs."""
        result = self._conn.execute(self._queries["select_runs"], [limit])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def get_run_results(
        self,
        run_id: str,
        *,
        adapter: str | None = None,
        protocol: str | None = None,
        transport: str | None = None,
        broker_mode: str | None = None,
        load_mode: str | None = None,
        gil_enabled: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Get results for a specific run, optionally filtered by matrix dimensions."""
        filters: list[tuple[str, Any]] = [
            (col, val)
            for col, val in (
                ("adapter", adapter),
                ("protocol", protocol),
                ("transport", transport),
                ("broker_mode", broker_mode),
                ("load_mode", load_mode),
                ("gil_enabled", gil_enabled),
            )
            if val is not None
        ]
        if not filters:
            result = self._conn.execute(self._queries["select_run_results"], [run_id])
        else:
            clauses = ["run_id = $1"]
            params: list[Any] = [run_id]
            for idx, (col, val) in enumerate(filters, start=2):
                if col not in _RESULT_FILTER_COLUMNS:
                    msg = f"Unsupported filter column: {col}"
                    raise ValueError(msg)
                clauses.append(f"{col} = ${idx}")
                params.append(val)
            sql = (
                "SELECT\n"
                "    target, topology, scenario, concurrency, duration_s,\n"
                "    rps, latency_mean_ms, latency_p50_ms, latency_p99_ms, latency_max_ms,\n"
                "    total_requests, errors, url, timestamp,\n"
                "    adapter, protocol, transport, broker_mode, load_mode,\n"
                "    target_rps, pool_size, gil_enabled\n"
                "FROM benchmark_results\n"
                f"WHERE {' AND '.join(clauses)}\n"
                "ORDER BY target, scenario, protocol, adapter, broker_mode, load_mode, concurrency"
            )
            result = self._conn.execute(sql, params)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def compare(self, current_run_id: str, baseline_run_id: str) -> list[dict[str, Any]]:
        """Compare two runs, returning delta percentages."""
        result = self._conn.execute(self._queries["compare_runs"], [current_run_id, baseline_run_id])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def get_latest_run_id(self) -> str | None:
        """Get the most recent run ID."""
        result = self._conn.execute(self._queries["select_latest_run_id"])
        row = result.fetchone()
        return row[0] if row else None

    def get_previous_run_id(self, run_id: str) -> str | None:
        """Get the run ID immediately before the given one."""
        result = self._conn.execute(self._queries["select_previous_run_id"], [run_id])
        row = result.fetchone()
        return row[0] if row else None

    def get_trend(self, target: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get historical trend data for a target."""
        result = self._conn.execute(self._queries["select_trend"], [target, limit])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


def _git(*args: str) -> str:
    """Run a git command and return stripped stdout, or empty string on failure."""
    git_bin = shutil.which("git")
    if git_bin is None:
        return ""
    try:
        result = subprocess.run(
            [git_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""


def _read_sql(filename: str) -> str:
    """Read a SQL file from the sql/ directory."""
    return (_SQL_DIR / filename).read_text()


def _parse_named_queries(sql_text: str) -> dict[str, str]:
    """Parse SQL file with ``-- name: query_name`` comments into a dict."""
    queries: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name is not None:
                queries[current_name] = "\n".join(current_lines).strip()
            name = stripped.removeprefix("-- name:").strip().rstrip("!")
            current_name = name
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)

    if current_name is not None:
        queries[current_name] = "\n".join(current_lines).strip()

    return queries
