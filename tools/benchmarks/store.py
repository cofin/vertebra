"""DuckDB-backed benchmark result storage."""

from __future__ import annotations

import platform
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SQL_DIR = Path(__file__).parent / "sql"
_DEFAULT_DB = Path(__file__).parent / "results" / "benchmarks.duckdb"


def _git(*args: str) -> str:
    """Run a git command and return stripped stdout, or empty string on failure."""
    try:
        result = subprocess.run(
            ["git", *args],  # noqa: S607
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


class BenchmarkStore:
    """Manages benchmark data in a DuckDB database."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        import duckdb

        self.db_path = Path(db_path) if db_path else _DEFAULT_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path))

        # Pre-parse all SQL files
        self._schema = _parse_named_queries(_read_sql("schema.sql"))
        self._inserts = _parse_named_queries(_read_sql("insert.sql"))
        self._queries = _parse_named_queries(_read_sql("queries.sql"))

        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Create tables and sequences if they don't exist."""
        self._conn.execute(self._schema["create_bench_result_seq"])
        self._conn.execute(self._schema["create_benchmark_runs"])
        self._conn.execute(self._schema["create_benchmark_results"])
        self._migrate_add_topology()

    def _migrate_add_topology(self) -> None:
        """Add topology column to benchmark_results if missing (pre-existing DBs)."""
        cols = {
            row[0]
            for row in self._conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'benchmark_results'"
            ).fetchall()
        }
        if "topology" not in cols:
            self._conn.execute("ALTER TABLE benchmark_results ADD COLUMN topology VARCHAR DEFAULT 'unknown'")

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
            ],
        )

    def ingest_jsonl(self, jsonl_path: str | Path, note: str = "") -> str:
        """Ingest a JSONL file into the database.

        Returns:
            The run_id created for the ingested data.
        """
        import json

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

    def get_run_results(self, run_id: str) -> list[dict[str, Any]]:
        """Get all results for a specific run."""
        result = self._conn.execute(self._queries["select_run_results"], [run_id])
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
