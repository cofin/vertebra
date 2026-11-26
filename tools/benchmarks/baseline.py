"""Performance baseline record and regression comparator."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import msgspec

from tools.benchmarks.micro import run_all_micro_benchmarks
from vertebra.utils.module_loader import import_optional


class BaselineRecord(msgspec.Struct, frozen=True):
    """Immutable baseline performance recording."""

    name: str
    commit: str
    timestamp: str
    criterion_ns_per_iter: dict[str, float]
    micro_ops_per_sec: dict[str, float]
    micro_p99_ns: dict[str, float]
    import_ms: dict[str, float]
    e2e_rps: dict[str, float]


def get_git_commit(repo_root: Path) -> str:
    """Retrieve the current Git commit hash or fallback string."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    except Exception:
        pass
    return "unknown"


def find_criterion_estimates(repo_root: Path) -> dict[str, float]:
    """Search for Criterion estimates.json files and parse median/mean ns per iter."""
    results: dict[str, float] = {}

    search_dirs: list[Path] = []
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        search_dirs.append(Path(cargo_target) / "criterion")
    search_dirs.extend(
        [
            repo_root / "src/rs/target/criterion",
            repo_root / "target/criterion",
        ]
    )

    seen_benchmarks: set[str] = set()

    for cdir in search_dirs:
        if not cdir.is_dir():
            continue
        for est_path in cdir.rglob("estimates.json"):
            if "new" not in est_path.parts:
                continue
            try:
                rel_parts = est_path.parent.parent.relative_to(cdir).parts
                bench_name = "::".join(rel_parts)
                if bench_name in seen_benchmarks:
                    continue

                raw = json.loads(est_path.read_text(encoding="utf-8"))
                val: float | None = None
                for key in ("median", "Median", "mean", "Mean"):
                    sec = raw.get(key)
                    if isinstance(sec, dict) and "point_estimate" in sec:
                        val = float(sec["point_estimate"])
                        break
                if val is not None:
                    results[bench_name] = val
                    seen_benchmarks.add(bench_name)
            except Exception:
                continue

    return results


def find_latest_e2e_results(repo_root: Path) -> dict[str, float]:
    """Query DuckDB benchmark results if available, returning latest target:scenario rps."""
    db_path = repo_root / "tools/benchmarks/results/benchmarks.duckdb"
    if not db_path.exists():
        return {}

    duckdb_mod = import_optional("duckdb")
    if duckdb_mod is None:
        return {}

    try:
        conn = duckdb_mod.connect(str(db_path), read_only=True)
        query = (
            "SELECT target, scenario, rps FROM benchmark_results "
            "WHERE run_id = (SELECT run_id FROM benchmark_runs ORDER BY started_at DESC LIMIT 1)"
        )
        rows = conn.execute(query).fetchall()
        return {f"{row[0]}::{row[1]}": float(row[2]) for row in rows}
    except Exception:
        return {}


def collect_baseline(
    name: str,
    repo_root: Path,
    *,
    skip_e2e: bool = False,
    micro_iterations: int = 1000,
) -> BaselineRecord:
    """Collect Criterion, micro-benchmarks, import times, and e2e metrics into a BaselineRecord."""
    commit = get_git_commit(repo_root)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    criterion_metrics = find_criterion_estimates(repo_root)

    suite = run_all_micro_benchmarks(iterations=micro_iterations)
    micro_ops = {k: v.ops_per_sec for k, v in suite.samples.items()}
    micro_p99 = {k: v.p99_ns for k, v in suite.samples.items()}
    import_timings = suite.import_ms

    e2e_metrics = {} if skip_e2e else find_latest_e2e_results(repo_root)

    return BaselineRecord(
        name=name,
        commit=commit,
        timestamp=timestamp,
        criterion_ns_per_iter=criterion_metrics,
        micro_ops_per_sec=micro_ops,
        micro_p99_ns=micro_p99,
        import_ms=import_timings,
        e2e_rps=e2e_metrics,
    )


def compare_baselines(
    baseline: BaselineRecord,
    current: BaselineRecord,
    *,
    throughput_threshold_pct: float = -3.0,
    latency_threshold_pct: float = 5.0,
) -> list[str]:
    """Compare current measurements against baseline, identifying any regressions."""
    regressions: list[str] = []

    throughput_groups = (
        ("micro_ops_per_sec", baseline.micro_ops_per_sec, current.micro_ops_per_sec),
        ("e2e_rps", baseline.e2e_rps, current.e2e_rps),
    )
    for category, base_dict, cur_dict in throughput_groups:
        for metric_name, base_val in base_dict.items():
            if base_val <= 0:
                continue
            cur_val = cur_dict.get(metric_name)
            if cur_val is None:
                continue
            pct = ((cur_val - base_val) / base_val) * 100.0
            if pct < throughput_threshold_pct:
                regressions.append(
                    f"REGRESSION [{category}] {metric_name}: {pct:+.2f}% exceeds {throughput_threshold_pct:+.2f}% threshold"
                )

    latency_groups = (
        ("criterion_ns_per_iter", baseline.criterion_ns_per_iter, current.criterion_ns_per_iter),
        ("micro_p99_ns", baseline.micro_p99_ns, current.micro_p99_ns),
        ("import_ms", baseline.import_ms, current.import_ms),
    )
    for category, base_dict, cur_dict in latency_groups:
        for metric_name, base_val in base_dict.items():
            if base_val <= 0:
                continue
            cur_val = cur_dict.get(metric_name)
            if cur_val is None:
                continue
            pct = ((cur_val - base_val) / base_val) * 100.0
            if pct > latency_threshold_pct:
                regressions.append(
                    f"REGRESSION [{category}] {metric_name}: {pct:+.2f}% exceeds +{latency_threshold_pct:.2f}% threshold"
                )

    return regressions


def record_command(args: argparse.Namespace, repo_root: Path) -> int:
    """Handle baseline record command."""
    out_dir = repo_root / "tools/benchmarks/baselines"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{args.name}.json"

    record = collect_baseline(
        args.name,
        repo_root,
        skip_e2e=args.skip_e2e,
        micro_iterations=args.iterations,
    )

    data = msgspec.to_builtins(record)
    out_file.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"Recorded baseline '{args.name}' to {out_file}\n")
    return 0


def check_command(args: argparse.Namespace, repo_root: Path) -> int:
    """Handle baseline check command."""
    baseline_dir = repo_root / "tools/benchmarks/baselines"
    baseline_file = baseline_dir / f"{args.baseline}.json"

    if not baseline_file.exists():
        sys.stderr.write(f"Baseline file {baseline_file} does not exist\n")
        return 1

    baseline_data = json.loads(baseline_file.read_text(encoding="utf-8"))
    baseline = msgspec.convert(baseline_data, BaselineRecord)

    if args.current:
        cur_path = Path(args.current)
        cur_data = json.loads(cur_path.read_text(encoding="utf-8"))
        current = msgspec.convert(cur_data, BaselineRecord)
    else:
        current = collect_baseline(
            "current",
            repo_root,
            skip_e2e=True,
            micro_iterations=args.iterations,
        )

    regressions = compare_baselines(baseline, current)

    if regressions:
        sys.stderr.write("Benchmark check FAILED with regressions:\n")
        for reg in regressions:
            sys.stderr.write(f"{reg}\n")
        return 1

    sys.stdout.write(f"Benchmark check PASSED: all metrics within thresholds compared to '{args.baseline}'.\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for recording and verifying benchmark baselines."""
    parser = argparse.ArgumentParser(description="Vertebra benchmark baseline utility")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    rec = subparsers.add_parser("record", help="Record current measurements as a named baseline")
    rec.add_argument("--name", required=True, help="Baseline name (e.g. pre-refactor)")
    rec.add_argument("--skip-e2e", action="store_true", help="Skip querying DuckDB e2e runs")
    rec.add_argument("--iterations", type=int, default=1000, help="Micro-benchmark iterations")

    chk = subparsers.add_parser("check", help="Compare current measurements against a named baseline")
    chk.add_argument("--baseline", required=True, help="Baseline name to compare against")
    chk.add_argument("--current", help="Optional path to current JSON file instead of measuring live")
    chk.add_argument("--iterations", type=int, default=1000, help="Micro-benchmark iterations")

    args = parser.parse_args(argv)
    repo_root = Path.cwd()

    if args.subcommand == "record":
        return record_command(args, repo_root)
    if args.subcommand == "check":
        return check_command(args, repo_root)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
