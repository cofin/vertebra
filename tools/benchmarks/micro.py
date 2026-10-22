"""Micro-benchmarks for Vertebra hot-path primitives and imports."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from collections.abc import Callable

import msgspec

from vertebra._vertebra import Request, Response
from vertebra.utils.serializers import decode_json, encode_json_bytes


class MicroBenchSample(msgspec.Struct, frozen=True, gc=False):
    """Measured performance metrics for a single micro-benchmark."""

    name: str
    iterations: int
    median_ns: float
    p99_ns: float
    ops_per_sec: float


class MicroBenchSuiteResult(msgspec.Struct, frozen=True, gc=False):
    """Aggregated suite result containing all micro samples and import timings."""

    samples: dict[str, MicroBenchSample]
    import_ms: dict[str, float]
    timestamp: str


def _time_callable(
    fn: Callable[[], None],
    *,
    iterations: int = 1000,
    rounds: int = 5,
    warmup_rounds: int = 2,
) -> tuple[float, float, float]:
    """Execute warmup and timed rounds for a callable, returning median_ns, p99_ns, and ops_per_sec."""
    for _ in range(warmup_rounds):
        for _ in range(iterations):
            fn()

    round_durations_ns: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter_ns()
        for _ in range(iterations):
            fn()
        elapsed = time.perf_counter_ns() - start
        round_durations_ns.append(elapsed / iterations)

    sorted_durations = sorted(round_durations_ns)
    median_ns = statistics.median(sorted_durations)
    p99_idx = max(0, int(len(sorted_durations) * 0.99) - 1)
    p99_ns = sorted_durations[p99_idx]
    ops_per_sec = 1_000_000_000.0 / median_ns if median_ns > 0 else 0.0

    return median_ns, p99_ns, ops_per_sec


def benchmark_request_headers_materialization(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark Request.headers materialization on a 16-header native request."""
    pairs = [(f"x-custom-header-{i}", f"custom-header-value-{i}") for i in range(16)]

    def run() -> None:
        req = Request.from_parts("GET", "/benchmark/headers", headers=pairs)
        _ = req.headers

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="request_headers_materialization",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def benchmark_asgi_scope_build_and_copy(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark AsgiScope construction, key lookup, and dict(scope) copy."""
    pairs = [(f"header-{i}", f"value-{i}") for i in range(8)]
    req = Request.from_parts("GET", "/api/v1/users", headers=pairs)

    def run() -> None:
        scope = req.asgi_scope()
        _ = scope["method"]
        _ = scope["path"]
        _ = scope["headers"]
        _ = dict(scope)

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="asgi_scope_build_and_copy",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def benchmark_wsgi_environ_items(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark WsgiEnviron construction and .items() materialization."""
    pairs = [(f"header-{i}", f"value-{i}") for i in range(8)]
    req = Request.from_parts("POST", "/wsgi/submit", headers=pairs)

    def run() -> None:
        environ = req.wsgi_environ()
        _ = environ.items()

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="wsgi_environ_items",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def benchmark_rsgi_scope_build(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark RsgiScope construction and attribute reads."""
    pairs = [(f"header-{i}", f"value-{i}") for i in range(8)]
    req = Request.from_parts("GET", "/rsgi/stream", headers=pairs)

    def run() -> None:
        scope = req.rsgi_scope()
        _ = (scope.proto, scope.method, scope.path, scope.headers)

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="rsgi_scope_build",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def benchmark_response_construct(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark Response construction and to_bytes() serialization."""
    headers = [("content-type", "application/json"), ("cache-control", "no-cache")]
    body = b'{"status":"ok","message":"benchmark response payload"}'

    def run() -> None:
        resp = Response(status=200, headers=headers, body=body)
        _ = resp.to_bytes()

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="response_construct",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def benchmark_serializers_msgpack_json(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark encode_json_bytes / decode_json round-trip on a 1 KiB payload."""
    payload = {
        "status": "success",
        "entity_id": 12345678,
        "metadata": {
            "region": "us-east-1",
            "tier": "production",
            "cluster_nodes": [f"node-{i}.internal" for i in range(12)],
        },
        "description": "a" * 500,
    }

    def run() -> None:
        encoded = encode_json_bytes(payload)
        _ = decode_json(encoded)

    median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    return MicroBenchSample(
        name="serializers_msgpack_json",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def measure_import_time_ms(module_name: str, rounds: int = 5) -> float:
    """Measure warm subprocess import time in milliseconds."""
    subprocess.run([sys.executable, "-c", f"import {module_name}"], check=True)

    durations: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter_ns()
        subprocess.run([sys.executable, "-c", f"import {module_name}"], check=True)
        durations.append((time.perf_counter_ns() - start) / 1_000_000.0)

    return float(statistics.median(durations))


def run_all_micro_benchmarks(*, iterations: int = 2000) -> MicroBenchSuiteResult:
    """Execute all registered micro-benchmarks and import measurements."""
    samples: dict[str, MicroBenchSample] = {}

    benchmarks = (
        benchmark_request_headers_materialization,
        benchmark_asgi_scope_build_and_copy,
        benchmark_wsgi_environ_items,
        benchmark_rsgi_scope_build,
        benchmark_response_construct,
        benchmark_serializers_msgpack_json,
    )

    for bench in benchmarks:
        sample = bench(iterations=iterations)
        samples[sample.name] = sample

    import_timings: dict[str, float] = {
        "import_vertebra_ms": measure_import_time_ms("vertebra"),
        "import_vertebra_framework_ms": measure_import_time_ms("vertebra.framework"),
    }

    return MicroBenchSuiteResult(
        samples=samples,
        import_ms=import_timings,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running micro-benchmarks."""
    parser = argparse.ArgumentParser(description="Vertebra micro-benchmarks runner")
    parser.add_argument("--iterations", type=int, default=1000, help="Iterations per round")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON")
    args = parser.parse_args(argv)

    suite = run_all_micro_benchmarks(iterations=args.iterations)

    if args.json:
        raw_dict = {
            "timestamp": suite.timestamp,
            "samples": {k: msgspec.to_builtins(v) for k, v in suite.samples.items()},
            "import_ms": suite.import_ms,
        }
        sys.stdout.write(json.dumps(raw_dict, indent=2) + "\n")
    else:
        sys.stdout.write(f"=== Vertebra Micro-Benchmarks ({suite.timestamp}) ===\n\n")
        sys.stdout.write(f"{'Benchmark':<35} {'Ops/sec':>12} {'Median (ns)':>14} {'p99 (ns)':>12}\n")
        sys.stdout.write("-" * 75 + "\n")
        for sample in suite.samples.values():
            sys.stdout.write(
                f"{sample.name:<35} {sample.ops_per_sec:>12.0f} {sample.median_ns:>14.1f} {sample.p99_ns:>12.1f}\n"
            )
        sys.stdout.write("\n=== Subprocess Warm Import Latencies ===\n")
        for mod, ms in suite.import_ms.items():
            sys.stdout.write(f"{mod:<35} {ms:>8.2f} ms\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
