"""Micro-benchmarks for Vertebra hot-path primitives and imports."""

from __future__ import annotations

import argparse
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra._vertebra import Request, Response
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import decode_json, encode_json_bytes, format_json


class MicroBenchSample(msgspec.Struct, frozen=True, gc=False):
    """Measured performance metrics for a single micro-benchmark."""

    name: str
    iterations: int
    median_ns: float
    p99_ns: float
    ops_per_sec: float
    peak_memory_kib: float | None = None


class ArrowPipelineBenchResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Throughput and peak-memory comparison for direct streaming vs buffered Arrow writes."""

    format_hint: str
    rows: int
    payload_bytes: int
    direct: MicroBenchSample
    buffered: MicroBenchSample


class ArrowIpcWorkloadBenchResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Open and full-iteration latency metrics for one Arrow IPC stream workload."""

    workload: str
    batches: int
    rows_per_batch: int
    columns: int
    payload_bytes: int
    open_sample: MicroBenchSample
    iterate_sample: MicroBenchSample


class ArrowIpcBenchResult(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Combined Arrow IPC stream benchmarks across tiny-batch and large-batch shapes."""

    tiny_batches: ArrowIpcWorkloadBenchResult
    large_batches: ArrowIpcWorkloadBenchResult


class MicroBenchSuiteResult(msgspec.Struct, frozen=True, gc=False):
    """Aggregated suite result containing all micro samples and import timings."""

    samples: dict[str, MicroBenchSample]
    import_ms: dict[str, float]
    timestamp: str
    arrow_storage: ArrowPipelineBenchResult | None = None
    arrow_ipc: ArrowIpcBenchResult | None = None


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
    p99_idx = max(0, math.ceil(len(sorted_durations) * 0.99) - 1)
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


def benchmark_asgi_dispatch_batch(iterations: int = 2000) -> MicroBenchSample:
    """Benchmark AsgiRunner batch execution across 8 native ASGI requests."""
    requests = [
        Request.from_parts("GET", f"/api/v1/items/{i}", headers=[("accept", "application/json")]) for i in range(8)
    ]

    async def asgi_app(
        scope: Any,
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        del scope, receive
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b'{"ok":true}',
                "more_body": False,
            }
        )

    runner = AsgiRunner(asgi_app)
    try:

        def run() -> None:
            _ = runner(requests)

        median_ns, p99_ns, ops_per_sec = _time_callable(run, iterations=iterations)
    finally:
        runner.close()

    return MicroBenchSample(
        name="asgi_dispatch_batch",
        iterations=iterations,
        median_ns=median_ns,
        p99_ns=p99_ns,
        ops_per_sec=ops_per_sec,
    )


def _measure_peak_memory_kib(fn: Callable[[], None]) -> float:
    """Measure peak Python heap growth in KiB across a single execution of ``fn``."""
    already_tracing = tracemalloc.is_tracing()
    baseline_bytes = 0
    if already_tracing:
        baseline_bytes, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
    else:
        tracemalloc.start()
    try:
        fn()
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        if not already_tracing:
            tracemalloc.stop()
    return max(0.0, (peak_bytes - baseline_bytes) / 1024.0)


def _arrow_bench_extension(format_hint: str) -> str:
    """Return the storage key suffix matching ``format_hint``."""
    lowered = format_hint.strip().lower()
    if lowered == "parquet":
        return "parquet"
    if lowered == "csv":
        return "csv"
    return "ipc"


def _default_arrow_bench_table(pa: Any) -> Any:
    """Build a multi-batch ~25 MiB Arrow table exceeding the 8 MiB multipart buffer."""
    batch = pa.record_batch(
        [
            pa.array(range(256), type=pa.int64()),
            pa.array(["x" * 4096] * 256, type=pa.string()),
        ],
        names=["id", "payload"],
    )
    return pa.Table.from_batches([batch] * 24)


def benchmark_arrow_storage_pipeline(
    *,
    table: Any | None = None,
    format_hint: str = "parquet",
    iterations: int = 4,
    rounds: int = 3,
    warmup_rounds: int = 1,
    compression: str | None = None,
    write_options: dict[str, Any] | None = None,
) -> ArrowPipelineBenchResult | None:
    """Compare direct streaming ``SyncStoragePipeline.write_arrow`` against buffered ``encode_arrow_payload`` writes.

    Returns ``None`` cleanly when optional ``pyarrow``, ``sqlspec``, or native storage extras are unavailable.
    """
    pa = import_optional("pyarrow")
    if pa is None:
        return None
    if format_hint == "parquet" and import_optional("pyarrow.parquet") is None:
        return None
    if format_hint == "csv" and import_optional("pyarrow.csv") is None:
        return None
    pipeline_mod = import_optional("sqlspec.storage.pipeline")
    payload_mod = import_optional("sqlspec.storage._arrow_payload")
    native_storage = import_optional("vertebra._storage")
    if pipeline_mod is None or payload_mod is None or native_storage is None:
        return None
    sqlspec_storage = import_optional("vertebra.extensions.sqlspec.storage")
    if sqlspec_storage is None:
        return None

    bench_table = _default_arrow_bench_table(pa) if table is None else table
    ext = _arrow_bench_extension(format_hint)
    resolved_write_options: dict[str, Any] | None
    if write_options is not None:
        resolved_write_options = dict(write_options)
    elif format_hint == "parquet":
        resolved_write_options = {"row_group_size": 256, "use_dictionary": False}
    else:
        resolved_write_options = None

    with tempfile.TemporaryDirectory(prefix="vtb-arrow-bench-") as tmp_dir:
        registry = sqlspec_storage.ObjectStoreRegistry()
        sqlspec_storage.register_storage_alias(
            "bench",
            Path(tmp_dir).as_uri(),
            registry=registry,
            backend="vertebra",
        )
        backend = registry.get("bench")
        pipeline = pipeline_mod.SyncStoragePipeline(registry=registry)
        encode_arrow_payload = payload_mod.encode_arrow_payload
        direct_dest = f"alias://bench/direct.{ext}"
        buffered_key = f"buffered.{ext}"

        def run_direct() -> None:
            pipeline.write_arrow(
                bench_table,
                direct_dest,
                format_hint=format_hint,
                compression=compression,
                write_options=resolved_write_options,
                overwrite=True,
            )

        def run_buffered() -> None:
            payload = encode_arrow_payload(
                bench_table,
                format_hint,
                compression=compression,
                write_options=resolved_write_options,
            )
            backend.write_bytes_sync(buffered_key, payload, overwrite=True)

        direct_median_ns, direct_p99_ns, direct_ops = _time_callable(
            run_direct,
            iterations=iterations,
            rounds=rounds,
            warmup_rounds=warmup_rounds,
        )
        buffered_median_ns, buffered_p99_ns, buffered_ops = _time_callable(
            run_buffered,
            iterations=iterations,
            rounds=rounds,
            warmup_rounds=warmup_rounds,
        )

        if warmup_rounds == 0:
            run_direct()
            run_buffered()

        direct_peak_kib = _measure_peak_memory_kib(run_direct)
        buffered_peak_kib = _measure_peak_memory_kib(run_buffered)
        meta = backend.get_metadata_sync(f"direct.{ext}")
        payload_bytes = int(meta["size"])

    return ArrowPipelineBenchResult(
        format_hint=format_hint,
        rows=int(bench_table.num_rows),
        payload_bytes=payload_bytes,
        direct=MicroBenchSample(
            name="arrow_pipeline_direct_stream",
            iterations=iterations,
            median_ns=direct_median_ns,
            p99_ns=direct_p99_ns,
            ops_per_sec=direct_ops,
            peak_memory_kib=direct_peak_kib,
        ),
        buffered=MicroBenchSample(
            name="arrow_pipeline_buffered_bytes",
            iterations=iterations,
            median_ns=buffered_median_ns,
            p99_ns=buffered_p99_ns,
            ops_per_sec=buffered_ops,
            peak_memory_kib=buffered_peak_kib,
        ),
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


def benchmark_arrow_ipc_stream(
    *,
    tiny_batches: int = 2000,
    tiny_rows_per_batch: int = 8,
    large_batches: int = 20,
    large_rows_per_batch: int = 50_000,
    columns: int = 20,
    iterations: int = 5,
    rounds: int = 5,
    warmup_rounds: int = 1,
) -> ArrowIpcBenchResult | None:
    """Benchmark SHM ArrowCursor stream open and single-pass iteration across tiny and large batches."""
    pa = import_optional("pyarrow")
    native = import_optional("vertebra._vertebra")
    if pa is None or native is None:
        return None

    arrow_cursor_cls = native.ArrowCursor
    pid_hex = f"{os.getpid() & 0xFFFFF:x}"
    workloads = (
        ("tiny_2000x8x20", tiny_batches, tiny_rows_per_batch, 0),
        ("large_20x50000x20", large_batches, large_rows_per_batch, 1),
    )
    results: list[ArrowIpcWorkloadBenchResult] = []

    for workload_name, batches, rows_per_batch, idx in workloads:
        payload = _build_arrow_ipc_workload_bytes(
            pa,
            batches=batches,
            rows_per_batch=rows_per_batch,
            columns=columns,
        )
        segment_name = f"/vtb-ipc-{pid_hex}-{idx}"
        cursor = arrow_cursor_cls.write_ipc_to_shm(segment_name, payload)
        try:

            def run_open(target: Any = cursor) -> None:
                _ = target.__arrow_c_stream__()

            def run_iterate(target: Any = cursor, pa_mod: Any = pa) -> None:
                reader = pa_mod.RecordBatchReader.from_stream(target)
                for _batch in reader:
                    pass

            open_median, open_p99, open_ops = _time_callable(
                run_open,
                iterations=iterations,
                rounds=rounds,
                warmup_rounds=warmup_rounds,
            )
            iter_median, iter_p99, iter_ops = _time_callable(
                run_iterate,
                iterations=iterations,
                rounds=rounds,
                warmup_rounds=warmup_rounds,
            )
            results.append(
                ArrowIpcWorkloadBenchResult(
                    workload=workload_name,
                    batches=batches,
                    rows_per_batch=rows_per_batch,
                    columns=columns,
                    payload_bytes=len(payload),
                    open_sample=MicroBenchSample(
                        name=f"arrow_ipc_{workload_name}_open",
                        iterations=iterations,
                        median_ns=open_median,
                        p99_ns=open_p99,
                        ops_per_sec=open_ops,
                    ),
                    iterate_sample=MicroBenchSample(
                        name=f"arrow_ipc_{workload_name}_iterate",
                        iterations=iterations,
                        median_ns=iter_median,
                        p99_ns=iter_p99,
                        ops_per_sec=iter_ops,
                    ),
                )
            )
        finally:
            cursor.release()

    return ArrowIpcBenchResult(
        tiny_batches=results[0],
        large_batches=results[1],
    )


def run_all_micro_benchmarks(*, iterations: int = 2000) -> MicroBenchSuiteResult:
    """Execute all registered micro-benchmarks and import measurements."""
    samples: dict[str, MicroBenchSample] = {}

    benchmarks = (
        benchmark_request_headers_materialization,
        benchmark_asgi_scope_build_and_copy,
        benchmark_asgi_dispatch_batch,
        benchmark_wsgi_environ_items,
        benchmark_rsgi_scope_build,
        benchmark_response_construct,
        benchmark_serializers_msgpack_json,
    )

    for bench in benchmarks:
        sample = bench(iterations=iterations)
        samples[sample.name] = sample

    arrow_iterations = 1 if iterations < 10 else min(iterations, 4)
    arrow_rounds = 2 if iterations < 10 else 3
    arrow_storage = benchmark_arrow_storage_pipeline(
        iterations=arrow_iterations,
        rounds=arrow_rounds,
    )
    if iterations < 10:
        arrow_ipc = benchmark_arrow_ipc_stream(
            tiny_batches=8,
            large_batches=2,
            large_rows_per_batch=32,
            iterations=1,
            rounds=1,
            warmup_rounds=0,
        )
    else:
        arrow_ipc = benchmark_arrow_ipc_stream(
            iterations=arrow_iterations,
            rounds=arrow_rounds,
        )

    import_timings: dict[str, float] = {
        "import_vertebra_ms": measure_import_time_ms("vertebra"),
        "import_vertebra_framework_ms": measure_import_time_ms("vertebra.framework"),
    }

    return MicroBenchSuiteResult(
        samples=samples,
        import_ms=import_timings,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        arrow_storage=arrow_storage,
        arrow_ipc=arrow_ipc,
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for running micro-benchmarks."""
    parser = argparse.ArgumentParser(description="Vertebra micro-benchmarks runner")
    parser.add_argument("--iterations", type=int, default=1000, help="Iterations per round")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON")
    parser.add_argument(
        "--arrow-ipc-only",
        action="store_true",
        help="Run only the Arrow IPC stream open and iteration micro-benchmarks",
    )
    args = parser.parse_args(argv)

    if args.arrow_ipc_only:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        arrow_ipc = benchmark_arrow_ipc_stream()
        if args.json:
            payload = {
                "timestamp": timestamp,
                "arrow_ipc": msgspec.to_builtins(arrow_ipc) if arrow_ipc is not None else None,
            }
            sys.stdout.write(format_json(payload, indent=2) + "\n")
        else:
            sys.stdout.write(f"=== Arrow IPC Stream Benchmarks ({timestamp}) ===\n")
            _write_arrow_ipc_summary(arrow_ipc)
        return 0

    suite = run_all_micro_benchmarks(iterations=args.iterations)

    if args.json:
        raw_dict = {
            "timestamp": suite.timestamp,
            "samples": {k: msgspec.to_builtins(v) for k, v in suite.samples.items()},
            "arrow_storage": msgspec.to_builtins(suite.arrow_storage) if suite.arrow_storage is not None else None,
            "arrow_ipc": msgspec.to_builtins(suite.arrow_ipc) if suite.arrow_ipc is not None else None,
            "import_ms": suite.import_ms,
        }
        sys.stdout.write(format_json(raw_dict, indent=2) + "\n")
    else:
        sys.stdout.write(f"=== Vertebra Micro-Benchmarks ({suite.timestamp}) ===\n\n")
        sys.stdout.write(f"{'Benchmark':<35} {'Ops/sec':>12} {'Median (ns)':>14} {'p99 (ns)':>12}\n")
        sys.stdout.write("-" * 75 + "\n")
        for sample in suite.samples.values():
            sys.stdout.write(
                f"{sample.name:<35} {sample.ops_per_sec:>12.0f} {sample.median_ns:>14.1f} {sample.p99_ns:>12.1f}\n"
            )
        sys.stdout.write("\n=== Arrow Storage Pipeline (Direct Streaming vs Buffered) ===\n")
        if suite.arrow_storage is None:
            sys.stdout.write("skipped (optional pyarrow/sqlspec/storage extras unavailable)\n")
        else:
            arrow = suite.arrow_storage
            sys.stdout.write(
                f"format={arrow.format_hint} rows={arrow.rows} payload={arrow.payload_bytes / 1024.0:.1f} KiB\n"
            )
            sys.stdout.write(
                f"{'Benchmark':<35} {'Ops/sec':>12} {'Median (ns)':>14} {'p99 (ns)':>12} {'Peak (KiB)':>12}\n"
            )
            sys.stdout.write("-" * 89 + "\n")
            for sample in (arrow.direct, arrow.buffered):
                peak_kib = 0.0 if sample.peak_memory_kib is None else sample.peak_memory_kib
                sys.stdout.write(
                    f"{sample.name:<35} {sample.ops_per_sec:>12.2f} "
                    f"{sample.median_ns:>14.1f} {sample.p99_ns:>12.1f} {peak_kib:>12.1f}\n"
                )
        sys.stdout.write("\n=== Arrow IPC Stream (Open vs Full Iteration) ===\n")
        _write_arrow_ipc_summary(suite.arrow_ipc)
        sys.stdout.write("\n=== Subprocess Warm Import Latencies ===\n")
        for mod, ms in suite.import_ms.items():
            sys.stdout.write(f"{mod:<35} {ms:>8.2f} ms\n")

    return 0


def _build_arrow_ipc_workload_bytes(
    pa: Any,
    *,
    batches: int,
    rows_per_batch: int,
    columns: int,
) -> bytes:
    """Serialize a multi-batch Arrow IPC stream with cycling scalar column types."""
    column_types = (pa.int64(), pa.float64(), pa.string(), pa.bool_(), pa.binary())
    fields = [pa.field(f"col_{i}", column_types[i % len(column_types)]) for i in range(columns)]
    schema = pa.schema(fields)
    arrays: list[Any] = []
    for i in range(columns):
        kind = i % len(column_types)
        if kind == 0:
            arrays.append(pa.array(range(rows_per_batch), type=pa.int64()))
        elif kind == 1:
            arrays.append(pa.array([float(r) * 0.5 for r in range(rows_per_batch)], type=pa.float64()))
        elif kind == 2:
            arrays.append(pa.array([f"val-{r % 64}" for r in range(rows_per_batch)], type=pa.string()))
        elif kind == 3:
            arrays.append(pa.array([(r & 1) == 0 for r in range(rows_per_batch)], type=pa.bool_()))
        else:
            arrays.append(pa.array([b"payload-bytes" for _ in range(rows_per_batch)], type=pa.binary()))
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for _ in range(batches):
            writer.write_batch(batch)
    return bytes(sink.getvalue().to_pybytes())


def _write_arrow_ipc_summary(arrow_ipc: ArrowIpcBenchResult | None) -> None:
    """Render Arrow IPC benchmark results in plain text."""
    if arrow_ipc is None:
        sys.stdout.write("skipped (optional pyarrow or vertebra._vertebra unavailable)\n")
        return
    sys.stdout.write(f"{'Benchmark':<38} {'Ops/sec':>10} {'Median (ms)':>13} {'p99 (ms)':>11}\n")
    sys.stdout.write("-" * 76 + "\n")
    for workload in (arrow_ipc.tiny_batches, arrow_ipc.large_batches):
        sys.stdout.write(
            f"workload={workload.workload} batches={workload.batches} "
            f"rows_per_batch={workload.rows_per_batch} cols={workload.columns} "
            f"payload={workload.payload_bytes / 1024.0:.1f} KiB\n"
        )
        for sample in (workload.open_sample, workload.iterate_sample):
            sys.stdout.write(
                f"{sample.name:<38} {sample.ops_per_sec:>10.2f} "
                f"{sample.median_ns / 1_000_000.0:>13.3f} {sample.p99_ns / 1_000_000.0:>11.3f}\n"
            )


if __name__ == "__main__":
    raise SystemExit(main())
